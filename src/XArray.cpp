/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2020-2021 Paradigm4 Inc.
* All Rights Reserved.
*
* bridge is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* bridge is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* bridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with bridge.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include "XArray.h"
#include "XInputSettings.h"

// SciDB
#include <array/MemoryBuffer.h>

// Arrow
#include <arrow/builder.h>
#include <arrow/ipc/reader.h>
#include <arrow/util/compression.h>


namespace scidb {

    //
    // ScopedMutex
    //
    class ScopedMutex {
    public:
        ScopedMutex(std::mutex &lock):_lock(lock) { _lock.lock(); }
        ~ScopedMutex() { _lock.unlock(); }
    private:
        std::mutex &_lock;
    };

    //
    // XCache
    //
    XCache::XCache(
        std::shared_ptr<ArrowReader> arrowReader,
        const std::string &path,
        const Dimensions &dims,
        size_t cacheSize):
        _arrowReader(arrowReader),
        _path(path),
        _dims(dims),
        _size(0),
        _sizeMax(cacheSize)
    {}

    std::shared_ptr<arrow::RecordBatch> XCache::get(Coordinates pos)
    {
        {
            ScopedMutex lock(_lock); // LOCK

            std::shared_ptr<arrow::RecordBatch> arrowBatch;
            if (_mem.find(pos) == _mem.end()) {
                // Download Chunk
                auto objectName =
                    "chunks/" + Metadata::coord2ObjectName(pos, _dims);
                auto arrowSize = _arrowReader->readObject(objectName,
                                                          false,
                                                          arrowBatch);

                // Check if Record Batch Fits in Cache
                if (arrowSize > _sizeMax) {
                    std::ostringstream out;
                    out << "Size " << arrowSize << " of object "
                        << _path << "/" << objectName
                        << " for position " << pos
                        << " is bigger than cache size " << _sizeMax;
                    throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                           SCIDB_LE_UNKNOWN_ERROR) << out.str();
                }

                // Make Space in Cache
                while (_size + arrowSize > _sizeMax && !_lru.empty()) {
                    // Remove Last Recently Used (LRU)
                    auto rm = _lru.back();
                    _lru.pop_back();
                    _size -= _mem[rm].arrowSize;
                    _mem.erase(rm);
                    LOG4CXX_DEBUG(logger, "XCACHE||get delete:" << rm << " size:" << _size);
                }

                // Add to Cache
                _lru.push_front(pos);
                _mem[pos] = XCacheCell{_lru.begin(), arrowBatch, arrowSize};
                _size += arrowSize;
                LOG4CXX_DEBUG(logger, "XCACHE||get add:" << pos << " size:" << _size);

                return arrowBatch;
            }
            // Read from Cache
            else {
                LOG4CXX_DEBUG(logger, "XCACHE||get read:" << pos);

                // Read and Move to Front
                auto &cell = _mem[pos];
                arrowBatch = cell.arrowBatch;
                _lru.erase(cell.lruIt);
                _lru.push_front(pos);
                _mem[pos].lruIt = _lru.begin(); // Iterator
            }

            return arrowBatch;
        }
    }

    //
    // XChunk Iterator
    //
    XChunkIterator::XChunkIterator(const XArray& array,
                                     const XChunk* chunk,
                                     int iterationMode,
                                     std::shared_ptr<arrow::RecordBatch> arrowBatch):
        _array(array),
        _nAtts(array._desc.getAttributes(true).size()),
        _nDims(array._desc.getDimensions().size()),
        _chunk(chunk),
        _iterationMode(iterationMode),
        _firstPos(chunk->_nDims),
        _lastPos(chunk->_nDims),
        _currPos(chunk->_nDims),
        _value(TypeLibrary::getType(chunk->getAttributeDesc().getType())),
        _nullable(chunk->getAttributeDesc().isNullable()),
        _arrowBatch(arrowBatch),
        _arrowLength(arrowBatch->column(array._desc.getAttributes(true).size())->length())
    {
        _trueValue.setBool(true);
        _nullValue.setNull();

        if (_arrowLength > 0) {
            if (!_chunk->getAttributeDesc().isEmptyIndicator()) {
                _arrowArray = arrowBatch->column(chunk->_attrID);
                _arrowNullCount = arrowBatch->column(chunk->_attrID)->null_count();
                _arrowNullBitmap = arrowBatch->column(chunk->_attrID)->null_bitmap_data();
            }
            for (size_t i = 0; i < _nDims; ++i) {
                _firstPos[i] = getCoord(i, 0);
                _lastPos[i] = getCoord(i, _arrowLength - 1);
            }
        }
        restart();
    }

    void XChunkIterator::restart()
    {
        _currPos = _firstPos;
        _arrowIndex = 0;
        if (_arrowLength > _arrowIndex)
            _hasCurrent = true;
        else
            _hasCurrent = false;
    }

    Value const& XChunkIterator::getItem()
    {
        if (!_hasCurrent)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if (_chunk->getAttributeDesc().isEmptyIndicator())
            return _trueValue;

        if (_arrowNullCount != 0 &&
            ! (_arrowNullBitmap[_arrowIndex / 8] & 1 << _arrowIndex % 8))
            _value = _nullValue;
        else
            switch (_chunk->_attrType) {
            case TE_BINARY:
            {
                int32_t sz;
                const uint8_t* ptr =
                    std::static_pointer_cast<const arrow::BinaryArray>(
                        _arrowArray)->GetValue(_arrowIndex, &sz);
                _value.setData(ptr, sz);
                break;
            }
            case TE_STRING:
                _value.setString(
                    std::static_pointer_cast<const arrow::StringArray>(
                        _arrowArray)->GetString(_arrowIndex));
                break;
            case TE_CHAR:
            {
                std::string str =
                    std::static_pointer_cast<const arrow::StringArray>(
                        _arrowArray)->GetString(_arrowIndex);
                if (str.length() != 1) {
                    std::ostringstream out;
                    out << "Invalid value for attribute "
                        << _chunk->getArrayDesc().getAttributes(true).findattr(_chunk->_attrID).getName();
                    throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                           SCIDB_LE_ILLEGAL_OPERATION) << out.str();
                }
                _value.setChar(str[0]);
                break;
            }
            case TE_BOOL:
                _value.setBool(
                    std::static_pointer_cast<const arrow::BooleanArray>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_DATETIME:
                _value.setDateTime(
                    std::static_pointer_cast<const arrow::Date64Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_FLOAT:
                _value.setFloat(
                    std::static_pointer_cast<const arrow::FloatArray>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_DOUBLE:
                _value.setDouble(
                    std::static_pointer_cast<const arrow::DoubleArray>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_INT8:
                _value.setInt8(
                    std::static_pointer_cast<const arrow::Int8Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_INT16:
                _value.setInt16(
                    std::static_pointer_cast<const arrow::Int16Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_INT32:
                _value.setInt32(
                    std::static_pointer_cast<const arrow::Int32Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_INT64:
                _value.setInt64(
                    std::static_pointer_cast<const arrow::Int64Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_UINT8:
                _value.setInt8(
                    std::static_pointer_cast<const arrow::Int8Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_UINT16:
                _value.setInt16(
                    std::static_pointer_cast<const arrow::Int16Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_UINT32:
                _value.setInt32(
                    std::static_pointer_cast<const arrow::Int32Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            case TE_UINT64:
                _value.setInt64(
                    std::static_pointer_cast<const arrow::Int64Array>(
                        _arrowArray)->Value(_arrowIndex));
                break;
            default:
            {
                std::ostringstream out;
                out << "Type "
                    << _chunk->getArrayDesc().getAttributes(true).findattr(_chunk->_attrID).getType()
                    << " not supported";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                       SCIDB_LE_ILLEGAL_OPERATION) << out.str();
            }
            }

        if (!_nullable && _value.isNull())
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);

        return _value;
    }

    void XChunkIterator::operator ++()
    {
        if (!_hasCurrent)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        _arrowIndex++;
        if (_arrowIndex >= _arrowLength) {
            _hasCurrent = false;
            return;
        }

        _hasCurrent = true;
        for (size_t i = 0; i < _nDims; ++i)
            _currPos[i] = getCoord(i, _arrowIndex);
    }

    bool XChunkIterator::setPosition(Coordinates const& pos)
    {
        if (_arrowLength <= 0) {
            _hasCurrent = false;
            return _hasCurrent;
        }

        for (size_t i = 0; i < _nDims; i++)
            if (pos[i] < _firstPos[i] || pos[i] > _lastPos[i]) {
                _hasCurrent = false;
                return _hasCurrent;
            }

        // Find Arrow coordiantes matching pos
        _hasCurrent = false;
        bool isPossible = true;
        for (int64_t j = 0; j < _arrowLength; j++) {

            // Check that all coordinates in pos match the Arrow
            // coordinates
            bool isMatch = true;
            for (size_t i = 0; i < _nDims; ++i) {
                int64_t coord = getCoord(i, j);
                if (pos[i] != coord) {
                    isMatch = false;

                    // The first Arrow coordinate is greater, no match
                    // can be found
                    if (i == 0 && pos[i] < coord)
                        isPossible = false;
                    break;
                }
            }
            // Found Arrow coordinates matching pos
            if (isMatch) {
                _hasCurrent = true;
                _currPos = pos;
                _arrowIndex = j;
                break;
            }
            if (!isPossible)
                break;
        }

        return _hasCurrent;
    }

    int64_t XChunkIterator::getCoord(size_t dim, int64_t index)
    {
        return std::static_pointer_cast<arrow::Int64Array>(
            _arrowBatch->column(_nAtts + dim))->raw_values()[index];
    }

    bool XChunkIterator::end()
    {
        return !_hasCurrent;
    }

    bool XChunkIterator::isEmpty() const
    {
        return _arrowLength == 0;
    }

    int XChunkIterator::getMode() const
    {
        return _iterationMode;
    }

    Coordinates const& XChunkIterator::getPosition()
    {
        return _currPos;
    }

    ConstChunk const& XChunkIterator::getChunk()
    {
        return *_chunk;
    }

    std::shared_ptr<Query> XChunkIterator::getQuery() {
        return _array._query;
    }

    //
    // X Chunk
    //
    XChunk::XChunk(XArray& array, AttributeID attrID):
        _array(array),
        _dims(array._desc.getDimensions()),
        _nDims(array._desc.getDimensions().size()),
        _firstPos(array._desc.getDimensions().size()),
        _lastPos(array._desc.getDimensions().size()),
        _firstPosWithOverlap(array._desc.getDimensions().size()),
        _lastPosWithOverlap(array._desc.getDimensions().size()),
        _attrID(attrID),
        _attrDesc(array._desc.getAttributes().findattr(attrID)),
        _attrType(typeId2TypeEnum(array._desc.getAttributes().findattr(attrID).getType(), true))
    {}

    void XChunk::download()
    {
        if (_array._cache != NULL)
            _arrowBatch = _array._cache->get(_firstPos);
        else {
            // Cache is disabled
            auto objectName =
                "chunks/" + Metadata::coord2ObjectName(_firstPos, _dims);
            _array._arrowReader->readObject(objectName, true, _arrowBatch);
        }
    }

    void XChunk::setPosition(Coordinates const& pos)
    {
        // Set _firstPos, _firstPosWithOverlap, _lastPos, and
        // _lastPosWithOverlap based on given pos
        _firstPos = pos;
        for (size_t i = 0; i < _nDims; i++) {
            _firstPosWithOverlap[i] = _firstPos[i] - _dims[i].getChunkOverlap();
            if (_firstPosWithOverlap[i] < _dims[i].getStartMin())
                _firstPosWithOverlap[i] = _dims[i].getStartMin();
            _lastPos[i] = _firstPos[i] + _dims[i].getChunkInterval() - 1;
            _lastPosWithOverlap[i] = _lastPos[i] + _dims[i].getChunkOverlap();
            if (_lastPos[i] > _dims[i].getEndMax())
                _lastPos[i] = _dims[i].getEndMax();
            if (_lastPosWithOverlap[i] > _dims[i].getEndMax())
                _lastPosWithOverlap[i] = _dims[i].getEndMax();
        }
    }

    std::shared_ptr<ConstChunkIterator> XChunk::getConstIterator(int iterationMode) const
    {
        return std::shared_ptr<ConstChunkIterator>(
            new XChunkIterator(_array, this, iterationMode, _arrowBatch));
    }

    Array const& XChunk::getArray() const
    {
        return _array;
    }

    const ArrayDesc& XChunk::getArrayDesc() const
    {
        return _array._desc;
    }

    const AttributeDesc& XChunk::getAttributeDesc() const
    {
        return _attrDesc;
    }

    Coordinates const& XChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? _firstPosWithOverlap : _firstPos;
    }

    Coordinates const& XChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? _lastPosWithOverlap : _lastPos;
    }

    CompressorType XChunk::getCompressionMethod() const
    {
        return _array._desc.getAttributes().findattr(_attrID).getDefaultCompressionMethod();
    }

    //
    // X Array Iterator
    //
    XArrayIterator::XArrayIterator(XArray& array, AttributeID attrID):
        ConstArrayIterator(array),
        _array(array),
        _attrID(attrID),
        _dims(array._desc.getDimensions()),
        _chunk(array, attrID),
        _hasCurrent(false)
    {
        restart();
    }

    void XArrayIterator::operator ++()
    {
        if (!_hasCurrent)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if (_currIndex == _array._index.end())
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        Query::getValidQueryPtr(_array._query);

        ++_currIndex;
        _nextChunk();
    }

    void XArrayIterator::_nextChunk()
    {
        _chunkInitialized = false;

        if (_currIndex == _array._index.end())
            _hasCurrent = false;
        else {
            _hasCurrent = true;
            _currPos = *_currIndex;
        }
    }

    bool XArrayIterator::setPosition(Coordinates const& pos)
    {
        Query::getValidQueryPtr(_array._query);

        // Check that coords are inside array
        for (size_t i = 0, n = _currPos.size(); i < n; i++)
            if (pos[i] < _dims[i].getStartMin() || pos[i] > _dims[i].getEndMax()) {
                _hasCurrent = false;
                return _hasCurrent;
            }

        _currPos = pos;
        // Convert cell coords to chunk coords
        Coordinates chunkPos = pos;
        _array._desc.getChunkPositionFor(chunkPos);

        _chunkInitialized = false;
        _currIndex = _array._index.find(chunkPos);
        if (_currIndex != _array._index.end())
            _hasCurrent = true;
        else
            _hasCurrent = false;

        return _hasCurrent;
    }

    void XArrayIterator::restart()
    {
        Query::getValidQueryPtr(_array._query);

        _currIndex = _array._index.begin();
        _nextChunk();
    }

    ConstChunk const& XArrayIterator::getChunk()
    {
        if (!_hasCurrent)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        Query::getValidQueryPtr(_array._query);

        if (!_chunkInitialized) {
            _chunk.setPosition(_currPos);
            _chunk.download();
            _chunkInitialized = true;
        }

        return _chunk;
    }

    bool XArrayIterator::end()
    {
        return !_hasCurrent;
    }

    Coordinates const& XArrayIterator::getPosition()
    {
        if (!_hasCurrent)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        return _currPos;
    }

    //
    // X Array
    //
    XArray::XArray(std::shared_ptr<Query> query,
                     const ArrayDesc& desc,
                     const std::shared_ptr<XInputSettings> settings):
        _query(query),
        _desc(desc),
        _settings(settings),
        _index(desc)
    {
        auto nInst = _query->getInstancesCount();
        SCIDB_ASSERT(nInst > 0 && _query->getInstanceID() < nInst);

        _driver = Driver::makeDriver(_settings->getURL());
        Metadata metadata;
        _driver->readMetadata(metadata);

        auto compressionPair = metadata.find("compression");
        if (compressionPair == metadata.end())
            throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_METADATA,
                                   scidb::SCIDB_LE_UNKNOWN_ERROR)
                << "Compression missing from metadata";
        auto compression = Metadata::string2Compression(compressionPair->second);

        _arrowReader = std::make_shared<ArrowReader>(compression,
                                                       _driver);

        // If cache size is 0, the cache will be disabled
        auto cacheSize = settings->getCacheSize();
        if (cacheSize > 0)
            _cache = std::make_unique<XCache>(_arrowReader,
                                               _driver->getURL(),
                                               _desc.getDimensions(),
                                               cacheSize);

        // Load index
        _index.load(_driver, _query);
    }

    ArrayDesc const& XArray::getArrayDesc() const {
        return _desc;
    }

    std::shared_ptr<ConstArrayIterator> XArray::getConstIteratorImpl(
        const AttributeDesc& attr) const {
        return std::shared_ptr<ConstArrayIterator>(
            new XArrayIterator(*(XArray*)this, attr.getId()));
    }
} // scidb namespace
