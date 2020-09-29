/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2020 Paradigm4 Inc.
* All Rights Reserved.
*
* s3bridge is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* s3bridge is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* s3bridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with s3bridge.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include "S3Array.h"

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

#include "S3Common.h"
#include "S3LoadSettings.h"
#include "S3Index.h"

// TODO use __builtin_expect
#define THROW_NOT_OK(s)                                                 \
    {                                                                   \
        arrow::Status _s = (s);                                         \
        if (!_s.ok())                                                   \
        {                                                               \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)      \
                    << _s.ToString().c_str();                           \
        }                                                               \
    }

using namespace std;

namespace scidb {
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.s3array"));

    //
    // S3 chunk iterator methods
    //
    S3ChunkIterator::S3ChunkIterator(const S3Array& array,
                                     S3Chunk const* chunk,
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
        _query(Query::getValidQueryPtr(array._query)),
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

        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _chunk->_attrID
                      << "|ChunkIt nAtts: " << _nAtts << " attrID: " << _chunk->_attrID
                      << " nDims: " << _nDims << " arrowLen: " << _arrowLength);
    }

    void S3ChunkIterator::restart()
    {
        _currPos = _firstPos;
        _arrowIndex = 0;
        if (_arrowLength > _arrowIndex)
            _hasCurrent = true;
        else
            _hasCurrent = false;

        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _chunk->_attrID
                      << "|ChunkIt::restart pos: " << _currPos << " [" << _firstPos << ", " << _lastPos << "] arrowLen: " << _arrowLength << " hasCurr: " << _hasCurrent);
    }

    Value const& S3ChunkIterator::getItem()
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _chunk->_attrID
                      << "|ChunkIt::getItem  hasCurr: " << _hasCurrent
                      << " pos: " << _currPos);

        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

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
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
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
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                     SCIDB_LE_ILLEGAL_OPERATION) << out.str();
            }
            }

        if (!_nullable && _value.isNull())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);

        return _value;
    }

    void S3ChunkIterator::operator ++()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        _arrowIndex++;
        if (_arrowIndex >= _arrowLength) {
            _hasCurrent = false;
            return;
        }

        _hasCurrent = true;
        for (size_t i = 0; i < _nDims; ++i)
            _currPos[i] = getCoord(i, _arrowIndex);
    }

    bool S3ChunkIterator::setPosition(Coordinates const& pos)
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _chunk->_attrID
                      << "|ChunkIt::setPosition: " << pos);

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

    int64_t S3ChunkIterator::getCoord(size_t dim, int64_t index)
    {
        return std::static_pointer_cast<arrow::Int64Array>(
            _arrowBatch->column(_nAtts + dim))->raw_values()[index];
    }

    bool S3ChunkIterator::end()
    {
        return !_hasCurrent;
    }

    bool S3ChunkIterator::isEmpty() const
    {
        return _arrowLength == 0;
    }

    int S3ChunkIterator::getMode() const
    {
        return _iterationMode;
    }

    Coordinates const& S3ChunkIterator::getPosition()
    {
        return _currPos;
    }

    ConstChunk const& S3ChunkIterator::getChunk()
    {
        return *_chunk;
    }

    //
    // S3 chunk methods
    //
    S3Chunk::S3Chunk(S3Array& array, AttributeID attrID):
        _array(array),
        _dims(array._desc.getDimensions()),
        _nDims(array._desc.getDimensions().size()),
        _firstPos(array._desc.getDimensions().size()),
        _lastPos(array._desc.getDimensions().size()),
        _firstPosWithOverlap(array._desc.getDimensions().size()),
        _lastPosWithOverlap(array._desc.getDimensions().size()),
        _attrID(attrID),
        _attrDesc(array._desc.getAttributes().findattr(attrID)),
        _attrType(typeId2TypeEnum(array._desc.getAttributes().findattr(attrID).getType(), true)),
        _arrowSizeAlloc(0)
    {
    }

    void S3Chunk::download()
    {
        // Download Chunk
        Aws::String &bucketName = *_array._awsBucketName;
        Aws::String objectName(coord2ObjectName(
                                   _array._settings->getBucketPrefix(),
                                   _firstPos,
                                   _dims).c_str());
        Aws::S3::Model::GetObjectRequest objectRequest;
        objectRequest.SetBucket(bucketName);
        objectRequest.SetKey(objectName);
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|Chunk::download: " << objectName);

        auto outcome = _array._awsClient->GetObject(objectRequest);
        S3_EXCEPTION_NOT_SUCCESS("Get");
        const long long arrowSize = outcome.GetResult().GetContentLength();
        if (arrowSize > CHUNK_MAX_SIZE) {
            std::ostringstream out;
            out << "Object size " << arrowSize
                << " too large. Max size is " << CHUNK_MAX_SIZE;
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                 SCIDB_LE_ILLEGAL_OPERATION) << out.str();
        }

        // Check size of current buffer and re-allocate the buffer if
        // it is too small
        if (arrowSize > _arrowSizeAlloc) {
            LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                          << "|Chunk::download arrowSizeAlloc: " << _arrowSizeAlloc
                          << " arrowSize: " << arrowSize);

            _arrowSizeAlloc = arrowSize;
            _arrowData = std::make_unique<char[]>(_arrowSizeAlloc);
        }

        auto& bodyStream = outcome.GetResultWithOwnership().GetBody();
        bodyStream.read(_arrowData.get(),
                        static_cast<std::streamsize>(arrowSize));

        _arrowBufferReader = std::make_shared<arrow::io::BufferReader>(
            reinterpret_cast<const uint8_t*>(_arrowData.get()),
            arrowSize); // zero copy

        // Read Record Batch using Stream Reader
        THROW_NOT_OK(arrow::ipc::RecordBatchStreamReader::Open(
                         _arrowBufferReader, &_arrowBatchReader));

        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowReader,
        //     arrow::ipc::RecordBatchStreamReader::Open(&arrowBufferReader));
        THROW_NOT_OK(_arrowBatchReader->ReadNext(&_arrowBatch));
        // One SciDB Chunk equals one Arrow Batch
    }

    void S3Chunk::setPosition(Coordinates const& pos)
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|Chunk::setPosition: " << pos);

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

    std::shared_ptr<ConstChunkIterator> S3Chunk::getConstIterator(int iterationMode) const
    {
        return std::shared_ptr<ConstChunkIterator>(
            new S3ChunkIterator(_array, this, iterationMode, _arrowBatch));
    }

    Array const& S3Chunk::getArray() const
    {
        return _array;
    }

    const ArrayDesc& S3Chunk::getArrayDesc() const
    {
        return _array._desc;
    }

    const AttributeDesc& S3Chunk::getAttributeDesc() const
    {
        return _attrDesc;
    }

    Coordinates const& S3Chunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? _firstPosWithOverlap : _firstPos;
    }

    Coordinates const& S3Chunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? _lastPosWithOverlap : _lastPos;
    }

    CompressorType S3Chunk::getCompressionMethod() const
    {
        return _array._desc.getAttributes().findattr(_attrID).getDefaultCompressionMethod();
    }

    //
    // S3 Array Iterator
    //
    S3ArrayIterator::S3ArrayIterator(S3Array& array, AttributeID attrID):
        ConstArrayIterator(array),
        _array(array),
        _attrID(attrID),
        _dims(array._desc.getDimensions()),
        _chunk(array, attrID)
    {
        restart();
    }

    void S3ArrayIterator::operator ++()
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|ArrayIt::++ hasCurr: " << _hasCurrent
                      << " Index curr: " << *_currIndex
                      << " end? " << (_currIndex == _array._index.end()));

        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if (_currIndex == _array._index.end())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        Query::getValidQueryPtr(_array._query);

        ++_currIndex;
        _nextChunk();
    }

    void S3ArrayIterator::_nextChunk()
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|ArrayIt::_nextChunk hasCurr: " << _hasCurrent << " end? " << (_currIndex == _array._index.end()));

        _chunkInitialized = false;

        if (_currIndex == _array._index.end())
            _hasCurrent = false;
        else {
            _hasCurrent = true;
            _currPos = *_currIndex;
        }

        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|ArrayIt::_nextChunk hasCurr: " << _hasCurrent << " currPos: " << _currPos);
    }

    bool S3ArrayIterator::setPosition(Coordinates const& pos)
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|ArrayIt::setPosition: " << pos);

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

        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|ArrayIt::setPosition chunkPos: " << chunkPos
                      << " hasCurr: " << _hasCurrent);

        return _hasCurrent;
    }

    void S3ArrayIterator::restart()
    {
        Query::getValidQueryPtr(_array._query);

        _currIndex = _array._index.begin();
        _nextChunk();
    }

    ConstChunk const& S3ArrayIterator::getChunk()
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|ArrayIt::getChunk hasCurr: " << _hasCurrent);

        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        Query::getValidQueryPtr(_array._query);

        if (!_chunkInitialized) {
            _chunk.setPosition(_currPos);
            _chunk.download();
            _chunkInitialized = true;
        }

        return _chunk;
    }

    bool S3ArrayIterator::end()
    {
        return !_hasCurrent;
    }

    Coordinates const& S3ArrayIterator::getPosition()
    {
        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _array._instanceID << ">" << _attrID
                      << "|ArrayIt::getChunk hasCurr: " << _hasCurrent);

        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        return _currPos;
    }

    //
    // S3 array methods
    //
    S3Array::S3Array(std::shared_ptr<Query>& query,
                     ArrayDesc const& desc,
                     std::shared_ptr<S3LoadSettings> settings):
        _nInstances(query->getInstancesCount()),
        _instanceID(query->getInstanceID()),
        _desc(desc),
        _settings(settings),
        _index(*query, desc)
    {
        SCIDB_ASSERT(_nInstances > 0 && _instanceID < _nInstances);
        _query = Query::getValidQueryPtr(query);

        // Init AWS
        Aws::InitAPI(_awsOptions);
        _awsClient = std::make_shared<Aws::S3::S3Client>();
        _awsBucketName = std::make_shared<Aws::String>(_settings->getBucketName().c_str());

        // -- - Using Index to Get the List of Chunks - --
        // Download Chunk Coordinate List
        Aws::S3::Model::GetObjectRequest objectRequest;
        Aws::String objectName((_settings->getBucketPrefix() + "/index").c_str());
        Aws::String &bucketName = *_awsBucketName;
        objectRequest.SetBucket(bucketName);
        objectRequest.SetKey(objectName);

        auto outcome = _awsClient->GetObject(objectRequest);
        S3_EXCEPTION_NOT_SUCCESS("Get");

        // Parse S3Index
        auto& indexStream = outcome.GetResultWithOwnership().GetBody();
        indexStream >> _index;

        // TODO Remove (debugging)
        LOG4CXX_DEBUG(logger, "S3ARRAY|" << _instanceID << "|Array index:" << _index);
    }

    S3Array::~S3Array() {
        Aws::ShutdownAPI(_awsOptions);
    }

    ArrayDesc const& S3Array::getArrayDesc() const
    {
        return _desc;
    }

    std::shared_ptr<ConstArrayIterator> S3Array::getConstIteratorImpl(
        const AttributeDesc& attr) const
    {
        return std::shared_ptr<ConstArrayIterator>(
            new S3ArrayIterator(*(S3Array*)this, attr.getId()));
    }
}
