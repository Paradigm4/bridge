/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 * BuildArray.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "S3Array.h"

#include <array/MemArray.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

#include "S3Common.h"

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
    int S3ChunkIterator::getMode() const
    {
        return _iterationMode;
    }

    Value const& S3ChunkIterator::getItem()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if (getChunk().getAttributeDesc().isEmptyIndicator())
            return _trueValue;

        if (_arrowNullCount != 0 &&
            ! (_arrowNullBitmap[_arrowIndex / 8] & 1 << _arrowIndex % 8))
            _value = _nullValue;
        else
            switch (_chunk->_attrType) {
            case TE_INT64:
                _value.setInt64(
                    std::static_pointer_cast<arrow::Int64Array>(
                        _arrowArray)->raw_values()[_arrowIndex]);
                break;
            default:
            {
                std::ostringstream out;
                out << "Type "
                    << _array._desc.getAttributes(true).findattr(_chunk->_attrID).getType()
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
        if (_arrowIndex < _arrowArray->length())
            _hasCurrent = true;
        else
            _hasCurrent = false;
            // TODO return

        // TODO Look-up coords in the Arrow coord column and set them
        // in _currPos

        for (int i = safe_static_cast<int>(_currPos.size()); --i >= 0;) {
            if (++_currPos[i] > _lastPos[i]) {
                _currPos[i] = _firstPos[i];
        //     } else {
        //         _hasCurrent = true;
        //         return;
            }
        }
        // _hasCurrent = false;
    }

    bool S3ChunkIterator::end()
    {
        return !_hasCurrent;
    }

    bool S3ChunkIterator::isEmpty() const
    {
        return false;
    }

    Coordinates const& S3ChunkIterator::getPosition()
    {
        return _currPos;
    }

    bool S3ChunkIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = _currPos.size(); i < n; i++) {
            if (pos[i] < _firstPos[i] || pos[i] > _lastPos[i]) {
                return _hasCurrent = false;
            }
        }
        _currPos = pos;
        return _hasCurrent = true;
    }

    void S3ChunkIterator::restart()
    {
        _arrowIndex = 0;
        _currPos = _firstPos;
        _hasCurrent = true;
    }

    ConstChunk const& S3ChunkIterator::getChunk()
    {
        return *_chunk;
    }

    S3ChunkIterator::S3ChunkIterator(S3Array& outputArray,
                                     S3Chunk const* chunk,
                                     int mode,
                                     std::shared_ptr<arrow::Array> arrowArray)
    :   _iterationMode(mode),
        _array(outputArray),
        _firstPos(chunk->getFirstPosition((mode & IGNORE_OVERLAPS) == 0)),
        _lastPos(chunk->getLastPosition((mode & IGNORE_OVERLAPS) == 0)),
        _currPos(_firstPos.size()),
        _chunk(chunk),
        _value(TypeLibrary::getType(chunk->getAttributeDesc().getType())),
        _nullable(chunk->getAttributeDesc().isNullable()),
        _query(Query::getValidQueryPtr(_array._query)),
        _arrowArray(arrowArray),
        _arrowNullCount(_arrowArray->null_count()),
        _arrowNullBitmap(_arrowArray->null_bitmap_data())
    {
        _trueValue.setBool(true);
        _nullValue.setNull();
        restart();
    }

    //
    // S3 chunk methods
    //

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
        return _array._desc.getAttributes().findattr(_attrID);
    }

    Coordinates const& S3Chunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? _firstPosWithOverlap : _firstPos;
    }

    Coordinates const& S3Chunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? _lastPosWithOverlap : _lastPos;
    }

    std::shared_ptr<ConstChunkIterator> S3Chunk::getConstIterator(int iterationMode) const
    {
        Dimensions const& dims = _array._desc.getDimensions();
        size_t const nDims = dims.size();

        // Download Chunk
        Aws::String &bucketName = *_array._awsBucketName;
        Aws::String objectName(coord2ObjectName(_array._settings->getBucketPrefix(),
                                                _firstPos,
                                                dims).c_str());
        Aws::S3::Model::GetObjectRequest objectRequest;
        objectRequest.SetBucket(bucketName);
        objectRequest.SetKey(objectName);

        auto outcome = _array._awsClient->GetObject(objectRequest);
        S3_EXCEPTION_NOT_SUCCESS("Get");
        const long long objectSize = outcome.GetResult().GetContentLength();
        if (objectSize > CHUNK_MAX_SIZE) {
            std::ostringstream out;
            out << "Object size " << objectSize
                << " too large. Max size is " << CHUNK_MAX_SIZE;
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                 SCIDB_LE_ILLEGAL_OPERATION) << out.str();
        }

        auto& data_stream = outcome.GetResultWithOwnership().GetBody();
        char data[objectSize];
        data_stream.read(data, (std::streamsize)objectSize);

        arrow::io::BufferReader arrowBufferReader(
            reinterpret_cast<const uint8_t*>(data), objectSize); // zero copy

        // Read Record Batch using Stream Reader
        std::shared_ptr<arrow::RecordBatchReader> arrowReader;
        THROW_NOT_OK(arrow::ipc::RecordBatchStreamReader::Open(
                         &arrowBufferReader, &arrowReader));

        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowReader,
        //     arrow::ipc::RecordBatchStreamReader::Open(&arrowBufferReader));
        THROW_NOT_OK(arrowReader->ReadNext(&arrowBatch));
        // One SciDB Chunk equals one Arrow Batch

        std::shared_ptr<arrow::Array> arrowArray = arrowBatch->column(_attrID);

        return std::shared_ptr<ConstChunkIterator>(
            new S3ChunkIterator(_array, this, iterationMode, arrowArray));
    }

    CompressorType S3Chunk::getCompressionMethod() const
    {
        return _array._desc.getAttributes().findattr(_attrID).getDefaultCompressionMethod();
    }

    void S3Chunk::setPosition(Coordinates const& pos)
    {
        // Ser positions to array boundaries if they exceed the bounderies
        _firstPos = pos;
        Dimensions const& dims = _array._desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            _firstPosWithOverlap[i] = _firstPos[i] - dims[i].getChunkOverlap();
            if (_firstPosWithOverlap[i] < dims[i].getStartMin()) {
                _firstPosWithOverlap[i] = dims[i].getStartMin();
            }
            _lastPos[i] = _firstPos[i] + dims[i].getChunkInterval() - 1;
            _lastPosWithOverlap[i] = _lastPos[i] + dims[i].getChunkOverlap();
            if (_lastPos[i] > dims[i].getEndMax()) {
                _lastPos[i] = dims[i].getEndMax();
            }
            if (_lastPosWithOverlap[i] > dims[i].getEndMax()) {
                _lastPosWithOverlap[i] = dims[i].getEndMax();
            }
        }
    }

    S3Chunk::S3Chunk(S3Array& arr, AttributeID attr)
    : _array(arr),
      _firstPos(arr._desc.getDimensions().size()),
      _lastPos(_firstPos.size()),
      _firstPosWithOverlap(_firstPos.size()),
      _lastPosWithOverlap(_firstPos.size()),
      _attrID(attr),
      _attrType(typeId2TypeEnum(
                    arr._desc.getAttributes().findattr(attr).getType(),
                    true))
    {
    }


    //
    // S3 array iterator methods
    //

    void S3ArrayIterator::operator ++()
    {
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        Query::getValidQueryPtr(_array._query);
        _nextChunk();
    }

    bool S3ArrayIterator::end()
    {
        return !_hasCurrent;
    }

    Coordinates const& S3ArrayIterator::getPosition()
    {
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        return _currPos;
    }

    void S3ArrayIterator::_nextChunk()
    {
        _chunkInitialized = false;

        // TODO Remove
        // search delegated to Distribution::getNextChunkCoord::getNextChunk() after sha 2be3e13a
        auto desc = _array.getArrayDesc();
        auto dist = desc.getDistribution();
        auto dims = desc.getDimensions();
        _hasCurrent = dist->getNextChunkCoord(_currPos, dims, _currPos, _array._nInstances, _array._instanceID);

        // TODO Use chunkCoord to figure out the next chunk?
    }

    bool S3ArrayIterator::setPosition(Coordinates const& pos)
    {
        Query::getValidQueryPtr(_array._query);
        // Check that coords are inside array
        for (size_t i = 0, n = _currPos.size(); i < n; i++) {
            if (pos[i] < _dims[i].getStartMin() || pos[i] > _dims[i].getEndMax()) {
                return _hasCurrent = false;
            }
        }
        _currPos = pos;
        // Convert cell coords to chunk coords
        _array._desc.getChunkPositionFor(_currPos);
        _chunkInitialized = false;
        // Am I responsible for this position?
        if (isReplicated(_array.getArrayDesc().getDistribution()->getDistType())) {
            _hasCurrent = true;
        }
        else {
            _hasCurrent = _array._desc.getPrimaryInstanceId(_currPos, _array._nInstances) == _array._instanceID;
        }
        return _hasCurrent;

        // TODO binary search/hash lookup on index to see if I have the chunk
    }

    void S3ArrayIterator::restart()
    {
        _chunkInitialized = false;

        // Check if query has not been canceled
        Query::getValidQueryPtr(_array._query);
        size_t nDims = _currPos.size();
        // TODO Set it to the first chunk from the index
        for (size_t i = 0; i < nDims; i++) {
            _currPos[i] = _dims[i].getStartMin();
        }

        // TODO Remove
        auto desc = _array.getArrayDesc();
        auto dist = desc.getDistribution();
        auto dims = desc.getDimensions();
        _hasCurrent = dist->getFirstChunkCoord(_currPos, dims, _currPos, _array._nInstances, _array._instanceID);
    }

    ConstChunk const& S3ArrayIterator::getChunk()
    {
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        Query::getValidQueryPtr(_array._query);
        if (!_chunkInitialized) {
            // TODO Remove
            // _chunk.initialize(&_array,
            //                   &_array._desc,
            //                   Address(_attrID, _currPos),
            //                   _array._desc.getAttributes().findattr(_attrID).getDefaultCompressionMethod());
            _chunk.setPosition(_currPos);
            _chunkInitialized = true;
        }
        return _chunk;
    }


    S3ArrayIterator::S3ArrayIterator(S3Array& arr, AttributeID attrID)
        : ConstArrayIterator(arr),
        _array(arr),
        _chunk(arr, attrID),
        _dims(arr._desc.getDimensions()),
        _currPos(_dims.size())
    {
        restart();
    }


    //
    // S3 array methods
    //

    ArrayDesc const& S3Array::getArrayDesc() const
    {
        return _desc;
    }

    std::shared_ptr<ConstArrayIterator> S3Array::getConstIteratorImpl(
        const AttributeDesc& attr) const
    {
        LOG4CXX_DEBUG(logger, "S3ARRAY >> iter: " << attr.getId());

        return std::shared_ptr<ConstArrayIterator>(
            new S3ArrayIterator(*(S3Array*)this, attr.getId()));
    }

    S3Array::S3Array(std::shared_ptr<Query>& query,
                     ArrayDesc const& desc,
                     std::shared_ptr<S3LoadSettings> settings)
        : _desc(desc),
          _nInstances(0),
          _instanceID(INVALID_INSTANCE),
          _settings(settings)
    {
       SCIDB_ASSERT(query);
       _query=query;
       _nInstances = query->getInstancesCount();
       _instanceID = query->getInstanceID();
        const auto& fda = _desc.getAttributes().firstDataAttribute();
        TypeId attrType = fda.getType();

        SCIDB_ASSERT(_nInstances > 0 && _instanceID < _nInstances);


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

        // Parse Index and Build Chunk Coordinate List
        auto& chunkCoordsStream = outcome.GetResultWithOwnership().GetBody();
        std::string chunkCoordsLine;
        size_t const nDims = _desc.getDimensions().size();
        while (std::getline(chunkCoordsStream, chunkCoordsLine)) {

            std::istringstream stm(chunkCoordsLine);
            Coordinate coord;
            Coordinates coords;
            for (Coordinate coord; stm >> coord;)
                coords.push_back(coord);

            if (coords.size() != nDims)
                throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                     SCIDB_LE_UNKNOWN_ERROR)
                    << "Invalid index line '" << chunkCoordsLine
                    << "', expected " << nDims << " values";

            // Keep Only Chunks for this Instance
            if (_desc.getPrimaryInstanceId(
                    coords, query->getInstancesCount()) == query->getInstanceID())
                _chunkCoords.push_back(coords);
        }

        // TODO Remove (debugging)
        for (size_t i = 0; i < _chunkCoords.size(); i++) {
            std::stringstream s;
            std::copy(_chunkCoords[i].begin(), _chunkCoords[i].end(), std::ostream_iterator<Coordinate>(s, ", "));
            LOG4CXX_DEBUG(logger, "S3ARRAY >> id: " << query->getInstanceID() << " coord[" << i << "]:" << s.str());
        }
    }

    S3Array::~S3Array() {
        Aws::ShutdownAPI(_awsOptions);
    }
}
