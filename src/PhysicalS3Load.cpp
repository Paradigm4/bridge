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

#include <array/TileIteratorAdaptors.h>
#include <query/PhysicalOperator.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

#include <aws/s3/model/ListObjectsRequest.h>

#include "S3Common.h"
#include "S3LoadSettings.h"


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.s3load"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
    if (!cond)
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
}

class PhysicalS3Load : public PhysicalOperator
{
public:
    PhysicalS3Load(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _nDims(schema.getDimensions().size()),
        _nAtts(schema.getAttributes(true).size()),
        _arrayIterators(_nAtts),
        _chunkIterators(_nDims)
    {}

    std::shared_ptr<Array> execute(
        std::vector< std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        LOG4CXX_DEBUG(logger, "S3LOAD >> execute");

        S3LoadSettings settings(_parameters, _kwParameters, false, query);
        Aws::String bucketName(settings.getBucketName().c_str());

        // Init Output
        shared_ptr<Array> array = std::make_shared<MemArray>(_schema, query);
        for (const auto& attr : _schema.getAttributes(true))
            _arrayIterators[attr.getId()] = array->getIterator(attr);


        // Init AWS
        Aws::SDKOptions options;
        Aws::InitAPI(options);
        Aws::S3::S3Client s3Client;

        // Get Metadata
        // map<string, string> metadata;
        // getMetadata(s3Client,
        //             bucketName,
        //             Aws::String((settings.getBucketPrefix() +
        //                          "/metadata").c_str()),
        //             metadata);

        Aws::S3::Model::ListObjectsRequest request;
        request.WithBucket(bucketName);
        Aws::String objectName((settings.getBucketPrefix() + "/chunk_").c_str());
        request.WithPrefix(objectName);

        auto outcome = s3Client.ListObjects(request);
        S3_EXCEPTION_NOT_SUCCESS("List");

        Aws::Vector<Aws::S3::Model::Object> objects = outcome.GetResult().GetContents();
        Coordinates pos(_nDims);

        for (Aws::S3::Model::Object& object : objects) {
            objectName = object.GetKey();
            long long objectSize = object.GetSize();
            LOG4CXX_DEBUG(
                logger,
                "S3LOAD >> list: " << objectName << " (" << objectSize << ")");

            // Parse Object Name and Extract Dimensions
            size_t idx = objectName.find_last_of("_");
            if (idx == string::npos)
                S3_EXCEPTION_OBJECT_NAME;

            size_t i = 0;
            istringstream objectNameStream(objectName.c_str());
            objectNameStream.seekg(idx + 1);
            for (int coord; objectNameStream >> coord;) {
                pos[i] = coord;
                i++;
                if (objectNameStream.peek() == '_')
                    objectNameStream.ignore();
            }
            if (i != _nDims)
                S3_EXCEPTION_OBJECT_NAME;

            if (_schema.getPrimaryInstanceId(
                    pos, query->getInstancesCount()) == query->getInstanceID()) {
                for (i = 0; i < _nDims; i++)
                    LOG4CXX_DEBUG(
                        logger,
                        "S3LOAD >> inst: " << query->getInstanceID() << " dim: " << i << " coord: " << pos[i]);
                readChunk(query, pos, s3Client, bucketName, objectName, objectSize);
            }
        }

        Aws::ShutdownAPI(options);

        // Finalize Array
        for(AttributeID i =0; i< _nAtts; ++i) {
            if(_chunkIterators[i].get())
                _chunkIterators[i]->flush();
            _chunkIterators[i].reset();
            _arrayIterators[i].reset();
        }

        return array;
    }

private:
    const size_t _nDims;
    const size_t _nAtts;
    vector<shared_ptr<ArrayIterator> > _arrayIterators;
    vector<shared_ptr<ChunkIterator> > _chunkIterators;

    arrow::Status readChunk(std::shared_ptr<Query> query,
                            Coordinates& posStart,
                            Aws::S3::S3Client& s3Client,
                            const Aws::String& bucketName,
                            const Aws::String& objectName,
                            const long long objectSize) {
        // Download Chunk
        Aws::S3::Model::GetObjectRequest objectRequest;
        objectRequest.SetBucket(bucketName);
        objectRequest.SetKey(objectName);

        auto outcome = s3Client.GetObject(objectRequest);
        S3_EXCEPTION_NOT_SUCCESS("Get");

        auto& data_stream = outcome.GetResultWithOwnership().GetBody();
        char data[objectSize];
        // TODO check objectSize before converting it
        data_stream.read(data, (streamsize)objectSize);

        arrow::io::BufferReader arrowBufferReader(
            reinterpret_cast<const uint8_t*>(data), objectSize); // zero copy

        // Read Record Batch using Stream Reader
        std::shared_ptr<arrow::RecordBatchReader> arrowReader;
        ARROW_RETURN_NOT_OK(
            arrow::ipc::RecordBatchStreamReader::Open(
                &arrowBufferReader, &arrowReader));

        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowReader,
        //     arrow::ipc::RecordBatchStreamReader::Open(&arrowBufferReader));
        ARROW_RETURN_NOT_OK(arrowReader->ReadNext(&arrowBatch));

        // Set Chunk Iterators
        for (AttributeID i = 0; i < _nAtts; ++i) {
            if (_chunkIterators[i].get())
                _chunkIterators[i]->flush();
            _chunkIterators[i] = _arrayIterators[i]->newChunk(posStart).getIterator(
                query,
                i == 0 ?
                ChunkIterator::SEQUENTIAL_WRITE :
                ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }

        const int64_t* coordArrow[_nDims];
        for (size_t i = 0; i < _nDims; ++i)
            coordArrow[i] = std::static_pointer_cast<arrow::Int64Array>(
                arrowBatch->column(_nAtts + i))->raw_values();
        Coordinates pos(_nDims);
        Value val, nullVal;
        nullVal.setNull();

        for (AttributeID i = 0; i < _nAtts; ++i) {
            std::shared_ptr<arrow::Array> array = arrowBatch->column(i);
            const int64_t* arrayData =
                std::static_pointer_cast<arrow::Int64Array>(
                    array)->raw_values();
            const int64_t nullCount = array->null_count();
            const uint8_t* nullBitmap = array->null_bitmap_data();

            for (int64_t j = 0; j < array->length(); ++j) {
                // Set Position
                for (size_t k = 0; k < _nDims; ++k)
                    pos[k] = coordArrow[k][j];
                _chunkIterators[i]->setPosition(pos);

                // Set Value
                if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                    _chunkIterators[i]->writeItem(nullVal);
                else {
                    val.setInt64(arrayData[j]);
                    _chunkIterators[i]->writeItem(val);
                }
            }

        }

        return arrow::Status::OK();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalS3Load, "s3load", "PhysicalS3Load");

} // end namespace scidb
