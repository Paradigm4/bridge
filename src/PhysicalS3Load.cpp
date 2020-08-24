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
        _typeAtts(_nAtts)
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
        std::vector<std::shared_ptr<ArrayIterator> > arrayIterators(_nAtts);
        std::vector<std::shared_ptr<ChunkIterator> > chunkIterators(_nDims);
        size_t i = 0;
        for (const auto& attr : _schema.getAttributes(true)) {
            _typeAtts[i++] = typeId2TypeEnum(attr.getType(), true);
            arrayIterators[attr.getId()] = array->getIterator(attr);
        }

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
            size_t idx = objectName.find_last_of("/");
            if (idx == string::npos)
                S3_EXCEPTION_OBJECT_NAME;

            size_t i = 0;
            istringstream objectNameStream(objectName.c_str());
            objectNameStream.seekg(idx + 7); // "/chunk_"
            for (int coord; objectNameStream >> coord;) {
                LOG4CXX_DEBUG(
                    logger,
                    "S3LOAD >> list: " << objectName << " coord: " << coord);
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
                readChunk(query,
                          arrayIterators,
                          chunkIterators,
                          pos,
                          s3Client,
                          bucketName,
                          objectName,
                          objectSize);
            }
        }

        Aws::ShutdownAPI(options);

        // Finalize Array
        for(AttributeID i =0; i< _nAtts; ++i) {
            if(chunkIterators[i].get())
                chunkIterators[i]->flush();
            chunkIterators[i].reset();
            arrayIterators[i].reset();
        }

        return array;
    }

private:
    const size_t _nDims;
    const size_t _nAtts;
    std::vector<TypeEnum> _typeAtts;

    arrow::Status readChunk(
        std::shared_ptr<Query> query,
        std::vector<std::shared_ptr<ArrayIterator> >& arrayIterators,
        std::vector<std::shared_ptr<ChunkIterator> >& chunkIterators,
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
        // Data is in Row Major Order
        for (AttributeID i = 0; i < _nAtts; ++i) {
            if (chunkIterators[i].get())
                chunkIterators[i]->flush();
            chunkIterators[i] = arrayIterators[i]->newChunk(posStart).getIterator(
                query,
                i == 0 ?
                ChunkIterator::SEQUENTIAL_WRITE :
                ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }

        const int64_t* arrowCoord[_nDims];
        for (size_t i = 0; i < _nDims; ++i)
            arrowCoord[i] = std::static_pointer_cast<arrow::Int64Array>(
                arrowBatch->column(_nAtts + i))->raw_values();

        // Common Variables when a Populator is not Used
        Coordinates pos(_nDims);
        Value val, nullVal;
        nullVal.setNull();

        for (AttributeID i = 0; i < _nAtts; ++i) {
            std::shared_ptr<arrow::Array> arrowArray = arrowBatch->column(i);

            // Common Variables when a Populator is not Used
            const int64_t nullCount = arrowArray->null_count();
            const uint8_t* nullBitmap = arrowArray->null_bitmap_data();

            switch(_typeAtts[i]) {
                // Arrow Binary & String length is int32_t
                // Long Binary length is int64_t
            case TE_BINARY:
            {
                for (int64_t j = 0; j < arrowArray->length(); ++j) {
                    // Set Position
                    for (size_t k = 0; k < _nDims; ++k)
                        pos[k] = arrowCoord[k][j];
                    chunkIterators[i]->setPosition(pos);

                    // Set Value
                    if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                        chunkIterators[i]->writeItem(nullVal);
                    else {
                        const uint8_t* ptr_val;
                        int32_t size_val;
                        ptr_val = std::static_pointer_cast<arrow::BinaryArray>(
                            arrowArray)->GetValue(j, &size_val);
                        val.setData(ptr_val, size_val);
                        chunkIterators[i]->writeItem(val);
                    }
                }
                break;
            }
            case TE_STRING:
            {
                for (int64_t j = 0; j < arrowArray->length(); ++j) {
                    // Set Position
                    for (size_t k = 0; k < _nDims; ++k)
                        pos[k] = arrowCoord[k][j];
                    chunkIterators[i]->setPosition(pos);

                    // Set Value
                    if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                        chunkIterators[i]->writeItem(nullVal);
                    else {
                        val.setString(
                            std::static_pointer_cast<arrow::StringArray>(
                                arrowArray)->GetString(j));
                        chunkIterators[i]->writeItem(val);
                    }
                }
                break;
            }
            case TE_CHAR:
            {
                for (int64_t j = 0; j < arrowArray->length(); ++j) {
                    // Set Position
                    for (size_t k = 0; k < _nDims; ++k)
                        pos[k] = arrowCoord[k][j];
                    chunkIterators[i]->setPosition(pos);

                    // Set Value
                    if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                        chunkIterators[i]->writeItem(nullVal);
                    else {
                        val.setChar(
                            std::static_pointer_cast<arrow::StringArray>(
                                arrowArray)->GetString(j)[0]);
                        chunkIterators[i]->writeItem(val);
                    }
                }
                break;
            }
            case TE_BOOL:
            {
                populateValue<arrow::BooleanArray>(arrowArray,
                                                   chunkIterators,
                                                   arrowCoord,
                                                   i,
                                                   &arrow::BooleanArray::Value,
                                                   &Value::setBool);
                break;
            }
            case TE_DATETIME:
            {
                populateRaw<int64_t, arrow::Date64Array>(arrowArray,
                                                         chunkIterators,
                                                         arrowCoord,
                                                         i,
                                                         &Value::setDateTime);
                break;
            }
            case TE_FLOAT:
            {
                populateRaw<float, arrow::FloatArray>(arrowArray,
                                                      chunkIterators,
                                                      arrowCoord,
                                                      i,
                                                      &Value::setFloat);
                break;
            }
            case TE_DOUBLE:
            {
                populateRaw<double, arrow::DoubleArray>(arrowArray,
                                                        chunkIterators,
                                                        arrowCoord,
                                                        i,
                                                        &Value::setDouble);
                break;
            }
            case TE_INT8:
            {
                populateRaw<int8_t, arrow::Int8Array>(arrowArray,
                                                      chunkIterators,
                                                      arrowCoord,
                                                      i,
                                                      &Value::setInt8);
                break;
            }
            case TE_INT16:
            {
                populateRaw<int16_t, arrow::Int16Array>(arrowArray,
                                                        chunkIterators,
                                                        arrowCoord,
                                                        i,
                                                        &Value::setInt16);
                break;
            }
            case TE_INT32:
            {
                populateRaw<int32_t, arrow::Int32Array>(arrowArray,
                                                        chunkIterators,
                                                        arrowCoord,
                                                        i,
                                                        &Value::setInt32);
                break;
            }
            case TE_INT64:
            {
                populateRaw<int64_t, arrow::Int64Array>(arrowArray,
                                                        chunkIterators,
                                                        arrowCoord,
                                                        i,
                                                        &Value::setInt64);
                break;
            }
            case TE_UINT8:
            {
                populateRaw<uint8_t, arrow::UInt8Array>(arrowArray,
                                                        chunkIterators,
                                                        arrowCoord,
                                                        i,
                                                        &Value::setUint8);
                break;
            }
            case TE_UINT16:
            {
                populateRaw<uint16_t, arrow::UInt16Array>(arrowArray,
                                                          chunkIterators,
                                                          arrowCoord,
                                                          i,
                                                          &Value::setUint16);
                break;
            }
            case TE_UINT32:
            {
                populateRaw<uint32_t, arrow::UInt32Array>(arrowArray,
                                                          chunkIterators,
                                                          arrowCoord,
                                                          i,
                                                          &Value::setUint32);
                break;
            }
            case TE_UINT64:
            {
                populateRaw<uint64_t, arrow::UInt64Array>(arrowArray,
                                                          chunkIterators,
                                                          arrowCoord,
                                                          i,
                                                          &Value::setUint64);
                break;
            }
            default:
            {
                ostringstream out;
                out << "Type "
                    << _schema.getAttributes(true).findattr(i).getType()
                    << " not supported";
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                     SCIDB_LE_ILLEGAL_OPERATION) << out.str();
            }
            }
        }

        return arrow::Status::OK();
    }

    template <typename Type,
              typename ArrowArray,
              typename ValueFunc> inline
    void populateRaw(
        std::shared_ptr<arrow::Array> arrowArray,
        std::vector<std::shared_ptr<ChunkIterator> >& chunkIterators,
        const int64_t** arrowCoord,
        const size_t i,
        ValueFunc valueSetter) {

        Coordinates pos(_nDims);
        const Type* arrayData =
            std::static_pointer_cast<ArrowArray>(arrowArray)->raw_values();
        const int64_t nullCount = arrowArray->null_count();
        const uint8_t* nullBitmap = arrowArray->null_bitmap_data();
        Value val, nullVal;
        nullVal.setNull();

        for (int64_t j = 0; j < arrowArray->length(); ++j) {
            // Set Position
            for (size_t k = 0; k < _nDims; ++k)
                pos[k] = arrowCoord[k][j];
            chunkIterators[i]->setPosition(pos);

            // Set Value
            if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                chunkIterators[i]->writeItem(nullVal);
            else {
                (val.*valueSetter)(arrayData[j]);
                chunkIterators[i]->writeItem(val);
            }
        }
    }

    template <typename ArrowArray,
              typename GetValueFunc,
              typename SetValueFunc> inline
    void populateValue(
        std::shared_ptr<arrow::Array> arrowArray,
        std::vector<std::shared_ptr<ChunkIterator> >& chunkIterators,
        const int64_t** arrowCoord,
        const size_t i,
        GetValueFunc valueGetter,
        SetValueFunc valueSetter) {

        Coordinates pos(_nDims);
        const int64_t nullCount = arrowArray->null_count();
        const uint8_t* nullBitmap = arrowArray->null_bitmap_data();
        Value val, nullVal;
        nullVal.setNull();

        for (int64_t j = 0; j < arrowArray->length(); ++j) {
            // Set Position
            for (size_t k = 0; k < _nDims; ++k)
                pos[k] = arrowCoord[k][j];
            chunkIterators[i]->setPosition(pos);

            // Set Value
            if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                chunkIterators[i]->writeItem(nullVal);
            else {
                (val.*valueSetter)(
                    (*std::static_pointer_cast<ArrowArray>(
                        arrowArray).*valueGetter)(j));
                chunkIterators[i]->writeItem(val);
            }
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalS3Load, "s3load", "PhysicalS3Load");

} // end namespace scidb
