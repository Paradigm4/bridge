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
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "S3SaveSettings.h"


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


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.alt_save"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
    if (!cond)
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
}

std::shared_ptr<arrow::Schema> attributes2ArrowSchema(ArrayDesc const &arrayDesc)
{
    Attributes const &attrs = arrayDesc.getAttributes(true);
    Dimensions const &dims = arrayDesc.getDimensions();

    size_t nAttrs = attrs.size();
    size_t nDims = dims.size();

    std::vector<std::shared_ptr<arrow::Field>> arrowFields(nAttrs + nDims);
    size_t i = 0;
    for (const auto& attr : attrs)
    {
        auto type = attr.getType();
        auto typeEnum = typeId2TypeEnum(type, true);
        std::shared_ptr<arrow::DataType> arrowType;

        switch (typeEnum)
        {
        case TE_BINARY:
        {
            arrowType = arrow::binary();
            break;
        }
        case TE_BOOL:
        {
            arrowType = arrow::boolean();
            break;
        }
        case TE_CHAR:
        {
            arrowType = arrow::utf8();
            break;
        }
        case TE_DATETIME:
        {
            arrowType = arrow::timestamp(arrow::TimeUnit::SECOND);
            break;
        }
        case TE_DOUBLE:
        {
            arrowType = arrow::float64();
            break;
        }
        case TE_FLOAT:
        {
            arrowType = arrow::float32();
            break;
        }
        case TE_INT8:
        {
            arrowType = arrow::int8();
            break;
        }
        case TE_INT16:
        {
            arrowType = arrow::int16();
            break;
        }
        case TE_INT32:
        {
            arrowType = arrow::int32();
            break;
        }
        case TE_INT64:
        {
            arrowType = arrow::int64();
            break;
        }
        case TE_UINT8:
        {
            arrowType = arrow::uint8();
            break;
        }
        case TE_UINT16:
        {
            arrowType = arrow::uint16();
            break;
        }
        case TE_UINT32:
        {
            arrowType = arrow::uint32();
            break;
        }
        case TE_UINT64:
        {
            arrowType = arrow::uint64();
            break;
        }
        case TE_STRING:
        {
            arrowType = arrow::utf8();
            break;
        }
        default:
        {
            ostringstream error;
            error << "Type " << type << " not supported in arrow format";
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        }

        arrowFields[i] = arrow::field(attr.getName(), arrowType);
        i++;
    }
    for (size_t i = 0; i < nDims; ++i)
    {
        arrowFields[nAttrs + i] = arrow::field(dims[i].getBaseName(), arrow::int64());
    }

    return arrow::schema(arrowFields);
}

class ArrayCursor
{
private:
    shared_ptr<Array> _input;
    size_t const _nAttrs;
    vector <Value const *> _currentCell;
    bool _end;
    vector<shared_ptr<ConstArrayIterator> > _inputArrayIters;
    vector<shared_ptr<ConstChunkIterator> > _inputChunkIters;

public:
    ArrayCursor (shared_ptr<Array> const& input):
        _input(input),
        _nAttrs(input->getArrayDesc().getAttributes(true).size()),
        _currentCell(_nAttrs, 0),
        _end(false),
        _inputArrayIters(_nAttrs, 0),
        _inputChunkIters(_nAttrs, 0)
    {
        const auto& inputSchemaAttrs = input->getArrayDesc().getAttributes(true);
        for (const auto& attr : inputSchemaAttrs)
        {
            _inputArrayIters[attr.getId()] = _input->getConstIterator(attr);
        }
        if (_inputArrayIters[0]->end())
        {
            _end=true;
        }
        else
        {
            advance();
        }
    }

    bool end() const
    {
        return _end;
    }

    size_t nAttrs() const
    {
        return _nAttrs;
    }

    void advanceChunkIters()
    {
        if (_end)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal error: iterating past end of cursor";
        }
        if (_inputChunkIters[0] == 0) // 1st time!
        {
            for(size_t i = 0; i < _nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS);
            }
        }
        else if (!_inputChunkIters[0]->end()) // not the first time!
        {
            for(size_t i = 0; i < _nAttrs; ++i)
            {
                ++(*_inputChunkIters[i]);
            }
        }
        while(_inputChunkIters[0]->end())
        {
            for(size_t i =0; i < _nAttrs; ++i)
            {
                ++(*_inputArrayIters[i]);
            }
            if(_inputArrayIters[0]->end())
            {
                _end = true;
                return;
            }
            for(size_t i =0; i < _nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS);
            }
        }
    }

    void advance()
    {
        if (_end)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal error: iterating past end of cursor";
        }
        advanceChunkIters();
        if (_end)
        {
            return;
        }
        for(size_t i = 0; i < _nAttrs; ++i)
        {
            _currentCell[i] = &(_inputChunkIters[i]->getItem());
        }
    }

    vector <Value const *> const& getCell()
    {
        return _currentCell;
    }

    shared_ptr<ConstChunkIterator> getChunkIter(size_t i)
    {
        return _inputChunkIters[i];
    }

    Coordinates const& getPosition()
    {
        return _inputChunkIters[0]->getPosition();
    }
};

class PhysicalS3Save : public PhysicalOperator
{
public:
    PhysicalS3Save(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        LOG4CXX_DEBUG(logger, "S3SAVE >> PhysicalS3Save");
    }

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        LOG4CXX_DEBUG(logger, "S3SAVE >> execute");

        S3SaveSettings settings (_parameters, _kwParameters, false, query);
        shared_ptr<Array>& inputArray = inputArrays[0];
        ArrayDesc const& inputSchema = inputArray->getArrayDesc();

        if (haveChunk(inputArray, inputSchema))
        {

            ArrayCursor inputCursor(inputArray);
            THROW_NOT_OK(setArrowOutput(inputSchema, inputCursor));

            Aws::SDKOptions options;
            Aws::InitAPI(options);
            uploadS3(settings.getBucketName(), settings.getObjectPath());
            Aws::ShutdownAPI(options);
        }

        return shared_ptr<Array>(new MemArray(_schema, query));
    }

private:
    std::vector<std::shared_ptr<arrow::Array>>        _arrowArrays;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> _arrowBuilders;
    const char* _arrowOutput;
    size_t      _arrowOutputSize;

    bool haveChunk(shared_ptr<Array>& array, ArrayDesc const& schema)
    {
        LOG4CXX_DEBUG(logger, "S3SAVE >> haveChunk");
        shared_ptr<ConstArrayIterator> iter = array->getConstIterator(schema.getAttributes(true).firstDataAttribute());
        return !(iter->end());
    }

    void uploadS3(string bucketName, string objectPath)
    {
        const Aws::String& s3_bucket_name = Aws::String(bucketName.c_str());
        const Aws::String& s3_object_name = Aws::String(objectPath.c_str());

        Aws::Client::ClientConfiguration clientConfig;
        Aws::S3::S3Client s3_client(clientConfig);
        Aws::S3::Model::PutObjectRequest object_request;

        object_request.SetBucket(s3_bucket_name);
        object_request.SetKey(s3_object_name);
        const std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::StringStream>("");
        input_data->write(_arrowOutput, _arrowOutputSize);
        object_request.SetBody(input_data);

        auto put_object_outcome = s3_client.PutObject(object_request);
        LOG4CXX_DEBUG(logger, "S3SAVE >> PutObject " << put_object_outcome.IsSuccess());
        LOG4CXX_DEBUG(logger, "S3SAVE >> PutObject " << put_object_outcome.GetError());
        LOG4CXX_DEBUG(logger, "S3SAVE >> PutObject " << s3_bucket_name);
        LOG4CXX_DEBUG(logger, "S3SAVE >> PutObject " << s3_object_name);

        if (!put_object_outcome.IsSuccess()) {
            ostringstream out;
            out << "Upload to s3://" << s3_bucket_name << "/" << s3_object_name
                << " failed. ";
            auto error = put_object_outcome.GetError();
            out << error.GetMessage() << ". ";
            if (error.GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN)
                out << "See https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-403/";
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                 SCIDB_LE_UNKNOWN_ERROR) << out.str();
        }
    }

    arrow::Status setArrowOutput(ArrayDesc const& schema, ArrayCursor& cursor)
    {
        const Attributes& attrs = schema.getAttributes(true);
        const size_t nAttrs = attrs.size();
        const size_t nDims = schema.getDimensions().size();
        std::vector<TypeEnum> types(nAttrs);

        arrow::MemoryPool* arrowPool = arrow::default_memory_pool();
        _arrowBuilders.resize(nAttrs + nDims);
        _arrowArrays.resize(nAttrs + nDims);
        const std::shared_ptr<arrow::Schema> arrowSchema = attributes2ArrowSchema(schema);

        // Create Arrow Builders
        size_t i = 0;
        for (const auto& attr : attrs)
        {
            types[i] = typeId2TypeEnum(attr.getType(), true);

            THROW_NOT_OK(arrow::MakeBuilder(arrowPool,
                                            arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]));
            i++;
        }
        for(size_t i = nAttrs; i < nAttrs + nDims; ++i)
        {
            THROW_NOT_OK(arrow::MakeBuilder(arrowPool,
                                            arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]));
        }

        // Setup coordinates buffers
        std::vector<std::vector<int64_t>> dimValues(nDims);

        // Append to Arrow Builders
        while (!cursor.end())
        {
            for (size_t attrIdx = 0; attrIdx < nAttrs; ++attrIdx)
            {
                shared_ptr<ConstChunkIterator> citer = cursor.getChunkIter(attrIdx);

                // Reset coordinate buffers
                if (attrIdx == 0)
                {
                    for (size_t j = 0; j < nDims; ++j)
                    {
                        dimValues[j].clear();
                    }
                }

                switch (types[attrIdx])
                {
                case TE_BINARY:
                {
                    while (!citer->end())
                    {
                        Value const& value = citer->getItem();
                        if(value.isNull())
                        {
                            ARROW_RETURN_NOT_OK(
                                static_cast<arrow::BinaryBuilder*>(
                                    _arrowBuilders[attrIdx].get())->AppendNull());
                        }
                        else
                        {
                            ARROW_RETURN_NOT_OK(
                                static_cast<arrow::BinaryBuilder*>(
                                    _arrowBuilders[attrIdx].get())->Append(
                                        reinterpret_cast<const char*>(
                                            value.data()),
                                        value.size()));
                        }

                        // Store coordinates in the buffer
                        if (attrIdx == 0)
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < nDims; ++j)
                                dimValues[j].push_back(coords[j]);
                        }

                        ++(*citer);
                    }
                    break;
                }
                case TE_STRING:
                {
                    vector<string> values;
                    vector<uint8_t> is_valid;

                    while (!citer->end())
                    {
                        Value const& value = citer->getItem();
                        if(value.isNull())
                        {
                            values.push_back("");
                            is_valid.push_back(0);
                        }
                        else
                        {
                            values.push_back(value.getString());
                            is_valid.push_back(1);
                        }

                        // Store coordinates in the buffer
                        if (attrIdx == 0)
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < nDims; ++j)
                                dimValues[j].push_back(coords[j]);
                        }

                        ++(*citer);
                    }

                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[attrIdx].get())->AppendValues(values, is_valid.data()));
                    break;
                }
                case TE_CHAR:
                {
                    vector<string> values;
                    vector<uint8_t> is_valid;

                    while (!citer->end())
                    {
                        Value const& value = citer->getItem();
                        if(value.isNull())
                        {
                            values.push_back("");
                            is_valid.push_back(0);
                        }
                        else
                        {
                            values.push_back(string(1, value.getChar()));
                            is_valid.push_back(1);
                        }

                        // Store coordinates in the buffer
                        if (attrIdx == 0)
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < nDims; ++j)
                                dimValues[j].push_back(coords[j]);
                        }

                        ++(*citer);
                    }

                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[attrIdx].get())->AppendValues(values, is_valid.data()));
                    break;
                }
                case TE_BOOL:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<bool,arrow::BooleanBuilder>(
                            citer,
                            &Value::getBool,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_DATETIME:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int64_t,arrow::Date64Builder>(
                            citer,
                            &Value::getDateTime,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_DOUBLE:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<double,arrow::DoubleBuilder>(
                            citer,
                            &Value::getDouble,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_FLOAT:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<float,arrow::FloatBuilder>(
                            citer,
                            &Value::getFloat,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_INT8:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int8_t,arrow::Int8Builder>(
                            citer,
                            &Value::getInt8,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_INT16:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int16_t,arrow::Int16Builder>(
                            citer,
                            &Value::getInt16,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_INT32:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int32_t,arrow::Int32Builder>(
                            citer,
                            &Value::getInt32,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_INT64:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int64_t,arrow::Int64Builder>(
                            citer,
                            &Value::getInt64,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_UINT8:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint8_t,arrow::UInt8Builder>(
                            citer,
                            &Value::getUint8,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_UINT16:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint16_t,arrow::UInt16Builder>(
                            citer,
                            &Value::getUint16,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_UINT32:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint32_t,arrow::UInt32Builder>(
                            citer,
                            &Value::getUint32,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                case TE_UINT64:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint64_t,arrow::UInt64Builder>(
                            citer,
                            &Value::getUint64,
                            attrIdx,
                            nDims,
                            dimValues)));
                    break;
                }
                default:
                {
                    ostringstream error;
                    error << "Type "
                          << types[attrIdx]
                          << " not supported in arrow format";
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                         SCIDB_LE_ILLEGAL_OPERATION) << error.str();
                }
                }

                if (attrIdx == 0)
                {
                    // Store coordinates in Arrow arrays
                    for (size_t j = 0; j < nDims; ++j)
                    {
                        ARROW_RETURN_NOT_OK(
                            static_cast<arrow::Int64Builder*>(
                                _arrowBuilders[nAttrs + j].get()
                                )->AppendValues(dimValues[j]));
                    }
                }
            }

            cursor.advanceChunkIters();
        }

        // Finalize Arrow Builders and populate Arrow Arrays (resets builders)
        for (size_t i = 0; i < nAttrs + nDims; ++i)
        {
            ARROW_RETURN_NOT_OK(
                _arrowBuilders[i]->Finish(&_arrowArrays[i])); // Resets builder
        }

        // Create Arrow Record Batch
        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        arrowBatch = arrow::RecordBatch::Make(arrowSchema, _arrowArrays[0]->length(), _arrowArrays);
        ARROW_RETURN_NOT_OK(arrowBatch->Validate());

        // Stream Arrow Record Batch to Arrow Buffer using Arrow
        // Record Batch Writer and Arrow Buffer Output Stream
        std::shared_ptr<arrow::io::BufferOutputStream> arrowStream;
        ARROW_ASSIGN_OR_RAISE(
            arrowStream,
            // TODO Better initial estimate for Create
            arrow::io::BufferOutputStream::Create(4096, arrowPool));

        std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
        ARROW_RETURN_NOT_OK(
            arrow::ipc::RecordBatchStreamWriter::Open(
                &*arrowStream, arrowSchema, &arrowWriter));
        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowWriter,
        //     arrow::ipc::NewStreamWriter(&*arrowStream, arrowSchema));

        ARROW_RETURN_NOT_OK(arrowWriter->WriteRecordBatch(*arrowBatch));
        ARROW_RETURN_NOT_OK(arrowWriter->Close());

        std::shared_ptr<arrow::Buffer> arrowBuffer;
        ARROW_ASSIGN_OR_RAISE(arrowBuffer, arrowStream->Finish());

        LOG4CXX_DEBUG(logger, "S3SAVE >> arrowBuffer::size: " << arrowBuffer->size());

        _arrowOutput = reinterpret_cast<const char*>(arrowBuffer->data());
        _arrowOutputSize = arrowBuffer->size();

        return arrow::Status::OK();
    }

    template <typename SciDBType,
              typename ArrowBuilder,
              typename ValueFunc> inline
    arrow::Status populateCell(shared_ptr<ConstChunkIterator> citer,
                               ValueFunc valueGetter,
                               const size_t attrIdx,
                               const size_t nDims,
                               std::vector<std::vector<int64_t>> &dimValues)
    {
        vector<SciDBType> values;
        vector<bool> is_valid;

        while (!citer->end())
        {
            Value const& value = citer->getItem();
            if(value.isNull())
            {
                values.push_back(0);
                is_valid.push_back(false);
            }
            else
            {
                values.push_back((value.*valueGetter)());
                is_valid.push_back(true);
            }

            // Store coordinates in the buffer
            if (attrIdx == 0)
            {
                Coordinates const &coords = citer->getPosition();
                for (size_t j = 0; j < nDims; ++j)
                    dimValues[j].push_back(coords[j]);
            }

            ++(*citer);
        }

        return static_cast<ArrowBuilder*>(
            _arrowBuilders[attrIdx].get())->AppendValues(values, is_valid);
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalS3Save, "s3save", "PhysicalS3Save");

} // end namespace scidb
