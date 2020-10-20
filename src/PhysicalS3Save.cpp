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
#include <network/Network.h>
#include <query/PhysicalOperator.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include <aws/s3/model/PutObjectRequest.h>

#include "S3Common.h"
#include "S3SaveSettings.h"
#include "S3Index.h"


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

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.s3save"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
    if (!cond)
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
}

std::shared_ptr<arrow::Schema> atts2ArrowSchema(ArrayDesc const &schema)
{
    Attributes const &attrs = schema.getAttributes(true);
    Dimensions const &dims = schema.getDimensions();

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
            std::ostringstream error;
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

class ArrowPopulator
{
private:
    const size_t                                      _nAttrs;
    const size_t                                      _nDims;
    std::vector<TypeEnum>                             _attrTypes;
    std::vector<std::vector<int64_t>>                 _dimValues;

    const std::shared_ptr<arrow::Schema>              _arrowSchema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> _arrowBuilders;
    std::vector<std::shared_ptr<arrow::Array>>        _arrowArrays;

    arrow::MemoryPool*                                _arrowPool =
        arrow::default_memory_pool();

public:
    ArrowPopulator(ArrayDesc const &schema):
        _nAttrs(schema.getAttributes(true).size()),
        _nDims(schema.getDimensions().size()),
        _attrTypes(_nAttrs),
        _dimValues(_nDims),

        _arrowSchema(atts2ArrowSchema(schema)),
        _arrowBuilders(_nAttrs + _nDims),
        _arrowArrays(_nAttrs + _nDims)
    {
        // Create Arrow Builders
        const Attributes& attrs = schema.getAttributes(true);
        size_t i = 0;
        for (const auto& attr : attrs)
        {
            _attrTypes[i] = typeId2TypeEnum(attr.getType(), true);

            THROW_NOT_OK(arrow::MakeBuilder(_arrowPool,
                                            _arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]));
            i++;
        }
        for(size_t i = _nAttrs; i < _nAttrs + _nDims; ++i)
        {
            THROW_NOT_OK(arrow::MakeBuilder(_arrowPool,
                                            _arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]));
        }
    }

   arrow::Status populateArrowBuffer(ArrayDesc const& schema,
                                      std::vector<std::shared_ptr<ConstChunkIterator> >& chunkIters,
                                      std::shared_ptr<arrow::Buffer>& arrowBuffer)
    {
        // Append to Arrow Builders
        for (size_t attrIdx = 0; attrIdx < _nAttrs; ++attrIdx)
        {
            std::shared_ptr<ConstChunkIterator> chunkIter = chunkIters[attrIdx];

            // Reset coordinate buffers
            if (attrIdx == 0) for (size_t i = 0; i < _nDims; ++i) _dimValues[i].clear();

            switch (_attrTypes[attrIdx])
            {
            case TE_BINARY:
            {
                while (!chunkIter->end())
                {
                    Value const& value = chunkIter->getItem();
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
                        Coordinates const &coords = chunkIter->getPosition();
                        for (size_t i = 0; i < _nDims; ++i)
                            _dimValues[i].push_back(coords[i]);
                    }

                    ++(*chunkIter);
                }
                break;
            }
            case TE_STRING:
            {
                std::vector<std::string> values;
                std::vector<uint8_t> is_valid;

                while (!chunkIter->end())
                {
                    Value const& value = chunkIter->getItem();
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
                        Coordinates const &coords = chunkIter->getPosition();
                        for (size_t i = 0; i < _nDims; ++i)
                            _dimValues[i].push_back(coords[i]);
                    }

                    ++(*chunkIter);
                }

                ARROW_RETURN_NOT_OK(
                    static_cast<arrow::StringBuilder*>(
                        _arrowBuilders[attrIdx].get())->AppendValues(
                            values, is_valid.data()));
                break;
            }
            case TE_CHAR:
            {
                std::vector<std::string> values;
                std::vector<uint8_t> is_valid;

                while (!chunkIter->end())
                {
                    Value const& value = chunkIter->getItem();
                    if(value.isNull())
                    {
                        values.push_back("");
                        is_valid.push_back(0);
                    }
                    else
                    {
                        values.push_back(std::string(1, value.getChar()));
                        is_valid.push_back(1);
                    }

                    // Store coordinates in the buffer
                    if (attrIdx == 0)
                    {
                        Coordinates const &coords = chunkIter->getPosition();
                        for (size_t i = 0; i < _nDims; ++i)
                            _dimValues[i].push_back(coords[i]);
                    }

                    ++(*chunkIter);
                }

                ARROW_RETURN_NOT_OK(static_cast<arrow::StringBuilder*>(
                                        _arrowBuilders[attrIdx].get())->AppendValues(
                                            values, is_valid.data()));
                break;
            }
            case TE_BOOL:
            {
                ARROW_RETURN_NOT_OK((populateCell<bool,arrow::BooleanBuilder>(
                                         chunkIter, &Value::getBool, attrIdx)));
                break;
            }
            case TE_DATETIME:
            {
                ARROW_RETURN_NOT_OK((populateCell<int64_t,arrow::Date64Builder>(
                                         chunkIter, &Value::getDateTime, attrIdx)));
                break;
            }
            case TE_DOUBLE:
            {
                ARROW_RETURN_NOT_OK((populateCell<double,arrow::DoubleBuilder>(
                                         chunkIter, &Value::getDouble, attrIdx)));
                break;
            }
            case TE_FLOAT:
            {
                ARROW_RETURN_NOT_OK((populateCell<float,arrow::FloatBuilder>(
                                         chunkIter, &Value::getFloat, attrIdx)));
                break;
            }
            case TE_INT8:
            {
                ARROW_RETURN_NOT_OK((populateCell<int8_t,arrow::Int8Builder>(
                                         chunkIter, &Value::getInt8, attrIdx)));
                break;
            }
            case TE_INT16:
            {
                ARROW_RETURN_NOT_OK((populateCell<int16_t,arrow::Int16Builder>(
                                         chunkIter, &Value::getInt16, attrIdx)));
                break;
            }
            case TE_INT32:
            {
                ARROW_RETURN_NOT_OK((populateCell<int32_t,arrow::Int32Builder>(
                                         chunkIter, &Value::getInt32, attrIdx)));
                break;
            }
            case TE_INT64:
            {
                ARROW_RETURN_NOT_OK((populateCell<int64_t,arrow::Int64Builder>(
                                         chunkIter, &Value::getInt64, attrIdx)));
                break;
            }
            case TE_UINT8:
            {
                ARROW_RETURN_NOT_OK((populateCell<uint8_t,arrow::UInt8Builder>(
                                         chunkIter, &Value::getUint8, attrIdx)));
                break;
            }
            case TE_UINT16:
            {
                ARROW_RETURN_NOT_OK((populateCell<uint16_t,arrow::UInt16Builder>(
                                         chunkIter, &Value::getUint16, attrIdx)));
                break;
            }
            case TE_UINT32:
            {
                ARROW_RETURN_NOT_OK((populateCell<uint32_t,arrow::UInt32Builder>(
                                         chunkIter, &Value::getUint32, attrIdx)));
                break;
            }
            case TE_UINT64:
            {
                ARROW_RETURN_NOT_OK((populateCell<uint64_t,arrow::UInt64Builder>(
                                         chunkIter, &Value::getUint64, attrIdx)));
                break;
            }
            default:
            {
                std::ostringstream error;
                error << "Type "
                      << _attrTypes[attrIdx]
                      << " not supported in arrow format";
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                     SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }
            }

            if (attrIdx == 0)
                // Store coordinates in Arrow arrays
                for (size_t i = 0; i < _nDims; ++i)
                    ARROW_RETURN_NOT_OK(static_cast<arrow::Int64Builder*>(
                                            _arrowBuilders[_nAttrs + i].get()
                                            )->AppendValues(_dimValues[i]));
        }

        // Finalize Arrow Builders and populate Arrow Arrays (resets builders)
        for (size_t i = 0; i < _nAttrs + _nDims; ++i)
            ARROW_RETURN_NOT_OK(
                _arrowBuilders[i]->Finish(&_arrowArrays[i])); // Resets builder

        // Create Arrow Record Batch
        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        arrowBatch = arrow::RecordBatch::Make(
            _arrowSchema, _arrowArrays[0]->length(), _arrowArrays);
        ARROW_RETURN_NOT_OK(arrowBatch->Validate());

        // Stream Arrow Record Batch to Arrow Buffer using Arrow
        // Record Batch Writer and Arrow Buffer Output Stream
        std::shared_ptr<arrow::io::BufferOutputStream> arrowStream;
        ARROW_ASSIGN_OR_RAISE(
            arrowStream,
            // TODO Better initial estimate for Create
            arrow::io::BufferOutputStream::Create(4096, _arrowPool));

        std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
        ARROW_RETURN_NOT_OK(
            arrow::ipc::RecordBatchStreamWriter::Open(
                &*arrowStream, _arrowSchema, &arrowWriter));
        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowWriter,
        //     arrow::ipc::NewStreamWriter(&*arrowStream, _arrowSchema));

        ARROW_RETURN_NOT_OK(arrowWriter->WriteRecordBatch(*arrowBatch));
        ARROW_RETURN_NOT_OK(arrowWriter->Close());

        ARROW_ASSIGN_OR_RAISE(arrowBuffer, arrowStream->Finish());
        LOG4CXX_DEBUG(logger, "S3SAVE|arrowBuffer::size: " << arrowBuffer->size());

        return arrow::Status::OK();
    }

private:
    template <typename SciDBType,
              typename ArrowBuilder,
              typename ValueFunc> inline
    arrow::Status populateCell(std::shared_ptr<ConstChunkIterator> chunkIter,
                               ValueFunc valueGetter,
                               const size_t attrIdx)
    {
        std::vector<SciDBType> values;
        std::vector<bool> is_valid;

        while (!chunkIter->end())
        {
            Value const& value = chunkIter->getItem();
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
                Coordinates const &coords = chunkIter->getPosition();
                for (size_t i = 0; i < _nDims; ++i)
                    _dimValues[i].push_back(coords[i]);
            }

            ++(*chunkIter);
        }

        return static_cast<ArrowBuilder*>(
            _arrowBuilders[attrIdx].get())->AppendValues(values, is_valid);
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
    {}

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        S3SaveSettings settings(_parameters, _kwParameters, false, query);
        std::shared_ptr<Array> result(new MemArray(_schema, query));

        std::shared_ptr<Array>& inputArray = inputArrays[0];
        ArrayDesc inputSchema(inputArray->getArrayDesc());
        inputSchema.setName("");
        bool haveChunk_ = haveChunk(inputArray, inputSchema);
        LOG4CXX_DEBUG(logger,
                      "S3SAVE|" << query->getInstanceID()
                      << "|executed isCoord " << query->isCoordinator()
                      << " haveChunk " << haveChunk_);

        // Chunk Coordinate Index
        S3Index index(inputSchema);

        // Exit Early
        if (!haveChunk_ && !query->isCoordinator()) {
            // Send EMPTY Index to Coordinator
            BufSend(query->getCoordinatorID(), index.serialize(), query);
            return result;
        }

        // Init AWS
        Aws::SDKOptions options;
        Aws::InitAPI(options);

        // Coordiantor Creates S3 Metadata Object
        Aws::String bucketName = Aws::String(settings.getBucketName().c_str());
        if (query->isCoordinator())
        {
            // Prep Metadata
            const std::shared_ptr<Aws::IOStream> metaData =
                Aws::MakeShared<Aws::StringStream>("");
            *metaData << "schema\t";
            printSchema(*metaData, inputSchema);
            *metaData << "\n";
            *metaData << "version\t" << S3BRIDGE_VERSION << "\n";
            *metaData << "attribute\tALL\n";
            *metaData << "format\tarrow\n";

            // Set Object Name
            std::ostringstream out;
            out << settings.getBucketPrefix() << "/metadata";

            uploadToS3(bucketName,
                       Aws::String(out.str().c_str()),
                       metaData);
        }

        const Dimensions &dims = inputSchema.getDimensions();
        if (haveChunk_) {
            // Init Array & Chunk Iterators
            size_t const nAttrs = inputSchema.getAttributes(true).size();
            std::vector<std::shared_ptr<ConstArrayIterator> > inputArrayIters(nAttrs);
            std::vector<std::shared_ptr<ConstChunkIterator> > inputChunkIters(nAttrs);
            for (auto const &attr : inputSchema.getAttributes(true))
                inputArrayIters[attr.getId()] = inputArray->getConstIterator(attr);

            // Init Populator
            // if (settings.isArrowFormat())
            ArrowPopulator populator(inputSchema);

            while (!inputArrayIters[0]->end()) {
                if (!inputArrayIters[0]->getChunk().getConstIterator(
                        ConstChunkIterator::IGNORE_OVERLAPS)->end()) {
                    // Init Iterators for Current Chunk
                    for(size_t i = 0; i < nAttrs; ++i)
                        inputChunkIters[i] = inputArrayIters[i]->getChunk(
                            ).getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS);

                    // Set Object Name using Top-Left Coordinates
                    Coordinates const &pos = inputChunkIters[0]->getFirstPosition();
                    // Add Chunk Coordinates to the Chunk Index
                    index.insert(pos);
                    Aws::String objectName(coord2ObjectName(
                                               settings.getBucketPrefix(),
                                               pos,
                                               dims).c_str());

                    std::shared_ptr<arrow::Buffer> arrowBuffer;
                    THROW_NOT_OK(
                        populator.populateArrowBuffer(
                            inputSchema, inputChunkIters, arrowBuffer));

                    // Set Chunk Data
                    const std::shared_ptr<Aws::IOStream> chunkData =
                        Aws::MakeShared<Aws::StringStream>("");
                    chunkData->write(reinterpret_cast<const char*>(arrowBuffer->data()),
                                     arrowBuffer->size());

                    uploadToS3(bucketName, objectName, chunkData);
                }

                // Advance Array Iterators
                for(size_t i =0; i < nAttrs; ++i) ++(*inputArrayIters[i]);
            }
        }

        // Centralize Chunk Coordinate Lists
        if (query->isCoordinator()) {
            size_t const nInst = query->getInstancesCount();
            InstanceID localID = query->getInstanceID();

            for(InstanceID remoteID = 0; remoteID < nInst; ++remoteID)
                if(remoteID != localID)
                    // Receive and De-Serialize Index
                    index.deserialize_insert(BufReceive(remoteID, query));

            // Sort Chunk Coordinate List
            index.sort();

            // Serialize Chunk Coordinate List
            const std::shared_ptr<Aws::IOStream> metaData =
                Aws::MakeShared<Aws::StringStream>("");
            (*metaData) << index;

            // Set Object Name
            std::ostringstream out;
            out << settings.getBucketPrefix() << "/index";

            // Upload Chunk Coordinate List to S3
            uploadToS3(bucketName,
                       Aws::String(out.str().c_str()),
                       metaData);
        }
        else
            BufSend(query->getCoordinatorID(), index.serialize(), query);

        Aws::ShutdownAPI(options);

        return result;
    }

private:
    void uploadToS3(const Aws::String& bucketName,
                    const Aws::String& objectName,
                    const std::shared_ptr<Aws::IOStream> inputData)
    {
        Aws::S3::S3Client s3Client;
        Aws::S3::Model::PutObjectRequest objectRequest;

        objectRequest.SetBucket(bucketName);
        objectRequest.SetKey(objectName);
        objectRequest.SetBody(inputData);

        auto outcome = s3Client.PutObject(objectRequest);
        S3_EXCEPTION_NOT_SUCCESS("Upload");
    }

    bool haveChunk(std::shared_ptr<Array>& array, ArrayDesc const& schema)
    {
        std::shared_ptr<ConstArrayIterator> iter = array->getConstIterator(
            schema.getAttributes(true).firstDataAttribute());
        return !(iter->end());
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalS3Save, "s3save", "PhysicalS3Save");

} // end namespace scidb
