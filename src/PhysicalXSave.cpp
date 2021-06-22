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

#include "XSaveSettings.h"

#include "Driver.h"
#include "XArray.h"
#include "XIndex.h"

// SciDB
#include <array/TileIteratorAdaptors.h>
#include <network/Network.h>
#include <query/PhysicalOperator.h>

// Arrow
#include <arrow/builder.h>
#include <arrow/io/compressed.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>


namespace scidb {

class ArrowWriter
{
private:
    const size_t                                      _nAttrs;
    const size_t                                      _nDims;
    const Metadata::Compression                       _compression;
    std::vector<TypeEnum>                             _attrTypes;
    std::vector<std::vector<int64_t>>                 _dimValues;


    const std::shared_ptr<arrow::Schema>              _arrowSchema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> _arrowBuilders;
    std::vector<std::shared_ptr<arrow::Array>>        _arrowArrays;

    arrow::MemoryPool*                                _arrowPool =
        arrow::default_memory_pool();

public:
    ArrowWriter(const Attributes &attributes,
                const Dimensions &dimensions,
                const Metadata::Compression compression):
        _nAttrs(attributes.size()),
        _nDims(dimensions.size()),
        _compression(compression),
        _attrTypes(_nAttrs),
        _dimValues(_nDims),

        _arrowSchema(ArrowReader::scidb2ArrowSchema(attributes, dimensions)),
        _arrowBuilders(_nAttrs + _nDims),
        _arrowArrays(_nAttrs + _nDims) {

        // Create Arrow Builders
        size_t i = 0;
        for (const auto& attr : attributes) {
            _attrTypes[i] = typeId2TypeEnum(attr.getType(), true);

            THROW_NOT_OK(arrow::MakeBuilder(_arrowPool,
                                            _arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]),
                         "make builder");
            i++;
        }
        for(size_t i = _nAttrs; i < _nAttrs + _nDims; ++i) {
            THROW_NOT_OK(arrow::MakeBuilder(_arrowPool,
                                            _arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]),
                         "make builder");
        }
    }

    arrow::Status writeArrowBuffer(const std::vector<std::shared_ptr<ConstChunkIterator> > &chunkIters,
                                   std::shared_ptr<arrow::Buffer> &arrowBuffer) {
        // Append to Arrow Builders
        for (size_t attrIdx = 0; attrIdx < _nAttrs; ++attrIdx) {
            auto chunkIter = chunkIters[attrIdx];

            // Reset coordinate buffers
            if (attrIdx == 0) for (size_t i = 0; i < _nDims; ++i) _dimValues[i].clear();

            switch (_attrTypes[attrIdx]) {
            case TE_BINARY: {
                while (!chunkIter->end()) {
                    Value const& value = chunkIter->getItem();
                    if(value.isNull()) {
                        ARROW_RETURN_NOT_OK(
                            static_cast<arrow::BinaryBuilder*>(
                                _arrowBuilders[attrIdx].get())->AppendNull());
                    }
                    else {
                        ARROW_RETURN_NOT_OK(
                            static_cast<arrow::BinaryBuilder*>(
                                _arrowBuilders[attrIdx].get())->Append(
                                    reinterpret_cast<const char*>(
                                        value.data()),
                                    value.size()));
                    }

                    // Store coordinates in the buffer
                    if (attrIdx == 0) {
                        Coordinates const &coords = chunkIter->getPosition();
                        for (size_t i = 0; i < _nDims; ++i)
                            _dimValues[i].push_back(coords[i]);
                    }

                    ++(*chunkIter);
                }
                break;
            }
            case TE_STRING: {
                std::vector<std::string> values;
                std::vector<uint8_t> is_valid;

                while (!chunkIter->end()) {
                    Value const& value = chunkIter->getItem();
                    if(value.isNull()) {
                        values.push_back("");
                        is_valid.push_back(0);
                    }
                    else {
                        values.push_back(value.getString());
                        is_valid.push_back(1);
                    }

                    // Store coordinates in the buffer
                    if (attrIdx == 0) {
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
            case TE_CHAR: {
                std::vector<std::string> values;
                std::vector<uint8_t> is_valid;

                while (!chunkIter->end()) {
                    Value const& value = chunkIter->getItem();
                    if(value.isNull()) {
                        values.push_back("");
                        is_valid.push_back(0);
                    }
                    else {
                        values.push_back(std::string(1, value.getChar()));
                        is_valid.push_back(1);
                    }

                    // Store coordinates in the buffer
                    if (attrIdx == 0) {
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
            case TE_BOOL: {
                ARROW_RETURN_NOT_OK((writeChunk<bool,arrow::BooleanBuilder>(
                                         chunkIter, &Value::getBool, attrIdx)));
                break;
            }
            case TE_DATETIME: {
                ARROW_RETURN_NOT_OK((writeChunk<int64_t,arrow::Date64Builder>(
                                         chunkIter, &Value::getDateTime, attrIdx)));
                break;
            }
            case TE_DOUBLE: {
                ARROW_RETURN_NOT_OK((writeChunk<double,arrow::DoubleBuilder>(
                                         chunkIter, &Value::getDouble, attrIdx)));
                break;
            }
            case TE_FLOAT: {
                ARROW_RETURN_NOT_OK((writeChunk<float,arrow::FloatBuilder>(
                                         chunkIter, &Value::getFloat, attrIdx)));
                break;
            }
            case TE_INT8: {
                ARROW_RETURN_NOT_OK((writeChunk<int8_t,arrow::Int8Builder>(
                                         chunkIter, &Value::getInt8, attrIdx)));
                break;
            }
            case TE_INT16: {
                ARROW_RETURN_NOT_OK((writeChunk<int16_t,arrow::Int16Builder>(
                                         chunkIter, &Value::getInt16, attrIdx)));
                break;
            }
            case TE_INT32: {
                ARROW_RETURN_NOT_OK((writeChunk<int32_t,arrow::Int32Builder>(
                                         chunkIter, &Value::getInt32, attrIdx)));
                break;
            }
            case TE_INT64: {
                ARROW_RETURN_NOT_OK((writeChunk<int64_t,arrow::Int64Builder>(
                                         chunkIter, &Value::getInt64, attrIdx)));
                break;
            }
            case TE_UINT8: {
                ARROW_RETURN_NOT_OK((writeChunk<uint8_t,arrow::UInt8Builder>(
                                         chunkIter, &Value::getUint8, attrIdx)));
                break;
            }
            case TE_UINT16: {
                ARROW_RETURN_NOT_OK((writeChunk<uint16_t,arrow::UInt16Builder>(
                                         chunkIter, &Value::getUint16, attrIdx)));
                break;
            }
            case TE_UINT32: {
                ARROW_RETURN_NOT_OK((writeChunk<uint32_t,arrow::UInt32Builder>(
                                         chunkIter, &Value::getUint32, attrIdx)));
                break;
            }
            case TE_UINT64: {
                ARROW_RETURN_NOT_OK((writeChunk<uint64_t,arrow::UInt64Builder>(
                                         chunkIter, &Value::getUint64, attrIdx)));
                break;
            }
            default: {
                std::ostringstream error;
                error << "Type "
                      << _attrTypes[attrIdx]
                      << " not supported in arrow format";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
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

        return finalize(arrowBuffer);
    }

    arrow::Status writePartArrowBuffer(const std::vector<std::shared_ptr<ConstChunkIterator> > &chunkIters,
                                       const Coordinates *pos,
                                       std::shared_ptr<arrow::Buffer> &arrowBuffer) {
        // Append to Arrow Builders
        for (size_t attrIdx = 0; attrIdx < _nAttrs; ++attrIdx) {
            const Value& value = chunkIters[attrIdx]->getItem();

            switch (_attrTypes[attrIdx]) {
            case TE_BINARY: {
                if(value.isNull()) {
                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::BinaryBuilder*>(
                            _arrowBuilders[attrIdx].get())->AppendNull());
                }
                else {
                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::BinaryBuilder*>(
                            _arrowBuilders[attrIdx].get())->Append(
                                reinterpret_cast<const char*>(
                                    value.data()),
                                value.size()));
                }
                break;
            }
            case TE_STRING: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::StringBuilder>(
                                         value, &Value::getString, attrIdx)));
                break;
            }
            case TE_CHAR: {
                if(value.isNull()) {
                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[attrIdx].get())->AppendNull());
                }
                else {
                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[attrIdx].get())->Append(
                                std::string(1, value.getChar())));
                }
                break;
            }
            case TE_BOOL: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::BooleanBuilder>(
                                         value, &Value::getBool, attrIdx)));
                break;
            }
            case TE_DATETIME: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::Date64Builder>(
                                         value, &Value::getDateTime, attrIdx)));
                break;
            }
            case TE_DOUBLE: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::DoubleBuilder>(
                                         value, &Value::getDouble, attrIdx)));
                break;
            }
            case TE_FLOAT: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::FloatBuilder>(
                                         value, &Value::getFloat, attrIdx)));
                break;
            }
            case TE_INT8: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::Int8Builder>(
                                         value, &Value::getInt8, attrIdx)));
                break;
            }
            case TE_INT16: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::Int16Builder>(
                                         value, &Value::getInt16, attrIdx)));
                break;
            }
            case TE_INT32: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::Int32Builder>(
                                         value, &Value::getInt32, attrIdx)));
                break;
            }
            case TE_INT64: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::Int64Builder>(
                                         value, &Value::getInt64, attrIdx)));
                break;
            }
            case TE_UINT8: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::UInt8Builder>(
                                         value, &Value::getUint8, attrIdx)));
                break;
            }
            case TE_UINT16: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::UInt16Builder>(
                                         value, &Value::getUint16, attrIdx)));
                break;
            }
            case TE_UINT32: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::UInt32Builder>(
                                         value, &Value::getUint32, attrIdx)));
                break;
            }
            case TE_UINT64: {
                ARROW_RETURN_NOT_OK((writeValue<arrow::UInt64Builder>(
                                         value, &Value::getUint64, attrIdx)));
                break;
            }
            default: {
                std::ostringstream error;
                error << "Type "
                      << _attrTypes[attrIdx]
                      << " not supported in arrow format";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                       SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }
            }
        }

        // Store coordinates in Arrow arrays
        for (size_t i = 0; i < _nDims; ++i)
            ARROW_RETURN_NOT_OK(static_cast<arrow::Int64Builder*>(
                                    _arrowBuilders[_nAttrs + i].get()
                                    )->Append((*pos)[i]));

        return arrow::Status::OK();
    }

    arrow::Status writeArrowBuffer(const XIndexStore::const_iterator begin,
                                   const XIndexStore::const_iterator end,
                                   const size_t size,
                                   std::shared_ptr<arrow::Buffer>& arrowBuffer) {
        // Append to Arrow Builders
        // Store coordinates in Arrow arrays
        for (auto posPtr = begin; posPtr != end && posPtr != begin + size; ++posPtr)
            for (size_t i = 0; i < _nDims; ++i) {
                ARROW_RETURN_NOT_OK(
                    static_cast<arrow::Int64Builder*>(
                        _arrowBuilders[i].get())->Append((*posPtr)[i]));
            }

        return finalize(arrowBuffer);
    }

    arrow::Status finalize(std::shared_ptr<arrow::Buffer>& arrowBuffer) {
        // Finalize Arrow Builders and write Arrow Arrays (resets builders)
        for (size_t i = 0; i < _nAttrs + _nDims; ++i)
            ARROW_RETURN_NOT_OK(
                _arrowBuilders[i]->Finish(&_arrowArrays[i])); // Resets Builder!

        // Create Arrow Record Batch
        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        arrowBatch = arrow::RecordBatch::Make(
            _arrowSchema, _arrowArrays[0]->length(), _arrowArrays);
        ARROW_RETURN_NOT_OK(arrowBatch->Validate());

        // Stream Arrow Record Batch to Arrow Buffer using Arrow
        // Record Batch Writer and Arrow Buffer Output Stream
        std::shared_ptr<arrow::io::BufferOutputStream> arrowBufferStream;
        ASSIGN_OR_THROW(
            arrowBufferStream,
            // TODO Better initial estimate for Create
            arrow::io::BufferOutputStream::Create(4096, _arrowPool),
            "create buffer output stream");

        // Setup Arrow Compression, If Enabled
        std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
        std::shared_ptr<arrow::io::CompressedOutputStream> arrowCompressedStream;
        if (_compression != Metadata::Compression::NONE) {
            std::unique_ptr<arrow::util::Codec> codec = *arrow::util::Codec::Create(
                Metadata::compression2Arrow(_compression));
            ASSIGN_OR_THROW(
                arrowCompressedStream,
                arrow::io::CompressedOutputStream::Make(codec.get(), arrowBufferStream),
                "make compressed output stream");
            ARROW_RETURN_NOT_OK(
                arrow::ipc::RecordBatchStreamWriter::Open(
                    &*arrowCompressedStream, _arrowSchema, &arrowWriter));
        }
        else {
            ARROW_RETURN_NOT_OK(
                arrow::ipc::RecordBatchStreamWriter::Open(
                    &*arrowBufferStream, _arrowSchema, &arrowWriter));
        }
        // TODO Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowWriter,
        //     arrow::ipc::NewStreamWriter(&*arrowStream, _arrowSchema));

        ARROW_RETURN_NOT_OK(arrowWriter->WriteRecordBatch(*arrowBatch));
        ARROW_RETURN_NOT_OK(arrowWriter->Close());

        // Close Arrow Compression Stream, If Enabled
        if (_compression != Metadata::Compression::NONE) {
            ARROW_RETURN_NOT_OK(arrowCompressedStream->Close());
        }

        ASSIGN_OR_THROW(arrowBuffer,
                        arrowBufferStream->Finish(),
                        "finish buffer output stream");
        LOG4CXX_DEBUG(logger, "XSAVE|arrowBuffer::size: " << arrowBuffer->size());

        return arrow::Status::OK();
    }

private:
    template <typename ArrowBuilder,
              typename ValueFunc> inline
    arrow::Status writeValue(const Value& value, ValueFunc valueGetter, const size_t attrIdx) {
        if(value.isNull())
            return static_cast<ArrowBuilder*>(
                _arrowBuilders[attrIdx].get())->AppendNull();
        else
            return static_cast<ArrowBuilder*>(
                _arrowBuilders[attrIdx].get())->Append(
                    (value.*valueGetter)());
    }

    template <typename SciDBType,
              typename ArrowBuilder,
              typename ValueFunc> inline
    arrow::Status writeChunk(std::shared_ptr<ConstChunkIterator> chunkIter,
                             ValueFunc valueGetter,
                             const size_t attrIdx) {

        std::vector<SciDBType> values;
        std::vector<bool> is_valid;

        while (!chunkIter->end()) {
            Value const& value = chunkIter->getItem();
            if(value.isNull()) {
                values.push_back(0);
                is_valid.push_back(false);
            }
            else {
                values.push_back((value.*valueGetter)());
                is_valid.push_back(true);
            }

            // Store coordinates in the buffer
            if (attrIdx == 0) {
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

class PhysicalXSave : public PhysicalOperator
{
public:
    PhysicalXSave(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    void preSingleExecute(std::shared_ptr<Query> query) override
    {
        // Initialize Settings
        if (_settings == NULL)
            _settings = std::make_shared<XSaveSettings>(_parameters, _kwParameters, false, query);

        // Create Driver
        if (_driver == NULL)
            _driver = Driver::makeDriver(
                _settings->getURL(),
                _settings->isUpdate() ? Driver::Mode::UPDATE : Driver::Mode::WRITE,
                _settings->getS3SSE());

        // Coordinator Creates/Checks Directories
        _driver->init(*query);
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override
    {
        // Initialize Settings If Not Already Set By preSingleExecute
        if (_settings == NULL)
            _settings = std::make_shared<XSaveSettings>(_parameters, _kwParameters, false, query);

        // Create Driver If Not Already Set By preSingleExecute
        if (_driver == NULL)
            _driver = Driver::makeDriver(
                _settings->getURL(),
                _settings->isUpdate() ? Driver::Mode::UPDATE : Driver::Mode::WRITE,
                _settings->getS3SSE());

        std::shared_ptr<Array> result(new MemArray(_schema, query));
        auto instID = query->getInstanceID();
        std::shared_ptr<Array>& inputArray = inputArrays[0];
        ArrayDesc inputSchema(inputArray->getArrayDesc());
        inputSchema.setName("");
        bool haveChunk_ = !inputArray->getConstIterator(
            _schema.getAttributes(true).firstDataAttribute())->end();
        LOG4CXX_DEBUG(logger,
                      "XSAVE|" << instID
                      << "|execute isCoord " << query->isCoordinator()
                      << " haveChunk " << haveChunk_);

        // Chunk Coordinate Index
        std::shared_ptr<XIndex> index = std::make_shared<XIndex>(inputSchema);
        std::shared_ptr<Metadata> metadataPtr = std::make_shared<Metadata>();
        Metadata &metadata = *metadataPtr; // Easier to Use with "[]"

        // If Update, Read metadata and check that it matches
        if (_settings->isUpdate()) {
            // Get Metadata
            _driver->readMetadata(metadataPtr);

            LOG4CXX_DEBUG(logger, "XSAVE|" << instID << "|found schema: " << metadata["schema"]);
            std::ostringstream error;

            // Check Schema
            auto existingSchema = metadata.getSchema(query);
            std::ostringstream existingAttrs, inputAttrs;
            inputSchema.getAttributes().stream_out(inputAttrs);
            existingSchema.getAttributes().stream_out(existingAttrs);
            if (!(inputSchema.sameSchema(
                      existingSchema,
                      ArrayDesc::SchemaFieldSelector(
                          ).startMin(true
                              ).endMax(true
                                  ).chunkInterval(true
                                      ).chunkOverlap(true))
                 && inputAttrs.str() == existingAttrs.str())) {
                error << "Existing schema ";
                printSchema(error, existingSchema);
                error << " and provided schema ";
                printSchema(error, inputSchema);
                error << " do not match";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                       SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }

            // Check Version
            if (STR(BRIDGE_VERSION) != metadata["version"]) {
                error << "Existing array Bridge version "
                      << metadata["version"]
                      << " and current version "
                      << BRIDGE_VERSION
                      << " do not match";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                       SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }

            // Check Attributes
            if ("ALL" != metadata["attribute"]) {
                error << "Existing array attributes "
                      << metadata["attributes"]
                      << " and current attributes "
                      << "ALL"
                      << " do not match";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                       SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }

            // Check Format
            if ("arrow" != metadata["format"]) {
                error << "Existing array format "
                      << metadata["format"]
                      << " and current format "
                      << "arrow"
                      << " do not match";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                       SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }

            // Compression and Index Split Parameters Not Allowed for
            // Updates

            // Set index_split from Existing Metadata
            _settings->setIndexSplit(std::stoi(metadata["index_split"]));

            // Set compressio from Existing Metadata
            _settings->setCompression(metadata.getCompression());

            // Load Index
            index->load(_driver, query);
        }
        else
            // New Array
            // Coordinator Creates Metadata
            if (query->isCoordinator()) {
                // Prep Metadata
                metadata["attribute"] = "ALL";
                metadata["format"] = "arrow";
                metadata["index_split"] = std::to_string(_settings->getIndexSplit());
                metadata["namespace"] = _settings->getNamespace().getName();
                metadata["version"] = STR(BRIDGE_VERSION);
                metadata.setSchema(inputSchema);
                metadata.setCompression(_settings->getCompression());

                // Write Metadata
                _driver->writeMetadata(metadataPtr);
            }

        const Dimensions &dims = inputSchema.getDimensions();
        if (haveChunk_) {
            // Init Array & Chunk Iterators
            size_t const nAttrs = inputSchema.getAttributes(true).size();
            std::vector<std::shared_ptr<ConstArrayIterator> > inputArrayIters(nAttrs);
            std::vector<std::shared_ptr<ConstChunkIterator> > inputChunkIters(nAttrs);
            for (auto const &attr : inputSchema.getAttributes(true))
                inputArrayIters[attr.getId()] = inputArray->getConstIterator(attr);
            auto compression = _settings->getCompression();

            // Allocate Existing Array and Iterators
            std::shared_ptr<XArray> existingArray;
            std::vector<std::shared_ptr<ConstArrayIterator> > existingArrayIters(
                nAttrs);

            // Index For New Chunks On Update Queries
            std::shared_ptr<XIndex> extraIndex = std::make_shared<XIndex>(
                inputSchema);

            // Init Existing and Merge Arrays and Iterators If Applicable
            if (_settings->isUpdate()) {
                // Init Existing Array
                existingArray = std::make_shared<XArray>(
                    inputSchema,
                    query,
                    _driver,
                    index,
                    compression, 0);
                for (auto const &attr : inputSchema.getAttributes(true))
                    existingArrayIters[attr.getId()] =
                        existingArray->getConstIterator(attr);
            }

            // Init Writer
            // if (_settings->isArrowFormat())
            ArrowWriter dataWriter(inputSchema.getAttributes(true),
                                   inputSchema.getDimensions(),
                                   compression);

            while (!inputArrayIters[0]->end()) {
                if (!inputArrayIters[0]->getChunk().getConstIterator(
                        ConstChunkIterator::IGNORE_OVERLAPS)->end()) {

                    // Init Iterators for Current Chunk
                    for(size_t i = 0; i < nAttrs; ++i)
                        inputChunkIters[i] = inputArrayIters[i]->getChunk(
                            ).getConstIterator(
                                ConstChunkIterator::IGNORE_OVERLAPS);

                    // Set Object Name using Top-Left Coordinates
                    Coordinates const &pos = inputChunkIters[0]->getFirstPosition();
                    auto objectName = Metadata::coord2ObjectName(pos, dims);

                    // Declare Output;
                    std::shared_ptr<arrow::Buffer> arrowBuffer;

                    // -- -
                    // Merge Chunks
                    // -- -
                    if (_settings->isUpdate() &&
                        existingArrayIters[0]->setPosition(pos)) {

                        // Prep Existing Array
                        for(size_t i = 1; i < nAttrs; ++i) // First Iterator Set Above in "if"
                            existingArrayIters[i]->setPosition(pos);

                        std::vector<std::shared_ptr<ConstChunkIterator> > existingChunkIters(nAttrs);
                        for(size_t i = 0; i < nAttrs; ++i)
                            existingChunkIters[i] = existingArrayIters[i]->getChunk(
                                ).getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS);

                        // Merge Loop
                        Coordinates const* inputPos    = inputChunkIters[0]->end() ? nullptr : &inputChunkIters[0]->getPosition();
                        Coordinates const* existingPos = existingChunkIters[0]->end() ? nullptr : &existingChunkIters[0]->getPosition();
                        while (inputPos != nullptr || existingPos != nullptr) {
                            bool nextInput    = false;
                            bool nextExisting = false;

                            if (inputPos == nullptr) { // Ended Input, Write Existing
                                THROW_NOT_OK(
                                    dataWriter.writePartArrowBuffer(
                                        existingChunkIters, existingPos, arrowBuffer),
                                    "serialize existing pos (input ended) to " + objectName);
                                nextExisting = true;
                            }
                            else if (existingPos == nullptr) { // Ended Existing, Write Input
                                THROW_NOT_OK(
                                    dataWriter.writePartArrowBuffer(
                                        inputChunkIters, inputPos, arrowBuffer),
                                    "serialize input pos (existing ended) to " + objectName);
                                nextInput = true;
                            }
                            else {
                                int64_t res = coordinatesCompare(*inputPos, *existingPos);

                                if (res < 0) { // Input Has Lower Coordinate, Write Input
                                    THROW_NOT_OK(
                                        dataWriter.writePartArrowBuffer(
                                            inputChunkIters, inputPos, arrowBuffer),
                                        "serialize input pos (lower) to " + objectName);
                                    nextInput = true;
                                }
                                else if ( res > 0 ) { // Existing Has Lower Coordinate, Write Existing
                                    THROW_NOT_OK(
                                        dataWriter.writePartArrowBuffer(
                                            existingChunkIters, existingPos, arrowBuffer),
                                        "serialize existing pos (lower) to " + objectName);
                                    nextExisting = true;
                                }
                                else { // Coordinates are Equal, Write Input, Advance Both
                                    THROW_NOT_OK(
                                        dataWriter.writePartArrowBuffer(
                                            inputChunkIters, inputPos, arrowBuffer),
                                        "serialize input pos (equal) to " + objectName);
                                    nextInput = true;
                                    nextExisting = true;
                                }
                            }

                            // Advance Iterators
                            if(inputPos != nullptr && nextInput) {
                                for(size_t i = 0; i < nAttrs; ++i)
                                    ++(*inputChunkIters[i]);
                                inputPos = inputChunkIters[0]->end() ? nullptr : &inputChunkIters[0]->getPosition();
                            }
                            if(existingPos != nullptr && nextExisting) {
                                for(size_t i = 0; i < nAttrs; ++i)
                                    ++(*existingChunkIters[i]);
                                existingPos = existingChunkIters[0]->end() ? nullptr : &existingChunkIters[0]->getPosition();
                            }
                        }

                        // Finalize Chunk
                        THROW_NOT_OK(dataWriter.finalize(arrowBuffer),
                                     "finalize " + objectName);
                    }

                    // -- -
                    // Add Input Chunk
                    // -- -
                    else {
                        if (_settings->isUpdate())
                            // Add Chunk Coordinates to Extra Index (Update Query)
                            extraIndex->insert(pos);
                        else
                            // Add Chunk Coordinates to Current Index
                            index->insert(pos);

                        // Serialize Chunk
                        THROW_NOT_OK(dataWriter.writeArrowBuffer(
                                         inputChunkIters, arrowBuffer),
                                     "serialize input chunk to " + objectName);
                    }

                    // Write Chunk
                    _driver->writeArrow(objectName, arrowBuffer);
                }

                // Advance Array Iterators
                for(size_t i =0; i < nAttrs; ++i) ++(*inputArrayIters[i]);
            }

            if (extraIndex->size() > 0)
                // Append New Chunks to Current Index
                index->insert(*extraIndex);
        }

        // Centralize Index
        if (query->isCoordinator()) {
            size_t const nInst = query->getInstancesCount();

            for(InstanceID remoteID = 0; remoteID < nInst; ++remoteID)
                if(remoteID != instID)
                    // Receive and De-Serialize Index
                    index->deserialize_insert(BufReceive(remoteID, query));

            // Sort Index
            index->sort();

            // Serialize Index
            size_t nDims = dims.size();
            size_t szSplit = static_cast<int>(_settings->getIndexSplit() / nDims);
            size_t split = 0;

            LOG4CXX_DEBUG(logger, "XSAVE|" << instID
                          << "|execute szSplit:" << szSplit);

            ArrowWriter indexWriter(Attributes(),
                                    inputSchema.getDimensions(),
                                    Metadata::Compression::GZIP);

            auto splitPtr = index->begin();
            while (splitPtr != index->end()) {
                // Assemble Object Name
                std::ostringstream out;
                out << "index/" << split;
                auto objectName = out.str();

                // Convert to Arrow
                std::shared_ptr<arrow::Buffer> arrowBuffer;
                THROW_NOT_OK(
                    indexWriter.writeArrowBuffer(
                        splitPtr, index->end(), szSplit, arrowBuffer),
                    "serialize index to " + objectName);

                // Write Index
                _driver->writeArrow(objectName, arrowBuffer);

                // Advance to Next Index Split
                splitPtr += std::min<size_t>(
                    szSplit,
                    std::distance(splitPtr, index->end()));
                split++;
            }
        }
        else
            BufSend(query->getCoordinatorID(), index->serialize(), query);

        return result;
    }

private:
    std::shared_ptr<XSaveSettings> _settings;
    std::shared_ptr<Driver> _driver;

    void writeFrom(std::vector<std::shared_ptr<ConstChunkIterator> > &sourceIters,
                   std::vector<std::shared_ptr<ChunkIterator> > &outputIters,
                   const size_t nAttrs,
                   const Coordinates *pos,
                   bool &flag) {
        for(size_t i = 0; i < nAttrs; ++i) {
            outputIters[0]->setPosition(*pos);
            outputIters[0]->writeItem(sourceIters[0]->getItem());
        }
        flag = true;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalXSave, "xsave", "PhysicalXSave");

} // namespace scidb
