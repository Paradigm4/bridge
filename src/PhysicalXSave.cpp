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
#include <arrow/util/compression.h>


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
                Metadata::Compression compression):
        _nAttrs(attributes.size()),
        _nDims(dimensions.size()),
        _compression(compression),
        _attrTypes(_nAttrs),
        _dimValues(_nDims),

        _arrowSchema(scidb2ArrowSchema(attributes, dimensions)),
        _arrowBuilders(_nAttrs + _nDims),
        _arrowArrays(_nAttrs + _nDims) {

        // Create Arrow Builders
        size_t i = 0;
        for (const auto& attr : attributes) {
            _attrTypes[i] = typeId2TypeEnum(attr.getType(), true);

            THROW_NOT_OK(arrow::MakeBuilder(_arrowPool,
                                            _arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]));
            i++;
        }
        for(size_t i = _nAttrs; i < _nAttrs + _nDims; ++i) {
            THROW_NOT_OK(arrow::MakeBuilder(_arrowPool,
                                            _arrowSchema->field(i)->type(),
                                            &_arrowBuilders[i]));
        }
    }

    arrow::Status writeArrowBuffer(const std::vector<std::shared_ptr<ConstChunkIterator> >& chunkIters,
                                   std::shared_ptr<arrow::Buffer>& arrowBuffer) {
        // Append to Arrow Builders
        for (size_t attrIdx = 0; attrIdx < _nAttrs; ++attrIdx) {
            std::shared_ptr<ConstChunkIterator> chunkIter = chunkIters[attrIdx];

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
                ARROW_RETURN_NOT_OK((writeCell<bool,arrow::BooleanBuilder>(
                                         chunkIter, &Value::getBool, attrIdx)));
                break;
            }
            case TE_DATETIME: {
                ARROW_RETURN_NOT_OK((writeCell<int64_t,arrow::Date64Builder>(
                                         chunkIter, &Value::getDateTime, attrIdx)));
                break;
            }
            case TE_DOUBLE: {
                ARROW_RETURN_NOT_OK((writeCell<double,arrow::DoubleBuilder>(
                                         chunkIter, &Value::getDouble, attrIdx)));
                break;
            }
            case TE_FLOAT: {
                ARROW_RETURN_NOT_OK((writeCell<float,arrow::FloatBuilder>(
                                         chunkIter, &Value::getFloat, attrIdx)));
                break;
            }
            case TE_INT8: {
                ARROW_RETURN_NOT_OK((writeCell<int8_t,arrow::Int8Builder>(
                                         chunkIter, &Value::getInt8, attrIdx)));
                break;
            }
            case TE_INT16: {
                ARROW_RETURN_NOT_OK((writeCell<int16_t,arrow::Int16Builder>(
                                         chunkIter, &Value::getInt16, attrIdx)));
                break;
            }
            case TE_INT32: {
                ARROW_RETURN_NOT_OK((writeCell<int32_t,arrow::Int32Builder>(
                                         chunkIter, &Value::getInt32, attrIdx)));
                break;
            }
            case TE_INT64: {
                ARROW_RETURN_NOT_OK((writeCell<int64_t,arrow::Int64Builder>(
                                         chunkIter, &Value::getInt64, attrIdx)));
                break;
            }
            case TE_UINT8: {
                ARROW_RETURN_NOT_OK((writeCell<uint8_t,arrow::UInt8Builder>(
                                         chunkIter, &Value::getUint8, attrIdx)));
                break;
            }
            case TE_UINT16: {
                ARROW_RETURN_NOT_OK((writeCell<uint16_t,arrow::UInt16Builder>(
                                         chunkIter, &Value::getUint16, attrIdx)));
                break;
            }
            case TE_UINT32: {
                ARROW_RETURN_NOT_OK((writeCell<uint32_t,arrow::UInt32Builder>(
                                         chunkIter, &Value::getUint32, attrIdx)));
                break;
            }
            case TE_UINT64: {
                ARROW_RETURN_NOT_OK((writeCell<uint64_t,arrow::UInt64Builder>(
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

    arrow::Status writeArrowBuffer(const XIndexCont::const_iterator begin,
                                   const XIndexCont::const_iterator end,
                                   const size_t size,
                                   std::shared_ptr<arrow::Buffer>& arrowBuffer) {
        // Append to Arrow Builders
        // Store coordinates in Arrow arrays
        for (auto posPtr = begin; posPtr != end && posPtr != begin + size; ++posPtr)
            for (size_t i = 0; i < _nDims; ++i) {
                ARROW_RETURN_NOT_OK(
                    static_cast<arrow::Int64Builder*>(
                        _arrowBuilders[i].get())->Append(
                            (*posPtr)[i]
                            // - _dims[i].getStartMin()) / _dims[i].getChunkInterval()
                            ));
            }

        return finalize(arrowBuffer);
    }

    static std::shared_ptr<arrow::Schema> scidb2ArrowSchema(
        const Attributes &attributes,
        const Dimensions &dimensions) {

        size_t nAttrs = attributes.size();
        size_t nDims = dimensions.size();

        std::vector<std::shared_ptr<arrow::Field>> arrowFields(nAttrs + nDims);
        size_t i = 0;
        for (const auto& attr : attributes) {
            auto type = attr.getType();
            auto typeEnum = typeId2TypeEnum(type, true);
            std::shared_ptr<arrow::DataType> arrowType;

            switch (typeEnum) {
            case TE_BINARY: {
                arrowType = arrow::binary();
                break;
            }
            case TE_BOOL: {
                arrowType = arrow::boolean();
                break;
            }
            case TE_CHAR: {
                arrowType = arrow::utf8();
                break;
            }
            case TE_DATETIME: {
                arrowType = arrow::timestamp(arrow::TimeUnit::SECOND);
                break;
            }
            case TE_DOUBLE: {
                arrowType = arrow::float64();
                break;
            }
            case TE_FLOAT: {
                arrowType = arrow::float32();
                break;
            }
            case TE_INT8: {
                arrowType = arrow::int8();
                break;
            }
            case TE_INT16: {
                arrowType = arrow::int16();
                break;
            }
            case TE_INT32: {
                arrowType = arrow::int32();
                break;
            }
            case TE_INT64: {
                arrowType = arrow::int64();
                break;
            }
            case TE_UINT8: {
                arrowType = arrow::uint8();
                break;
            }
            case TE_UINT16: {
                arrowType = arrow::uint16();
                break;
            }
            case TE_UINT32: {
                arrowType = arrow::uint32();
                break;
            }
            case TE_UINT64: {
                arrowType = arrow::uint64();
                break;
            }
            case TE_STRING: {
                arrowType = arrow::utf8();
                break;
            }
            default: {
                std::ostringstream error;
                error << "Type " << type << " not supported in arrow format";
                throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                       SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }
            }

            arrowFields[i] = arrow::field(attr.getName(), arrowType);
            i++;
        }
        for (size_t i = 0; i < nDims; ++i)
            arrowFields[nAttrs + i] = arrow::field(
                dimensions[i].getBaseName(), arrow::int64());

        return arrow::schema(arrowFields);
    }

private:
    template <typename SciDBType,
              typename ArrowBuilder,
              typename ValueFunc> inline
    arrow::Status writeCell(std::shared_ptr<ConstChunkIterator> chunkIter,
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

    arrow::Status finalize(std::shared_ptr<arrow::Buffer>& arrowBuffer) {
        // Finalize Arrow Builders and write Arrow Arrays (resets builders)
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
        std::shared_ptr<arrow::io::BufferOutputStream> arrowBufferStream;
        ARROW_ASSIGN_OR_RAISE(
            arrowBufferStream,
            // TODO Better initial estimate for Create
            arrow::io::BufferOutputStream::Create(4096, _arrowPool));

        // Setup Arrow Compression, If Enabled
        std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
        std::shared_ptr<arrow::io::CompressedOutputStream> arrowCompressedStream;
        if (_compression == Metadata::Compression::GZIP) {
            std::unique_ptr<arrow::util::Codec> codec = *arrow::util::Codec::Create(
                arrow::Compression::type::GZIP);
            ARROW_ASSIGN_OR_RAISE(
                arrowCompressedStream,
                arrow::io::CompressedOutputStream::Make(codec.get(), arrowBufferStream));
            ARROW_RETURN_NOT_OK(
                arrow::ipc::RecordBatchStreamWriter::Open(
                    &*arrowCompressedStream, _arrowSchema, &arrowWriter));
        }
        else {
            ARROW_RETURN_NOT_OK(
                arrow::ipc::RecordBatchStreamWriter::Open(
                    &*arrowBufferStream, _arrowSchema, &arrowWriter));
        }
        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowWriter,
        //     arrow::ipc::NewStreamWriter(&*arrowStream, _arrowSchema));

        ARROW_RETURN_NOT_OK(arrowWriter->WriteRecordBatch(*arrowBatch));
        ARROW_RETURN_NOT_OK(arrowWriter->Close());

        // Close Arrow Compression Stream, If Enabled
        if (_compression == Metadata::Compression::GZIP) {
            ARROW_RETURN_NOT_OK(arrowCompressedStream->Close());
        }

        ARROW_ASSIGN_OR_RAISE(arrowBuffer, arrowBufferStream->Finish());
        LOG4CXX_DEBUG(logger, "XSAVE|arrowBuffer::size: " << arrowBuffer->size());

        return arrow::Status::OK();
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

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        _settings = std::make_shared<XSaveSettings>(_parameters, _kwParameters, false, query);
        _driver = Driver::makeDriver(
            _settings->getURL(),
            _settings->isUpdate() ? Driver::Mode::UPDATE : Driver::Mode::WRITE);

        // Coordinator Creates/Checks Directories
        _driver->init();
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        // Create Settings if not already set
        if (_settings == NULL)
            _settings = std::make_shared<XSaveSettings>(_parameters, _kwParameters, false, query);

        const Metadata::Compression compression = _settings->getCompression();
        std::shared_ptr<Array> result(new MemArray(_schema, query));

        std::shared_ptr<Array>& inputArray = inputArrays[0];
        ArrayDesc inputSchema(inputArray->getArrayDesc());
        inputSchema.setName("");
        bool haveChunk_ = haveChunk(inputArray, inputSchema);
        LOG4CXX_DEBUG(logger,
                      "XSAVE|" << query->getInstanceID()
                      << "|execute isCoord " << query->isCoordinator()
                      << " haveChunk " << haveChunk_);

        // Chunk Coordinate Index
        XIndex index(inputSchema);

        // Exit Early
        if (!haveChunk_ && !query->isCoordinator()) {
            // Send EMPTY Index to Coordinator
            BufSend(query->getCoordinatorID(), index.serialize(), query);
            return result;
        }

        // Create Driver if not already set
        if (_driver == NULL)
            _driver = Driver::makeDriver(
                _settings->getURL(),
                _settings->isUpdate() ? Driver::Mode::UPDATE : Driver::Mode::WRITE);

        // If Update, Read metadata and check that it matches
        if (_settings->isUpdate()) {
            // Get Metadata
            Metadata metadata;
            _driver->readMetadata(metadata);

            LOG4CXX_DEBUG(logger, "XSAVE|" << query->getInstanceID() << "|found schema: " << metadata["schema"]);
            ArrayDesc existingSchema = metadata.getArrayDesc(query);
            std::ostringstream error;

            // Check Schema
            if (!inputSchema.sameSchema(
                    existingSchema,
                    ArrayDesc::SchemaFieldSelector().wildcardInterval(true))) {
                error << "Existing schema "
                      << existingSchema
                      << " and provided schema "
                      << inputSchema
                      << " do not match";
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

            // Compression Not Allowed for Updates
        }
        else
            // Coordinator Creates Metadata
            if (query->isCoordinator()) {
                // Prep Metadata
                Metadata metadata;
                std::ostringstream out;
                printSchema(out, inputSchema);
                metadata["schema"] = out.str();
                metadata["version"] = STR(BRIDGE_VERSION);
                metadata["attribute"] = "ALL";
                metadata["format"] = "arrow";
                metadata["compression"] = Metadata::compression2String(compression);

                // Write Metadata
                _driver->writeMetadata(metadata);
            }

        const Dimensions &dims = inputSchema.getDimensions();
        if (haveChunk_) {
            // Init Array & Chunk Iterators
            size_t const nAttrs = inputSchema.getAttributes(true).size();
            std::vector<std::shared_ptr<ConstArrayIterator> > inputArrayIters(nAttrs);
            std::vector<std::shared_ptr<ConstChunkIterator> > inputChunkIters(nAttrs);
            for (auto const &attr : inputSchema.getAttributes(true))
                inputArrayIters[attr.getId()] = inputArray->getConstIterator(attr);

            // Init Writer
            // if (_settings->isArrowFormat())
            ArrowWriter dataWriter(inputSchema.getAttributes(true),
                                   inputSchema.getDimensions(),
                                   _settings->getCompression());

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

                    // Serialize Chunk
                    std::shared_ptr<arrow::Buffer> arrowBuffer;
                    THROW_NOT_OK(
                        dataWriter.writeArrowBuffer(inputChunkIters, arrowBuffer));

                    // Write Chunk
                    _driver->writeArrow(
                        "chunks/" +
                        Metadata::coord2ObjectName(pos, dims), arrowBuffer);
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
            size_t nDims = dims.size();
            size_t szSplit = static_cast<int>(_settings->getIndexSplit() / nDims);
            size_t split = 0;

            LOG4CXX_DEBUG(logger, "XSAVE|" << query->getInstanceID()
                          << "|execute szSplit:" << szSplit);

            ArrowWriter indexWriter(Attributes(),
                                    inputSchema.getDimensions(),
                                    Metadata::Compression::GZIP);

            auto splitPtr = index.begin();
            while (splitPtr != index.end()) {
                // Convert to Arrow
                std::shared_ptr<arrow::Buffer> arrowBuffer;
                THROW_NOT_OK(indexWriter.writeArrowBuffer(splitPtr,
                                                          index.end(),
                                                          szSplit,
                                                          arrowBuffer));

                // Write Chunk Coordinate List
                std::ostringstream out;
                out << "index/" << split;
                _driver->writeArrow(out.str(), arrowBuffer);

                // Advance to Next Index Split
                splitPtr += std::min<size_t>(
                    szSplit,
                    std::distance(splitPtr, index.end()));
                split++;
            }
        }
        else
            BufSend(query->getCoordinatorID(), index.serialize(), query);

        return result;
    }

private:
    std::shared_ptr<XSaveSettings> _settings;
    std::shared_ptr<Driver> _driver;

    bool haveChunk(std::shared_ptr<Array>& array, ArrayDesc const& schema)
    {
        std::shared_ptr<ConstArrayIterator> iter = array->getConstIterator(
            schema.getAttributes(true).firstDataAttribute());
        return !(iter->end());
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalXSave, "xsave", "PhysicalXSave");

} // namespace scidb
