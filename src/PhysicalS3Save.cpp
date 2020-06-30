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

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Sysinfo.h>

#ifdef USE_ARROW
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>
#endif

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <query/PhysicalOperator.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <util/Platform.h>
#include <network/Network.h>
#include <array/SinglePassArray.h>
#include <array/SynchableArray.h>
#include <array/PinBuffer.h>

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>

#include "S3SaveSettings.h"


#ifdef USE_ARROW
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

#define THROW_NOT_OK_FILE(s)                                            \
    {                                                                   \
        arrow::Status _s = (s);                                         \
        if (!_s.ok())                                                   \
        {                                                               \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)       \
                    << _s.ToString().c_str() << (int)_s.code();         \
        }                                                               \
    }
#endif


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.alt_save"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
    if (! cond)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
    }
}

#ifdef USE_ARROW
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
#endif

ArrayDesc const addDimensionsToArrayDesc(ArrayDesc const& arrayDesc,
                                         size_t nAttrs)
{
    ArrayDesc arrayDescWithDim(arrayDesc);

    Dimensions const &dims = arrayDesc.getDimensions();
    const size_t nDims = dims.size();

    for (size_t i = 0; i < nDims; ++i)
    {
        arrayDescWithDim.addAttribute(
            AttributeDesc(dims[i].getBaseName() + "val",
                          TID_INT64,
                          0,
                          CompressorType::NONE));
    }
    return arrayDescWithDim;
}

class MemChunkBuilder
{
private:
    size_t      _allocSize;
    char*       _chunkStartPointer;
    char*       _dataStartPointer;
    char*       _writePointer;
    uint32_t*   _sizePointer;
    uint64_t*   _dataSizePointer;
    MemChunk    _chunk;

public:
    static const size_t s_startingSize = 8*1024*1024 + 512;

    MemChunkBuilder():
        _allocSize(s_startingSize)
    {
        _chunk.allocate(_allocSize);
        _chunkStartPointer = (char*) _chunk.getWriteData();
        ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _chunkStartPointer;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = 1;
        hdr->_elemSize = 0;
        hdr->_dataSize = 0;
        _dataSizePointer = &(hdr->_dataSize);
        hdr->_varOffs = sizeof(varpart_offset_t);
        hdr->_isBoolean = 0;
        ConstRLEPayload::Segment* seg = (ConstRLEPayload::Segment*) (hdr+1);
        *seg =  ConstRLEPayload::Segment(0,0,false,false);
        ++seg;
        *seg =  ConstRLEPayload::Segment(1,0,false,false);
        varpart_offset_t* vp =  (varpart_offset_t*) (seg+1);
        *vp = 0;
        uint8_t* sizeFlag = (uint8_t*) (vp+1);
        *sizeFlag =0;
        _sizePointer = (uint32_t*) (sizeFlag + 1);
        _dataStartPointer = (char*) (_sizePointer+1);
        _writePointer = _dataStartPointer;
    }

    ~MemChunkBuilder()
    {}

    inline size_t getTotalSize() const
    {
        return (_writePointer - _chunkStartPointer);
    }

    inline void addData(char const* data, size_t const size)
    {
        if( getTotalSize() + size > _allocSize)
        {
            size_t const mySize = getTotalSize();
            while (mySize + size > _allocSize)
            {
                _allocSize = _allocSize * 2;
            }
            vector<char> buf(_allocSize);
            memcpy(&(buf[0]), _chunk.getWriteData(), mySize);
            _chunk.allocate(_allocSize);
            _chunkStartPointer = (char*) _chunk.getWriteData();
            memcpy(_chunkStartPointer, &(buf[0]), mySize);
            _dataStartPointer = _chunkStartPointer + S3SaveSettings::chunkDataOffset();
            _sizePointer = (uint32_t*) (_chunkStartPointer + S3SaveSettings::chunkSizeOffset());
            _writePointer = _chunkStartPointer + mySize;
            ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _chunkStartPointer;
            _dataSizePointer = &(hdr->_dataSize);
        }
        memcpy(_writePointer, data, size);
        _writePointer += size;
    }

    inline MemChunk& getChunk()
    {
        *_sizePointer = (_writePointer - _dataStartPointer);
        *_dataSizePointer = (_writePointer - _dataStartPointer) + 5 + sizeof(varpart_offset_t);
        return _chunk;
    }

    inline void reset()
    {
        _writePointer = _dataStartPointer;
    }
};

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

#ifdef USE_ARROW
class ArrowChunkPopulator
{

private:
    const Attributes&                                 _attrs;
    const size_t                                      _nDims;
    const std::shared_ptr<arrow::Schema>              _arrowSchema;

    std::vector<TypeEnum>                             _inputTypes;
    std::vector<size_t>                               _inputSizes;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> _arrowBuilders;
    std::vector<std::shared_ptr<arrow::Array>>        _arrowArrays;
    arrow::MemoryPool*                                _arrowPool =
      arrow::default_memory_pool();
    std::vector<std::vector<int64_t>>                 _dimsValues;

public:
    ArrowChunkPopulator(ArrayDesc const& inputArrayDesc,
                        S3SaveSettings const& settings):
        _attrs(inputArrayDesc.getAttributes(true)),
        _nDims(inputArrayDesc.getDimensions().size()),
        _arrowSchema(attributes2ArrowSchema(inputArrayDesc))
    {
        const size_t nAttrs = _attrs.size();

        _inputTypes.resize(nAttrs);
        _inputSizes.resize(nAttrs);
        _arrowBuilders.resize(nAttrs + _nDims);
        _arrowArrays.resize(nAttrs + _nDims);

        // Create Arrow Builders
        size_t i = 0;
        for (const auto& attr : _attrs)
        {
            _inputTypes[i] = typeId2TypeEnum(attr.getType(), true);
            _inputSizes[i] = attr.getSize() +
                (attr.isNullable() ? 1 : 0);

            THROW_NOT_OK(
                arrow::MakeBuilder(
                    _arrowPool,
                    _arrowSchema->field(i)->type(),
                    &_arrowBuilders[i]));
            i++;
        }
        for(size_t i = nAttrs; i < nAttrs + _nDims; ++i)
        {
            THROW_NOT_OK(
                arrow::MakeBuilder(
                    _arrowPool,
                    _arrowSchema->field(i)->type(),
                    &_arrowBuilders[i]));
        }

        // Setup coordinates buffers
        _dimsValues = std::vector<std::vector<int64_t>>(_nDims);
    }

    ~ArrowChunkPopulator()
    {}

    void populateChunk(MemChunkBuilder& builder,
                       ArrayCursor& cursor)
    {
        THROW_NOT_OK(populateChunkStatus(builder, cursor));
    }


private:
    arrow::Status populateChunkStatus(MemChunkBuilder& builder,
                                      ArrayCursor& cursor)
    {
        // Basic setup
        const size_t nAttrs = _attrs.size();
        size_t bytesCount = 0;

        // Append to Arrow Builders
        while (!cursor.end())
        {
            for (size_t i = 0; i < nAttrs; ++i)
            {
                shared_ptr<ConstChunkIterator> citer = cursor.getChunkIter(i);

                // Reset coordinate buffers
                if (i == 0)
                {
                    for (size_t j = 0; j < _nDims; ++j)
                    {
                        _dimsValues[j].clear();
                    }
                }

                switch (_inputTypes[i])
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
                                    _arrowBuilders[i].get())->AppendNull());
                        }
                        else
                        {
                            ARROW_RETURN_NOT_OK(
                                static_cast<arrow::BinaryBuilder*>(
                                    _arrowBuilders[i].get())->Append(
                                        reinterpret_cast<const char*>(
                                            value.data()),
                                        value.size()));
                        }
                        bytesCount += _inputSizes[i] + value.size();

                        // Store coordinates in the buffer
                        if (i == 0 )
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < _nDims; ++j)
                            {
                                _dimsValues[j].push_back(coords[j]);
                                bytesCount += 8;
                            }
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
                        bytesCount += _inputSizes[i] + value.size();

                        // Store coordinates in the buffer
                        if (i == 0 )
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < _nDims; ++j)
                            {
                                _dimsValues[j].push_back(coords[j]);
                                bytesCount += 8;
                            }
                        }

                        ++(*citer);
                    }

                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[i].get())->AppendValues(values, is_valid.data()));
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
                        bytesCount += _inputSizes[i] + value.size();

                        // Store coordinates in the buffer
                        if (i == 0 )
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < _nDims; ++j)
                            {
                                _dimsValues[j].push_back(coords[j]);
                                bytesCount += 8;
                            }
                        }

                        ++(*citer);
                    }

                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[i].get())->AppendValues(values, is_valid.data()));
                    break;
                }
                case TE_BOOL:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<bool,arrow::BooleanBuilder>(
                            citer,
                            &Value::getBool,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_DATETIME:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int64_t,arrow::Date64Builder>(
                            citer,
                            &Value::getDateTime,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_DOUBLE:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<double,arrow::DoubleBuilder>(
                            citer,
                            &Value::getDouble,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_FLOAT:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<float,arrow::FloatBuilder>(
                            citer,
                            &Value::getFloat,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT8:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int8_t,arrow::Int8Builder>(
                            citer,
                            &Value::getInt8,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT16:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int16_t,arrow::Int16Builder>(
                            citer,
                            &Value::getInt16,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT32:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int32_t,arrow::Int32Builder>(
                            citer,
                            &Value::getInt32,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT64:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int64_t,arrow::Int64Builder>(
                            citer,
                            &Value::getInt64,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT8:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint8_t,arrow::UInt8Builder>(
                            citer,
                            &Value::getUint8,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT16:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint16_t,arrow::UInt16Builder>(
                            citer,
                            &Value::getUint16,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT32:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint32_t,arrow::UInt32Builder>(
                            citer,
                            &Value::getUint32,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT64:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint64_t,arrow::UInt64Builder>(
                            citer,
                            &Value::getUint64,
                            i,
                            bytesCount)));
                    break;
                }
                default:
                {
                    ostringstream error;
                    error << "Type "
                          << _inputTypes[i]
                          << " not supported in arrow format";
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                         SCIDB_LE_ILLEGAL_OPERATION) << error.str();
                }
                }

                if (i == 0)
                {
                    // Store coordinates in Arrow arrays
                    for (size_t j = 0; j < _nDims; ++j)
                    {
                        ARROW_RETURN_NOT_OK(
                            static_cast<arrow::Int64Builder*>(
                                _arrowBuilders[nAttrs + j].get()
                                )->AppendValues(_dimsValues[j]));
                    }
                }
            }

            cursor.advanceChunkIters();
        }

        // Finalize Arrow Builders and populate Arrow Arrays (resets builders)
        for (size_t i = 0; i < nAttrs + _nDims; ++i)
        {
            ARROW_RETURN_NOT_OK(
                _arrowBuilders[i]->Finish(&_arrowArrays[i])); // Resets builder
        }

        // Create Arrow Record Batch
        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        arrowBatch = arrow::RecordBatch::Make(_arrowSchema, _arrowArrays[0]->length(), _arrowArrays);
        ARROW_RETURN_NOT_OK(arrowBatch->Validate());

        // Stream Arrow Record Batch to Arrow Buffer using Arrow
        // Record Batch Writer and Arrow Buffer Output Stream
        std::shared_ptr<arrow::io::BufferOutputStream> arrowStream;
        ARROW_ASSIGN_OR_RAISE(
            arrowStream,
            arrow::io::BufferOutputStream::Create(bytesCount * 2, _arrowPool));

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

        std::shared_ptr<arrow::Buffer> arrowBuffer;
        ARROW_ASSIGN_OR_RAISE(arrowBuffer, arrowStream->Finish());

        LOG4CXX_DEBUG(logger,
                      "S3SAVE>> ArrowChunkPopulator::populateChunkStatus bytesCount x2: "
                      << bytesCount * 2 << " arrowBuffer::size: " << arrowBuffer->size())

        // Copy data to Mem Chunk Builder
        builder.addData(reinterpret_cast<const char*>(arrowBuffer->data()),
                        arrowBuffer->size());

        return arrow::Status::OK();
    }

    template <typename SciDBType,
              typename ArrowBuilder,
              typename ValueFunc> inline
    arrow::Status populateCell(shared_ptr<ConstChunkIterator> citer,
                               ValueFunc valueGetter,
                               const size_t i,
                               size_t &bytesCount)
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
            bytesCount += _inputSizes[i];

            // Store coordinates in the buffer
            if (i == 0)
            {
                Coordinates const &coords = citer->getPosition();
                for (size_t j = 0; j < _nDims; ++j)
                {
                    _dimsValues[j].push_back(coords[j]);
                    bytesCount += 8;
                }
            }

            ++(*citer);
        }

        return static_cast<ArrowBuilder*>(
            _arrowBuilders[i].get())->AppendValues(values, is_valid);
    }
};
#endif

template <class ChunkPopulator>
class ConversionArray: public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t                                   _rowIndex;
    Address                                  _chunkAddress;
    ArrayCursor                              _inputCursor;
    MemChunkBuilder                          _chunkBuilder;
    weak_ptr<Query>                          _query;
    ChunkPopulator                           _populator;
    map<InstanceID, string>                  _instanceMap;
    map<InstanceID, string>::const_iterator  _mapIter;

public:
    ConversionArray(ArrayDesc const& schema,
                    shared_ptr<Array>& inputArray,
                    shared_ptr<Query>& query,
                    S3SaveSettings const& settings):
        super(schema),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(3,0)),
        _inputCursor(inputArray),
        _query(query),
        _populator(inputArray->getArrayDesc(), settings)
    {
    }

    size_t getCurrentRowIndex() const
    {
        return _rowIndex;
    }

    bool moveNext(size_t rowIndex)
    {
        if(_inputCursor.end())
        {
            return false;
        }
        _chunkBuilder.reset();
        _populator.populateChunk(_chunkBuilder, _inputCursor);
        ++_rowIndex;
        return true;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        _chunkAddress.coords[0] = _rowIndex  -1;
        _chunkAddress.coords[1] = _mapIter->first;
        ++_mapIter;
        if(_mapIter == _instanceMap.end())
        {
            _mapIter = _instanceMap.begin();
        }
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        MemChunk& ch = _chunkBuilder.getChunk();
        ch.initialize(this, &super::getArrayDesc(), _chunkAddress, CompressorType::NONE);
        return ch;
    }
};

#ifdef USE_ARROW
typedef ConversionArray <ArrowChunkPopulator>  ArrowConvertedArray;
#endif

class PhysicalS3Save : public PhysicalOperator
{
public:
    PhysicalS3Save(std::string const& logicalName,
                    std::string const& physicalName,
                    Parameters const& parameters,
                    ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    bool haveChunk(shared_ptr<Array>& input, ArrayDesc const& schema)
    {
        shared_ptr<ConstArrayIterator> iter = input->getConstIterator(schema.getAttributes(true).firstDataAttribute());
        return !(iter->end());
    }

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        S3SaveSettings settings (_parameters, _kwParameters, false, query);
        shared_ptr<Array>& input = inputArrays[0];
        ArrayDesc const& inputSchema = input->getArrayDesc();
        shared_ptr<Array> outArray;
#ifdef USE_ARROW
        if(settings.isArrowFormat())
        {
            outArray.reset(new ArrowConvertedArray(_schema, input, query, settings));
        }
#endif
        shared_ptr<Array> outArrayRedist;
        LOG4CXX_DEBUG(logger, "S3SAVE>> Starting SG")
        outArrayRedist = pullRedistribute(outArray,
                                          createDistribution(dtByCol),
                                          ArrayResPtr(),
                                          query,
                                          shared_from_this());
        bool const wasConverted = (outArrayRedist != outArray) ;
        if (wasConverted)
        {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(outArrayRedist.get());
            syncArray->sync();
        }
        return shared_ptr<Array>(new MemArray(_schema, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalS3Save, "s3save", "PhysicalS3Save");

} // end namespace scidb
