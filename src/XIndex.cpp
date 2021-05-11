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

#include "XIndex.h"

// SciDB
#include <array/MemoryBuffer.h>
#include <network/Network.h>
#include <query/Query.h>
#include <query/TypeSystem.h>
#include <system/UserException.h>

// Arrow
#include <arrow/builder.h>
#include <arrow/io/compressed.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>


#define ASSIGN_OR_THROW(lhs, rexpr)                     \
    {                                                   \
        auto status_name = (rexpr);                     \
        THROW_NOT_OK(status_name.status());             \
        lhs = std::move(status_name).ValueOrDie();      \
    }


namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.xindex"));

//
// Arrow Reader
//
ArrowReader::ArrowReader(
    const Attributes &attributes,
    const Dimensions &dimensions,
    const Metadata::Compression compression,
    std::shared_ptr<const Driver> driver):
    _schema(scidb2ArrowSchema(attributes, dimensions)),
    _compression(compression),
    _driver(driver)
{
    THROW_NOT_OK(arrow::AllocateResizableBuffer(0, &_arrowResizableBuffer));

    if (_compression == Metadata::Compression::GZIP)
        _arrowCodec = *arrow::util::Codec::Create(
            arrow::Compression::type::GZIP);
}

size_t ArrowReader::readObject(
    const std::string &name,
    bool reuse,
    std::shared_ptr<arrow::RecordBatch> &arrowBatch)
{
    // Download Chunk
    size_t arrowSize;
    if (reuse) {
        // Reuse an Arrow ResizableBuffer
        arrowSize = _driver->readArrow(name, _arrowResizableBuffer);

        _arrowBufferReader = std::make_shared<arrow::io::BufferReader>(
            _arrowResizableBuffer);
    }
    else {
        // Get a new Arrow Buffer
        std::shared_ptr<arrow::Buffer> arrowBuffer;
        arrowSize = _driver->readArrow(name, arrowBuffer);

        _arrowBufferReader = std::make_shared<arrow::io::BufferReader>(
            arrowBuffer);
    }

    // Setup Arrow Compression, If Enabled
    if (_compression != Metadata::Compression::NONE) {
        ASSIGN_OR_THROW(_arrowCompressedStream,
                        arrow::io::CompressedInputStream::Make(
                            _arrowCodec.get(), _arrowBufferReader));
        // Read Record Batch using Stream Reader
        THROW_NOT_OK(arrow::ipc::RecordBatchStreamReader::Open(
                         _arrowCompressedStream, &_arrowBatchReader));
    }
    else {
        THROW_NOT_OK(arrow::ipc::RecordBatchStreamReader::Open(
                         _arrowBufferReader, &_arrowBatchReader));
    }

    THROW_NOT_OK(_arrowBatchReader->ReadNext(&arrowBatch));

    // No More Record Batches are Expected
    std::shared_ptr<arrow::RecordBatch> arrowBatchNext;
    THROW_NOT_OK(_arrowBatchReader->ReadNext(&arrowBatchNext));
    if (arrowBatchNext != NULL) {
        std::ostringstream out;
        out << "More than one Arrow Record Batch found in "
            << _driver->getURL() << "/" << name;
        throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                               SCIDB_LE_UNKNOWN_ERROR) << out.str();
    }

    // Check Record Batch Schema
    auto readerSchema = _arrowBatchReader->schema();
    if (!_schema->Equals(readerSchema)) {
        std::ostringstream out;
        out << "Schema (" << readerSchema->ToString() << ") from "
            << _driver->getURL() << "/" << name
            << " does not match the expected schema ("
            << _schema->ToString() << ")";
        throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                               SCIDB_LE_UNKNOWN_ERROR) << out.str();
    }

    return arrowSize;
}

std::shared_ptr<arrow::Schema> ArrowReader::scidb2ArrowSchema(
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



//
// XIndex
//
XIndex::XIndex(const ArrayDesc &desc):
    _desc(desc),
    _dims(desc.getDimensions()),
    _nDims(_dims.size())
{}

size_t XIndex::size() const {
    return _values.size();
}

void XIndex::insert(const Coordinates &pos) {
    _values.push_back(pos);
    // _values.insert(pos);
}

void XIndex::insert(const XIndex &other) {
    std::copy(other.begin(), other.end(), std::back_inserter(_values));
}

void XIndex::sort() {
    std::sort(_values.begin(), _values.end(), CoordinatesLess());
}

void XIndex::load(std::shared_ptr<const Driver> driver,
                  std::shared_ptr<Query> query) {
    const InstanceID instID = query->getInstanceID();

    // -- - Get Count of Chunk Index Files - --
    size_t nIndex = driver->count("index/");
    LOG4CXX_DEBUG(logger, "XINDEX|" << instID << "|load nIndex:" << nIndex);

    // -- - Read Part of Chunk Index Files - --
    // Divide index files among instnaces

    const size_t nInst = query->getInstancesCount();
    const Dimensions dims = _desc.getDimensions();
    const size_t nDims = dims.size();
    scidb::Coordinates pos(nDims);

    // One coordBuf for each instance
    std::unique_ptr<std::vector<Coordinate>[]> coordBuf= std::make_unique<std::vector<Coordinate>[]>(nInst);
    ArrowReader arrowReader(Attributes(),
                            _desc.getDimensions(),
                            Metadata::Compression::GZIP,
                            driver);
    std::shared_ptr<arrow::RecordBatch> arrowBatch;

    for (size_t iIndex = instID; iIndex < nIndex; iIndex += nInst) {

        // Download One Chunk Index
        std::ostringstream out;
        out << "index/" << iIndex;
        std::string objectName(out.str());
        arrowReader.readObject(objectName, true, arrowBatch);
        // LOG4CXX_DEBUG(logger, "XINDEX|" << instID << "|load read:" << out.str());

        if (arrowBatch->num_columns() != static_cast<int>(nDims)) {
            out.str("");
            out << objectName
                << " Invalid number of columns";
            throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_METADATA,
                                   scidb::SCIDB_LE_UNKNOWN_ERROR)
                << out.str();
        }
        std::vector<const int64_t*> columns(nDims);
        for (size_t i = 0; i < nDims; i++)
            columns[i] = std::static_pointer_cast<arrow::Int64Array>(
                arrowBatch->column(i))->raw_values();
        size_t columnLen = arrowBatch->column(0)->length();

        for (size_t j = 0; j < columnLen; j++) {
            for (size_t i = 0; i < nDims; i++)
                pos[i] = columns[i][j];

            InstanceID primaryID = _desc.getPrimaryInstanceId(pos, nInst);
            // LOG4CXX_DEBUG(logger, "XINDEX|" << instID << "|load pos:" << pos << " primary:" << primaryID);
            if (primaryID == instID)
                insert(pos);
            else
                // Serialize in the right coordBuf
                std::copy(pos.begin(), pos.end(), std::back_inserter(coordBuf[primaryID]));
        }
    }

    // Distribute Index Splits to Each Instance
    for (InstanceID remoteID = 0; remoteID < nInst; ++remoteID)
        if (remoteID != instID) {
            // Prepare Shared Buffer
            std::shared_ptr<SharedBuffer> buf;
            if (coordBuf[remoteID].size() == 0)
                buf = std::shared_ptr<SharedBuffer>(new MemoryBuffer(NULL, 1));
            else
                buf = std::shared_ptr<SharedBuffer>(
                    // Have to copy it
                    new MemoryBuffer(
                        coordBuf[remoteID].data(),
                        coordBuf[remoteID].size() * sizeof(Coordinate)));

            // Send Shared Buffer
            BufSend(remoteID, buf, query);
            // LOG4CXX_DEBUG(logger, "XINDEX|" << instID << "|load send to:" << remoteID);
        }

    // Read Index Splits from Each Instance
    for (InstanceID remoteID = 0; remoteID < nInst; ++remoteID)
        if (remoteID != instID) {
            auto buf = BufReceive(remoteID, query);
            deserialize_insert(buf);
            // LOG4CXX_DEBUG(logger, "XINDEX|" << instID << "|load receive from:" << remoteID);
        }

    sort();

    LOG4CXX_DEBUG(logger, "XINDEX|" << instID << "|load size:" << size());
}

void XIndex::deserialize_insert(std::shared_ptr<SharedBuffer> buf) {
    // A One Byte Buffer is an "Empty" Buffer
    if (buf->getSize() == 1)
        return;

    const Coordinate* mem = static_cast<const Coordinate*>(buf->getConstData());

    // De-serialize Coordinates
    for (size_t i = 0; i < buf->getSize() / sizeof(Coordinate) / _nDims; ++i) {
        Coordinates pos;
        std::copy(
            mem + i * _nDims, mem + (i + 1) * _nDims, std::back_inserter(pos));
        insert(pos);
    }
}

std::shared_ptr<SharedBuffer> XIndex::serialize() const {
    // Send one byte if the size of the index to be sent is 0; either
    // the index is empty or there will be nothing sent after the
    // filter is applied
    if (size() == 0)
        return std::shared_ptr<SharedBuffer>(new MemoryBuffer(NULL, 1));

    // Serialize Coordinates
    // ---
    // MemoryBuffer will alocate the output buffer, memcopy is skipped
    // because of the NULL. We get a pointer to the buffer and write
    // the data in it.
    std::shared_ptr<SharedBuffer> buf(
        new MemoryBuffer(NULL, size() * _nDims * sizeof(Coordinate)));
    Coordinate *mem = static_cast<Coordinate*>(buf->getWriteData());
    int i = 0;
    for (auto posPtr = begin(); posPtr != end(); ++posPtr, ++i)
        std::copy(posPtr->begin(), posPtr->end(), mem + i * _nDims);

    return buf;
}

const XIndexStore::const_iterator XIndex::begin() const {
    return _values.begin();
}

const XIndexStore::const_iterator XIndex::end() const {
    return _values.end();
}

const XIndexStore::const_iterator XIndex::find(const Coordinates& pos) const {
    // return std::find(begin(), end(), pos);
    auto res = std::lower_bound(begin(), end(), pos);
    if (res == end() || *res != pos)
        return end();
    return res;
}

} // namespace scidb


namespace std {

std::ostream& operator<<(std::ostream& out, const scidb::Coordinates& pos) {
    out << "(";
    std::copy(pos.begin(),
              pos.end(),
              std::ostream_iterator<scidb::Coordinate>(out, ","));
    out << ")";
    return out;
}

std::ostream& operator<<(std::ostream& out, const scidb::XIndex& index) {
    out << "[";
    std::copy(index.begin(),
              index.end(),
              std::ostream_iterator<scidb::Coordinates>(out, ";"));
    out << "]";
    return out;
}
} // namespace std
