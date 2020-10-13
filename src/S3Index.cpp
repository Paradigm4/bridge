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

#include "S3Index.h"

#include <chrono>

#include <array/MemoryBuffer.h>
#include <query/Query.h>
#include <system/UserException.h>


namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.s3index"));

S3Index::S3Index(const Query& query, const ArrayDesc& desc):
    _desc(desc),
    _nDims(desc.getDimensions().size()),
    _nInst(query.getInstancesCount()),
    _instID(query.getInstanceID())
{
}

size_t S3Index::size() const {
    return _values.size();
}

void S3Index::insert(const Coordinates& pos) {
    _values.push_back(pos);
    // _values.insert(pos);
}

void S3Index::deserialize_insert(std::shared_ptr<SharedBuffer> buf) {
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

std::shared_ptr<SharedBuffer> S3Index::serialize() const {
    // Send One Byte if Index is Empty
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
    int j = 0;
    for (auto i = begin(); i != end(); ++i, ++j)
        std::copy(i->begin(), i->end(), mem + j * _nDims);

    return buf;
}

void S3Index::sort() {
    // TODO Remove
    auto start = std::chrono::high_resolution_clock::now();

    std::sort(_values.begin(), _values.end(), CoordinatesLess());

    // TODO Remove
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        stop - start);
    LOG4CXX_DEBUG(logger, "S3INDEX|" << _instID << "|Sort time: " << duration.count() << " microseconds");
}

const S3IndexCont::const_iterator S3Index::begin() const {
    return _values.begin();
}

const S3IndexCont::const_iterator S3Index::end() const {
    return _values.end();
}

const S3IndexCont::const_iterator S3Index::find(const Coordinates& pos) const {
    // TODO optimize index search
    return std::find(begin(), end(), pos);
}

}


namespace std {

std::ostream& operator<<(std::ostream& out, const scidb::Coordinates& pos) {
    out << "(";
    std::copy(pos.begin(),
              pos.end(),
              std::ostream_iterator<scidb::Coordinate>(out, ","));
    out << ")";
    return out;
}

Aws::IOStream& operator<<(Aws::IOStream& out, const scidb::Coordinates& pos) {
    std::copy(pos.begin(),
              pos.end(),
              std::ostream_iterator<scidb::Coordinate>(out, "\t"));
    return out;
}

std::ostream& operator<<(std::ostream& out, const scidb::S3Index& index) {
    out << "[";
    std::copy(index.begin(),
              index.end(),
              std::ostream_iterator<scidb::Coordinates>(out, ";"));
    out << "]";
    return out;
}

Aws::IOStream& operator<<(Aws::IOStream& out, const scidb::S3Index& index) {
    for (auto i = index.begin(); i != index.end(); ++i) {
        out << *i;
        out.seekp(-1, out.cur); // Remove last separator
        out << "\n";
    }
    return out;
}

Aws::IOStream& operator>>(Aws::IOStream& in, scidb::S3Index& index) {
    std::string line;
    scidb::Coordinates pos;
    pos.reserve(index._nDims);
    while (std::getline(in, line)) {
        std::istringstream stm(line);
        for (scidb::Coordinate coord; stm >> coord;)
            pos.push_back(coord);

        if (pos.size() != index._nDims)
            throw USER_EXCEPTION(scidb::SCIDB_SE_METADATA,
                                 scidb::SCIDB_LE_UNKNOWN_ERROR)
                << "Invalid index line '" << line
                << "', expected " << index._nDims << " values";

        // Keep Only Chunks for this Instance
        if (index._desc.getPrimaryInstanceId(pos, index._nInst) == index._instID)
            index.insert(pos);
        pos.clear();
    }
    return in;
}
}
