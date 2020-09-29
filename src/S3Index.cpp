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

#include <array/MemoryBuffer.h>
#include <query/Query.h>
#include <system/UserException.h>


namespace scidb {

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

void S3Index::push_back(const Coordinates& pos) {
    _values.push_back(pos);
}

void S3Index::deserialize_push_back(std::shared_ptr<SharedBuffer> buf) {
    Coordinate* mem = static_cast<Coordinate*>(buf->getWriteData());

    // De-serialize Coordinates
    for (size_t i = 0; i < buf->getSize() / sizeof(Coordinate) / _nDims; ++i) {
        Coordinates pos;
        std::copy(
            mem + i * _nDims, mem + (i + 1) * _nDims, std::back_inserter(pos));
        push_back(pos);
    }
}

void S3Index::sort() {
    std::sort(_values.begin(), _values.end(), CoordinatesLess());
}

std::shared_ptr<SharedBuffer> S3Index::serialize() const {
    // Serizalize Coordinates
    Coordinate mem[_nDims * size()];
    int j = 0;
    for (auto i = begin(); i != end(); ++i, ++j)
        std::copy(i->begin(), i->end(), mem + j * _nDims);

    return std::shared_ptr<SharedBuffer>(new MemoryBuffer(mem, sizeof(mem)));
}

const std::vector<Coordinates>::const_iterator S3Index::begin() const {
    return _values.begin();
}

const std::vector<Coordinates>::const_iterator S3Index::end() const {
    return _values.end();
}

const std::vector<Coordinates>::const_iterator S3Index::find(const Coordinates& pos) const {
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
    while (std::getline(in, line)) {
        std::istringstream stm(line);
        scidb::Coordinates pos;
        for (scidb::Coordinate coord; stm >> coord;)
            pos.push_back(coord);

        if (pos.size() != index._nDims)
            throw USER_EXCEPTION(scidb::SCIDB_SE_METADATA,
                                 scidb::SCIDB_LE_UNKNOWN_ERROR)
                << "Invalid index line '" << line
                << "', expected " << index._nDims << " values";

        // Keep Only Chunks for this Instance
        if (index._desc.getPrimaryInstanceId(pos, index._nInst) == index._instID)
            index.push_back(pos);
    }
    return in;
}
}
