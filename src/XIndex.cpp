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

#include <chrono>

#include <array/MemoryBuffer.h>
#include <query/Query.h>
#include <system/UserException.h>


namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.xindex"));

XIndex::XIndex(const ArrayDesc& desc):
    _desc(desc),
    _dims(desc.getDimensions()),
    _nDims(_dims.size())
{}

size_t XIndex::size() const {
    return _values.size();
}

void XIndex::insert(const Coordinates& pos) {
    _values.push_back(pos);
    // _values.insert(pos);
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

void XIndex::sort() {
    std::sort(_values.begin(), _values.end(), CoordinatesLess());
}

const XIndexCont::const_iterator XIndex::begin() const {
    return _values.begin();
}

const XIndexCont::const_iterator XIndex::end() const {
    return _values.end();
}

const XIndexCont::const_iterator XIndex::find(const Coordinates& pos) const {
    // return std::find(begin(), end(), pos);
    auto res = std::lower_bound(begin(), end(), pos);
    if (*res != pos)
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
