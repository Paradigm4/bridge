/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2020 Paradigm4 Inc.
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

#ifndef X_INDEX_H_
#define X_INDEX_H_

// SciDB
#include <array/Coordinate.h>
#include <array/Dimensions.h>
#include <query/InstanceID.h>


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace scidb {
    class ArrayDesc;
    class Query;
    class SharedBuffer;
}
// -- End of Forward Declarations


namespace scidb {
    class XIndex;
} // namespace scidb
namespace std {
    std::ostream& operator<<(std::ostream&, const scidb::Coordinates&);
    std::ostream& operator<<(std::ostream&, const scidb::XIndex&);
} // namespace std

namespace scidb {
// --
// -- - XIndex - --
// --

// Type of XIndex Container
typedef std::vector<Coordinates> XIndexCont;

class XIndex {

  public:
    XIndex(const ArrayDesc&);

    size_t size() const;
    void insert(const Coordinates&);
    void sort();

    // Serialize & De-serialize for inter-instance comms
    std::shared_ptr<SharedBuffer> serialize() const;
    void deserialize_insert(std::shared_ptr<SharedBuffer>);

    const XIndexCont::const_iterator begin() const;
    const XIndexCont::const_iterator end() const;

    const XIndexCont::const_iterator find(const Coordinates&) const;

  private:
    const ArrayDesc& _desc;
    const Dimensions& _dims;
    const size_t _nDims;

    XIndexCont _values;
};
} // namespace scidb

#endif  // XIndex
