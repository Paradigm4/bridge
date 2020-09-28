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

#ifndef S3_INDEX_H_
#define S3_INDEX_H_

#include <array/Coordinate.h>
#include <query/InstanceID.h>

#include <aws/core/utils/memory/stl/AWSStreamFwd.h>


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace scidb {
    class ArrayDesc;
    class Query;
    class SharedBuffer;
}
// -- End of Forward Declarations


namespace scidb {
    class S3Index;
}
namespace std {
    std::ostream& operator<<(std::ostream&, const scidb::Coordinates&);
    std::ostream& operator<<(std::ostream&, const scidb::S3Index&);

    // Serialize & De-serialize to S3
    Aws::IOStream& operator<<(Aws::IOStream&, const scidb::Coordinates&);
    Aws::IOStream& operator<<(Aws::IOStream&, const scidb::S3Index&);
    Aws::IOStream& operator>>(Aws::IOStream&, scidb::S3Index&);
}

namespace scidb {
// --
// -- - S3Index - --
// --
class S3Index {
    friend Aws::IOStream& std::operator>>(Aws::IOStream&, scidb::S3Index&);

  public:
    S3Index(const ArrayDesc&, const Query&);

    size_t size() const;
    void push_back(const Coordinates&);
    void sort();

    // Serialize & De-serialize for inter-instance comms
    std::shared_ptr<SharedBuffer> serialize() const;
    void deserialize_push_back(std::shared_ptr<SharedBuffer>);

    const std::vector<Coordinates>::const_iterator begin() const;
    const std::vector<Coordinates>::const_iterator end() const;

    const std::vector<Coordinates>::const_iterator find(const Coordinates&) const;

  private:
    const ArrayDesc& _desc;
    const size_t _nDims;
    const size_t _nInst;
    const InstanceID _instID;

    std::vector<Coordinates> _values;
};
}

#endif
