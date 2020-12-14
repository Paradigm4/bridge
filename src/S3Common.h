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

#ifndef S3_COMMON
#define S3_COMMON

#include "S3Driver.h"
#include "FSDriver.h"

// SciDB
#include <array/DimensionDesc.h>
#include <array/Dimensions.h>


#define S3BRIDGE_VERSION 1
#define INDEX_SPLIT_MIN 100
#define INDEX_SPLIT_DEFAULT 100000  // Number of Coordinates =
                                    // (Number-of-Chunks *
                                    // Number-of-Dimensions)
#define CACHE_SIZE_DEFAULT 268435456 // 256MB in Bytes

#define _STR(x) #x
#define STR(x) _STR(x)


namespace scidb {

class S3Metadata {
public:
    enum Format {
        ARROW  = 0
    };

    enum Compression {
        NONE  = 0,
        GZIP  = 1
    };

    static std::string compression2String(Compression compression) {
        switch (compression) {
        case Compression::NONE:
            return "none";
        case Compression::GZIP:
            return "gzip";
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "unsupported compression";
    }

    static Compression string2Compression(const std::string &compressionStr) {
        if (compressionStr == "none")
            return Compression::NONE;
        else if (compressionStr == "gzip")
            return Compression::GZIP;
        else
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "unsupported compression";
    }
};

static const std::string coord2ObjectName(const Coordinates &pos,
                                          const Dimensions &dims)
{
    std::ostringstream out;
    out << "c";
    for (size_t i = 0; i < dims.size(); ++i)
        out << "_" << (pos[i] -
                       dims[i].getStartMin()) / dims[i].getChunkInterval();
    return out.str();
}

static std::shared_ptr<Driver> makeDriver(const std::string url)
{
    if (url.rfind("file://", 0) == 0)
        return std::make_shared<FSDriver>(url);

    if (url.rfind("s3://", 0) == 0)
        return std::make_shared<S3Driver>(url);

    throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR)
        << "Invalid URL " << url;
}

}      // end namespace scidb
#endif //S3Common
