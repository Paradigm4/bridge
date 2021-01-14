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

#include "S3Driver.h"
#include "FSDriver.h"

namespace scidb {

std::string Metadata::compression2String(
        const Metadata::Compression compression)
{
    switch (compression) {
    case Metadata::Compression::NONE:
        return "none";
    case Metadata::Compression::GZIP:
        return "gzip";
    }
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
        << "unsupported compression";
}

Metadata::Compression Metadata::string2Compression(
    const std::string &compressionStr)
{
    if (compressionStr == "none")
        return Metadata::Compression::NONE;
    else if (compressionStr == "gzip")
        return Metadata::Compression::GZIP;
    else
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << "unsupported compression";
}

std::string Metadata::coord2ObjectName(const Coordinates &pos,
                                       const Dimensions &dims)
{
    std::ostringstream out;
    out << "c";
    for (size_t i = 0; i < dims.size(); ++i)
        out << "_" << (pos[i] -
                       dims[i].getStartMin()) / dims[i].getChunkInterval();
    return out.str();
}

std::shared_ptr<Driver> Driver::makeDriver(const std::string url,
                                           const Driver::Mode mode)
{
    if (url.rfind("file://", 0) == 0)
        return std::make_shared<FSDriver>(url, mode);

    if (url.rfind("s3://", 0) == 0)
        return std::make_shared<S3Driver>(url);

    throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
        << "Invalid URL " << url;
}

} // namespace scidb