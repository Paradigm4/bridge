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

// SciDB
#include <query/LogicalQueryPlan.h>
#include <query/Parser.h>
#include <query/Query.h>
#include <util/OnScopeExit.h>


namespace scidb {

const ArrayDesc& Metadata::getSchema(std::shared_ptr<Query> query) {
    if (_hasSchema)
        return _schema;

    // Build Fake Query and Extract Schema
    std::shared_ptr<Query> innerQuery = Query::createFakeQuery(
        query->getPhysicalCoordinatorID(),
        query->mapLogicalToPhysical(query->getInstanceID()),
        query->getCoordinatorLiveness());

    // Create a scope where the query's arena is responsible for
    // memory allocation.
    {
        arena::ScopedArenaTLS arenaTLS(innerQuery->getArena());

        OnScopeExit fqd([&innerQuery] () {
                            Query::destroyFakeQuery(innerQuery.get()); });

        std::ostringstream out;
        auto schemaPair = find("schema");
        if (schemaPair == end())
            throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_METADATA,
                                   scidb::SCIDB_LE_UNKNOWN_ERROR)
                << "Schema missing from metadata";
        out << "input(" << schemaPair->second << ", '/dev/null')";
        innerQuery->queryString = out.str();
        innerQuery->logicalPlan = std::make_shared<LogicalPlan>(
            parseStatement(innerQuery, true));
    }

    // Extract Schema and Set Distribution
    _schema = innerQuery->logicalPlan->inferTypes(innerQuery);
    _schema.setName("");
    _hasSchema = true;

    return _schema;
}

void Metadata::setSchema(const ArrayDesc &schema) {
    _schema = schema;

    std::ostringstream out;
    printSchema(out, schema);
    _metadata["schema"] = out.str();
}

Metadata::Compression Metadata::getCompression() const {
    auto compressionPair = _metadata.find("compression");
    if (compressionPair == _metadata.end())
        throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_METADATA,
                               scidb::SCIDB_LE_UNKNOWN_ERROR)
            << "Compression missing from metadata";

    auto compression = compressionPair->second;
    if (compression == "none")
        return Metadata::Compression::NONE;
    else if (compression == "gzip")
        return Metadata::Compression::GZIP;
    else {
        std::ostringstream error;
        error << "Unsupported compression '" << compression << "'";
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << error.str();
    }
}

void Metadata::setCompression(Metadata::Compression compression) {
    std::string compressionStr;
    switch (compression) {
    case Metadata::Compression::NONE: {
        compressionStr = "none";
        break;
    }
    case Metadata::Compression::GZIP: {
        compressionStr = "gzip";
        break;
    }
    default: {
        std::ostringstream error;
        error << "Unsupported compression '" << compression << "'";
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << error.str();
    }
    }
    _metadata["compression"] = compressionStr;
}

void Metadata::validate() const {
    for (std::string key : {
            "attribute",
            "compression",
            "format",
            "index_split",
            "namespace",
            "schema",
            "version"})
        if (_metadata.find(key) == _metadata.end()) {
            std::ostringstream error;
            error << "Key '" << key << "' missing from metadata";
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
                << error.str();
        }

    // Throws Exception If Not Supported
    getCompression();
}

std::string Metadata::coord2ObjectName(const Coordinates &pos,
                                       const Dimensions &dims) {
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
        return std::make_shared<S3Driver>(url, mode);

    throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
        << "Invalid URL " << url;
}

} // namespace scidb
