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

#include <array/TileIteratorAdaptors.h>
#include <query/PhysicalOperator.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "S3LoadSettings.h"


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.s3load"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
    if (!cond)
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
}

class PhysicalS3Load : public PhysicalOperator
{
public:
    PhysicalS3Load(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        LOG4CXX_DEBUG(logger, "S3LOAD >> execute");

        S3LoadSettings settings(_parameters, _kwParameters, false, query);
        shared_ptr<Array> result(new MemArray(_schema, query));

        // Init AWS
        Aws::SDKOptions options;
        Aws::InitAPI(options);


        Aws::ShutdownAPI(options);

        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalS3Load, "s3load", "PhysicalS3Load");

} // end namespace scidb
