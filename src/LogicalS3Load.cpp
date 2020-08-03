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

#include <query/LogicalQueryPlan.h>
#include <query/Parser.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include "S3Common.h"
#include "S3LoadSettings.h"


namespace scidb
{

class LogicalS3Load : public  LogicalOperator
{
public:
    LogicalS3Load(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { KW_BUCKET_NAME,   RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { KW_BUCKET_PREFIX, RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { KW_FORMAT,        RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        S3LoadSettings settings(_parameters, _kwParameters, true, query);

        // Init AWS
        Aws::SDKOptions options;
        Aws::InitAPI(options);

        // Download Metadata
        Aws::String bucketName = Aws::String(settings.getBucketName().c_str());
        ostringstream out;
        out << settings.getBucketPrefix() << "/metadata";
        Aws::String objectName = Aws::String(out.str().c_str());

        Aws::S3::S3Client s3Client;
        Aws::S3::Model::GetObjectRequest objectRequest;

        objectRequest.SetBucket(bucketName);
        objectRequest.SetKey(objectName);

        auto outcome = s3Client.GetObject(objectRequest);
        S3_EXCEPTION_NOT_SUCCESS("Get");

        // Parse Metadata
        map<string, string> metadata;
        auto& metadata_stream = outcome.GetResultWithOwnership().GetBody();
        string line;
        while (getline(metadata_stream, line)) {
          istringstream line_stream(line);
          string key, value;
          if (!getline(line_stream, key, '\t'))
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                 SCIDB_LE_UNKNOWN_ERROR)
              << "Invalid metadata line '" << line << "'";
          if (!getline(line_stream, value))
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                 SCIDB_LE_UNKNOWN_ERROR)
              << "Invalid metadata line '" << line << "'";
          metadata[key] = value;
        }
        string schema = metadata["schema"];
        LOG4CXX_DEBUG(logger, "S3LOAD >> schema: " << schema);
        Aws::ShutdownAPI(options);

        // Build Fake Query and Extract Schema
        std::shared_ptr<Query> innerQuery = Query::createFakeQuery(
                         query->getPhysicalCoordinatorID(),
                         query->mapLogicalToPhysical(query->getInstanceID()),
                         query->getCoordinatorLiveness());
        out.str("");
        out << "input(" << schema << ", '/dev/null')";
        innerQuery->queryString = out.str();
        innerQuery->logicalPlan = std::make_shared<LogicalPlan>(
            parseStatement(innerQuery, true));
        return innerQuery->logicalPlan->inferTypes(innerQuery);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalS3Load, "s3load");

} // emd namespace scidb
