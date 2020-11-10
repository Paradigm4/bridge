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

#include <map>

#include <system/ErrorCodes.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>


#define S3BRIDGE_VERSION 1
#define CHUNK_MAX_SIZE 2147483648

#define S3_EXCEPTION_NOT_SUCCESS(operation)                                      \
  {                                                                              \
      if (!outcome.IsSuccess()) {                                                \
          std::ostringstream exceptionOutput;                                         \
          exceptionOutput << (operation) << " operation on s3://"                \
                           << bucketName << "/" << objectName << " failed. ";    \
          auto error = outcome.GetError();                                       \
          exceptionOutput << error.GetMessage();                                 \
          if (error.GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN) \
              exceptionOutput << "See https://aws.amazon.com/premiumsupport/"    \
                               << "knowledge-center/s3-troubleshoot-403/";       \
          throw USER_EXCEPTION(SCIDB_SE_NETWORK,                                 \
                               SCIDB_LE_UNKNOWN_ERROR) << exceptionOutput.str(); \
      }                                                                          \
  }


#define S3_EXCEPTION_OBJECT_NAME                                        \
  {                                                                     \
      std::ostringstream exceptionOutput;                               \
      exceptionOutput << "Invalid object name '" << objectName << "'";  \
      throw USER_EXCEPTION(SCIDB_SE_METADATA,                           \
                                     SCIDB_LE_UNKNOWN_ERROR)            \
                                     << exceptionOutput.str();          \
  }


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

    static void getMetadata(Aws::S3::S3Client const &s3Client,
                            Aws::String const &bucketName,
                            Aws::String const &objectName,
                            std::map<std::string, std::string>& metadata) {
        // Download Metadata
        Aws::S3::Model::GetObjectRequest objectRequest;

        objectRequest.SetBucket(bucketName);
        objectRequest.SetKey(objectName);

        auto outcome = s3Client.GetObject(objectRequest);
        S3_EXCEPTION_NOT_SUCCESS("Get");

        // Parse Metadata
        auto& metadataStream = outcome.GetResultWithOwnership().GetBody();
        std::string line;
        while (std::getline(metadataStream, line)) {
            std::istringstream lineStream(line);
            std::string key, value;
            if (!std::getline(lineStream, key, '\t')
                || !std::getline(lineStream, value))
                throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                     SCIDB_LE_UNKNOWN_ERROR)
                    << "Invalid metadata line '" << line << "'";
            metadata[key] = value;
        }
    }
};

static std::string coord2ObjectName(std::string const &bucketPrefix,
                                    Coordinates const &pos,
                                    Dimensions const &dims) {
    std::ostringstream out;
    out << bucketPrefix << "/c";
    for (size_t i = 0; i < dims.size(); ++i)
        out << "_" << (pos[i] -
                       dims[i].getStartMin()) / dims[i].getChunkInterval();
    return out.str();
}

}

#endif //S3Common
