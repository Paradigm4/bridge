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

#ifndef S3_DRIVER_H_
#define S3_DRIVER_H_

#include <istream>

#include <aws/core/Aws.h>
#include <aws/s3/model/GetObjectResult.h>


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace arrow {
    class Buffer;               // #include <arrow/buffer.h>
}
namespace Aws {
    namespace S3 {
        class S3Client;         // #include <aws/s3/S3Client.h>
        namespace Model {
            class PutObjectRequest; // #include <aws/s3/model/PutObjectRequest.h>
        }
    }
}
// -- End of Forward Declarations


namespace scidb
{
class S3DriverChunk {
public:
    S3DriverChunk(Aws::S3::Model::GetObjectResult&&);

    size_t size() const;

    void read(std::shared_ptr<arrow::Buffer>,
              const size_t length);

private:
    Aws::S3::Model::GetObjectResult _result;
};

class S3Driver {
public:
    S3Driver(const std::string &bucket, const std::string &prefix);
    ~S3Driver();

    size_t size(const std::string&) const;

    S3DriverChunk readArrow(const std::string&) const;
    void writeArrow(const std::string&,
                    std::shared_ptr<const arrow::Buffer>) const;

    void readMetadata(std::map<std::string, std::string>&) const;
    void writeMetadata(const std::map<std::string,
                                      std::string>&) const;

    // Count number of objects with specified prefix
    size_t count(const std::string&) const;

    // Return print-friendly path used by driver
    const std::string& path() const;

private:
    const Aws::String _bucket;
    const std::string _prefix, _path;
    const Aws::SDKOptions _sdkOptions;
    std::shared_ptr<Aws::S3::S3Client> _client;

    void putRequest(Aws::S3::Model::PutObjectRequest&) const;
};

}

#endif
