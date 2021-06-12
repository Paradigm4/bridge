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

#ifndef S3_DRIVER_H_
#define S3_DRIVER_H_

#include "Driver.h"

#include <mutex>

#include <aws/core/Aws.h>
#include <aws/s3/model/GetObjectResult.h>


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace Aws {
    namespace S3 {
        class S3Client;         // #include <aws/s3/S3Client.h>
    }
}
// -- End of Forward Declarations


namespace scidb {

class S3Init {
public:
    S3Init();
    S3Init(const S3Init&) = delete;
    S3Init& operator=(const S3Init&) = delete;

private:
    static size_t s_count;

    // const
    Aws::SDKOptions _awsOptions;
    std::mutex _lock;
};


class S3Driver: public Driver {
public:
    S3Driver(const std::string& url,
             const Driver::Mode,
             const std::string& s3_sse);

    void init(const Query&);

    void writeArrow(const std::string&,
                    std::shared_ptr<const arrow::Buffer>) const;

    void writeMetadata(std::shared_ptr<const Metadata>) const;

    // Count number of objects with specified prefix
    size_t count(const std::string&) const;

    // Return print-friendly path used by driver
    const std::string& getURL() const;

protected:
    void _readMetadataFile(std::shared_ptr<Metadata>) const;

private:
    const S3Init _awsInit;

    Aws::String _bucket;
    std::string _prefix;
    std::shared_ptr<Aws::S3::S3Client> _client;
    Aws::S3::Model::ServerSideEncryption _s3_sse; // Server-Side Encryption

    size_t _readArrow(const std::string&, std::shared_ptr<arrow::Buffer>&, bool) const;

    Aws::S3::Model::GetObjectResult _getRequest(const Aws::String&) const;
    void _putRequest(const Aws::String&,
                     const std::shared_ptr<Aws::IOStream>) const;

    template <typename Outcome, typename Request, typename RequestFunc>
    Outcome _retryLoop(const std::string &name,
                       const Aws::String &key,
                       const Request &request,
                       RequestFunc requestFunc,
                       bool throwIfFails=true) const;

};

} // namespace scidb

#endif  // S3Driver
