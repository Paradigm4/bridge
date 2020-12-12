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

#include "S3Driver.h"

#include <log4cxx/logger.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>


#define RETRY_COUNT 5
#define RETRY_SLEEP 1000        // milliseconds

#define S3_EXCEPTION_NOT_SUCCESS(operation)                             \
    {                                                                   \
        if (!outcome.IsSuccess()) {                                     \
            std::ostringstream exceptionOutput;                         \
            exceptionOutput                                             \
            << (operation) << " operation on s3://"                     \
            << _bucket << "/" << key << " failed. ";                    \
            auto error = outcome.GetError();                            \
            exceptionOutput << error.GetMessage();                      \
            if (error.GetResponseCode() ==                              \
                Aws::Http::HttpResponseCode::FORBIDDEN)                 \
                exceptionOutput                                         \
                    << "See https://aws.amazon.com/premiumsupport/"     \
                    << "knowledge-center/s3-troubleshoot-403/";         \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_NETWORK,                                       \
                SCIDB_LE_UNKNOWN_ERROR) << exceptionOutput.str();       \
        }                                                               \
    }


namespace scidb {
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.s3driver"));

    //
    // S3Driver
    //
    S3Driver::S3Driver(const std::string &url):
        _url(url)
    {
        const size_t prefix_len = 5; // "s3://"
        size_t pos = _url.find("/", prefix_len);
        if (_url.rfind("s3://", 0) != 0 || pos == std::string::npos)
            throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                 SCIDB_LE_UNKNOWN_ERROR)
                << "Invalid S3 URL " << _url;
        _bucket = _url.substr(prefix_len, pos - prefix_len).c_str();
        _prefix = _url.substr(pos + 1);

        Aws::InitAPI(_sdkOptions);
        _client = std::make_unique<Aws::S3::S3Client>();
    }

    S3Driver::~S3Driver()
    {
        Aws::ShutdownAPI(_sdkOptions);
    }

    size_t S3Driver::_readArrow(const std::string &suffix,
                                std::shared_ptr<arrow::Buffer> &buffer,
                                bool reuse) const
    {
        Aws::String key((_prefix + "/" + suffix).c_str());

        auto&& result = _getRequest(key);

        auto length = result.GetContentLength();
        _setBuffer(suffix, buffer, reuse, length);

        auto& body = result.GetBody();
        body.read(reinterpret_cast<char*>(buffer->mutable_data()), length);

        return length;
    }

    void S3Driver::writeArrow(const std::string &suffix,
                              std::shared_ptr<const arrow::Buffer> buffer) const
    {
        Aws::String key((_prefix + "/" + suffix).c_str());

        std::shared_ptr<Aws::IOStream> data =
            Aws::MakeShared<Aws::StringStream>("");
        data->write(reinterpret_cast<const char*>(buffer->data()),
                    buffer->size());

        _putRequest(key, data);
    }

    void S3Driver::readMetadata(std::map<std::string,
                                         std::string> &metadata) const
    {
        Aws::String key((_prefix + "/metadata").c_str());

        auto&& result = _getRequest(key);
        auto& body = result.GetBody();
        std::string line;
        while (std::getline(body, line)) {
            std::istringstream stream(line);
            std::string key, value;
            if (!std::getline(stream, key, '\t')
                || !std::getline(stream, value))
                throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                     SCIDB_LE_UNKNOWN_ERROR)
                    << "Invalid metadata line '" << line << "'";
            metadata[key] = value;
        }
    }

    void S3Driver::writeMetadata(const std::map<std::string,
                                                std::string> &metadata) const
    {
        Aws::String key((_prefix + "/metadata").c_str());

        std::shared_ptr<Aws::IOStream> data =
            Aws::MakeShared<Aws::StringStream>("");
        for (auto i = metadata.begin(); i != metadata.end(); ++i)
            *data << i->first << "\t" << i->second << "\n";

        _putRequest(key, data);
    }

    size_t S3Driver::count(const std::string& suffix) const
    {
        Aws::String key((_prefix + "/" + suffix).c_str());
        Aws::S3::Model::ListObjectsRequest request;
        request.WithBucket(_bucket);
        request.WithPrefix(key);

        LOG4CXX_DEBUG(logger, "S3DRIVER|list:" << key);
        auto outcome = _client->ListObjects(request);

        // -- - Retry - --
        int retry = 1;
        while (!outcome.IsSuccess() && retry < RETRY_COUNT) {
            LOG4CXX_WARN(logger,
                         "S3DRIVER|List s3://" << _bucket << "/"
                         << key << " attempt #" << retry << " failed");
            retry++;

            std::this_thread::sleep_for(
                std::chrono::milliseconds(RETRY_SLEEP));

            outcome = _client->ListObjects(request);
        }

        S3_EXCEPTION_NOT_SUCCESS("List");
        return outcome.GetResult().GetContents().size();
    }

    const std::string& S3Driver::getURL() const
    {
        return _url;
    }

    Aws::S3::Model::GetObjectResult S3Driver::_getRequest(const Aws::String &key) const
    {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(_bucket);
        request.SetKey(key);

        LOG4CXX_DEBUG(logger, "S3DRIVER|get:" << key);
        auto outcome = _client->GetObject(request);

        // -- - Retry - --
        int retry = 1;
        while (!outcome.IsSuccess() && retry < RETRY_COUNT) {
            LOG4CXX_WARN(logger,
                         "S3DRIVER|Get s3://" << _bucket << "/"
                         << key << " attempt #" << retry << " failed");
            retry++;

            std::this_thread::sleep_for(
                std::chrono::milliseconds(RETRY_SLEEP));

            outcome = _client->GetObject(request);
        }

        S3_EXCEPTION_NOT_SUCCESS("Get");
        return outcome.GetResultWithOwnership();
    }

    void S3Driver::_putRequest(const Aws::String &key,
                              std::shared_ptr<Aws::IOStream> data) const
    {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(_bucket);
        request.SetKey(key);
        request.SetBody(data);

        LOG4CXX_DEBUG(logger, "S3DRIVER|upload:" << key);
        auto outcome = _client->PutObject(request);

        // -- - Retry - --
        int retry = 1;
        while (!outcome.IsSuccess() && retry < RETRY_COUNT) {
            LOG4CXX_WARN(logger,
                         "S3DRIVER|Put s3://" << _bucket << "/"
                         << key << " attempt #" << retry << " failed");
            retry++;

            std::this_thread::sleep_for(
                std::chrono::milliseconds(RETRY_SLEEP));

            outcome = _client->PutObject(request);
        }

        S3_EXCEPTION_NOT_SUCCESS("Put");
    }
}
