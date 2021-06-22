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

#include <boost/algorithm/string.hpp>

// AWS
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

// SciDB
#include <system/Config.h>

#define RETRY_COUNT 5
#define RETRY_SLEEP 1000        // milliseconds


namespace scidb {
    //
    // ScopedMutex
    //
    class ScopedMutex {
    public:
        ScopedMutex(std::mutex &lock):_lock(lock) { _lock.lock(); }
        ~ScopedMutex() { _lock.unlock(); }
    private:
        std::mutex &_lock;
    };

    //
    // S3Init
    //
    S3Init::S3Init()
    {
        {
             ScopedMutex lock(_lock); // LOCK

             if (s_count == 0) {
                 Aws::InitAPI(_awsOptions);

                 // Get and parse io-paths-list from the configuration.
                 Config& config = *Config::getInstance();
                 std::vector<std::string> dirs;
                 boost::split(
                     dirs,
                     config.getOption<std::string>(CONFIG_IO_PATHS_LIST),
                     boost::is_any_of(":"));

                 for (std::string& d : dirs) {
                     // Only interested in values that start with
                     // "s3/"
                     if (d.rfind("s3/", 0) != 0)
                         continue;

                     // Construct a valid S3 URL
                     s_paths_list.push_back(d.insert(2, ":/"));
                 }

                 std::stringstream out;
                 std::copy(s_paths_list.begin(),
                           s_paths_list.end(),
                           std::ostream_iterator<std::string>(out, ","));
                 LOG4CXX_DEBUG(logger, "S3DRIVER|io-paths-list:" << out.str());
             }
             s_count++;
        }
    }

    size_t S3Init::s_count = 0;
    std::vector<std::string> S3Init::s_paths_list;

    //
    // S3Driver
    //
    S3Driver::S3Driver(const std::string& url,
                       const Driver::Mode mode,
                       const std::string& s3_sse):
        Driver(url, mode)
    {
        const size_t prefix_len = 5; // "s3://"
        size_t pos = _url.find("/", prefix_len);
        if (_url.rfind("s3://", 0) != 0 || pos == std::string::npos) {
            std::ostringstream out;
            out << "Invalid S3 URL " << _url;
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
                << out.str();
        }

        // Check if URL in io-paths-list
        bool in_io_paths_list = false;
        for (std::string &path : S3Init::s_paths_list)
            if (_url.rfind(path, 0) == 0)
                in_io_paths_list = true;
        if (!in_io_paths_list) {
            Config& config = *Config::getInstance();
            std::ostringstream out;
            out << "S3 URL " << _url << " not listed in "
                << config.getOptionName(CONFIG_IO_PATHS_LIST);
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_INVALID_PERMISSIONS)
                << out.str();
        }

        _bucket = _url.substr(prefix_len, pos - prefix_len).c_str();
        _prefix = _url.substr(pos + 1);

        // Evaluate S3 SSE (Server-Side Encryption) Parameter
        if (s3_sse == "NOT_SET")
            _s3_sse = Aws::S3::Model::ServerSideEncryption::NOT_SET;
        else if (s3_sse == "AES256")
            _s3_sse = Aws::S3::Model::ServerSideEncryption::AES256;
        else if (s3_sse == "aws:kms")
            _s3_sse = Aws::S3::Model::ServerSideEncryption::aws_kms;
        else {
            std::ostringstream out;
            out << "Invalid s3_sse value " << s3_sse
                << ". Supported values are NOT_SET, AES256, and aws:kms";
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
                << out.str();
        }

        _client = std::make_unique<Aws::S3::S3Client>();
    }

    void S3Driver::init(const Query &query)
    {
        Aws::String key((_prefix + "/metadata").c_str());

        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(_bucket);
        request.SetKey(key);

        auto outcome = _retryLoop<Aws::S3::Model::GetObjectOutcome>(
            "Get",
            key,
            request,
            &Aws::S3::S3Client::GetObject,
            _mode == Driver::Mode::READ || _mode == Driver::Mode::UPDATE);

        if (_mode == Driver::Mode::WRITE && outcome.IsSuccess()) {
            std::ostringstream out;
            out << "Array found, metadata exists s3://"
                << _bucket << "/" << key;
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR)
                << out.str();
        }
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

    void S3Driver::_readMetadataFile(std::shared_ptr<Metadata> metadata) const
    {
        Aws::String key((_prefix + "/metadata").c_str());

        auto&& result = _getRequest(key);
        auto& body = result.GetBody();
        std::string line;
        while (std::getline(body, line)) {
            std::istringstream stream(line);
            std::string key, value;
            if (!std::getline(stream, key, '\t')
                || !std::getline(stream, value)) {
                std::ostringstream out;
                out << "Invalid metadata line '" << line << "'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR)
                    << out.str();
            }
            (*metadata)[key] = value;
        }
    }

    void S3Driver::writeMetadata(std::shared_ptr<const Metadata> metadata) const
    {
        Aws::String key((_prefix + "/metadata").c_str());

        std::shared_ptr<Aws::IOStream> data =
            Aws::MakeShared<Aws::StringStream>("");
        for (auto i = metadata->begin(); i != metadata->end(); ++i)
            *data << i->first << "\t" << i->second << "\n";

        _putRequest(key, data);
    }

    size_t S3Driver::count(const std::string& suffix) const
    {
        Aws::String key((_prefix + "/" + suffix).c_str());
        Aws::S3::Model::ListObjectsRequest request;
        request.WithBucket(_bucket);
        request.WithPrefix(key);

        auto outcome = _retryLoop<Aws::S3::Model::ListObjectsOutcome>(
            "List", key, request, &Aws::S3::S3Client::ListObjects);

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

        auto outcome = _retryLoop<Aws::S3::Model::GetObjectOutcome>(
            "Get", key, request, &Aws::S3::S3Client::GetObject);

        return outcome.GetResultWithOwnership();
    }

    void S3Driver::_putRequest(const Aws::String &key,
                               const std::shared_ptr<Aws::IOStream> data) const
    {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(_bucket);
        request.SetKey(key);
        request.SetBody(data);
        request.SetServerSideEncryption(_s3_sse);

        _retryLoop<Aws::S3::Model::PutObjectOutcome>(
            "Put", key, request, &Aws::S3::S3Client::PutObject);
    }

    // Re-try Loop Template
    template <typename Outcome, typename Request, typename RequestFunc>
    Outcome S3Driver::_retryLoop(const std::string &name,
                                 const Aws::String &key,
                                 const Request &request,
                                 RequestFunc requestFunc,
                                 bool throwIfFails) const
    {
        LOG4CXX_DEBUG(logger, "S3DRIVER|" << name << ":" << key);
        auto outcome = ((*_client).*requestFunc)(request);

        // -- - Retry - --
        int retry = 0;
        while (!outcome.IsSuccess() && retry < RETRY_COUNT) {
            LOG4CXX_WARN(logger,
                         "S3DRIVER|" << name << " s3://" << _bucket << "/"
                         << key << " attempt #" << (retry + 1) << " failed");
            retry++;

            std::this_thread::sleep_for(
                std::chrono::milliseconds(RETRY_SLEEP));

            outcome = ((*_client).*requestFunc)(request);
        }

        if (throwIfFails && !outcome.IsSuccess()) {
            std::ostringstream out;
            out << name << " operation on s3://"
                << _bucket << "/" << key << " failed. ";
            auto error = outcome.GetError();
            out << error.GetMessage();
            if (error.GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN)
                out << " See https://aws.amazon.com/premiumsupport/"
                    << "knowledge-center/s3-troubleshoot-403/";
            throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_ERROR)
                << out.str();
        }

        return outcome;
    }
} // namespace scidb
