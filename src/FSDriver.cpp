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

#include "FSDriver.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <log4cxx/logger.h>


namespace scidb {
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.fsdriver"));

    //
    // FSDriver
    //
    FSDriver::FSDriver(const std::string &url):
        _url(url)
    {
        const size_t prefix_len = 7; // "file://"
        if (_url.rfind("file://", 0) != 0) {
            std::ostringstream out;
            out << "Invalid file system URL " << _url;
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR) << out.str();
        }

        _prefix = _url.substr(prefix_len);
        boost::filesystem::create_directory(_prefix);
    }

    size_t FSDriver::_readArrow(const std::string &suffix,
                                std::shared_ptr<arrow::Buffer> &buffer,
                                bool reuse) const
    {
        auto path = _prefix + "/" + suffix;
        std::ifstream stream(path, std::ifstream::binary);
        auto begin = stream.tellg();

        stream.seekg(0, std::ios_base::end);
        auto end = stream.tellg();

        auto length = end - begin;
        _setBuffer(suffix, buffer, reuse, length);

        stream.seekg(0);
        stream.read(reinterpret_cast<char*>(buffer->mutable_data()),
                    length);
        if (!stream) {
            std::ostringstream out;
            out << "Read failed for " << path;
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR) << out.str();
        }

        return length;
    }

    void FSDriver::writeArrow(const std::string &suffix,
                              std::shared_ptr<const arrow::Buffer> buffer) const
    {
        std::string path;

        // Check if writing index file
        if (suffix.rfind("index/", 0) == 0) {
            path = _prefix + "/index";

            // Check if index directory exists
            if (!boost::filesystem::exists(path)) {

                // Create index directory
                auto ok = boost::filesystem::create_directory(path);
                if (!ok) {
                    std::ostringstream out;
                    out << "Failed creating directory " << path;
                    throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR) << out.str();
                }
            }
        }

        path = _prefix + "/" + suffix;
        std::ofstream stream(path, std::ofstream::binary);

        stream.write(reinterpret_cast<const char*>(buffer->data()),
                     buffer->size());
        if (!stream) {
            std::ostringstream out;
            out << "Write failed for " << path << "";
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR) << out.str();
        }

        stream.close();
    }

    void FSDriver::readMetadata(std::map<std::string,
                                         std::string> &metadata) const
    {
        auto path = _prefix + "/metadata";
        std::ifstream stream(path);

        std::string line;
        while (std::getline(stream, line)) {
            std::istringstream stream(line);
            std::string key, value;
            if (!std::getline(stream, key, '\t')
                || !std::getline(stream, value)) {
                std::ostringstream out;
                out << "Invalid metadata line '" << line << "'";
                throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR) << out.str();
            }
            metadata[key] = value;
        }
    }

    void FSDriver::writeMetadata(const std::map<std::string,
                                                std::string> &metadata) const
    {
        auto path = _prefix + "/metadata";
        std::ofstream stream(path);

        for (auto i = metadata.begin(); i != metadata.end(); ++i)
            stream << i->first << "\t" << i->second << "\n";
    }

    size_t FSDriver::count(const std::string& suffix) const
    {
        boost::filesystem::path path(_prefix + "/" + suffix);
        size_t count = 0;
        for (auto i = boost::filesystem::directory_iterator(path);
             i != boost::filesystem::directory_iterator();
             ++i)
            if (!is_directory(i->path()))
                count++;
        return count;
    }

    const std::string& FSDriver::getURL() const
    {
        return _url;
    }
}
