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

#include "FSDriver.h"

#include <boost/filesystem.hpp>
#include <fstream>

// SciDB
#include <util/PathUtils.h>


#define FAIL(op, path)                                                          \
    {                                                                           \
        std::ostringstream out;                                                 \
        out << (op) << " failed for " << (path);                                \
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR)      \
            << out.str();                                                       \
    }


namespace scidb {
    //
    // FSDriver
    //
    FSDriver::FSDriver(const std::string &url,
                       const Driver::Mode mode):
        Driver(url, mode)
    {
        // Check the URL is valid
        const size_t prefix_len = 7; // "file://"
        if (_url.rfind("file://", 0) != 0) {
            std::ostringstream out;
            out << "Invalid file system URL " << _url;
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
                << out.str();
        }

        _prefix = _url.substr(prefix_len);
    }

    void FSDriver::init(const Query &query)
    {
        try {
            // Chech path and/or create directories
            if (_mode == Driver::Mode::READ
                || _mode == Driver::Mode::UPDATE) {
                // Sanitize and check path permissions
                _prefix = path::expandForRead(_prefix, query);

                // Check that path exists and is a directory
                if (!boost::filesystem::exists(_prefix) ||
                    !boost::filesystem::is_directory(_prefix))
                    FAIL("Find directory", _prefix);
            }
            else if (_mode == Driver::Mode::WRITE) {
                // Sanitize and check path permissions
                _prefix = path::expandForSave(_prefix, query);

                // Check that path does NOT exist
                if (boost::filesystem::exists(_prefix))
                    FAIL("Array found, path exists. Exists not", _prefix);

                // Create base, index, and chunks directories
                for (const std::string &postfix : {"", "/index", "/chunks"})
                    try {
                        // Not an error if the directory exists
                        boost::filesystem::create_directory(_prefix + postfix);
                    }
                    catch (const std::exception &ex) {
                        FAIL("Create directory", _prefix + postfix);
                    }
            }

        }
        // Catch all rest
        catch (const std::exception &ex) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR)
                << ex.what();
        }
    }

    size_t FSDriver::_readArrow(const std::string &suffix,
                                std::shared_ptr<arrow::Buffer> &buffer,
                                bool reuse) const
    {
        auto path = _prefix + "/" + suffix;
        std::ifstream stream(path, std::ifstream::binary);
        if (stream.fail()) FAIL("Open", path);

        auto begin = stream.tellg();
        if (stream.fail()) FAIL("Get position", path);

        stream.seekg(0, std::ios_base::end);
        if (stream.fail()) FAIL("Set position", path);

        auto end = stream.tellg();
        if (stream.fail()) FAIL("Get position", path);

        auto length = end - begin;
        _setBuffer(suffix, buffer, reuse, length);

        stream.seekg(0);
        if (stream.fail()) FAIL("Set position", path);

        stream.read(reinterpret_cast<char*>(buffer->mutable_data()),
                    length);
        if (!stream) FAIL("Read", path);

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
                try {
                    boost::filesystem::create_directory(path);
                }
                catch (const std::exception &ex) {
                    FAIL("Create directory", path);
                }
            }
        }

        path = _prefix + "/" + suffix;
        std::ofstream stream(path, std::ofstream::binary);
        if (stream.fail()) FAIL("Open", path);

        stream.write(reinterpret_cast<const char*>(buffer->data()),
                     buffer->size());
        if (!stream) FAIL("Read", path);

        stream.close();
        if (stream.fail()) FAIL("Close", path);
    }

    void FSDriver::_readMetadataFile(std::shared_ptr<Metadata> metadata) const
    {
        auto path = _prefix + "/metadata";
        std::ifstream stream(path);
        if (stream.fail()) FAIL("Open", path);

        std::string line;
        while (std::getline(stream, line) && stream.good()) {
            std::istringstream stream(line);
            std::string key, value;
            if (!std::getline(stream, key, '\t')
                || !std::getline(stream, value)) {
                std::ostringstream out;
                out << "Invalid metadata line '" << line << "' in " << path;
                throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_UNKNOWN_ERROR)
                    << out.str();
            }
            (*metadata)[key] = value;
        }
        if (!stream.eof() && stream.fail()) FAIL("Read", path);
    }

    void FSDriver::writeMetadata(std::shared_ptr<const Metadata> metadata) const
    {
        auto path = _prefix + "/metadata";
        std::ofstream stream(path);
        if (stream.fail()) FAIL("Open", path);

        for (auto i = metadata->begin(); i != metadata->end(); ++i) {
            stream << i->first << "\t" << i->second << "\n";
            if (stream.fail()) FAIL("Write", path);
        }
    }

    size_t FSDriver::count(const std::string& suffix) const
    {
        boost::filesystem::path path(_prefix + "/" + suffix);
        size_t count = 0;
        try {
            for (auto i = boost::filesystem::directory_iterator(path);
                 i != boost::filesystem::directory_iterator();
                 ++i)
                if (!is_directory(i->path()))
                    count++;
        }
        catch (const std::exception &ex) {
            FAIL("List directory", path.native());
        }
        return count;
    }

    const std::string& FSDriver::getURL() const
    {
        return _url;
    }
} // namespace scidb
