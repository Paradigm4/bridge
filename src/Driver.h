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

#ifndef DRIVER_H_
#define DRIVER_H_

#include <map>
#include <memory>
#include <sstream>

// SciDB
#include <array/ArrayDesc.h>
#include <array/DimensionDesc.h>
#include <array/Dimensions.h>
#include <system/UserException.h>

// Arrow
#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/util/compression.h>

#define BRIDGE_VERSION 1
#define INDEX_SPLIT_MIN 100
#define INDEX_SPLIT_DEFAULT 100000  // Number of Coordinates =
                                    // (Number-of-Chunks *
                                    // Number-of-Dimensions)
#define CACHE_SIZE_DEFAULT 268435456 // 256MB in Bytes
#define CHUNK_MAX_SIZE 2147483648

#define S3_SSE_DEFAULT "NOT_SET"

#define _STR(x) #x
#define STR(x) _STR(x)

#define PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))
#define THROW_NOT_OK(status, message)                                   \
    ({                                                                  \
        arrow::Status __s = (status);                                   \
        if (PREDICT_FALSE(!__s.ok()))                                   \
        {                                                               \
            throw SYSTEM_EXCEPTION(                                     \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)      \
                << __s.ToString().c_str() << " " << (message);          \
        }                                                               \
    })

#define ASSIGN_OR_THROW(lhs, rexpr, message)            \
    ({                                                  \
        auto __s_name = (rexpr);                        \
        THROW_NOT_OK(__s_name.status(), (message));     \
        lhs = std::move(__s_name).ValueOrDie();         \
    })


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace scidb {
    class Query;                // #include "Query.h"
}
// -- End of Forward Declarations


namespace scidb {

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(
    log4cxx::Logger::getLogger("scidb.operators.bridge"));

class Metadata {
public:
    enum Format {
        ARROW  = 0
    };

    enum Compression {
        NONE  = 0,
        GZIP  = 1,
        LZ4   = 2
    };

    Metadata():
        _hasSchema(false)
    {}

    inline std::string& operator[](const std::string &key) {
        return _metadata[key];
    }

    inline std::map<std::string, std::string>::const_iterator find(
        const std::string &key) const {
        return _metadata.find(key);
    }

    inline std::map<std::string, std::string>::const_iterator begin() const {
        return _metadata.begin();
    }

    inline std::map<std::string, std::string>::const_iterator end() const {
        return _metadata.end();
    }

    Metadata::Compression getCompression() const;

    void setCompression(Metadata::Compression compression);

    const ArrayDesc& getSchema(std::shared_ptr<Query> query);

    void setSchema(const ArrayDesc &schema);

    void validate() const;

    static Metadata::Compression string2Compression(const std::string&);
    static std::string compression2String(Metadata::Compression);
    static arrow::Compression::type compression2Arrow(Metadata::Compression);

    static std::string coord2ObjectName(const Coordinates &pos,
                                        const Dimensions &dims);

private:
    std::map<std::string, std::string> _metadata;
    bool _hasSchema;
    ArrayDesc _schema;
};

class Driver {
public:
    enum Mode {
        READ   = 0,
        WRITE  = 1,
        UPDATE = 2
    };

    Driver(const std::string& url, const Driver::Mode mode):
        _url(url),
        _mode(mode)
    {}

    virtual ~Driver() = 0;

    virtual void init(const Query&) = 0;

    inline size_t readArrow(const std::string &suffix,
                            std::shared_ptr<arrow::Buffer> &buffer) const {
        return _readArrow(suffix, buffer, false);
    }

    inline size_t readArrow(const std::string &suffix,
                            std::shared_ptr<arrow::ResizableBuffer> buffer) const {
        auto buf = std::static_pointer_cast<arrow::Buffer>(buffer);
        return _readArrow(suffix, buf, true);
    }

    virtual void writeArrow(const std::string&,
                            std::shared_ptr<const arrow::Buffer>) const = 0;

    inline void readMetadata(std::shared_ptr<Metadata> metadata) const {
        _readMetadataFile(metadata);
        metadata->validate();
    }
    virtual void writeMetadata(std::shared_ptr<const Metadata>) const = 0;

    // Count number of objects with specified prefix
    virtual size_t count(const std::string&) const = 0;

    // Return print-friendly path used by driver
    virtual const std::string& getURL() const = 0;

    static std::shared_ptr<Driver> makeDriver(const std::string& url,
                                              const Mode mode=Mode::READ,
                                              const std::string& s3_sse=S3_SSE_DEFAULT);

protected:
    const std::string _url;
    const Driver::Mode _mode;

    virtual void _readMetadataFile(std::shared_ptr<Metadata>) const = 0;

    inline void _setBuffer(const std::string &suffix,
                           std::shared_ptr<arrow::Buffer> &buffer,
                           bool reuse,
                           size_t length) const {
        if (length > CHUNK_MAX_SIZE) {
            std::ostringstream out;
            out << "Object " << getURL() << "/" << suffix
                << " size " << length
                << " exeeds max allowed " << CHUNK_MAX_SIZE;
            throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                   SCIDB_LE_ILLEGAL_OPERATION) << out.str();
        }
        if (reuse) {
            THROW_NOT_OK(std::static_pointer_cast<arrow::ResizableBuffer>(
                             buffer)->Resize(length, false),
                         "resize buffer for " + suffix);
        }
        else {
            ASSIGN_OR_THROW(buffer, arrow::AllocateBuffer(length),
                         "allocate buffer for " + suffix);
        }
    }

private:
    virtual size_t _readArrow(const std::string&,
                              std::shared_ptr<arrow::Buffer>&,
                              bool reuse) const = 0;

};

inline Driver::~Driver() {}

} // namespace scidb

#endif  // Driver
