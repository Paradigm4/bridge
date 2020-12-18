/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2020 Paradigm4 Inc.
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
#include <system/UserException.h>

// Arrow
#include <arrow/buffer.h>


#define CHUNK_MAX_SIZE 2147483648

// TODO use __builtin_expect
#define THROW_NOT_OK(status)                                            \
    {                                                                   \
        arrow::Status _status = (status);                               \
        if (!_status.ok())                                              \
        {                                                               \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)      \
                    << _status.ToString().c_str();                      \
        }                                                               \
    }

namespace scidb {

class Driver {
public:
    virtual ~Driver() = 0;

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

    virtual void readMetadata(std::map<std::string, std::string>&) const = 0;
    virtual void writeMetadata(const std::map<std::string,
                                              std::string>&) const = 0;

    // Count number of objects with specified prefix
    virtual size_t count(const std::string&) const = 0;

    // Return print-friendly path used by driver
    virtual const std::string& getURL() const = 0;

    static std::shared_ptr<Driver> makeDriver(const std::string url);

private:
    virtual size_t _readArrow(const std::string&,
                              std::shared_ptr<arrow::Buffer>&,
                              bool reuse) const = 0;

protected:
    inline void _setBuffer(const std::string &suffix,
                           std::shared_ptr<arrow::Buffer> &buffer,
                           bool reuse,
                           size_t length) const {
        if (length > CHUNK_MAX_SIZE) {
            std::ostringstream out;
            out << "Object " << getURL() << "/" << suffix
                << " size " << length
                << " exeeds max allowed " << CHUNK_MAX_SIZE;
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                 SCIDB_LE_ILLEGAL_OPERATION) << out.str();
        }
        if (reuse) {
            THROW_NOT_OK(std::static_pointer_cast<arrow::ResizableBuffer>(
                             buffer)->Resize(length, false));
        }
        else {
            THROW_NOT_OK(arrow::AllocateBuffer(length, &buffer));
        }
    }
};

inline Driver::~Driver() {}

} // namespace scidb

#endif  // Driver
