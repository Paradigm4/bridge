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

#ifndef DRIVER_H_
#define DRIVER_H_

#include <map>
#include <memory>


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace arrow {
    class Buffer;               // #include <arrow/buffer.h>
}
// -- End of Forward Declarations


namespace scidb
{
class DriverChunk {
public:
    virtual ~DriverChunk() = 0;

    virtual size_t size() const = 0;

    virtual void read(std::shared_ptr<arrow::Buffer>,
                      const size_t length) = 0;
};

class Driver {
public:
    virtual ~Driver() = 0;

    virtual std::unique_ptr<DriverChunk> readArrow(const std::string&) const = 0;
    virtual void writeArrow(const std::string&,
                            std::shared_ptr<const arrow::Buffer>) const = 0;

    virtual void readMetadata(std::map<std::string, std::string>&) const = 0;
    virtual void writeMetadata(const std::map<std::string,
                                              std::string>&) const = 0;

    // Count number of objects with specified prefix
    virtual size_t count(const std::string&) const = 0;

    // Return print-friendly path used by driver
    virtual const std::string& getURL() const = 0;
};

inline DriverChunk::~DriverChunk() {}
inline Driver::~Driver() {}

}

#endif
