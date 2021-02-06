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

#ifndef FS_DRIVER_H_
#define FS_DRIVER_H_

#include "Driver.h"


namespace scidb {

class FSDriver: public Driver {
public:
    FSDriver(const std::string &url, const Driver::Mode mode);

    void init();

    void writeArrow(const std::string&,
                    std::shared_ptr<const arrow::Buffer>) const;

    void writeMetadata(const Metadata&) const;

    // Count number of objects with specified prefix
    size_t count(const std::string&) const;

    // Return print-friendly path used by driver
    const std::string& getURL() const;

protected:
    void _readMetadataFile(Metadata&) const;

private:
    const std::string _url;
    const Driver::Mode _mode;
    std::string _prefix;

    size_t _readArrow(const std::string&, std::shared_ptr<arrow::Buffer>&, bool) const;
};

} // namespace scidb

#endif  // FSDriver
