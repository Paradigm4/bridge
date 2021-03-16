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

#include "XInputSettings.h"

#include "Driver.h"
#include "XArray.h"
#include "XIndex.h"

// SciDB
#include <query/PhysicalOperator.h>


namespace scidb {

class PhysicalXInput : public PhysicalOperator
{
public:
    PhysicalXInput(const std::string &logicalName,
                   const std::string &physicalName,
                   const Parameters &parameters,
                   const ArrayDesc &schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    std::shared_ptr<Array> execute(
        std::vector<std::shared_ptr<Array> > &inputArrays,
        std::shared_ptr<Query> query)
    {
        LOG4CXX_DEBUG(logger, "XINPUT|" << query->getInstanceID() << "|execute");

        std::shared_ptr<XInputSettings> settings = std::make_shared<XInputSettings>(
            _parameters, _kwParameters, false, query);

        auto driver = Driver::makeDriver(settings->getURL());

        std::shared_ptr<Metadata> metadata = std::make_shared<Metadata>();
        driver->readMetadata(metadata);

        std::shared_ptr<XIndex> index = std::make_shared<XIndex>(_schema);
        index->load(driver, query);

        std::shared_ptr<XArray> array = std::make_shared<XArray>(
            _schema, query, driver, index, metadata->getCompression(), settings->getCacheSize());

        return array;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalXInput, "xinput", "PhysicalXInput");

} // namespace scidb
