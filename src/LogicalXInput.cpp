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

#include <rbac/Rights.h>

namespace scidb {

class LogicalXInput : public  LogicalOperator
{
public:
    LogicalXInput(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {}

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::STAR, {
                      RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
                  })
            },
            { KW_FORMAT,        RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { KW_CACHE_SIZE,    RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))  }
        };
        return &argSpec;
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);

        if (_settings == NULL)
            _settings = std::make_shared<XInputSettings>(
                _parameters, _kwParameters, true, query);

        if (_driver == NULL)
            _driver = Driver::makeDriver(_settings->getURL());

        // Read Metadata
        if (_metadata == NULL) {
            _metadata = std::make_shared<Metadata>();
            _driver->readMetadata(_metadata);
        }

        auto namespaceName = (*_metadata)["namespace"];
        LOG4CXX_DEBUG(logger,
                      "XINPUT|" << query->getInstanceID()
                      << "|inferAccess ns:" << namespaceName);
        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName, rbac::P_NS_READ);
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        if (_settings == NULL)
            _settings = std::make_shared<XInputSettings>(
                _parameters, _kwParameters, true, query);

        // Init Driver
        if (_driver == NULL)
            _driver = Driver::makeDriver(_settings->getURL());
        _driver->init(*query);

        // Read Metadata
        if (_metadata == NULL) {
            _metadata = std::make_shared<Metadata>();
            _driver->readMetadata(_metadata);
        }

        LOG4CXX_DEBUG(logger,
                      "XINPUT|" << query->getInstanceID()
                      << "|schema: " << (*_metadata)["schema"]);
        ArrayDesc schema = _metadata->getSchema(query);
        schema.setDistribution(createDistribution(defaultDistType()));
        return schema;
    }

private:
    std::shared_ptr<XInputSettings> _settings;
    std::shared_ptr<Driver> _driver;
    std::shared_ptr<Metadata> _metadata;
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalXInput, "xinput");

} // namespace scidb
