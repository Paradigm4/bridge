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

#include "XSaveSettings.h"

namespace scidb {

class LogicalXSave : public  LogicalOperator
{
public:
    LogicalXSave(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {}

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
                 })
              })
            },
            { "", // positionals
              RE(RE::STAR, {
                      RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
                  })
            },
            { KW_UPDATE,        RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))   },
            { KW_FORMAT,        RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { KW_COMPRESSION,   RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { KW_INDEX_SPLIT,   RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))  }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas,
                          std::shared_ptr<Query> query)
    {
        XSaveSettings settings (_parameters, _kwParameters, true, query);
        std::vector<DimensionDesc> dimensions(3);
        size_t const nInstances = query->getInstancesCount();
        dimensions[0] = DimensionDesc("chunk_no",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("dest_instance_id",   0, 0, nInstances-1, nInstances-1, 1, 0);
        dimensions[2] = DimensionDesc("source_instance_id", 0, 0, nInstances-1, nInstances-1, 1, 0);
        Attributes attributes;
        attributes.push_back(
            AttributeDesc("val", TID_STRING, AttributeDesc::IS_NULLABLE, CompressorType::NONE));
        return ArrayDesc(
            "xsave",
            attributes,
            dimensions,
            createDistribution(defaultDistType()),
            query->getDefaultArrayResidency(),
            0,
            false);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalXSave, "xsave");

} // namespace scidb
