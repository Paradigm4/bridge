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

#ifndef X_INPUT_SETTINGS
#define X_INPUT_SETTINGS

#include "Driver.h"

// SciDB
#include <query/LogicalOperator.h>
#include <query/Query.h>


namespace scidb {

static const char* const KW_CACHE_SIZE	  = "cache_size";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;

class XInputSettings
{
private:
    std::string	     _url;
    size_t           _cacheSize;

    Parameter findKeyword(const KeywordParameters& kwParams,
                          const std::string& kw) const {
        // Copied from PhysicalOperator.h (same as in LogicalOperator.h)
        auto const kwPair = kwParams.find(kw);
        return kwPair == kwParams.end() ? Parameter() : kwPair->second;
    }

public:
    XInputSettings(const std::vector<std::shared_ptr<OperatorParam> >& params,
                   const KeywordParameters& kwParams,
                   bool logical,
                   const std::shared_ptr<Query>& query):
        _cacheSize(CACHE_SIZE_DEFAULT)
    {
        // Evaluate Parameters
        // ---
        // URL
        if (params.size() == 1) _url = paramToString(params[0]);

        // Cache Size
        auto param = findKeyword(kwParams, KW_CACHE_SIZE);
        if (param) _cacheSize = paramToUInt64(param);
    }

    const std::string& getURL() const {
        return _url;
    }

    size_t getCacheSize() const {
        return _cacheSize;
    }
};

} // namespace scidb

#endif  // XInputSettings
