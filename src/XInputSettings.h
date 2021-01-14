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
#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>


namespace scidb {

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.xinput"));

static const char* const KW_FORMAT	  = "format";
static const char* const KW_CACHE_SIZE	  = "cache_size";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;

class XInputSettings
{
private:
    enum FormatType
    {
        ARROW  = 0
    };

    std::string			_url;
    FormatType                  _format;
    size_t                      _cacheSize;

    void setParamFormat(std::vector<std::string> format)
    {
        if(format[0] == "arrow")
        {
            _format = ARROW;
        }
        else
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
                << "format must be 'arrow'";
        }
    }

    void setParamCacheSize(std::vector<int64_t> cacheSize)
    {
        _cacheSize = cacheSize[0];
    }

    Parameter getKeywordParam(KeywordParameters const& kwp, const std::string& kw) const
    {
        auto const& kwPair = kwp.find(kw);
        return kwPair == kwp.end() ? Parameter() : kwPair->second;
    }

    std::string getParamContentString(Parameter& param)
    {
        std::string paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_STRING).getString();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getString();
        }
        return paramContent;
    }

    int64_t getParamContentInt64(Parameter& param)
    {
        size_t paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_INT64).getInt64();
        }
        else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getInt64();
            LOG4CXX_DEBUG(logger, "aio_save integer param is " << paramContent)

        }
        return paramContent;
    }

    bool setKeywordParamString(KeywordParameters const& kwParams,
                               const char* const kw,
                               void (XInputSettings::* innersetter)(std::vector<std::string>))
    {
        std::vector <std::string> paramContent;
        bool retSet = false;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                for (size_t i = 0; i < gParams.size(); ++i) {
                    paramContent.push_back(getParamContentString(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentString(kwParam));
            }
            (this->*innersetter)(paramContent);
            retSet = true;
        } else {
            LOG4CXX_DEBUG(logger, "XINPUT|findKeyword null: " << kw);
        }
        return retSet;
    }

    bool setKeywordParamInt64(KeywordParameters const& kwParams,
                              const char* const kw,
                              void (XInputSettings::* innersetter)(std::vector<int64_t>) )
    {
        std::vector<int64_t> paramContent;
        size_t numParams;
        bool retSet = false;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                numParams = gParams.size();
                for (size_t i = 0; i < numParams; ++i)
                    paramContent.push_back(getParamContentInt64(gParams[i]));
            }
            else
                paramContent.push_back(getParamContentInt64(kwParam));

            (this->*innersetter)(paramContent);
            retSet = true;
        }
        else
            LOG4CXX_DEBUG(logger, "aio_save findKeyword null: " << kw);

        return retSet;
    }

public:
    XInputSettings(std::vector<std::shared_ptr<OperatorParam> > const& operatorParameters,
                   KeywordParameters const& kwParams,
                   bool logical,
                   std::shared_ptr<Query>& query):
                _format(ARROW),
                _cacheSize(CACHE_SIZE_DEFAULT)
    {
        if (operatorParameters.size() != 1)
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
                << "illegal number of parameters passed to xinput";
        std::shared_ptr<OperatorParam>const& param = operatorParameters[0];
        if (logical)
            _url = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(), TID_STRING).getString();
        else
            _url = ((std::shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();

        setKeywordParamString(kwParams, KW_FORMAT,        &XInputSettings::setParamFormat);
        setKeywordParamInt64( kwParams, KW_CACHE_SIZE,    &XInputSettings::setParamCacheSize);
    }

    const std::string& getURL() const
    {
        return _url;
    }

    bool isArrowFormat() const
    {
        return _format == ARROW;
    }

    size_t getCacheSize() const
    {
        return _cacheSize;
    }
};

} // namespace scidb

#endif  // XInputSettings
