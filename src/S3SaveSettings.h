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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <query/Expression.h>
#include <util/PathUtils.h>

#include <S3Common.h>

#ifndef S3SAVE_SETTINGS
#define S3SAVE_SETTINGS

using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.s3save"));

#define INDEX_SPLIT_MIN 100
#define INDEX_SPLIT_DEFAULT 100000

namespace scidb
{

static const char* const KW_BUCKET_NAME   = "bucket_name";
static const char* const KW_BUCKET_PREFIX = "bucket_prefix";
static const char* const KW_FORMAT	  = "format";
static const char* const KW_COMPRESSION	  = "compression";
static const char* const KW_INDEX_SPLIT	  = "index_split";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;

class S3SaveSettings
{
public:
    static size_t chunkDataOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 5);
    }

    static size_t chunkSizeOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 1);
    }


private:
    std::string			_bucketName;
    std::string			_bucketPrefix;
    S3Metadata::Format          _format;
    S3Metadata::Compression     _compression;
    size_t                      _indexSplit;

    void checkIfSet(bool alreadySet, const char* kw)
    {
        if (alreadySet)
        {
            std::ostringstream error;
            error << "Illegal attempt to set " << kw << " multiple times";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
    }

    void setParamBucketName(std::vector<std::string> bucketName)
    {
        if (_bucketName != "") checkIfSet(true, "bucket_name");
        _bucketName = bucketName[0];
    }

    void setParamBucketPrefix(std::vector<std::string> bucketPrefix)
    {
        if (_bucketPrefix != "") checkIfSet(true, "bucket_prefix");
        _bucketPrefix = bucketPrefix[0];
    }

    void setParamFormat(std::vector<std::string> format)
    {
        if (format[0] == "arrow")
            _format = S3Metadata::Format::ARROW;
        else
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "format must be 'arrow'";
    }

    void setParamCompression(std::vector<std::string> compression)
    {
        if (compression[0] == "none")
            _compression = S3Metadata::Compression::NONE;
        else if (compression[0] == "zlib")
            _compression = S3Metadata::Compression::ZLIB;
        else
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "unsupported compression";
    }

    void setParamIndexSplit(std::vector<int64_t> indexSplit)
    {
        _indexSplit = indexSplit[0];
        if(_indexSplit < INDEX_SPLIT_MIN) {
            std::ostringstream err;
            err << "index_split must be at or above " << INDEX_SPLIT_MIN;
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
        }
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

    bool setKeywordParamString(KeywordParameters const& kwParams,
                               const char* const kw,
                               void (S3SaveSettings::* innersetter)(std::vector<std::string>) )
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
            LOG4CXX_DEBUG(logger, "S3SAVE|findKeyword null: " << kw);
        }
        return retSet;
    }

    void setKeywordParamString(KeywordParameters const& kwParams,
                               const char* const kw,
                               bool& alreadySet, void (S3SaveSettings::* innersetter)(std::vector<std::string>) )
    {
        checkIfSet(alreadySet, kw);
        alreadySet = setKeywordParamString(kwParams, kw, innersetter);
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

    bool setKeywordParamInt64(KeywordParameters const& kwParams,
                              const char* const kw,
                              void (S3SaveSettings::* innersetter)(std::vector<int64_t>) )
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

    void setKeywordParamInt64(KeywordParameters const& kwParams,
                              const char* const kw,
                              bool& alreadySet,
                              void (S3SaveSettings::* innersetter)(std::vector<int64_t>) )
    {
        checkIfSet(alreadySet, kw);
        alreadySet = setKeywordParamInt64(kwParams, kw, innersetter);
    }

    Parameter getKeywordParam(KeywordParameters const& kwp, const std::string& kw) const
    {
        auto const& kwPair = kwp.find(kw);
        return kwPair == kwp.end() ? Parameter() : kwPair->second;
    }

public:
    S3SaveSettings(std::vector<std::shared_ptr<OperatorParam> > const& operatorParameters,
                   KeywordParameters const& kwParams,
                   bool logical,
                   std::shared_ptr<Query>& query):
                _bucketName(""),
                _bucketPrefix(""),
                _format(S3Metadata::Format::ARROW),
                _compression(S3Metadata::Compression::NONE),
                _indexSplit(INDEX_SPLIT_DEFAULT)
    {
        bool  bucketNameSet   = false;
        bool  bucketPrefixSet = false;
        bool  formatSet       = false;
        bool  compressionSet  = false;
        bool  indexSplit      = false;

        setKeywordParamString(kwParams, KW_BUCKET_NAME,   bucketNameSet,   &S3SaveSettings::setParamBucketName);
        setKeywordParamString(kwParams, KW_BUCKET_PREFIX, bucketPrefixSet, &S3SaveSettings::setParamBucketPrefix);
        setKeywordParamString(kwParams, KW_FORMAT,        formatSet,       &S3SaveSettings::setParamFormat);
        setKeywordParamString(kwParams, KW_COMPRESSION,   compressionSet,  &S3SaveSettings::setParamCompression);
        setKeywordParamInt64(kwParams,  KW_INDEX_SPLIT,   indexSplit,      &S3SaveSettings::setParamIndexSplit);

        if(_bucketName.size() == 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << KW_BUCKET_NAME << " was not provided, or failed to parse";

        if(_bucketPrefix.size() == 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << KW_BUCKET_PREFIX << " was not provided, or failed to parse";
    }

    bool isArrowFormat() const
    {
        return _format == S3Metadata::Format::ARROW;
    }

    std::string const& getBucketName() const
    {
        return _bucketName;
    }

    std::string const& getBucketPrefix() const
    {
        return _bucketPrefix;
    }

    S3Metadata::Compression getCompression() const
    {
        return _compression;
    }

    size_t getIndexSplit() const
    {
        return _indexSplit;
    }
};
}


#endif //S3SaveSettings
