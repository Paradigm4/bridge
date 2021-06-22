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

#ifndef X_SAVE_SETTINGS
#define X_SAVE_SETTINGS

#include "Driver.h"

// SciDB
#include <query/LogicalOperator.h>
#include <query/Query.h>


namespace scidb {

static const char* const KW_COMPRESSION	= "compression";
static const char* const KW_FORMAT	= "format";
static const char* const KW_INDEX_SPLIT	= "index_split";
static const char* const KW_NAMESPACE   = "namespace";
static const char* const KW_S3_SSE      = "s3_sse";
static const char* const KW_UPDATE	= "update";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t;

class XSaveSettings
{
private:
    std::string           _url;
    NamespaceDesc         _namespace;
    bool                  _isUpdate;
    Metadata::Format      _format;
    Metadata::Compression _compression;
    size_t                _indexSplit;
    std::string           _s3_sse;

    Parameter findKeyword(const KeywordParameters& kwParams,
                          const std::string& kw) const {
        // Copied from PhysicalOperator.h (same as in LogicalOperator.h)
        auto const kwPair = kwParams.find(kw);
        return kwPair == kwParams.end() ? Parameter() : kwPair->second;
    }

    void failIfUpdate(std::string param) {
        if (_isUpdate) {
            std::stringstream out;
            out << param << " cannot be specified for update queries";
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
                << out.str();
        }
    }

public:
    XSaveSettings(const std::vector<std::shared_ptr<OperatorParam> >& params,
                  const KeywordParameters& kwParams,
                  bool logical,
                  const std::shared_ptr<Query>& query):
        _namespace(NamespaceDesc("public")),
        _isUpdate(false),
        _format(Metadata::Format::ARROW),
        _compression(Metadata::Compression::NONE),
        _indexSplit(INDEX_SPLIT_DEFAULT),
        _s3_sse(S3_SSE_DEFAULT)
    {
        // Evaluate Parameters
        // ---
        // URL
        if (params.size() == 1) _url = paramToString(params[0]);

        // Update
        auto param = findKeyword(kwParams, KW_UPDATE);
        if (param) _isUpdate = paramToBool(param);

        // Namespace
        param = findKeyword(kwParams, KW_NAMESPACE);
        if (param) {
            failIfUpdate("namespace");

            _namespace = safe_dynamic_cast<OperatorParamNamespaceReference*>(
                param.get())->getNamespace();
        }

        // Format
        param = findKeyword(kwParams, KW_FORMAT);
        if (param) {
            failIfUpdate("format");

            auto format = paramToString(param);
            if (format == "arrow")
                _format = Metadata::Format::ARROW;
            else
                throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                     SCIDB_LE_ILLEGAL_OPERATION)
                    << "format must be 'arrow'";
        }

        // Compression
        param = findKeyword(kwParams, KW_COMPRESSION);
        if (param) {
            failIfUpdate("compression");

            auto compression = paramToString(param);
            if (compression == "none")
                _compression = Metadata::Compression::NONE;
            else if (compression == "gzip")
                _compression = Metadata::Compression::GZIP;
            else
                throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                     SCIDB_LE_ILLEGAL_OPERATION)
                    << "unsupported compression";
        }

        // Index Split
        param = findKeyword(kwParams, KW_INDEX_SPLIT);
        if (param) {
            failIfUpdate("index_split");

            auto indexSplit = paramToUInt64(param);

            if(indexSplit < INDEX_SPLIT_MIN) {
                std::ostringstream err;
                err << "index_split must be at or above " << INDEX_SPLIT_MIN;
                throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                     SCIDB_LE_ILLEGAL_OPERATION) << err.str();
            }
            _indexSplit = indexSplit;
        }

        // S3 SSE (Sever Side Encryption) | Validated in S3Driver
        param = findKeyword(kwParams, KW_S3_SSE);
        if (param) _s3_sse = paramToString(param);
    }

    const std::string& getURL() const {
        return _url;
    }

    const NamespaceDesc& getNamespace() const {
        return _namespace;
    }

    bool isUpdate() const {
        return _isUpdate;
    }

    bool isArrowFormat() const {
        return _format == Metadata::Format::ARROW;
    }

    Metadata::Compression getCompression() const {
        return _compression;
    }

    const std::string& getS3SSE() const {
        return _s3_sse;
    }

    // Used by Updates
    void setCompression(Metadata::Compression compression) {
        _compression = compression;
    }

    size_t getIndexSplit() const {
        return _indexSplit;
    }

    // Used by Updates
    void setIndexSplit(int indexSplit) {
        _indexSplit = indexSplit;
    }
};

} // namespace scidb

#endif  // XSaveSettings
