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

#include <boost/algorithm/string/predicate.hpp>

// SciDB
#include <query/Query.h>


namespace scidb {

XSaveSettings::XSaveSettings(
    const std::vector<std::shared_ptr<OperatorParam> >& params,
    const KeywordParameters& kwParams,
    bool logical,
    const std::shared_ptr<Query>& query):
    _namespace(NamespaceDesc("public")),
    _isUpdate(false),
    _format(Metadata::Format::ARROW),
    _compression(Metadata::Compression::LZ4),
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
        if (boost::iequals(format, "arrow"))
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

        auto foo = paramToString(param);
        _compression = Metadata::string2Compression(paramToString(param));
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

Parameter XSaveSettings::findKeyword(const KeywordParameters& kwParams,
                                     const std::string& kw) const {
    // Copied from PhysicalOperator.h (same as in LogicalOperator.h)
    auto const kwPair = kwParams.find(kw);
    return kwPair == kwParams.end() ? Parameter() : kwPair->second;
}

void XSaveSettings::failIfUpdate(std::string param) {
    if (_isUpdate) {
        std::stringstream out;
        out << param << " cannot be specified for update queries";
        throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_ILLEGAL_OPERATION)
            << out.str();
    }
}

} // namespace scidb
