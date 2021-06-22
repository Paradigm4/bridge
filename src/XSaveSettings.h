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

    Parameter findKeyword(const KeywordParameters&,
                          const std::string& kw) const;

    void failIfUpdate(std::string param);

public:
    XSaveSettings(const std::vector<std::shared_ptr<OperatorParam> >&,
                  const KeywordParameters&,
                  bool isLogicalOp,
                  const std::shared_ptr<Query>&);

    inline const std::string& getURL() const {
        return _url;
    }

    inline const NamespaceDesc& getNamespace() const {
        return _namespace;
    }

    inline bool isUpdate() const {
        return _isUpdate;
    }

    inline bool isArrowFormat() const {
        return _format == Metadata::Format::ARROW;
    }

    inline Metadata::Compression getCompression() const {
        return _compression;
    }

    inline const std::string& getS3SSE() const {
        return _s3_sse;
    }

    // Used by Updates
    inline void setCompression(Metadata::Compression compression) {
        _compression = compression;
    }

    inline size_t getIndexSplit() const {
        return _indexSplit;
    }

    // Used by Updates
    inline void setIndexSplit(int indexSplit) {
        _indexSplit = indexSplit;
    }
};

} // namespace scidb

#endif  // XSaveSettings
