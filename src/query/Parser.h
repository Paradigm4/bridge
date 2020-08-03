/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef QUERY_PARSER_H_
#define QUERY_PARSER_H_

/****************************************************************************/

#include <memory>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

class Query;
class Array;
class ArrayDesc;
class LogicalExpression;
class LogicalQueryPlanNode;

/****************************************************************************/
namespace arena {typedef std::shared_ptr<class Arena> ArenaPtr;}
/****************************************************************************/

std::shared_ptr<LogicalQueryPlanNode> parseStatement (const std::shared_ptr<Query>&,bool afl);
std::shared_ptr<LogicalExpression>    parseExpression(const std::string&);

/****************************************************************************/

void                                    loadPrelude();
void                                    loadModule(const std::string&);

/****************************************************************************/

ArrayDesc                             logicalListMacros (const std::shared_ptr<Query>& query,
                                                         bool showHidden);
std::shared_ptr<Array>                physicalListMacros(const arena::ArenaPtr&,
                                                         const std::shared_ptr<Query>& query,
                                                         bool showHidden);

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
