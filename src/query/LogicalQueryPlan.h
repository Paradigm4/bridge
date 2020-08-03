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

/*
 * LogicalQueryPlan.h
 * taken from QueryPlan.h
 */

#ifndef LOGICALQUERYPLAN_H_
#define LOGICALQUERYPLAN_H_

#include <query/LogicalOperator.h>
#include <query/OperatorLibrary.h>
#include <util/SerializedPtrConverter.h>

#include <boost/archive/text_iarchive.hpp>

namespace scidb
{

/**
 * Node of logical plan of query. Logical node keeps logical operator to
 * perform inferring result type and validate types.
 */
class LogicalQueryPlanNode
{
public:
    LogicalQueryPlanNode(std::shared_ptr<ParsingContext>  const&,
                         std::shared_ptr<LogicalOperator> const&);

    LogicalQueryPlanNode(std::shared_ptr<ParsingContext>  const&,
                         std::shared_ptr<LogicalOperator> const&,
                         std::vector<std::shared_ptr<LogicalQueryPlanNode> > const &children);

    void addChild(const std::shared_ptr<LogicalQueryPlanNode>& child)
    {
        _childNodes.push_back(child);
    }

    std::shared_ptr<LogicalOperator> getLogicalOperator()
    {
        return _logicalOperator;
    }

    std::vector<std::shared_ptr<LogicalQueryPlanNode> >& getChildren()
    {
        return _childNodes;
    }

    bool isDdl() const
    {
        return _logicalOperator->getProperties().ddl;
    }

    bool supportsTileMode() const
    {
        return _logicalOperator->getProperties().tile;
    }

    /** True iff operator is "SELECT-like", i.e. returns a result array. */
    bool isSelective() const;

    std::shared_ptr<ParsingContext> getParsingContext() const
    {
        return _parsingContext;
    }

    void inferAccess(const std::shared_ptr<Query>&);
    const ArrayDesc& inferTypes(const std::shared_ptr<Query>&, bool forceSchemaUpdate=false);

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &,int indent = 0,bool children = true) const;

  private:
    std::shared_ptr<LogicalOperator>                    _logicalOperator;
    std::vector<std::shared_ptr<LogicalQueryPlanNode> > _childNodes;
    std::shared_ptr<ParsingContext>                     _parsingContext;
};

/**
 * The LogicalPlan represents result of parsing query and is used for validation query.
 * It's input data for optimization and generation physical plan.
 */
class LogicalPlan
{
public:
    LogicalPlan(const std::shared_ptr<LogicalQueryPlanNode>& root);

    std::shared_ptr<LogicalQueryPlanNode> getRoot()
    {
        return _root;
    }

    void setRoot(const std::shared_ptr<LogicalQueryPlanNode>& root)
    {
        _root = root;
    }

    // @return true when _root is logically null
    bool empty() const
    {
        return _root == std::shared_ptr<LogicalQueryPlanNode>();
    }

    // @return whether the query is recognized as DDL
    bool isDdl() const
    {
        SCIDB_ASSERT(!empty());
        return _root->isDdl();
    }

    const ArrayDesc& inferTypes(const std::shared_ptr<Query>& query);

    void inferAccess(const std::shared_ptr<Query>& query);

	/**
	 * Retrieve a human-readable description.
	 * Append a human-readable description of this onto str. Description takes up
	 * one or more lines. Append indent spacer characters to the beginning of
	 * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &str, int indent = 0, bool children = true) const;

private:
    std::shared_ptr<LogicalQueryPlanNode> _root;
};

} // namespace


#endif /* LOGICALQUERYPLAN_H_ */
