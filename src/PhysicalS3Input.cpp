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

#include "S3InputSettings.h"
#include "S3Array.h"

#include <query/PhysicalOperator.h>


namespace scidb
{

class PhysicalS3Input : public PhysicalOperator
{
public:
    PhysicalS3Input(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    std::shared_ptr<Array> execute(
        std::vector< std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        LOG4CXX_DEBUG(logger, "S3INPUT|" << query->getInstanceID() << "|Execute");

        std::shared_ptr<S3InputSettings> settings = std::make_shared<S3InputSettings>(
            _parameters, _kwParameters, false, query);

        std::shared_ptr<S3Array> array = std::make_shared<S3Array>(
            query, _schema, settings);

        array->readIndex();

        return array;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalS3Input, "s3input", "PhysicalS3Input");

} // end namespace scidb
