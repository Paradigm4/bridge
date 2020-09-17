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

/**
 * @file BuildArray.h
 *
 * @brief The implementation of the array iterator for the build operator
 *
 */

#ifndef S3_ARRAY_H_
#define S3_ARRAY_H_

#include <string>
#include <vector>

#include <array/DelegateArray.h>
#include <query/FunctionDescription.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>

#include <arrow/record_batch.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

#include "S3LoadSettings.h"


namespace scidb
{

class S3Array;
class S3ArrayIterator;
class S3ChunkIterator;

class S3Chunk : public ConstChunk
{
friend class S3ChunkIterator;

  public:
    virtual const ArrayDesc& getArrayDesc() const;
    virtual const AttributeDesc& getAttributeDesc() const;
    virtual Coordinates const& getFirstPosition(bool withOverlap) const;
    virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    virtual CompressorType getCompressionMethod() const;
    virtual Array const& getArray() const;

    void setPosition(Coordinates const& pos);

    S3Chunk(S3Array& array, AttributeID attrID);

  private:
    S3Array& _array;
    Coordinates _firstPos;
    Coordinates _lastPos;
    Coordinates _firstPosWithOverlap;
    Coordinates _lastPosWithOverlap;
    AttributeID const _attrID;
    TypeEnum const _attrType;
};

class S3ChunkIterator : public ConstChunkIterator
{
public:
    int getMode() const override;
    bool isEmpty() const override;
    Value const& getItem() override;
    void operator ++() override;
    bool end() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    ConstChunk const& getChunk() override;
    virtual std::shared_ptr<Query> getQuery() { return _query; }

    S3ChunkIterator(S3Array& array,
                    S3Chunk const* chunk,
                    int iterationMode,
                    std::shared_ptr<arrow::Array> arrowArray);

  private:
    int _iterationMode;
    S3Array& _array;
    Coordinates const& _firstPos;
    Coordinates const& _lastPos;
    Coordinates _currPos;
    bool _hasCurrent;

    S3Chunk const* _chunk;
    Value _value;
    Value _trueValue;
    Value _nullValue;
    bool _nullable;
    std::shared_ptr<Query> _query;

    std::shared_ptr<arrow::Array> _arrowArray;
    const int64_t _arrowNullCount;
    const uint8_t* _arrowNullBitmap;
    int64_t _arrowIndex;
};

class S3ArrayIterator : public ConstArrayIterator
{
friend class S3ChunkIterator;

public:
    ConstChunk const& getChunk() override;
    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

    S3ArrayIterator(S3Array& array, AttributeID id);

private:
    void _nextChunk();

    S3Array& _array;
    S3Chunk _chunk;
    Dimensions const& _dims;
    Coordinates _currPos;
    bool _hasCurrent;
    bool _chunkInitialized;
};

class S3Array : public Array
{
friend class S3ArrayIterator;
friend class S3ChunkIterator;
friend class S3Chunk;

public:
    virtual ArrayDesc const& getArrayDesc() const;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const override;

    S3Array(std::shared_ptr<Query>& query,
            ArrayDesc const& desc,
            std::shared_ptr<S3LoadSettings> settings);

    ~S3Array();

    /// @see Array::hasInputPipe
    bool hasInputPipe() const override { return false; }

private:
    const ArrayDesc _desc;
    size_t _nInstances;
    size_t _instanceID;
    std::shared_ptr<S3LoadSettings> _settings;
    const Aws::SDKOptions _awsOptions;
    std::shared_ptr<Aws::S3::S3Client> _awsClient;
    std::shared_ptr<Aws::String> _awsBucketName;
    std::vector<Coordinates> _chunkCoords; // TODO shared_ptr, pass to iterator
};

}

#endif
