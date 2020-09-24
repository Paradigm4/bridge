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

#ifndef S3_ARRAY_H_
#define S3_ARRAY_H_

#include <array/DelegateArray.h>

#include <aws/core/Aws.h>


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace scidb {
    class S3LoadSettings;       // #include "S3LoadSettings.h"
}
namespace arrow {
    class Array;
    class RecordBatch;
    class RecordBatchReader;
    namespace io {
        class BufferReader;
    }
}
namespace Aws {
    namespace S3 {
        class S3Client;         // #include <aws/s3/S3Client.h>
    }
}
// -- End of Forward Declarations


namespace scidb
{

class S3Array;
class S3ArrayIterator;
class S3Chunk;

class S3ChunkIterator : public ConstChunkIterator
{
public:
    S3ChunkIterator(const S3Array& array,
                    S3Chunk const* chunk,
                    int iterationMode,
                    std::shared_ptr<arrow::RecordBatch> arrowBatch);

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

  private:
    int64_t getCoord(size_t dim, int64_t index);

    const S3Array& _array;
    const size_t _nAtts;
    const size_t _nDims;
    const S3Chunk* const _chunk;
    const int _iterationMode;

    Coordinates _firstPos;
    Coordinates _lastPos;
    Coordinates _currPos;

    std::shared_ptr<Query> _query;

    Value _value;
    Value _trueValue;
    Value _nullValue;
    const bool _nullable;

    std::shared_ptr<const arrow::RecordBatch> _arrowBatch;
    const int64_t _arrowLength;

    std::shared_ptr<const arrow::Array> _arrowArray;
    int64_t _arrowNullCount;
    const uint8_t* _arrowNullBitmap;

    int64_t _arrowIndex;
    bool _hasCurrent;
};

class S3Chunk : public ConstChunk
{
friend class S3ChunkIterator;

  public:
    S3Chunk(S3Array& array, AttributeID attrID);
    ~S3Chunk();

    virtual const ArrayDesc& getArrayDesc() const;
    virtual const AttributeDesc& getAttributeDesc() const;
    virtual Coordinates const& getFirstPosition(bool withOverlap) const;
    virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    virtual CompressorType getCompressionMethod() const;
    virtual Array const& getArray() const;

    void setPosition(Coordinates const& pos);
    void download();

  private:
    const S3Array& _array;
    const Dimensions _dims;
    const size_t _nDims;
    Coordinates _firstPos;
    Coordinates _lastPos;
    Coordinates _firstPosWithOverlap;
    Coordinates _lastPosWithOverlap;
    const AttributeID _attrID;
    const AttributeDesc& _attrDesc;
    const TypeEnum _attrType;

    long long _arrowSizeAlloc;
    char *_arrowData;
    std::shared_ptr<arrow::io::BufferReader> _arrowBufferReader;
    std::shared_ptr<arrow::RecordBatchReader> _arrowBatchReader;
    std::shared_ptr<arrow::RecordBatch> _arrowBatch;
};

class S3ArrayIterator : public ConstArrayIterator
{
public:
    S3ArrayIterator(S3Array& array, AttributeID attrID);

    ConstChunk const& getChunk() override;
    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

private:
    void _nextChunk();

    const S3Array& _array;
    const AttributeID _attrID;
    const Dimensions _dims;
    S3Chunk _chunk;
    const std::vector<Coordinates>::const_iterator _endChunkCoords;

    Coordinates _currPos;
    bool _hasCurrent;
    bool _chunkInitialized;
    std::vector<Coordinates>::const_iterator _currChunkCoords;
};

class S3Array : public Array
{
friend class S3ArrayIterator;
friend class S3ChunkIterator;
friend class S3Chunk;

public:
    S3Array(std::shared_ptr<Query>& query,
            ArrayDesc const& desc,
            std::shared_ptr<S3LoadSettings> settings);

    ~S3Array();

    virtual ArrayDesc const& getArrayDesc() const;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const override;

    /// @see Array::hasInputPipe
    bool hasInputPipe() const override { return false; }

private:
    const size_t _nInstances;
    const size_t _instanceID;
    const ArrayDesc _desc;
    const std::shared_ptr<const S3LoadSettings> _settings;

    const Aws::SDKOptions _awsOptions;
    std::shared_ptr<Aws::S3::S3Client> _awsClient;
    std::shared_ptr<Aws::String> _awsBucketName;
    std::shared_ptr<std::vector<Coordinates> > _chunkCoords;
};

}

#endif
