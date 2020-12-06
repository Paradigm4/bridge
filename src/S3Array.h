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

#include <mutex>

#include <array/DelegateArray.h>

#include "S3Common.h"
#include "S3Index.h"


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace scidb {
    class S3InputSettings;       // #include "S3InputSettings.h"
    class S3Driver;              // #include "S3Driver.h"
}
namespace arrow {
    class Array;
    class RecordBatch;
    class RecordBatchReader;
    class ResizableBuffer;
    namespace io {
        class BufferReader;
        class CompressedInputStream;
    }
    namespace util {
        class Codec;
    }
}
// -- End of Forward Declarations


namespace scidb
{

class S3ArrowReader {
public:
    S3ArrowReader(S3Metadata::Compression,
                  std::shared_ptr<const S3Driver>);

    size_t readObject(const std::string &name,
                      bool reuse,
                      std::shared_ptr<arrow::RecordBatch>&);

private:
    const S3Metadata::Compression _compression;

    std::shared_ptr<const S3Driver> _driver;

    std::shared_ptr<arrow::ResizableBuffer> _arrowResizableBuffer;
    std::shared_ptr<arrow::io::BufferReader> _arrowBufferReader;
    std::unique_ptr<arrow::util::Codec> _arrowCodec;
    std::shared_ptr<arrow::io::CompressedInputStream> _arrowCompressedStream;
    std::shared_ptr<arrow::RecordBatchReader> _arrowBatchReader;
};

typedef struct {
    std::list<Coordinates>::iterator lruIt;
    std::shared_ptr<arrow::RecordBatch> arrowBatch;
    size_t arrowSize;
} S3CacheCell;

class S3Cache {
public:
    S3Cache(
        std::shared_ptr<S3ArrowReader>,
        const std::string &path,
        const Dimensions&,
        size_t);

    std::shared_ptr<arrow::RecordBatch> get(Coordinates);

private:
    const std::shared_ptr<S3ArrowReader> _arrowReader;
    const std::string _path;
    const Dimensions _dims;
    size_t _size;
    const size_t _sizeMax;
    std::list<Coordinates> _lru;
    std::unordered_map<Coordinates, S3CacheCell, CoordinatesHash> _mem;
    std::mutex _lock;
};

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
    virtual std::shared_ptr<Query> getQuery();

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

    Coordinates _currPos;
    bool _hasCurrent;
    bool _chunkInitialized;
    S3IndexCont::const_iterator _currIndex;
};

class S3Array : public Array
{
    friend class S3ArrayIterator;
    friend class S3ChunkIterator;
    friend class S3Chunk;

public:
    S3Array(std::shared_ptr<Query> query,
            const ArrayDesc& desc,
            const std::shared_ptr<S3InputSettings> settings);

    virtual ArrayDesc const& getArrayDesc() const;

    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const override;

    /// @see Array::hasInputPipe
    bool hasInputPipe() const override { return false; }

    void readIndex();

private:
    // SciDB members
    std::shared_ptr<Query> _query;
    const ArrayDesc _desc;
    const std::shared_ptr<const S3InputSettings> _settings;

    // S3Bridge members
    std::shared_ptr<const S3Driver> _driver;
    std::shared_ptr<S3ArrowReader> _arrowReader;
    S3Index _index;
    std::unique_ptr<S3Cache> _cache;
};

}

#endif
