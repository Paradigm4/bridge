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

#ifndef X_ARRAY_H_
#define X_ARRAY_H_

#include <mutex>

// SciDB
#include <array/DelegateArray.h>

#include "Driver.h"
#include "XIndex.h"


// Forward Declarastions to avoid including full headers - speed-up
// compilation
namespace scidb {
    class XInputSettings;       // #include "XInputSettings.h"
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


namespace scidb {

class ArrowReader {
public:
    ArrowReader(Metadata::Compression,
                std::shared_ptr<const Driver>);

    size_t readObject(const std::string &name,
                      bool reuse,
                      std::shared_ptr<arrow::RecordBatch>&);

private:
    const Metadata::Compression _compression;

    std::shared_ptr<const Driver> _driver;

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
} XCacheCell;

class XCache {
public:
    XCache(std::shared_ptr<ArrowReader>,
           const std::string &path,
           const Dimensions&,
           size_t);

    std::shared_ptr<arrow::RecordBatch> get(Coordinates);

private:
    const std::shared_ptr<ArrowReader> _arrowReader;
    const std::string _path;
    const Dimensions _dims;
    size_t _size;
    const size_t _sizeMax;
    std::list<Coordinates> _lru;
    std::unordered_map<Coordinates, XCacheCell, CoordinatesHash> _mem;
    std::mutex _lock;
};

class XArray;
class XArrayIterator;
class XChunk;

class XChunkIterator : public ConstChunkIterator
{
public:
    XChunkIterator(const XArray& array,
                   XChunk const* chunk,
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

    const XArray& _array;
    const size_t _nAtts;
    const size_t _nDims;
    const XChunk* const _chunk;
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

class XChunk : public ConstChunk
{
    friend class XChunkIterator;

public:
    XChunk(XArray& array, AttributeID attrID);

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
    const XArray& _array;
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

class XArrayIterator : public ConstArrayIterator
{
public:
    XArrayIterator(XArray& array, AttributeID attrID);

    ConstChunk const& getChunk() override;
    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

private:
    void _nextChunk();

    const XArray& _array;
    const AttributeID _attrID;
    const Dimensions _dims;
    XChunk _chunk;

    Coordinates _currPos;
    bool _hasCurrent;
    bool _chunkInitialized;
    XIndexCont::const_iterator _currIndex;
};

class XArray : public Array
{
    friend class XArrayIterator;
    friend class XChunkIterator;
    friend class XChunk;

public:
    XArray(std::shared_ptr<Query> query,
           const ArrayDesc& desc,
           const std::shared_ptr<XInputSettings> settings);

    virtual ArrayDesc const& getArrayDesc() const;

    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const override;

    /// @see Array::hasInputPipe
    bool hasInputPipe() const override { return false; }

    void readIndex();

private:
    // SciDB members
    std::shared_ptr<Query> _query;
    const ArrayDesc _desc;
    const std::shared_ptr<const XInputSettings> _settings;

    // XBridge members
    std::shared_ptr<const Driver> _driver;
    std::shared_ptr<ArrowReader> _arrowReader;
    XIndex _index;
    std::unique_ptr<XCache> _cache;
};

} // namespace scidb

#endif  // XArray
