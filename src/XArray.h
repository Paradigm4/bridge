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
// -- End of Forward Declarations


namespace scidb {

typedef struct {
    std::list<Coordinates>::iterator lruIt;
    std::shared_ptr<arrow::RecordBatch> arrowBatch;
    size_t arrowSize;
} XCacheCell;

class XCache {
public:
    XCache(const ArrayDesc&,
           const Metadata::Compression,
           std::shared_ptr<const Driver> driver,
           size_t);

    std::shared_ptr<arrow::RecordBatch> get(Coordinates);

private:
    ArrowReader _arrowReader;
    const std::string _path;
    const Dimensions _dims;
    size_t _size;
    const size_t _sizeMax;
    std::list<Coordinates> _lru;
    std::unordered_map<Coordinates, XCacheCell, CoordinatesHash> _mem;
    std::mutex _lock;
};

class XChunk;

class XChunkIterator : public ConstChunkIterator
{
public:
    XChunkIterator(const XChunk&,
                   int iterationMode,
                   std::shared_ptr<arrow::RecordBatch>);

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

    const XChunk& _chunk;
    const size_t _nAtts;
    const size_t _nDims;
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

class XArrayIterator;

class XChunk : public ConstChunk
{
    friend class XChunkIterator;

public:
    XChunk(XArrayIterator&);

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
    XArrayIterator& _arrayIt;
    Coordinates _firstPos;
    Coordinates _lastPos;
    Coordinates _firstPosWithOverlap;
    Coordinates _lastPosWithOverlap;

    std::shared_ptr<arrow::RecordBatch> _arrowBatch;
};

class XArray;

class XArrayIterator : public ConstArrayIterator
{
    friend class XChunk;
    friend class XChunkIterator;

public:
    XArrayIterator(const XArray&, AttributeID);

    ConstChunk const& getChunk() override;
    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(const Coordinates& pos) override;
    void restart() override;

private:
    void _nextChunk();

    const XArray& _array;
    const Dimensions& _dims;
    const AttributeID _attrID;
    const AttributeDesc& _attrDesc;
    const TypeEnum _attrType;
    ArrowReader _arrowReader;
    XChunk _chunk;

    Coordinates _currPos;
    bool _hasCurrent;
    bool _chunkInitialized;
    XIndexStore::const_iterator _currIndex;
};

class XArray : public Array
{
    friend class XArrayIterator;
    friend class XChunk;
    friend class XChunkIterator;

public:
    XArray(const ArrayDesc&,
           std::shared_ptr<Query>,
           std::shared_ptr<const Driver>,
           std::shared_ptr<const XIndex>,
           const Metadata::Compression compression,
           const size_t cacheSize);

    virtual ArrayDesc const& getArrayDesc() const;

    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const override;

    // @see Array::hasInputPipe
    bool hasInputPipe() const override { return false; }

private:
    // SciDB members
    const ArrayDesc _schema;
    std::shared_ptr<Query> _query;

    // Bridge members
    std::shared_ptr<const Driver> _driver;
    std::shared_ptr<const XIndex> _index;
    std::unique_ptr<XCache> _cache;
    const Metadata::Compression _compression;
};

} // namespace scidb

#endif  // XArray
