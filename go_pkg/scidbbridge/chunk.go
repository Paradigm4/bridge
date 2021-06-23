// Copyright (C) 2020-2021 Paradigm4 Inc.
// All Rights Reserved.
//
// scidbbridge is a plugin for SciDB, an Open Source Array DBMS
// maintained by Paradigm4. See http://www.paradigm4.com/
//
// scidbbridge is free software: you can redistribute it and/or modify
// it under the terms of the AFFERO GNU General Public License as
// published by the Free Software Foundation.
//
// scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
// KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
// NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
// AFFERO GNU General Public License for the complete license terms.
//
// You should have received a copy of the AFFERO GNU General Public
// License along with scidbbridge. If not, see
// <http://www.gnu.org/licenses/agpl-3.0.html>
package scidbbridge

import (
	"fmt"
	"log"
	"sync"

	"github.com/Paradigm4/bridge/go_pkg/scidb"
	"github.com/Paradigm4/gota/dataframe"
	"github.com/Paradigm4/gota/series"

	"github.com/apache/arrow/go/arrow/memory"
)

// Wrapper for SciDB array chunk that is cached in an LRU
type Chunk struct {
	refCount int
	mu       *sync.RWMutex
	array    *Array
	bounds   ChunkBounds // also used as LRU key
	dirty    bool
	df       dataframe.DataFrame // should always be stored sorted
}

// NewChunkLRU creates the LRU for an Array's Chunks
func NewChunkLRU(name string, capacity int, debug bool) *LRU {
	if capacity < 1 {
		capacity = 1
	}
	// set up LRU
	lru := NewLRU(name, capacity, debug)
	lru.OnGet = onGetChunk
	lru.OnProbation = onProbationChunk
	lru.OnRemove = onRemoveChunk
	lru.CanRemove = canRemoveChunk
	return lru
}

// Returns a Chunk as a DataFrame from cache, if not in cache attempts to read it. Failing that it returns an empty DataFrame.
// Caller needs to call Release when done
// Caller needs to hold Array's Lock
func (a *Array) getChunk(cb ChunkBounds) (*Chunk, error) {
	if cb.array != a {
		return nil, fmt.Errorf("bounds do NOT belong to this array")
	}
	if o, ok := a.lru.Get(cb.suffix); ok { // onGet updates refCount
		c := o.(*Chunk)
		return c, nil
	}
	// not cached see if it exists and if so get it
	if a.driver.Exists(fmt.Sprintf("chunks/%s", cb.suffix)) {
		c, err := readChunk(cb, a.alloc) // refCount is now 1
		if err != nil {
			return nil, fmt.Errorf("[GetChunk] %v", err)
		}
		// add to cache
		a.lru.Set(cb.suffix, c)
		return c, nil
	}

	// missing so make an empty df
	s := make([]series.Series, a.schema.NumAttributes()+a.schema.NumDimensions())
	numAttrs := a.schema.NumAttributes()
	for i := 0; i < numAttrs; i++ {
		if attr, ok := a.schema.GetAttribute(i); ok {
			seriesType, err := ScidbToSeriesType(attr.TypeName)
			if err != nil {
				return nil, err
			}
			s[i] = series.New(nil, seriesType, attr.Name)
		} else {
			return nil, fmt.Errorf("error getting attribute %v", i)
		}
	}
	numDims := a.schema.NumDimensions()
	for i := 0; i < numDims; i++ {
		if dim, ok := a.schema.GetDimension(i); ok {
			s[numAttrs+i] = series.New(nil, series.Int, dim.Name)
		} else {
			return nil, fmt.Errorf("error getting dimension %v", i)
		}
	}
	df := dataframe.New(s...)
	c := &Chunk{array: a, df: df, bounds: cb, refCount: 1, mu: &sync.RWMutex{}, dirty: false}
	a.lru.Set(cb.suffix, c)
	return c, df.Err
}

// handles locks and releasing of Chunk
func (a *Array) merge(c BoundedDataFrame, keys []dataframe.Order) error {
	if c.df.Err != nil {
		return c.df.Err
	}

	// missing any attributes in the new chunk c?
	cnames := c.df.Names()
	cattrs := []series.Series{} // any missing attr/cols
	numAttrs := a.schema.NumAttributes()
	for i := 0; i < numAttrs; i++ {
		if attr, ok := a.schema.GetAttribute(i); ok {
			if findInStringSlice(attr.Name, cnames) < 0 {
				switch attr.TypeName {
				case scidb.Bool:
					cattrs = append(cattrs, series.NewDefault(nil, attr.DefaultValue, series.Bool, attr.Name, c.df.Nrow()))
				case scidb.Int8, scidb.Int16, scidb.Int32, scidb.Int64:
					cattrs = append(cattrs, series.NewDefault(nil, attr.DefaultValue, series.Int, attr.Name, c.df.Nrow()))
				case scidb.Uint8, scidb.Uint16, scidb.Uint32, scidb.Uint64:
					cattrs = append(cattrs, series.NewDefault(nil, attr.DefaultValue, series.Uint, attr.Name, c.df.Nrow()))
				case scidb.Float32, scidb.Float64:
					cattrs = append(cattrs, series.NewDefault(nil, attr.DefaultValue, series.Float64, attr.Name, c.df.Nrow()))
				case scidb.String:
					cattrs = append(cattrs, series.NewDefault(nil, attr.DefaultValue, series.String, attr.Name, c.df.Nrow()))
				default:
					return fmt.Errorf("unsupported type for attribute %v: %v", attr.Name, attr.TypeName)
				}
			}
		} else {
			return fmt.Errorf("error getting attribute %v", i)
		}
	}
	if len(cattrs) > 0 {
		c.df = dataframe.New(cattrs...).CBind(c.df)
	}

	// Now try to get the chunk and update it
	b, err := a.GetChunk(c.cb)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer func() {
		b.mu.Unlock()
		b.Release() // this also locks
	}()
	if b.df.Nrow() > 0 {
		c.df = b.df.Merge(c.df, keys...) // merge new data into existing (replacing or appending)
		if err != nil {
			return err
		}
	} // else just use c.df as is

	b.df = c.df
	b.dirty = true
	return nil
}

func (a *Array) updateChunk(c BoundedDataFrame) error {
	// Try to get the chunk and update it
	b, err := a.GetChunk(c.cb) // mutex on Array
	if err != nil {
		return err
	}
	defer b.Release()
	b.mu.Lock()
	defer b.mu.Unlock()

	b.df = c.df
	b.dirty = true
	return b.df.Err
}

// reads in a Chunk using the driver. Will error if it does not exist
// Caller needs to call Release when done with this
func readChunk(cb ChunkBounds, mem memory.Allocator) (*Chunk, error) {
	compression := cb.array.metadata["compression"]
	url_path := fmt.Sprintf("%s/chunks/%s", cb.array.driver.url.String(), cb.suffix)
	reader, err := CreateArrayReader(url_path, compression, mem)
	if err != nil {
		return nil, fmt.Errorf("[newChunk] %s: %v", url_path, err)
	}
	tbl, err := ReadAllRecords(reader, mem)
	if err != nil {
		return nil, fmt.Errorf("[newChunk] %s: %v", url_path, err)
	}
	df := dataframe.TableToDataframe(tbl)
	if df.Err != nil {
		return nil, fmt.Errorf("[newChunk] %s: %v", url_path, err)
	}
	tbl.Release()
	cc := &Chunk{array: cb.array, bounds: cb, df: df, refCount: 1, mu: &sync.RWMutex{}, dirty: false}
	return cc, df.Err
}

// Returns a copy of the Chunk's DataFrame
func (o *Chunk) GetDataFrame() dataframe.DataFrame {
	o.mu.RLock()
	defer o.mu.RUnlock()
	df := dataframe.DataFrame{Err: fmt.Errorf("Empty")}
	if o.array != nil {
		return o.df.Copy()
	} else {
		return df
	}
}

// Flush a Chunk's array
func (o *Chunk) Flush() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.df.Err != nil {
		return o.df.Err
	}
	o.flush()
	return nil
}

// Close attempts to close the chunk (save if dirty) and remove from cache. If outstanding reference this will fail
func (o *Chunk) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.refCount != 0 {
		return fmt.Errorf("[Chunk.Close] attempting to close chunk with outstanding references: %d", o.refCount)
	}
	if o.df.Err != nil {
		return o.df.Err
	}
	o.close()
	return nil
}

func (o *Chunk) Retain() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.refCount++
	return o.refCount
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the Chunk can be freed.
// If the underlining DataFrame is dirty, it will eventually be saved (possible with errors)
// Release may be called simultaneously from multiple goroutines.
func (o *Chunk) Release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.refCount == 0 {
		log.Fatalf("[Chunk.Release] Attempting to release Chunk that has no references")
	}
	o.refCount--
}

// onGetChunk increases the reference count by 1.
// onGetChunk may be called simultaneously from multiple goroutines.

func onGetChunk(key interface{}, value interface{}) {
	skey := key.(string)
	if cc, ok := value.(*Chunk); ok {
		if skey != cc.bounds.suffix {
			log.Fatalf("[Chunk.onGetChunk] Chunk with wrong key: %v vs %v", skey, cc.bounds.suffix)
		}
		cc.mu.Lock()
		defer cc.mu.Unlock()
		cc.refCount++
	} else {
		log.Fatalf("[Chunk.onGetChunk] LRU value not a Chunk: %T", value)
	}
}

func canRemoveChunk(key interface{}, value interface{}) bool {
	skey := key.(string)
	if cc, ok := value.(*Chunk); ok {
		if skey != cc.bounds.suffix {
			log.Fatalf("[Chunk.canRemoveChunk] Chunk with wrong key: %v vs %v", skey, cc.bounds.suffix)
		}
		cc.mu.RLock()
		defer cc.mu.RUnlock()
		return cc.refCount == 0 // only ref should be LRU
	} else {
		log.Fatalf("[Chunk.canRemoveChunk] LRU value not a Chunk: %T", value)
	}
	return true
}

// onProbationChunk callback which might get called before onRemoveChunk (and canRemoveChunk)
func onProbationChunk(key interface{}, value interface{}) {
	skey := key.(string)
	if cc, ok := value.(*Chunk); ok {
		if skey != cc.bounds.suffix {
			log.Fatalf("[Chunk.onProbationChunk] Chunk with wrong key: %v vs %v", skey, cc.bounds.suffix)
		}
		go func() {
			cc.mu.Lock()
			defer cc.mu.Unlock()
			cc.flush()
		}()
	}
}

// onRemoveChunk callback for LRU when a cacheChunk is removed
// this will flush and close the Cache, it will panic if refCount != 0
func onRemoveChunk(key interface{}, value interface{}) {
	// make sure that the key points to the correct chunk
	skey := key.(string)
	if cc, ok := value.(*Chunk); ok {
		if skey != cc.bounds.suffix {
			log.Fatalf("[Chunk.onRemoveChunk] removing Chunk with wrong key: %v vs %v", skey, cc.bounds.suffix)
		}
		cc.mu.Lock()
		defer cc.mu.Unlock()
		if cc.refCount != 0 {
			log.Fatalf("[Chunk.onRemoveChunk] removing chunk with outstanding references: %d", cc.refCount)
		}
		cc.close()
	} else {
		log.Fatalf("[Chunk.onRemoveChunk] LRU value not a Chunk: %T", value)
	}
}

// only call once WLock held
// only flushes if dirty
func (o *Chunk) flush() {
	if o.dirty && o.df.Err == nil {
		err := o.array.writeDataframeChunk(o.df, o.bounds.suffix)
		if err != nil {
			log.Fatal(err)
		}
		o.dirty = false
	}
}

// only call once WLock held
// this first flushes then cleans up
func (o *Chunk) close() {
	o.flush()
	o.refCount = 0
	o.array = nil
	o.bounds = ChunkBounds{}
	o.dirty = false
	o.df = dataframe.DataFrame{Err: fmt.Errorf("Empty")}
}
