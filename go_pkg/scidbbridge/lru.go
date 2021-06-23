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
	"container/list"
	"log"
	"sync"
	"time"
)

// LRUEntry is a single key-value entry in the LRU.
type LRUEntry struct {
	Key   interface{}
	Value interface{}
}

// entry is what gets stored in the LRU items map.
type entry struct {
	element   *list.Element // element from list, Value is the key
	probation bool
	value     interface{}
}

// LRU uses a Segmented LRU (SLRU) cache policy for long term retention.
// An entry starts in the protected segment (capped at 80% capacity).
// When the protected segment is full it evicts into the probationary segment, which may trigger a probationary entry to be discarded.
// This ensures that entries with a small reuse interval (the hottest) are retained and those that are less often reused (the coldest)
// become eligible for eviction. Those evicted into probation segment are signaled via a callback.
type LRU struct {
	capacity        int // total capacity
	protection      int // size of protected cache
	probation       int // size of probation cache
	debug           bool
	items           map[interface{}]*entry
	mu              *sync.Mutex
	name            string
	OnAdd           func(key interface{}, value interface{})
	OnGet           func(key interface{}, value interface{})
	CanRemove       func(key interface{}, value interface{}) bool // Called to check if item can be evicted
	OnRemove        func(key interface{}, value interface{})      // Called after item is evicted
	OnProbation     func(key interface{}, value interface{})      // Called is moved into probation. Not guaranteed to be called before OnRemove
	OnUpdate        func(key interface{}, oldValue interface{}, newValue interface{})
	protected_order *list.List
	probation_order *list.List
}

// NewLRU creates a new LRU cache with the specified capacity.
func NewLRU(name string, capacity int, debug bool) *LRU {
	if capacity <= 0 {
		maxUint := ^uint(0)
		capacity = int(maxUint >> 1)
	}
	protection := int(float64(capacity) * 0.8)
	if protection == 0 {
		protection = 1
	}
	probation := capacity - protection
	o := &LRU{
		capacity:        capacity,
		protection:      protection,
		probation:       probation,
		debug:           debug,
		items:           map[interface{}]*entry{},
		mu:              &sync.Mutex{},
		name:            name,
		protected_order: list.New(),
		probation_order: list.New(),
	}

	if debug {
		log.Printf(
			"LRU | name: %v, capacity: %v, protected: %v, probation: %v, len(items): %v, len(protected): %v, len(probation): %v",
			o.name, o.capacity, o.protection, o.probation, (o.items), o.protected_order.Len(), o.probation_order.Len(),
		)
		go func() {
			for {
				time.Sleep(time.Minute)
				log.Printf(
					"LRU | name: %v, capacity: %v, protected: %v, probation: %v, len(items): %v, len(protected): %v, len(probation): %v",
					o.name, o.capacity, o.protection, o.probation, (o.items), o.protected_order.Len(), o.probation_order.Len(),
				)
			}
		}()
	}
	return o
}

// Get returns the value for a given key.
func (o *LRU) Get(key interface{}) (interface{}, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if e, ok := o.items[key]; ok {
		if e.probation {
			e.probation = false
			e.element = o.protected_order.PushFront(key)
			if oldest_protected := o.protected_order.Back(); oldest_protected != nil {
				o.protected_order.Remove(oldest_protected)
				protected_e := o.items[oldest_protected.Value]
				protected_e.probation = true
				protected_e.element = o.probation_order.PushFront(oldest_protected.Value)
				if o.OnProbation != nil {
					o.OnProbation(oldest_protected.Value, protected_e.value)
				}
			}
		} else {
			o.protected_order.MoveToFront(e.element)
		}
		if o.OnGet != nil {
			o.OnGet(key, e.value)
		}
		return e.value, true
	}
	return nil, false
}

// Has indicates if a given key is present in the cache.
func (o *LRU) Has(key interface{}) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.items[key]
	return ok
}

// Keys returns a slice of the current keys in the cache.
func (o *LRU) Keys() []interface{} {
	o.mu.Lock()
	defer o.mu.Unlock()
	keys := []interface{}{}
	for key := range o.items {
		keys = append(keys, key)
	}
	return keys
}

// Purge removes all entries from the LRU cache.
func (o *LRU) Purge() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for key := range o.items {
		o.remove(key)
	}
}

// Remove removes the value for a given key.
func (o *LRU) Remove(key interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.remove(key)
}

// remove internal implementation.
func (o *LRU) remove(key interface{}) {
	if e, ok := o.items[key]; ok {
		if o.CanRemove != nil {
			tries := 0
			for !o.CanRemove(key, e.value) {
				tries++
				if tries%20000 == 0 {
					elapsed := (tries * 50) / 1000000
					log.Printf("Warning: have been waiting around %v second(s) to remove key: %v from LRU `%v`", elapsed, key, o.name)
				}
				time.Sleep(time.Microsecond * 50)
			}
		}
		if e.probation {
			o.probation_order.Remove(e.element)
		} else {
			o.protected_order.Remove(e.element)
		}
		delete(o.items, key)
		if o.OnRemove != nil {
			o.OnRemove(key, e.value)
		}
	} else {
		if o.debug {
			log.Printf("remove: unknown key %v", key)
		}
	}
}

// RemoveMulti removes multiple entries at one time.
func (o *LRU) RemoveMulti(keys []interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, key := range keys {
		o.remove(key)
	}
}

// Set sets the value for a given key.
func (o *LRU) Set(key interface{}, value interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.set(key, value)
}

// set internal implementation.
func (o *LRU) set(key interface{}, value interface{}) {
	if e, ok := o.items[key]; ok {
		// Update.
		oldValue := e.value
		e.value = value
		if e.probation {
			// move out of probation and into protected
			e.probation = false
			o.probation_order.Remove(e.element)
			e.element = o.protected_order.PushFront(key)
		} else {
			o.protected_order.MoveToFront(e.element)
		}
		if o.OnUpdate != nil {
			o.OnUpdate(key, oldValue, value)
		}
	} else {
		for len(o.items) >= o.capacity {
			// move oldest protected (if any) into probation
			if oldest_protected := o.protected_order.Back(); oldest_protected != nil {
				o.protected_order.Remove(oldest_protected)
				protected_e := o.items[oldest_protected.Value]
				protected_e.probation = true
				protected_e.element = o.probation_order.PushFront(oldest_protected.Value) // Value of element is the key
				if o.OnProbation != nil {
					o.OnProbation(oldest_protected.Value, protected_e.value)
				}
			}
			// evict oldest probation (if any)
			if oldest_probation := o.probation_order.Back(); oldest_probation != nil {
				o.probation_order.Remove(oldest_probation)
				o.remove(oldest_probation.Value)
			}
		}
		// Add.
		element := o.protected_order.PushFront(key)
		e := &entry{element: element, probation: false, value: value}
		o.items[key] = e
		if o.OnAdd != nil {
			o.OnAdd(key, value)
		}
		// Move to probation.
		for o.protected_order.Len() >= o.protection {
			oldest_protected := o.protected_order.Back()
			o.protected_order.Remove(oldest_protected)
			protected_e := o.items[oldest_protected.Value]
			protected_e.probation = true
			protected_e.element = o.probation_order.PushFront(oldest_protected.Value)
			if o.OnProbation != nil {
				o.OnProbation(oldest_protected.Value, protected_e.value)
			}
		}
	}
}

// SetMultiple sets multiple entries at one time.
func (o *LRU) SetMultiple(entries []*LRUEntry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, e := range entries {
		o.set(e.Key, e.Value)
	}
}
