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
	"strings"

	"github.com/Paradigm4/bridge/go_pkg/scidb"
	"github.com/apache/arrow/go/arrow/memory"
)

type config struct {
	attribute   string
	namespace   string
	schema      *scidb.Schema
	version     string
	compression string
	format      string
	index_split int
	alloc       memory.Allocator
	lru         int // capacity, default 0 (no caching)
	err         error
}

func newConfig(opts ...Option) *config {
	cfg := &config{
		alloc:       memory.NewGoAllocator(),
		attribute:   "ALL",
		namespace:   "public",
		compression: "none",
		format:      "arrow",
		version:     "1",
		index_split: 100000,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// check user supplied configs
	if cfg.compression == "" {
		cfg.compression = "none"
	}
	cfg.compression = strings.ToLower(cfg.compression)
	if findInStringSlice(cfg.compression, []string{"none", "gzip", "zstd", "lz4"}) < 0 {
		cfg.err = fmt.Errorf("unknown/unsupported compression: %v", cfg.compression)
		return cfg
	}
	if cfg.namespace == "" {
		cfg.err = fmt.Errorf("namespace cannot be empty")
		return cfg
	}
	// schema can be empty if using for opening an existing array
	if cfg.alloc == nil {
		cfg.alloc = memory.NewGoAllocator()
	}
	if cfg.index_split <= 0 { // is there a min/max we should limit?
		cfg.err = fmt.Errorf("IndexSplit cannot be zero or less")
		return cfg
	}
	if cfg.lru <= 0 {
		cfg.lru = 1
	}
	// format should only be "arrow"
	return cfg
}

func (cfg *config) buildMetadata() map[string]string {
	if cfg.err != nil {
		return map[string]string{"err": cfg.err.Error()}
	}
	md := map[string]string{
		"attribute":   cfg.attribute,
		"namespace":   cfg.namespace,
		"compression": cfg.compression,
		"schema":      cfg.schema.String(),
		"format":      cfg.format,
		"version":     cfg.version,
		"index_split": fmt.Sprintf("%d", cfg.index_split),
	}
	return md
}

// Option is a functional option to configure opening or creating SciDB Arrays.
type Option func(*config)

// WithSchema specifies the Arrow schema to be used for reading or writing.
func WithSchema(schema *scidb.Schema) Option {
	return func(cfg *config) {
		cfg.schema = schema
	}
}

func WithNamespace(namespace string) Option {
	return func(cfg *config) {
		cfg.namespace = namespace
	}
}

// WithCompression specifies compression used when writing arrays; cannot change once Array is created
func WithCompression(compression string) Option {
	return func(cfg *config) {
		cfg.compression = compression
	}
}

func WithIndexSplit(split int) Option {
	return func(cfg *config) {
		cfg.index_split = split
	}
}

// WithAllocator specifies the Arrow memory allocator used while building Arrow Arrays.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg *config) {
		cfg.alloc = mem
	}
}

// WithLRU specifies using an LRU (cache)
func WithLRU(capacity int) Option {
	return func(cfg *config) {
		cfg.lru = capacity
	}
}

func AsParquet() Option {
	return func(cfg *config) {
		cfg.format = "parquet"
	}
}
