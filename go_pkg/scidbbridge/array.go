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
	"net/url"
	"strings"
	"sync"

	"github.com/Paradigm4/bridge/go_pkg/scidb"
	"github.com/Paradigm4/gota/dataframe"
	"github.com/Paradigm4/gota/series"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

// Wrapper for SciDB array stored externally
type Array struct {
	driver   *Driver
	index    *ArrayIndex
	schema   *scidb.Schema // must be insync with the string version in metadata
	metadata map[string]string
	alloc    memory.Allocator
	lru      *LRU
	mu       *sync.Mutex
	err      error
}

// An index of all the known chunks for an Array
type ArrayIndex struct {
	array *Array
	df    dataframe.DataFrame
}

// Bounds for a Chunk
// inclusive bounds [l, u]; handles "unbounded" dims and chunk_lengths
type ChunkBounds struct {
	array  *Array
	lower  []int64
	upper  []int64
	suffix string // suffix for appending to array URL: "c_0_1"
}

type BoundedDataFrame struct {
	df dataframe.DataFrame
	cb ChunkBounds
}

func (b BoundedDataFrame) GetChunkBounds() ChunkBounds {
	return b.cb
}

func findInStringSlice(str string, s []string) int {
	for i, e := range s {
		if e == str {
			return i
		}
	}
	return -1
}

func NewArray(urlStr string, opts ...Option) (*Array, error) {
	if len(urlStr) == 0 {
		return nil, fmt.Errorf("`urlStr` cannot be empty")
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	driver, err := NewDriver(u)
	if err != nil {
		return nil, err
	}

	if driver.ArrayExists() {
		return nil, fmt.Errorf("array already exists")
	}

	cfg := newConfig(opts...)
	if cfg.err != nil {
		return nil, cfg.err
	}
	if cfg.schema == nil {
		return nil, fmt.Errorf("require a SciDB Schema when creating a new array")
	}
	a := &Array{
		driver:   driver,
		schema:   cfg.schema,
		metadata: cfg.buildMetadata(),
		alloc:    cfg.alloc,
		mu:       &sync.Mutex{},
	}
	err = a.validateMetadata()
	if err != nil {
		return nil, err
	}
	a.lru = NewChunkLRU(urlStr, cfg.lru, false)

	// create array directory
	err = driver.InitializeArray(a)
	if err != nil {
		return nil, err
	}

	return a, nil
}

// Initialize from given url
// If cache is greater than 0, then n-chunks will be cached and writes will be lazy (must call Flush() before GC'ed)
func OpenArray(urlStr string, opts ...Option) (*Array, error) {
	if len(urlStr) == 0 {
		return nil, fmt.Errorf("`urlStr` cannot be empty")
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	driver, err := NewDriver(u)
	if err != nil {
		return nil, err
	}
	if !driver.ArrayExists() {
		return nil, fmt.Errorf("Array either doesn't exist or is malformed")
	}
	cfg := newConfig(opts...)
	if cfg.err != nil {
		return nil, cfg.err
	}
	a := &Array{driver: driver,
		alloc: cfg.alloc,
		mu:    &sync.Mutex{},
	}

	// attempt to read metadata
	metadata, err := a.driver.ReadMetadata()
	if err != nil {
		return nil, err
	}
	a.metadata, err = metadataFromString(metadata)
	if err != nil {
		return nil, fmt.Errorf("bad metadata: %v", err)
	}
	err = a.validateMetadata()
	if err != nil {
		return nil, err
	}
	schema, err := scidb.SchemaFromString(a.metadata["schema"])
	if err != nil {
		return nil, err
	}
	a.schema = schema
	// check if a supplied schema matches the arrays
	if cfg.schema != nil && !cfg.schema.Equals(*schema) {
		return nil, fmt.Errorf("supplied schema does not match arrays `%v`", schema)
	}
	a.lru = NewChunkLRU(urlStr, cfg.lru, false)

	return a, nil
}

func (a *Array) validateMetadata() error {
	if a.metadata == nil || len(a.metadata) == 0 {
		return fmt.Errorf("missing metadata")
	}
	// check format
	if str, ok := a.metadata["format"]; ok {
		switch strings.ToLower(str) {
		case "arrow":
		default:
			return fmt.Errorf("expecting arrow format")
		}
	} else {
		return fmt.Errorf("metadata is missing `format`")
	}
	// get schema
	if str, ok := a.metadata["schema"]; ok {
		schema, err := scidb.SchemaFromString(str)
		if err != nil {
			return fmt.Errorf("bad schema: %v", err)
		}
		a.schema = schema
	} else {
		return fmt.Errorf("metadata is missing `schema`")
	}
	return nil
}

func metadataFromString(input string) (map[string]string, error) {
	rs := map[string]string{}
	for i, s := range strings.Split(input, "\n") {
		s = strings.TrimSpace(s)
		if len(s) == 0 {
			continue
		}
		d := strings.Split(s, "\t")
		if len(d) != 2 {
			return rs, fmt.Errorf("line %d is not a a key-value pair: %s", i, s)
		}
		rs[d[0]] = d[1]
	}
	if _, ok := rs["compression"]; !ok {
		rs["compression"] = "none"
	}
	return rs, nil
}

func metadataToString(metadata map[string]string) string {
	rs := []string{}
	for k, v := range metadata {
		rs = append(rs, fmt.Sprintf("%v\t%v", k, v))
	}
	return strings.Join(rs, "\n")
}

func ScidbToSeriesType(typeName scidb.TypeName) (series.Type, error) {
	switch typeName {
	case scidb.Bool:
		return series.Bool, nil
	case scidb.Int8, scidb.Int16, scidb.Int32, scidb.Int64:
		return series.Int, nil
	case scidb.Uint8, scidb.Uint16, scidb.Uint32, scidb.Uint64:
		return series.Uint, nil
	case scidb.Float32, scidb.Float64:
		return series.Float64, nil
	case scidb.String:
		return series.String, nil
	default:
		return series.Bool, fmt.Errorf("unsupported type: %v", typeName)
	}
}

func (a *Array) GetDriver() *Driver {
	return a.driver
}

func (a *Array) Delete() error {
	if a.err != nil {
		return a.err
	}
	if a.lru != nil {
		a.lru.Purge() // this will attempt to save first
	}
	a.err = fmt.Errorf("deleted")
	return a.driver.DeleteArray()
}

// Flushes any lazy changes to the underlining driver storage and clears the local cache
// Warning: this will overwrite any existing data
func (a *Array) Flush() error {
	if a.err != nil {
		return a.err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, k := range a.lru.Keys() {
		if err := a.flushChunk(k.(string)); err != nil {
			return err
		}
	}
	return nil
}

func (a *Array) flushChunk(k string) error {
	if o, ok := a.lru.Get(k); ok {
		if c, ok := o.(*Chunk); ok {
			defer c.Release()
			return c.Flush()
		}
	}
	return nil
}

func (a *Array) Close() error {
	if a.err != nil {
		return a.err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	a.lru.Purge() // this will attempt to save, chunks shouldn't try to lock the array!

	// did we encounter an error while saving?
	if a.err != nil {
		return a.err
	}
	a.err = fmt.Errorf("closed")
	return nil
}

// Returns Array's SciDB Schema
func (a *Array) GetSchema() *scidb.Schema {
	if a.schema == nil {
		schema, err := scidb.SchemaFromString(a.metadata["schema"])
		if err != nil {
			return nil
		}
		a.schema = schema
	}
	return a.schema
}

// Returns the attributes and dimension series.Types
func (a *Array) GetSchemaSeriesTypes() (map[string]series.Type, error) {
	coltypes := make(map[string]series.Type)
	schema := a.GetSchema()
	numAttrs := schema.NumAttributes()
	for i := 0; i < numAttrs; i++ {
		attr, _ := schema.GetAttribute(i)
		seriesType, err := ScidbToSeriesType(attr.TypeName)
		if err != nil {
			return coltypes, err
		}
		coltypes[attr.Name] = seriesType
	}
	for i := 0; i < schema.NumDimensions(); i++ {
		dim, _ := schema.GetDimension(i)
		coltypes[dim.Name] = series.Int
	}
	return coltypes, nil
}

// Check if the given DataFrame has all the Dimensions
// This doesn't check if they are in the correct order or have valid values
func (a *Array) DataframeHasDimensions(df dataframe.DataFrame) bool {
	nd := a.schema.NumDimensions()
	colnames := df.Names()
	for i := 0; i < nd; i++ {
		d, _ := a.schema.GetDimension(i)
		if findInStringSlice(d.Name, colnames) < 0 {
			return false
		}
	}
	return true
}

// Returns a Chunk as a DataFrame, caller needs to call Release when done!
func (a *Array) GetChunk(cb ChunkBounds) (*Chunk, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.getChunk(cb)
}

// Merge (sorted-merge join) replaces the matching rows (based on the dimensions) or inserts them in their respective chunks.
// This will error if any of the dimensions are out of bounds of the schema
func (a *Array) Merge(df dataframe.DataFrame) error {
	if !a.DataframeHasDimensions(df) {
		return fmt.Errorf("[Array.Merge] DataFrame is missing dimensions")
	}
	// only keep columns that have attrs or dims
	extraCols := []string{}
	for _, n := range df.Names() {
		if _, oka := a.schema.GetAttributeByName(n); !oka {
			if _, okd := a.schema.GetDimensionByName(n); !okd {
				extraCols = append(extraCols, n)
			}
		}
	}

	if len(extraCols) > 0 {
		return fmt.Errorf("[Array.Merge] DataFrame has additional columns not in the schema: %v", extraCols)
		/*

			// only do this if we have extra cols because this results in a copy
			for i := 0; i < a.schema.NumDimensions(); i++ {
				d, _ := a.schema.Dimension(i)
				attrs = append(attrs, d.Name)
			}
			df = df.Select(attrs)
		*/
	}
	// TODO: is df valid? Does is have duplicate keys (dim values), need this feature in gota/dataframe
	df = a.SortDataFrame(df)
	if df.Err != nil {
		return df.Err
	}

	splits, err := a.SplitDataFrame(df)
	if err != nil {
		return err
	}

	ndims := a.schema.NumDimensions()
	keys := make([]dataframe.Order, ndims)
	for i := 0; i < ndims; i++ {
		if d, ok := a.schema.GetDimension(i); ok {
			keys[i] = dataframe.Sort(d.Name)
		} else {
			return fmt.Errorf("error getting dimension %d", i)
		}
	}

	// merge
	for _, c := range splits {
		if err := a.merge(c, keys); err != nil {
			return err
		}
	}

	return nil
}

// This replaces the existing chunk with the values given
// This will error if any rows are out of bounds of the chunk
func (a *Array) Update(c BoundedDataFrame) error {
	if !a.DataframeHasDimensions(c.df) {
		return fmt.Errorf("[Array.UpdateChunk] DataFrame is missing dimensions")
	}
	if c.df.Nrow() == 0 {
		return nil
	}

	// only keep columns that have attrs or dims
	extraCols := []string{}
	for _, n := range c.df.Names() {
		if _, oka := a.schema.GetAttributeByName(n); !oka {
			if _, okd := a.schema.GetDimensionByName(n); !okd {
				extraCols = append(extraCols, n)
			}
		}
	}

	if len(extraCols) > 0 {
		return fmt.Errorf("DataFrame has additional columns not in the schema: %v", extraCols)
	}
	return a.updateChunk(c)
}

// SortDataFrame sorts the DataFrame by dims and check dims are within bounds of the Array
func (a *Array) SortDataFrame(df dataframe.DataFrame) dataframe.DataFrame {
	ndims := a.schema.NumDimensions()
	order := []dataframe.Order{}
	for i := 0; i < ndims; i++ {
		dim, _ := a.schema.GetDimension(i)
		order = append(order, dataframe.Sort(dim.Name))
		if df.Col(dim.Name).HasInvalid() {
			return dataframe.DataFrame{Err: fmt.Errorf("dimension %s has invalid values which are not allowed", dim.Name)}
		}
	}
	return df.Arrange(order...) // returns a copy which isn't ideal
}

//SplitDataFrame  splits a sorted DataFrame by dimensions with number of rows > 0
func (a *Array) SplitDataFrame(df dataframe.DataFrame) ([]BoundedDataFrame, error) {
	bounds, err := a.getDataframeBounds(df) // lowerBounds, upperBounds
	if err != nil {
		return nil, err
	}
	splits := []BoundedDataFrame{}
	if bounds[0].suffix == bounds[1].suffix {
		// all in same chunk
		splits = append(splits, BoundedDataFrame{df: df, cb: bounds[0]})
	} else {
		// multiple chunks
		ndims := a.schema.NumDimensions()
		coords := make([]int64, ndims)
		indexes := []int64{}
		b := bounds[0]
		nrow := df.Nrow()
		for i := 0; i < nrow; i++ {
			for d := 0; d < ndims; d++ {
				dim, _ := a.schema.GetDimension(d)
				elem := df.Val(i, dim.Name)
				v, err := elem.Int()
				if err != nil {
					return nil, fmt.Errorf("invalid value for dimension %v at row %d", dim.Name, i)
				}
				coords[d] = v
			}
			if b.InBounds(coords...) {
				indexes = append(indexes, int64(i))
			} else {
				// slice up and start over
				slice := df.Subset(series.Ints(indexes))
				if slice.Err != nil {
					return nil, fmt.Errorf("couldn't subset DataFrame with bounds of %v", b)
				}
				splits = append(splits, BoundedDataFrame{df: slice, cb: b})
				indexes = []int64{int64(i)}
				b, err = a.CoordsToChunkBounds(coords...)
				if err != nil {
					return nil, fmt.Errorf("couldn't get bounds: %v", err)
				}
			}
		}
		if len(indexes) > 0 {
			slice := df.Subset(series.Ints(indexes))
			if slice.Err != nil {
				return nil, fmt.Errorf("couldn't subset DataFrame with bounds of %v", b)
			}
			splits = append(splits, BoundedDataFrame{df: slice, cb: b})
		}
	}
	return splits, nil
}

// Returns the lower (top row) and upper (bottom row) ChunkBounds
// Assumes df is sorted by dimensions and Nrows > 0
func (a *Array) getDataframeBounds(df dataframe.DataFrame) ([]ChunkBounds, error) {
	if df.Nrow() > 0 {
		if !a.DataframeHasDimensions(df) {
			return nil, fmt.Errorf("DataFrame is missing dimensions")
		}
		ndims := a.schema.NumDimensions()
		nrow := df.Nrow()
		lowerCoords := make([]int64, ndims)
		upperCoords := make([]int64, ndims)
		for i := 0; i < ndims; i++ {
			dim, _ := a.schema.GetDimension(i)
			elem := df.Val(0, dim.Name)
			if elem == nil {
				return nil, fmt.Errorf("invalid value, nil, for first element of dimension %v", dim.Name)
			}
			v, err := elem.Int()
			if err != nil {
				return nil, fmt.Errorf("invalid value for first element of dimension %v", dim.Name)
			}
			lowerCoords[i] = v
			elem = df.Val(nrow-1, dim.Name)
			if elem == nil {
				return nil, fmt.Errorf("invalid value, nil, for last element (%d) of dimension %v ", (nrow - 1), dim.Name)
			}
			v, err = elem.Int()
			if err != nil {
				return nil, fmt.Errorf("invalid value for last element (%d) of dimension %v: %v", (nrow - 1), dim.Name, err)
			}
			upperCoords[i] = v
		}
		lowerBounds, err := a.CoordsToChunkBounds(lowerCoords...)
		if err != nil {
			return nil, err
		}
		upperBounds, err := a.CoordsToChunkBounds(upperCoords...)
		if err != nil {
			return nil, err
		}
		return []ChunkBounds{lowerBounds, upperBounds}, nil
	}
	return nil, fmt.Errorf("no rows in DataFrame")

}

// validate df outside of this (see UpdateChunk/Merge)
func (a *Array) writeDataframeChunk(df dataframe.DataFrame, suffix string) error {
	// build schema
	numCols := df.Ncol()
	fields := make([]arrow.Field, numCols)
	numAttrs := a.schema.NumAttributes()
	for i := 0; i < numAttrs; i++ {
		if attr, ok := a.schema.GetAttribute(i); ok {
			//nullable := !attr.NotNull // true (NOT NULL) -> nullable false
			nullable := true // C++ and Python default is true, GO default is false
			switch attr.TypeName {
			case scidb.Bool:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.FixedWidthTypes.Boolean, Nullable: nullable}
			case scidb.Int8:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Int8, Nullable: nullable}
			case scidb.Int16:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Int16, Nullable: nullable}
			case scidb.Int32:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Int32, Nullable: nullable}
			case scidb.Int64:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Int64, Nullable: nullable}
			case scidb.Uint8:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Uint8, Nullable: nullable}
			case scidb.Uint16:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Uint16, Nullable: nullable}
			case scidb.Uint32:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Uint32, Nullable: nullable}
			case scidb.Uint64:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Uint64, Nullable: nullable}
			case scidb.Float32:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Float32, Nullable: nullable}
			case scidb.Float64:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.PrimitiveTypes.Float64, Nullable: nullable}
			case scidb.String:
				fields[i] = arrow.Field{Name: attr.Name, Type: arrow.BinaryTypes.String, Nullable: nullable}
			default:
				return fmt.Errorf("unsupported type: %v", attr.TypeName)
			}
		} else {
			return fmt.Errorf("error getting attribute %v", i)
		}
	}
	// dim names and attrs are required to be unique by SciDB
	numDims := a.schema.NumDimensions()
	for i := 0; i < numDims; i++ {
		if dim, ok := a.schema.GetDimension(i); ok {
			fields[numAttrs+i] = arrow.Field{Name: dim.Name, Type: arrow.PrimitiveTypes.Int64, Nullable: true} // really not, but to work with the scidb operator
		} else {
			return fmt.Errorf("error getting dimension %v", i)
		}
	}
	schema := arrow.NewSchema(fields, nil) // TODO: should we add metadata, like stick the schema string in it?
	if len(schema.Fields()) == 0 {
		// something went wrong
		return fmt.Errorf("something went wrong creating the Arrow Schema")
	}
	if len(schema.Fields()) != numDims+numAttrs {
		return fmt.Errorf("wrong number of fields when creating the Arrow Schema: %d ncol vs %d", len(schema.Fields()), numAttrs+numDims)
	}

	// build table
	rec, err := dataframe.DataframeToRecordWithSchema(df, schema, a.alloc)
	if err != nil {
		return err
	}
	defer rec.Release()

	// save
	compression := a.metadata["compression"]
	url_path := fmt.Sprintf("/chunks/%s", suffix)
	var aw *ArrayWriter
	aw, err = a.driver.CreateArrowWriter(url_path, compression, a.alloc, rec.Schema())
	if err != nil {
		return err
	}
	if err = aw.w.Write(rec); err != nil {
		return err
	}
	return aw.Close()
}

/* TODO see DataFrame.Filter
func (a *Array) Filter() {
}
*/

// Returns the Chunk where the given coords (dimension) would be found
// Will error if coords are outside a dimensions boundaries
func (a *Array) CoordsToChunkBounds(coords ...int64) (ChunkBounds, error) {
	b := ChunkBounds{array: a}
	if len(coords) != a.schema.NumDimensions() {
		return b, fmt.Errorf("coords and dim have different lengths: %d vs %d", len(coords), a.schema.NumDimensions())
	}
	suffix := []string{"c"}
	lower := []int64{}
	upper := []int64{}
	for i := 0; i < len(coords); i++ {
		coord := coords[i]
		dim, _ := a.schema.GetDimension(i)
		if !dim.InBounds(coord) {
			return b, fmt.Errorf("coordinate for dimension %s is not within bounds: %v", dim.Name, coord)
		}
		if dim.ChunkLength > 0 && dim.ChunkLength <= scidb.MaxChunkSize {
			part := coord / dim.ChunkLength
			lower = append(lower, part*dim.ChunkLength)
			upper = append(upper, (part*dim.ChunkLength)+dim.ChunkLength)
			cpart := (coord - dim.LowValue) / dim.ChunkLength
			suffix = append(suffix, fmt.Sprintf("%d", cpart))
		} else {
			lower = append(lower, dim.LowValue)
			upper = append(upper, scidb.MaxDimSize)                  // TODO: MaxChunkSize < MaxDimSize!
			suffix = append(suffix, fmt.Sprintf("%d", dim.LowValue)) // WARNING: this might be wrong
		}
	}
	return ChunkBounds{array: a, lower: lower, upper: upper, suffix: strings.Join(suffix, "_")}, nil
}

func (b ChunkBounds) InBounds(coords ...int64) bool {
	if coords == nil || len(coords) != len(b.lower) {
		return false
	}
	for i, c := range coords {
		if c < b.lower[i] || c >= b.upper[i] {
			return false
		}
	}
	return true
}

// GetChunkName returns the actually name of the chunk
func (b ChunkBounds) GetChunkName() string {
	return fmt.Sprintf("c_%s", b.suffix)
}

func (b ChunkBounds) String() string {
	delim := ","
	return fmt.Sprintf("[[%s],[%s]]",
		strings.Trim(strings.Replace(fmt.Sprint(b.lower), " ", delim, -1), "[]"),
		strings.Trim(strings.Replace(fmt.Sprint(b.upper), " ", delim, -1), "[]"),
	)
}
