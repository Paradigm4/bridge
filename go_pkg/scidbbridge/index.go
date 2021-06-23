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
	"path"
	"strconv"
	"strings"

	"github.com/Paradigm4/gota/dataframe"
	"github.com/Paradigm4/gota/series"

	_array "github.com/apache/arrow/go/arrow/array"
)

func (a *Array) ReadIndex() (dataframe.DataFrame, error) {
	edf := dataframe.DataFrame{Err: fmt.Errorf("Empty")}
	if a.driver == nil {
		return edf, fmt.Errorf("[Array.ReadIndex] Array is uninitalized")
	}
	a.index = nil // blank it incase we fail
	u, err := url.Parse(fmt.Sprintf("%s/index", a.driver.url.String()))
	if err != nil {
		return edf, err
	}
	records := []_array.Record{}
	files, err := List(u)
	if err != nil {
		return edf, err
	}
	for _, f := range files {
		reader, err := CreateArrayReader(f, "gzip", a.alloc)
		if err != nil {
			return edf, fmt.Errorf("[Array.ReadIndex] ReadIndex - %s: %v", f, err)
		}
		for reader.Next() {
			r := reader.Record()
			if len(records) > 1 && !records[0].Schema().Equal(r.Schema()) {
				// NewTable will panic if schemas not the same
				return edf, fmt.Errorf("[Array.ReadIndex] Record has different schema: %v", f)
			}
			r.Retain() // reader.Next calls r.Release() then tries to fetch the next. So now our record is down to 0 causing NewTable to panic
			records = append(records, r)
		}
		reader.Release()
	}
	if len(records) == 0 {
		cols := []series.Series{}
		for i := 0; i < a.schema.NumDimensions(); i++ {
			dim, _ := a.schema.GetDimension(i)
			cols = append(cols, series.New(nil, series.Int, dim.Name, 0))
		}
		df := dataframe.New(cols...)
		return df, df.Err
	}
	tbl := _array.NewTableFromRecords(records[0].Schema(), records)
	df := dataframe.TableToDataframe(tbl)
	tbl.Release()

	if !a.DataframeHasDimensions(df) {
		df.Err = fmt.Errorf("[Array.ReadIndex] Missing Dimensions")
		return df, df.Err
	}

	// sort
	order := []dataframe.Order{}
	for i := 0; i < df.Ncol(); i++ {
		dim, _ := a.schema.GetDimension(i)
		order = append(order, dataframe.Sort(dim.Name))
	}

	a.index = &ArrayIndex{array: a, df: df.Arrange(order...)}
	return a.index.df, nil
}

// Rebuilds the Index files
// This lists all the chunks which can take some time if there are thousands of chunks
// This DOES NOT save out the rebuilt index
func (a *Array) RebuildIndex() dataframe.DataFrame {
	edf := dataframe.DataFrame{Err: fmt.Errorf("Empty")}
	if a.driver == nil {
		edf.Err = fmt.Errorf("[Array.RebuildIndex] Array is uninitalized")
		return edf
	}
	a.index = nil // blank it incase we fail
	u, err := url.Parse(fmt.Sprintf("%s/chunks", a.driver.url.String()))
	if err != nil {
		edf.Err = err
		return edf
	}
	files, err := List(u)
	if err != nil {
		edf.Err = err
		return edf
	}
	dims := make([]series.Series, a.schema.NumDimensions())
	order := make([]dataframe.Order, a.schema.NumDimensions())
	for d := 0; d < len(dims); d++ {
		dim, _ := a.schema.GetDimension(d)
		dims[d] = series.New(nil, series.Int, dim.Name)
		order[d] = dataframe.Sort(dim.Name)
	}
	for _, f := range files {
		u, err := url.Parse(f)
		if err != nil {
			edf.Err = err
			return edf
		}
		coords, err := a.ChunkUrlToCoords(*u)
		if err != nil {
			edf.Err = err
			return edf
		}
		for d := 0; d < len(dims); d++ {
			dims[d].Append(coords[d])
		}
	}
	df := dataframe.New(dims...)
	if df.Err != nil {
		edf.Err = err
		return edf
	}
	df = df.Arrange(order...)
	if df.Err != nil {
		edf.Err = err
		return edf
	}

	a.index = &ArrayIndex{array: a, df: df}
	return df
}

// WriteIndex writes out a new index
// rebuild will rebuild the whole index (it will also rebuild if an index is missing)
func (a *Array) WriteIndex(rebuild bool) error {
	if a.driver == nil {
		return fmt.Errorf("[Array.WriteIndex] Array is uninitalized")
	}
	if a.index == nil || rebuild {
		if df := a.RebuildIndex(); df.Err != nil {
			return df.Err
		}
	}

	// create schema just dims as int64

	// should be sorted
	// find duplicates (not implemented in DataFrame)
	// delete all existing
	// find split points
	// right out all splits, as compressed
	return fmt.Errorf("[Array.WriteIndex] Not Supported")
}

// ChunkUrlToCoords returns the starting coordinates of the given array chunk
// chunk filenames (base) start with "c_"
func (a *Array) ChunkUrlToCoords(u url.URL) ([]int64, error) {
	bounds := make([]int64, a.schema.NumDimensions())
	coords := strings.Split(path.Base(u.Path), "_")
	if len(coords)-1 != a.schema.NumDimensions() {
		return nil, fmt.Errorf("coords and dim have different lengths: %d vs %d", len(coords)-1, a.schema.NumDimensions())
	}
	for b, v := range coords[1:] {
		coord, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, err
		}
		dim, _ := a.schema.GetDimension(b)
		bounds[b] = coord*dim.ChunkLength + dim.LowValue
	}
	return bounds, nil
}
