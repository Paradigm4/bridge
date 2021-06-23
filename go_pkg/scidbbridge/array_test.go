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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Paradigm4/gota/dataframe"
	"github.com/Paradigm4/gota/series"
)

var TEST_ARRAYS = "../test-arrays/"

func TestArray_InitUrl(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))

	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error: %v for %v", err, foo_array)
		return
	}
	if _, ok := array.schema.GetAttributeByName("v"); !ok {
		t.Errorf("Missing attribute 'v'")
	}
	if _, ok := array.schema.GetDimensionByName("i"); !ok {
		t.Errorf("Missing dimension 'i'")
	}
}

func TestArray_ReadIndex(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))

	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	df, err := array.ReadIndex()
	if err != nil {
		t.Errorf("Error - %v", err)
	}
	if df.Err != nil {
		t.Errorf("Error - %v", err)
	}
	if v, err := df.Col("i").Elem(3).Int(); err != nil && v != 5 {
		t.Errorf("Error or wrong value for col i")
	}
	if v, err := df.Col("j").Elem(6).Int(); err != nil && v != 5 {
		t.Errorf("Error or wrong value for col j")
	}
	t.Log(df.String())
}

func TestArray_RebuildIndex(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))

	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	rebuiltIndex := array.RebuildIndex()
	if rebuiltIndex.Err != nil {
		t.Fatalf("Error - %v for %v", rebuiltIndex.Err, foo_array)
	}
	t.Log(rebuiltIndex.String())

	index, err := array.ReadIndex()
	if err != nil {
		t.Fatalf("Error - %v for %v", err, foo_array)
	}
	ir, _ := index.Records(true)
	rr, _ := rebuiltIndex.Records(true)
	if !reflect.DeepEqual(ir, rr) {
		t.Errorf("Different values:\nReadIndex:%v\nRebuildIndex:%v", ir, rr)
	}
}

// TODO: WriteIndex

func TestArray_GetChunkDf(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))

	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	cb, err := array.CoordsToChunkBounds(0, 0)
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	chunk, err := array.GetChunk(cb)
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
	}
	t.Logf("c_0_0: \n%v\n", chunk.df.String())
}

func TestArray_ChunkBounds(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))

	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}

	bounds, err := array.CoordsToChunkBounds(6, 1)
	if err != nil {
		t.Errorf("Error getting ChunkBounds(6,1) - %v", err)
	} else {
		t.Logf("c_2_0 bounds: %v", bounds)
		bounds_tests := [][]int64{{4, 1}, {5, 1}, {9, 4}, {10, 5}}
		bound_exp := []bool{false, true, true, false}
		for i, bt := range bounds_tests {
			if bounds.InBounds(bt...) != bound_exp[i] {
				t.Errorf("Test c_2_0:%v\nExpected:\n%v\nReceived:\n%v", i, bound_exp[i], !bound_exp[i])
			}
		}
	}
	bounds, err = array.CoordsToChunkBounds(16, 6)
	if err != nil {
		t.Errorf("Error getting ChunkBounds(16,6) - %v", err)
	} else {
		t.Logf("c_3_1 bounds: %s", bounds)
		bounds_tests := [][]int64{{1, 1}, {5, 5}, {6, 1}, {16, 9}}
		bound_exp := []bool{false, false, false, true}
		for i, bt := range bounds_tests {
			if bounds.InBounds(bt...) != bound_exp[i] {
				t.Errorf("Test c_3_1:%v\nExpected:\n%v\nReceived:\n%v", i, bound_exp[i], !bound_exp[i])
			}
		}
	}
}

// create new array and add chunk to it
func TestArray_NewArray(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))

	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	b1, err := array.CoordsToChunkBounds(0, 0)
	if err != nil {
		t.Errorf("Error getting bounds 0,0 - %v for %v", err, foo_array)
		return
	}
	c1, err := array.GetChunk(b1)
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	t.Log(c1.df.String())
	// make new array to save out
	tmp_array := t.TempDir()
	new_array, err := NewArray(fmt.Sprintf("file://%s/", tmp_array), WithSchema(array.schema))
	if err != nil {
		t.Errorf("Error - %v for %v", err, tmp_array)
		return
	}
	idf, err := new_array.ReadIndex()
	if err != nil {
		t.Errorf("Error - %v", err)
	}
	// t.Logf("Index: \n%v\n", idf.String())
	if idf.Nrow() != 0 {
		t.Errorf("Error - Index should have zero rows but has %d", idf.Nrow())
	}
	// try saving a chunk
	bc2, err := new_array.SplitDataFrame(c1.df)
	if err != nil {
		t.Errorf("Error spliting Chunked DataFrame - %v", err)
		return
	}
	if len(bc2) != 1 {
		t.Errorf("Error spliting Chunked DataFrame - expecting one got %d", len(bc2))
		return
	}
	err = new_array.Update(bc2[0])
	if err != nil {
		t.Errorf("Error updating new chunk - %v", err)
	}
}

func TestArray_UpdateChunk(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))
	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	cb, _ := array.CoordsToChunkBounds(0, 0)
	chunk, err := array.GetChunk(cb)
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	//t.Logf("Original c_0_0:\n%v\n", chunk.Df.String())
	// update a value
	colW := chunk.df.Col("w")
	err = colW.Elem(0).Set("mutated")
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
	}
	mdf := chunk.df.Update(colW)
	//t.Logf("Mutated c_0_0:\n%v\n", mdf.String())
	// make new array to save out
	tmp_array_dir := t.TempDir()
	tmp_array, err := NewArray(fmt.Sprintf("file://%s/", tmp_array_dir), WithSchema(array.schema))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
	}
	/*
		tcb, err := tmp_array.CoordsToChunkBounds(0, 0)
		if err != nil {
			t.Errorf("Error - %v for tmp_array (%v)", err, tmp_array_dir)
			return
		}
		//tmp_chunk, err := tmp_array.GetChunk(tcb)
		//tmp_chunk.df = mdf
		err = tmp_array.UpdateChunk(tmp_chunk)
	*/
	bc2, err := tmp_array.SplitDataFrame(mdf)
	if err != nil {
		t.Errorf("Error spliting Chunked DataFrame - %v", err)
		return
	}
	if len(bc2) != 1 {
		t.Errorf("Error spliting Chunked DataFrame - expecting one got %d", len(bc2))
		return
	}
	err = tmp_array.Update(bc2[0])
	if err != nil {
		t.Errorf("Error - %v for tmp_array (%v)", err, tmp_array_dir)
	}
	// need to flush it to disk
	tmp_array.Flush()
	// get it back
	tmp_chunk, err := tmp_array.GetChunk(bc2[0].GetChunkBounds())
	if err != nil {
		t.Errorf("Error - %v for tmp_array (%v)", err, tmp_array_dir)
	}
	//t.Logf("temp mutated\n%v\n", tmp_chunk.Df.String())
	er, _ := chunk.df.Records(true)
	rr, _ := tmp_chunk.df.Records(true)
	if reflect.DeepEqual(er, rr) {
		t.Errorf("Same values:\nOriginal:%v\nReceived:%v", er, rr)
	}
	er, _ = mdf.Records(true)
	if !reflect.DeepEqual(er, rr) {
		t.Errorf("Different values:\nExpected:%v\nReceived:%v", er, rr)
	}
	tmp_chunk.Release()
}

func TestArray_Merge(t *testing.T) {
	wd, _ := os.Getwd()
	foo_array, _ := filepath.Abs(filepath.Join(wd, TEST_ARRAYS, "foo"))

	array, err := OpenArray(fmt.Sprintf("file://%s/", foo_array))
	if err != nil {
		t.Errorf("Error - %v for %v", err, foo_array)
		return
	}
	tmp_array := t.TempDir()
	new_array, err := NewArray(fmt.Sprintf("file://%s/", tmp_array), WithSchema(array.schema))
	if err != nil {
		t.Errorf("Error - %v for %v", err, tmp_array)
		return
	}
	a := dataframe.New(
		series.New([]int{1, 4, 2, 6, 5, 7}, series.Int, "v"),
		series.New([]string{"a", "c", "b", "e", "d", "f"}, series.String, "w"),
		series.New([]int{0, 5, 4, 15, 11, 19}, series.Int, "i"),
		series.New([]int{0, 4, 4, 5, 4, 9}, series.Int, "j"),
	)
	//b := tc.dfb.Arrange(tc.keys...)
	err = new_array.Merge(a.Arrange(dataframe.Sort("i"), dataframe.Sort("j")))
	if err != nil {
		t.Errorf("Error merging A - %v", err)
		return
	}
	// fetch the chunks that should have been written
	table := []struct {
		coords []int64
		expDf  dataframe.DataFrame
	}{
		{
			[]int64{0, 0},
			dataframe.New(
				series.New([]int{1, 2}, series.Int, "v"),
				series.New([]string{"a", "b"}, series.String, "w"),
				series.New([]int{0, 4}, series.Int, "i"),
				series.New([]int{0, 4}, series.Int, "j"),
			),
		},
		{
			[]int64{15, 5},
			dataframe.New(
				series.New([]int{6, 7}, series.Int, "v"),
				series.New([]string{"e", "f"}, series.String, "w"),
				series.New([]int{15, 19}, series.Int, "i"),
				series.New([]int{5, 9}, series.Int, "j"),
			),
		},
	}
	for i, tc := range table {
		tcb, err := new_array.CoordsToChunkBounds(tc.coords...)
		if err != nil {
			t.Errorf("Test: %d\t%v", i, err)
		}
		c, err := new_array.GetChunk(tcb)
		if err != nil {
			t.Errorf("Test: %d\t%v", i, err)
		}
		tcr, _ := tc.expDf.Records(true)
		cr, _ := c.df.Records(true)
		if !reflect.DeepEqual(tcr, cr) {
			t.Errorf("Test: %d\nDifferent values:\nExpected:%v\nReceived:%v", i, tcr, cr)
		}
		c.Release()
	}
}
