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
package scidb

import (
	"fmt"
	"strings"
	"testing"
)

func TestAttribute_FromString(t *testing.T) {
	table := []struct {
		attr     string
		expected Attribute
	}{
		{
			"a:int8",
			Attribute{Name: "a", TypeName: "int8"},
		},
		{
			"<y:double>",
			Attribute{Name: "y", TypeName: Float64},
		},
		{
			"a:int8 NULL",
			Attribute{Name: "a", TypeName: "int8"},
		},
		{
			"a:int8 null",
			Attribute{Name: "a", TypeName: "int8"},
		},
		{
			"a:int8 NOT NULL",
			Attribute{Name: "a", TypeName: "int8", NotNull: true, DefaultValue: int64(0)},
		},
		{
			"a:int8 not null",
			Attribute{Name: "a", TypeName: "int8", NotNull: true, DefaultValue: int64(0)},
		},
		{
			"a:int8 DEFAULT 1",
			Attribute{Name: "a", TypeName: "int8", DefaultValue: "1"},
		},
		{
			"a:int8 default 1",
			Attribute{Name: "a", TypeName: "int8", DefaultValue: "1"},
		},
		{
			"a:int8 COMPRESSION 'zlib'",
			Attribute{Name: "a", TypeName: "int8", Compression: "zlib"},
		},
		{
			"a:int8 compression 'zlib'",
			Attribute{Name: "a", TypeName: "int8", Compression: "zlib"},
		},
		{
			"a:int8 NULL DEFAULT 1 COMPRESSION 'zlib'",
			Attribute{Name: "a", TypeName: "int8", DefaultValue: "1", Compression: "zlib"},
		},
		{
			"a:int8 NOT NULL DEFAULT 1 COMPRESSION 'zlib'",
			Attribute{Name: "a", TypeName: "int8", NotNull: true, DefaultValue: "1", Compression: "zlib"},
		},
	}
	for testnum, test := range table {
		a, err := AttributeFromString(test.attr)
		if err != nil {
			t.Errorf("Test:%v\n%v", testnum, err)
		} else {
			if !test.expected.Equals(a) {
				t.Errorf("Test:%v\nExpected:\n%v\nReceived:\n%v\n", testnum, test.expected, a) // redo when we have a string method
			}
		}
	}
}

func TestAttribute_Totring(t *testing.T) {
	table := []struct {
		attr     Attribute
		expected string
	}{
		{
			Attribute{Name: "a", TypeName: "int8"},
			"a:int8",
		},
		{
			Attribute{Name: "a", TypeName: "int8", NotNull: true},
			"a:int8 NOT NULL",
		},
		{
			Attribute{Name: "a", TypeName: "int8", DefaultValue: "1"},
			"a:int8 DEFAULT '1'",
		},
		{
			Attribute{Name: "a", TypeName: "int8", Compression: "zlib"},
			"a:int8 COMPRESSION 'zlib'",
		},
		{
			Attribute{Name: "a", TypeName: "int8", DefaultValue: "1", Compression: "zlib"},
			"a:int8 DEFAULT '1' COMPRESSION 'zlib'",
		},
		{
			Attribute{Name: "a", TypeName: "int8", NotNull: true, DefaultValue: "1", Compression: "zlib"},
			"a:int8 NOT NULL DEFAULT '1' COMPRESSION 'zlib'",
		},
	}
	for testnum, test := range table {
		a := test.attr.String()
		if strings.Compare(a, test.expected) != 0 {
			t.Errorf("Test:%v\nReceived:\n%v\nExpected:\n%v\n", testnum, a, test.expected) // redo when we have a string method
		}
	}
}

func TestDimension_FromString(t *testing.T) {
	table := []struct {
		dim      string
		expected Dimension
	}{
		{
			"i=0:99:0:10",
			Dimension{"i", 0, 99, 0, 10, "i=0:99:0:10"},
		},
		{
			"i=0:99",
			Dimension{"i", 0, 99, 0, MaxChunkSize, fmt.Sprintf("i=0:99:0:%d", MaxChunkSize)},
		},
		{
			"[i=0:*",
			Dimension{"i", 0, MaxDimSize, 0, MaxChunkSize, fmt.Sprintf("i=0:%d:0:%d", MaxDimSize, MaxChunkSize)},
		},
		{
			"[i=0:*]",
			Dimension{"i", 0, MaxDimSize, 0, MaxChunkSize, fmt.Sprintf("i=0:%d:0:%d", MaxDimSize, MaxChunkSize)},
		},
		{
			"i=0:*]",
			Dimension{"i", 0, MaxDimSize, 0, MaxChunkSize, fmt.Sprintf("i=0:%d:0:%d", MaxDimSize, MaxChunkSize)},
		},
	}
	for testnum, test := range table {
		a, err := DimensionFromString(test.dim)
		if err != nil {
			t.Errorf("Test:%v\n%v", testnum, err)
		} else {
			if !test.expected.Equals(a) {
				t.Errorf("Test:%v\nExpected:\n%v\nReceived:\n%v", testnum, test.expected, a)
			}
		}
	}
}

func TestSchema_FromString(t *testing.T) {
	table := []struct {
		schema   string
		expected Schema
	}{
		{
			"<x:int64 not null, y:double>[i=0:*; j=-100:0:0:10]",
			Schema{
				atts: []Attribute{
					{Name: "x", TypeName: "int64", NotNull: true, DefaultValue: int64(0)},
					{Name: "y", TypeName: "double"}},
				dims: []Dimension{
					{"i", 0, MaxDimSize, 0, MaxChunkSize, fmt.Sprintf("i=0:%d:0:%d", MaxDimSize, MaxChunkSize)},
					{"j", -100, 0, 0, 10, "j=-100:0:0:10"},
				},
			},
		},
		{
			"<x:int64 not null, y:double COMPRESSION 'zlib'>[i=0:*; j=-100:0:0:10]",
			Schema{
				atts: []Attribute{
					{Name: "x", TypeName: "int64", NotNull: true, DefaultValue: int64(0)},
					{Name: "y", TypeName: "double", Compression: "zlib"}},
				dims: []Dimension{
					{"i", 0, MaxDimSize, 0, MaxChunkSize, fmt.Sprintf("i=0:%d:0:%d", MaxDimSize, MaxChunkSize)},
					{"j", -100, 0, 0, 10, "j=-100:0:0:10"},
				},
			},
		},
	}
	for testnum, test := range table {
		a, err := SchemaFromString(test.schema)
		if err != nil {
			t.Errorf("Test:%v\n%v", testnum, err)
		} else {
			if !test.expected.Equals(*a) {
				t.Errorf("Test:%v\nExpected:\n%v\nReceived:\n%v", testnum, test.expected, a)
			}
		}
	}
}
