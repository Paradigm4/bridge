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
	"regexp"
	"strconv"
	"strings"
)

type TypeName string

// Supported TypeNames
const (
	Int8    TypeName = "int8"
	Int16   TypeName = "int16"
	Int32   TypeName = "int32"
	Int64   TypeName = "int64"
	Uint8   TypeName = "uint8"
	Uint16  TypeName = "uint16"
	Uint32  TypeName = "uint32"
	Uint64  TypeName = "uint64"
	Float32 TypeName = "float"
	Float64 TypeName = "double"
	Bool    TypeName = "bool"
	String  TypeName = "string"
)

var TypeNameMap = map[string]TypeName{
	"int8": Int8, "int16": Int16, "int32": Int32, "int64": Int64,
	"uint8": Uint8, "uint16": Uint16, "uint32": Uint32, "uint64": Uint64,
	"float": Float32, "double": Float64,
	"bool":   Bool,
	"string": String,
}

var MaxDimSize = int64(0x3FFFFFFFFFFFFFFF) // 2^62-1
var MinDimSize = -MaxDimSize
var MaxChunkSize = int64(0x7FFFFFFF) // Max Arrow Array size 2^31-1 TODO: this should be moved into scidbbridge with checks there

// Represents a SciDB array attribute
// Nullability (Optional): Specify 'NULL' or 'NOT NULL', to indicate whether null values are allowed for the attribute. The default is 'NULL', meaning null values are allowed.
// DefaultValue (Optional): Specify the value to automatically substitute when you do not explicitly supply a value. If unspecified, the system chooses a value to substitute using the following rules:
//    * If the attribute is nullable, use null.
//    * Otherwise, use 0 for numeric types, or an empty string "" for string type.
type Attribute struct {
	Name         string
	TypeName     TypeName
	NotNull      bool        // Nullability - If NotNull is true, then 'NOT NULL'
	DefaultValue interface{} // ideally should be series.Element, but that eventually gives an interface{}
	Compression  string      // should make this a type as well; 'zlib' | 'bzlib'
	attribute    string
}

// Represents a SciDB array dimension (v16.9+)
// You can supply a ? for any dimension parameter, and SciDB will assign a default value (currently not supported)
// Dimension size is determined by the range from the dimension start to end, so, for example ranges of 0:99 and 1:100 would create the same dimension size.
// You can use expressions to define dimension size, or let SciDB assign default values. Expressions must evaluate to a scalar value. For example, an expression might be 100, or 500 * 4, and so on.
type Dimension struct {
	Name         string // Can only contain alphanumeric characters and underscores (_); SciDB maximum length of a dimension name is 1024 bytes.
	LowValue     int64  // An expression for the dimension start value.
	HighValue    int64  // Either an expression or an asterisk (*) as the dimension end value. Asterisks indicate the dimension has no limit (referred to as an unbounded dimension).
	ChunkOverlap int64  // [Deprecated 21.+] The optional expression of overlapping dimension values for adjacent chunks.
	ChunkLength  int64  // The optional expression of dimension values between consecutive chunk boundaries.
	dimension    string // actual string representation
}

type Schema struct {
	atts []Attribute
	dims []Dimension
}

// should replace these with the real grammer and use a Go Bison-like tool to generate a parser
var attributeRegex = regexp.MustCompile(
	`\s*(?P<name>\w+)\s*:\s*` +
		`(?P<type_name>\w+)\s*` +
		`(?i:(?P<not_null>NOT)?\s+NULL)?\s*` +
		`(?i:DEFAULT\s+'(?P<default>\S*)')?\s*` +
		`(?i:COMPRESSION\s+'(?P<compression>\w+)')?[\s>]*` +
		`$`)

var dimensionRegex = regexp.MustCompile(
	`\s*(?P<name>\w+)\s*` +
		`(?:` + `=\s*(?P<low_value>[^:\t\n\f\r\]]+)` +
		`\s*:\s*` +
		`(?P<high_value>[^:\t\n\f\r\]]+)\s*` +
		`(?:` + `:\s*(?P<chunk_overlap>[^:\t\n\f\r\]]+)\s*` +
		`(?:` + `:\s*(?P<chunk_length>[^:\t\n\f\r\]]+))?` +
		`)?` + `)?` +
		`[\s\]]*$`)

var schemaAttsRegex = regexp.MustCompile(`\s*<([^,>]+\s*(?:,\s*[^,>]+\s*)*)>`)
var schemaDimsRegex = regexp.MustCompile(`\s*\[([^;\]]+\s*(?:;\s*[^;\]]+\s*)*)\]\s*`)

func findNamedMatches(regex *regexp.Regexp, str string) map[string]string {
	match := regex.FindStringSubmatch(str)

	results := map[string]string{}
	for i, name := range match {
		if name != "'%!s(int64=0)'" {
			results[regex.SubexpNames()[i]] = name
		} // (?P<default>)? if empty returns that, believe it is %s of a nil
	}
	return results
}

func AttributeFromString(attr string) (Attribute, error) {
	fields := findNamedMatches(attributeRegex, attr)
	if len(fields) == 0 {
		return Attribute{}, fmt.Errorf("[AttributeFromString] unable to parse `%s`", attr)
	}
	a := Attribute{attribute: attr, NotNull: false}
	defaultValue := ""
	for k, v := range fields {
		switch k {
		case "name":
			a.Name = v
		case "type_name":
			if tn, ok := TypeNameMap[strings.ToLower(v)]; ok {
				a.TypeName = tn
			} else {
				return a, fmt.Errorf("[AttributeFromString] unsupported type: %v", v)
			}
		case "not_null":
			if strings.EqualFold("not", v) {
				a.NotNull = true
			} else {
				a.NotNull = false
			}
		case "default":
			defaultValue = v
		case "compression":
			a.Compression = v
		case "":
		default:
			return a, fmt.Errorf("[AttributeFromString] unknown named parameter: '%v'='%v'", k, v)
		}
	}
	// double check mandatory fields
	if a.Name == "" || a.TypeName == "" {
		return a, fmt.Errorf("[AttributeFromString] incomplete Attribute: %v", attr)
	}
	// figure out default value
	if defaultValue != "" {
		switch a.TypeName {
		case String:
			a.DefaultValue = defaultValue
		case Int8, Int16, Int32, Int64:
			i, err := strconv.ParseInt(defaultValue, 10, 64)
			if err != nil {
				return a, fmt.Errorf("[AttributeFromString] error parsing default value %v, expected int value: %v", defaultValue, fields)
			}
			a.DefaultValue = i
		case Uint8, Uint16, Uint32, Uint64:
			i, err := strconv.ParseUint(defaultValue, 10, 64)
			if err != nil {
				return a, fmt.Errorf("[AttributeFromString] error parsing default value, expected uint value: %v", defaultValue)
			}
			a.DefaultValue = i
		case Float32:
			f, err := strconv.ParseFloat(defaultValue, 32)
			if err != nil {
				return a, fmt.Errorf("[AttributeFromString] error parsing default value, expected float32 value: %v", defaultValue)
			}
			a.DefaultValue = f
		case Float64:
			f, err := strconv.ParseFloat(defaultValue, 64)
			if err != nil {
				return a, fmt.Errorf("[AttributeFromString] error parsing default value, expected float64 value: %v", defaultValue)
			}
			a.DefaultValue = f
		case Bool:
			switch strings.ToLower(defaultValue) {
			case "true", "t", "1":
				a.DefaultValue = true
			case "false", "f", "0":
				a.DefaultValue = false
			default:
				return a, fmt.Errorf("[AttributeFromString] error parsing default value, expected boolean value: %v", defaultValue)
			}
		default:
			return a, fmt.Errorf("[AttributeFromString] unknown type when setting default value: %v", a.TypeName)
		}
	} else {
		a.DefaultValue = nil
	}
	/*
		else if !a.NotNull {
			a.DefaultValue = nil
		}
		else {
			// NOT NULL and not given default value, figure out type and set it SciDB defaults
			// A default value (Optional): Specify the value to automatically substitute when you do not explicitly supply a value. If unspecified, the system chooses a value to substitute using the following rules:
			// If the attribute is nullable, use null. Otherwise, use 0 for numeric types, or an empty string "" for string type.
			switch a.TypeName {
			case String:
				a.DefaultValue = "" // "\"\""
			case Int8, Int16, Int32, Int64:
				a.DefaultValue = int64(0)
			case Uint8, Uint16, Uint32, Uint64:
				a.DefaultValue = uint64(0)
			case Float32, Float64:
				a.DefaultValue = float64(0.0)
			case Bool:
				a.DefaultValue = false
			default:
				return a, fmt.Errorf("[AttributeFromString] unknown type when setting default value: %v", a.TypeName)
			}
		}
	*/
	return a, nil
}

// TODO
//func (a Attribute) FromBytes(buf []byte, offset int, size int, promo bool) or use bytes.Buffer
//func (a Attribute) ToBytes() ([]byte, error)

// String representation that can be used as input
func (a Attribute) String() string {
	attr := []string{a.Name, ":", string(a.TypeName)}
	if a.NotNull {
		attr = append(attr, " NOT NULL")
	} else {
		attr = append(attr, "")
	}
	if a.DefaultValue != nil {
		attr = append(attr, fmt.Sprintf(" DEFAULT '%v'", a.DefaultValue))
	} else {
		attr = append(attr, "")
	}
	if a.Compression != "" {
		attr = append(attr, fmt.Sprintf(" COMPRESSION '%s'", a.Compression))
	} else {
		attr = append(attr, "")
	}

	return strings.Join(attr, "")
}

func (a Attribute) Equals(b Attribute) bool {
	return a.String() == b.String()
}

func DimensionFromString(dim string) (Dimension, error) {
	fields := findNamedMatches(dimensionRegex, dim)
	if len(fields) == 0 {
		return Dimension{}, fmt.Errorf("[DimensionFromString] unable to parse `%s`", dim)
	}
	d := Dimension{"", 0, MaxDimSize, 0, 1000000, dim} // defaults [i=?:?:?:?] or [i=*:*:*:*]
	for k, v := range fields {
		switch k {
		case "name":
			d.Name = v // TODO: validate (no illegal characters)
		case "low_value":
			// dim_lo         ::= expression | ?
			switch v {
			case "*":
				return d, fmt.Errorf("'*' is not allowed for `low_value`")
			case "?":
				d.LowValue = 0
			default:
				i, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return d, fmt.Errorf("low_value: %v", err)
				}
				if i < MinDimSize {
					return d, fmt.Errorf("low_value %v < min_dim (%v)", i, MinDimSize)
				}
				d.LowValue = i
			}
		case "high_value":
			// dim_hi         ::= expression | * | ?
			if v == "*" || v == "?" {
				d.HighValue = MaxDimSize
			} else {
				i, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return d, fmt.Errorf("[DimensionFromString] high_value: %v", err)
				}
				if i > MaxDimSize {
					return d, fmt.Errorf("[DimensionFromString] high_value %v > max_dim (%v)", i, MaxDimSize)
				}
				d.HighValue = i
			}
		case "chunk_overlap":
			// overlap        ::= expression | ?
			// Going away; this package doesn't use this
			switch v {
			case "*":
				return d, fmt.Errorf("[DimensionFromString] '*' is not allowed for `chunk_overlap`")
			case "", "?":
				d.ChunkOverlap = 0
			default:
				i, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return d, fmt.Errorf("[DimensionFromString]  chunk_overlap: %v", err)
				}
				d.ChunkOverlap = i
			}
		case "chunk_length":
			// chunk_length   ::= expression | ?
			switch v {
			case "*":
				return d, fmt.Errorf("[DimensionFromString] '*' is not allowed for `chunk_length`")
			case "", "?":
				d.ChunkLength = MaxChunkSize
			default:
				i, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return d, fmt.Errorf("[DimensionFromString] chunk_length: %v", err)
				}
				if i > MaxChunkSize {
					return d, fmt.Errorf("[DimensionFromString] chunk_length %v > max_dim (%v)", i, MaxChunkSize)
				}
				if i < 1 {
					return d, fmt.Errorf("[DimensionFromString] chunk_length %v < 1", i)
				}
				d.ChunkLength = i
			}
		case "":
		default:
			return d, fmt.Errorf("[DimensionFromString] unknown named parameter: '%v'='%v'", k, v)
		}
	}
	d.dimension = fmt.Sprintf("%s=%d:%d:%d:%d", d.Name, d.LowValue, d.HighValue, d.ChunkOverlap, d.ChunkLength)
	err := ValidateDimension(d)
	return d, err
}

func NewDimension(name string, lowValue string, highValue string, chunkOverlap string, chunkLength string) (Dimension, error) {
	return DimensionFromString(fmt.Sprintf("%s=%s:%s:%s:%s", name, lowValue, highValue, chunkOverlap, chunkLength))
}

// Validates if Dimension is OK
func ValidateDimension(d Dimension) error {
	if d.Name == "" {
		return fmt.Errorf("[ValidateDimension] empty 'name' is not allowed for a Dimension")
	}
	if d.LowValue > d.HighValue {
		return fmt.Errorf("[ValidateDimension] 'low_value' cannot be greater than `high_value` for a Dimension")
	}
	if d.ChunkLength < 0 {
		return fmt.Errorf("[ValidateDimension] negative `chunk_length` is not allowed for a Dimension")
	}
	return nil
}

// Is LowerValue <= coord <= HighValue true?
func (d Dimension) InBounds(coord int64) bool {
	if coord < d.LowValue || coord > d.HighValue {
		return false
	}
	return true
}

func (d Dimension) String() string {
	return d.dimension
}

func (a Dimension) Equals(b Dimension) bool {
	return a.dimension == b.dimension
}

func SchemaFromString(schema string) (*Schema, error) {
	s := &Schema{}
	matched := schemaAttsRegex.FindStringSubmatchIndex(schema)
	if len(matched) == 0 {
		return nil, fmt.Errorf("[SchemaFromString] unable to parse attributes: %v", schema)
	}
	attStrs := strings.Split(schema[matched[0]:matched[1]], ",")
	remaining := schema[matched[1]:]
	dimStrs := []string{}
	if len(remaining) > 0 {
		matched = schemaDimsRegex.FindStringSubmatchIndex(remaining)
		if matched != nil || len(matched) > 0 {
			dimStrs = strings.Split(remaining[matched[0]:matched[1]], ";")
		}
	}
	s.atts = make([]Attribute, len(attStrs))
	for i, v := range attStrs {
		a, err := AttributeFromString(v)
		if err != nil {
			return s, fmt.Errorf("[SchemaFromString] error parsing attribute %d: %v", i, err)
		}
		s.atts[i] = a
	}
	s.dims = make([]Dimension, len(dimStrs))
	for i, v := range dimStrs {
		d, err := DimensionFromString(v)
		if err != nil {
			return s, fmt.Errorf("[SchemaFromString] error parsing dimension %d: %v", i, err)
		}
		s.dims[i] = d
	}
	return s, nil
}

func (s Schema) NumAttributes() int {
	return len(s.atts)
}

func (s Schema) GetAttribute(i int) (Attribute, bool) {
	if i >= 0 && i < len(s.atts) {
		return s.atts[i], true
	}
	return Attribute{}, false
}

func (s Schema) GetAttributeByName(name string) (Attribute, bool) {
	for _, a := range s.atts {
		if a.Name == name {
			return a, true
		}
	}
	return Attribute{}, false
}

// Add (append) an attribute to the end of the list of Attributes
func (s *Schema) AddAttribute(attr Attribute) {
	s.atts = append(s.atts, attr)
}

// Return a dimension. If found returns true, else false
func (s Schema) GetDimensionByName(name string) (Dimension, bool) {
	for _, d := range s.dims {
		if d.Name == name {
			return d, true
		}
	}
	return Dimension{}, false
}

func (s Schema) NumDimensions() int {
	return len(s.dims)
}

func (s Schema) GetDimension(d int) (Dimension, bool) {
	if d < 0 || d >= len(s.dims) {
		return Dimension{}, false
	}
	return s.dims[d], true
}

// Add (append) a Dimension to the end of the list of Dimensions
func (s *Schema) AddDimension(dim Dimension) {
	s.dims = append(s.dims, dim)
}

// String output the array's schema as attributes and dimensions
func (s Schema) String() string {
	//<x:int64 not null, y:double>[i=0:*; j=-100:0:0:10]
	schema := []string{}
	attrs := []string{}
	for _, a := range s.atts {
		attrs = append(attrs, a.String())
	}
	if len(attrs) > 0 {
		schema = append(schema, fmt.Sprintf("<%s>", strings.Join(attrs, ", ")))
	}
	dims := []string{}
	for _, d := range s.dims {
		dims = append(dims, d.String())
	}
	if len(dims) > 0 {
		schema = append(schema, fmt.Sprintf("[%s]", strings.Join(dims, "; ")))
	}
	return strings.Join(schema, "")
}

// TODO
// func (s Schema) FromBytes([]byte buf)  error
// func (s Schema) ToBytes() ([]byte, error)

// Compares if the Strings are equal
func (a Schema) Equals(b Schema) bool {
	// TODO make more robust
	return a.String() == b.String()
}
