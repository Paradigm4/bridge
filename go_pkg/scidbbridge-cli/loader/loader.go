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
package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"

	"strconv"
	"time"
	"unicode/utf8"

	"github.com/Paradigm4/bridge/go_pkg/scidb"
	"github.com/Paradigm4/bridge/go_pkg/scidbbridge"
	"github.com/Paradigm4/gota/dataframe"
	"github.com/Paradigm4/gota/series"

	"github.com/akamensky/argparse"
)

func main() {
	// Create new parser object
	parser := argparse.NewParser("loader", "Converts DataFrames to SciDB Bridge Arrays")
	parser.ExitOnHelp(true)

	dfArg := parser.String("", "input",
		&argparse.Options{
			Required: true,
			Help:     "path to dataframe to load, '-' uses stdin",
		})
	urlArg := parser.String("", "url",
		&argparse.Options{
			Required: true,
			Help:     "output base url. The array name from the schema will be appended.",
		})
	schemaArg := parser.String("", "schema",
		&argparse.Options{
			Required: true,
			Help:     "SciDB Full Schema",
		})
	compressionOpt := parser.Selector("c", "compression", []string{"none", "gzip"},
		&argparse.Options{
			Required: false,
			Help:     "array compression type",
			Default:  "none",
		})
	rowIdxOpt := parser.String("", "rowIdx",
		&argparse.Options{
			Required: false,
			Help:     "Use the the row index in the given dim",
		})
	// add option for csv-like or json-like (when json parsing works again)
	sepOpt := parser.String("", "sep",
		&argparse.Options{
			Required: false,
			Help:     "Separator to use (one character), default is comma",
			Default:  ",",
		})
	appendOpt := parser.Flag("a", "append",
		&argparse.Options{
			Required: false,
			Help:     "Append to an existing array",
			Default:  false,
		})
	// add option for null/NAs
	err := parser.Parse(os.Args)
	if err != nil {
		// In case of error print error and print usage
		// This can also be done by passing -h or --help flags
		fmt.Print(parser.Usage(err))
		os.Exit(1)
	}

	// check sep - convert to a rune
	if len(*sepOpt) == 0 {
		fmt.Printf("Empty separator given: %v\n", *sepOpt)
		os.Exit(1)
	}
	convSep, err := strconv.Unquote(`"` + *sepOpt + `"`) // either pass in using $'\t' or do this
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	sep, size := utf8.DecodeRuneInString(convSep)
	if size == 0 {
		fmt.Printf("Invalid separator: %v\n", *sepOpt)
		os.Exit(1)
	}

	// check if input exists
	info, err := os.Stat(*dfArg)
	if os.IsNotExist(err) {
		fmt.Printf("Input file does not exist: '%v'\n", *dfArg)
		os.Exit(1)
	}
	if info.IsDir() {
		fmt.Printf("Input file is a directory: '%v'\n", *dfArg)
		os.Exit(1)
	}

	// validate schema
	schema, err := scidb.SchemaFromString(*schemaArg)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// are we using the row index as a dim (like i?)
	if len(*rowIdxOpt) > 0 {
		// is it a dim?
		if _, ok := schema.GetDimensionByName(*rowIdxOpt); !ok {
			log.Fatalf("row indexing dimension `%v` does not exist", *rowIdxOpt)
		}
	}

	var arr *scidbbridge.Array
	if *appendOpt {
		log.Printf("Appending to array at %v\n", *urlArg)
		arr, err = scidbbridge.OpenArray(*urlArg,
			scidbbridge.WithCompression(*compressionOpt),
			scidbbridge.WithSchema(schema),
			scidbbridge.WithLRU(3))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Printf("Creating array at %v\n", *urlArg)
		arr, err = scidbbridge.NewArray(*urlArg,
			scidbbridge.WithCompression(*compressionOpt),
			scidbbridge.WithSchema(schema),
			scidbbridge.WithLRU(3))
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}

	st := time.Now()
	var r io.Reader
	if *dfArg == "-" {
		log.Println("Using stdin")
		r = bufio.NewReader(os.Stdin)
	} else {
		log.Printf("Opening %v\n", *dfArg)
		// try reading in the DataFrame
		r, err = os.Open(*dfArg)
		if err != nil {
			fmt.Println(err.Error())
			if err = arr.Delete(); err != nil {
				fmt.Println(err.Error())
			}
			os.Exit(1)
		}
	}
	seriesTypeMap, err := arr.GetSchemaSeriesTypes()
	if err != nil {
		fmt.Println(err.Error())
		if err = arr.Delete(); err != nil {
			fmt.Println(err.Error())
		}
		os.Exit(1)
	}
	if len(*rowIdxOpt) > 0 {
		delete(seriesTypeMap, *rowIdxOpt)
	}

	df := dataframe.ReadCSV(r, dataframe.WithDelimiter(sep), dataframe.WithTypes(seriesTypeMap))
	if df.Err != nil {
		fmt.Println(err.Error())
		if err = arr.Delete(); err != nil {
			fmt.Println(err.Error())
		}
		os.Exit(1)
	}

	if len(*rowIdxOpt) > 0 {
		if ok := df.HasCol(*rowIdxOpt); ok {
			log.Fatalf("Loaded DataFrame has matching column name as row index column: %v", *rowIdxOpt)
		}
		idxDim, _ := schema.GetDimensionByName(*rowIdxOpt)
		nrows := df.Nrow()
		// Create index column
		idx := make([]int64, nrows)
		for i := 0; i < nrows; i++ {
			idx[i] = int64(i) + idxDim.LowValue
		}
		rowIdxDf := dataframe.New(series.New(idx, series.Int, *rowIdxOpt))
		df = df.CBind(rowIdxDf)
		if df.Err != nil {
			log.Fatal(df.Err)
		}
	}

	// TODO: validate dims making sure no NaN and all valid (merge sorts and checks bounds)

	// load
	log.Printf("Adding to array")
	if err = arr.Merge(df); err != nil {
		fmt.Println(err.Error())
		if err = arr.Delete(); err != nil {
			fmt.Println(err.Error())
		}
		os.Exit(1)
	}
	arr.Close()
	log.Printf("Loaded array with %d rows\n", df.Nrow())
	/*
		// reindex
		if indexDf := arr.RebuildIndex(); indexDf.Err != nil {
			fmt.Println(indexDf.Err)
		}
	*/
	log.Println("Bridge Array will need to be reindexed!")
	et := time.Now()
	log.Printf("Finished loading %d rows in %v minutes\n", df.Nrow(), et.Sub(st).Minutes())
}
