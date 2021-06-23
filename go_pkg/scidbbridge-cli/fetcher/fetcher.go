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
	"fmt"
	"log"
	"os"
	"strings"

	"strconv"
	"unicode/utf8"

	"github.com/Paradigm4/bridge/go_pkg/scidbbridge"
	"github.com/Paradigm4/gota/dataframe"
	"github.com/akamensky/argparse"
)

func main() {
	// Create new parser object
	parser := argparse.NewParser("fetcher", "Fetches and converts a SciDB Bridge Array Chunk to a DataFrame")
	parser.ExitOnHelp(true)

	dfArg := parser.String("o", "output",
		&argparse.Options{
			Required: true,
			Help:     "path to save dataframe to, '-' uses stdin",
		})
	urlArg := parser.String("u", "url",
		&argparse.Options{
			Required: true,
			Help:     "input bridge array",
		})
	dimsArg := parser.String("d", "dims",
		&argparse.Options{
			Required: true,
			Help:     "Comma-separated list of coordinates (dimensions) used to retrieve a chunk/dataframe",
		})

	// add option for csv-like or json-like (when json parsing works again)
	sepOpt := parser.String("", "sep",
		&argparse.Options{
			Required: false,
			Help:     "Separator to use (one character), default is comma",
			Default:  ",",
		})
	noHeaderOpt := parser.Flag("", "noHeader",
		&argparse.Options{
			Required: false,
			Help:     "Do not output the header, default is with header",
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

	chunkStrDims := strings.Split(*dimsArg, ",")
	if len(chunkStrDims) == 0 {
		log.Fatalf("No dims given: %v\n", *dimsArg)
	}
	chunkDims := make([]int64, len(chunkStrDims))
	for i, sd := range chunkStrDims {
		if d, err := strconv.ParseInt(sd, 10, 64); err != nil {
			log.Fatalf("Issue with parsing dimension (%d) %s: %v\n", i, sd, err)
		} else {
			chunkDims[i] = d
		}
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
	_, err = os.Stat(*dfArg)
	if !os.IsNotExist(err) {
		log.Fatalf("Output file exist: '%v'\n", *dfArg)
	}

	log.Printf("Opening array at %v\n", *urlArg)
	arr, err := scidbbridge.OpenArray(*urlArg, scidbbridge.WithLRU(3))
	if err != nil {
		log.Fatal(err)
	}
	if len(chunkDims) != arr.GetSchema().NumDimensions() {
		log.Fatalf("Expecting %d dimensions, given %d\n", arr.GetSchema().NumDimensions(), len(chunkDims))
	}
	cb, err := arr.CoordsToChunkBounds(chunkDims...)
	if err != nil {
		log.Fatal(err)
	}
	c, err := arr.GetChunk(cb)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Release()
	df := c.GetDataFrame()
	w, err := os.OpenFile(*dfArg, os.O_WRONLY|os.O_CREATE, 0644) // O_SYNC ?
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	if err := df.WriteCSV(w, dataframe.WriteDelimiter(sep), dataframe.WriteHeader(!*noHeaderOpt)); err != nil {
		log.Fatal(err)
	}
	log.Printf("Wrote chunk %s (%v) to %v\n", cb.GetChunkName(), cb, *dfArg)
}
