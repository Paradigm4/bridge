#!/bin/env python3

# BEGIN_COPYRIGHT
#
# Copyright (C) 2020-2021 Paradigm4 Inc.
# All Rights Reserved.
#
# scidbbridge is a plugin for SciDB, an Open Source Array DBMS
# maintained by Paradigm4. See http://www.paradigm4.com/
#
# scidbbridge is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as
# published by the Free Software Foundation.
#
# scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
# KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
# AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public
# License along with scidbbridge. If not, see
# <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

import os
import pyarrow
import sys

from scidbbridge import Array, type_map_pyarrow
from scidbbridge.driver import Driver

def wrong_arg():
    print("""Upgrade an existing Bridge Array from v1 to v2.

Usage:

{} URL index|chunks|both

    index:  fixes index only (not idempotent)
    chunks: fixes chunks only (idempotent)
    both:   fixes index and chunks""".format(os.path.basename(__file__)))
    sys.exit(2)

if len(sys.argv) != 3:
    wrong_arg()

url = sys.argv[1]
mode = sys.argv[2]

if mode not in ('index', 'chunks', 'both'):
    wrong_arg()

# -- - Fix Index - --
if mode in ('index', 'both'):
    for index_url in Driver.list('{}/index'.format(url)):
        print('Fixing', index_url)

        # Read index with GZIP compression
        reader = Driver.create_reader(index_url, 'gzip')
        table = reader.read_all()

        # Fix nullness in Arrow schema
        schema = pyarrow.schema([(name, pyarrow.int64(), False)
                                 for name in table.schema.names])
        table = table.cast(schema)

        # Re-write index with LZ4 compression
        sink = Driver.create_writer(index_url, table.schema, 'lz4')
        writer = next(sink)
        writer.write_table(table)
        sink.close()

# -- - Fix Chunks - --
if mode in ('chunks', 'both'):
    array = Array(url)
    compression = array.metadata['compression']

    # Fixed schema
    schema = pyarrow.schema(
        [(a.name, type_map_pyarrow[a.type_name], not a.not_null)
         for a in array.schema.atts] +
        [(d.name, pyarrow.int64(), False)
         for d in array.schema.dims])

    for (_, pos) in array.read_index().iterrows():
        chunk = array.get_chunk(*pos.tolist())
        print('Fixing', chunk.url)

        # Read chunk
        reader = Driver.create_reader(chunk.url, compression)
        table = reader.read_all()

        # Fix schema
        table = table.cast(schema)

        # Re-write index with LZ4 compression
        sink = Driver.create_writer(chunk.url, table.schema, compression)
        writer = next(sink)
        writer.write_table(table)
        sink.close()
