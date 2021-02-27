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

from scidbbridge.driver import Driver


if len(sys.argv) != 2:
    print('Missing argument:')
    print(os.path.basename(__file__), 'url')
    sys.exit(2)


url = sys.argv[1]
batches = []
for crt_url in Driver.list('/'.join((url, 'index'))):
    print(crt_url)
    reader = Driver.create_reader(crt_url, 'gzip')
    batches = batches + reader.read_all().to_batches()

table = pyarrow.Table.from_batches(batches)
df = table.to_pandas(split_blocks=True, self_destruct=True)

# Sort Index Values
df.sort_values(by=list(df.columns),
               inplace=True,
               ignore_index=True)

batch = pyarrow.RecordBatch.from_pandas(df)
batch = batch.replace_schema_metadata()
sink = Driver.create_writer(
    '{}/index.arrow.gz'.format(url), batch.schema, 'gzip')
writer = next(sink)
writer.write_batch(batch)
sink.close()
