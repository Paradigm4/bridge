#!/bin/env python3

import boto3
import io
# BEGIN_COPYRIGHT
#
# Copyright (C) 2020 Paradigm4 Inc.
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
import pyarrow.csv
import sys


if len(sys.argv) != 4:
    print('Missing arguments:')
    print(os.path.basename(__file__), 'bucket key INDEX_SPLIT_SIZE')
    sys.exit(2)

bucket = sys.argv[1]
key = sys.argv[2]
INDEX_SPLIT_SIZE = int(sys.argv[3])


client = boto3.client('s3')
obj = client.get_object(Bucket=bucket, Key='{}/index.tsv'.format(key))
data = obj['Body'].read()
chunks = data.split(b'\n')

while b'' in chunks:
    chunks.remove(b'')

n_dims = len(chunks[0].split(b'\t'))
split_sz = INDEX_SPLIT_SIZE // n_dims
print('TSV lines:', len(chunks))


# Delete Existing Split Index
bkt = boto3.resource('s3').Bucket(bucket)
bkt.objects.filter(Prefix='{}/index/'.format(key)).delete()

i = 0
for split_st in range(0, len(chunks), split_sz):
    split = chunks[split_st:(split_st+split_sz)]
    body = b'\n'.join(split) + b'\n'
    table = pyarrow.csv.read_csv(
        input_file=io.BytesIO(body),
        read_options=pyarrow.csv.ReadOptions(autogenerate_column_names=True),
        parse_options=pyarrow.csv.ParseOptions(delimiter='\t'))

    # Copy back and forth to Pandas in order to get a single record batch
    frame = table.to_pandas(split_blocks=True, self_destruct=True)
    batch = pyarrow.RecordBatch.from_pandas(frame)

    sink = pyarrow.BufferOutputStream()
    sink_comp = pyarrow.output_stream(sink, compression='gzip')
    writer = pyarrow.RecordBatchStreamWriter(sink_comp, batch.schema)
    writer.write_batch(batch)
    writer.close()
    sink_comp.close()
    buf = sink.getvalue()

    split_key = '{}/index/{}'.format(key, i)
    print(split_key)
    client.put_object(Body=buf.to_pybytes(), Bucket=bucket, Key=split_key)
    i += 1

    # body = b'\n'.join(chunks[split_st:(split_st+split_sz)])
    # client.put_object(Body=body, Bucket=bucket, Key=split_key)
