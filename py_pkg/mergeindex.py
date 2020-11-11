#!/bin/env python3

import boto3
import os
import pyarrow
import sys


if len(sys.argv) != 3:
    print('Missing arguments:')
    print(os.path.basename(__file__), 'bucket key')
    sys.exit(2)

bucket = sys.argv[1]
key = sys.argv[2]

client = boto3.client('s3')
pages = client.get_paginator('list_objects_v2').paginate(
    Bucket=bucket, Prefix='{}/index/'.format(key))

# index_all = []

sink = pyarrow.BufferOutputStream()
sink_comp = pyarrow.output_stream(sink, compression='gzip')
writer = None

for page in pages:
    for obj_ref in page['Contents']:
        print(obj_ref['Key'])
        obj = client.get_object(Bucket=bucket, Key=obj_ref['Key'])

        # index_all.append(obj['Body'].read())

        buf = obj['Body'].read()
        strm = pyarrow.input_stream(pyarrow.BufferReader(buf),
                                    compression='gzip')
        reader = pyarrow.RecordBatchStreamReader(strm)

        for batch in reader:
            if writer is None:
                writer = pyarrow.RecordBatchStreamWriter(sink_comp, batch.schema)
            writer.write_batch(batch)

# index_all = b'\n'.join(index_all) + b'\n'
# client.put_object(Body=index_all, Bucket=bucket, Key='{}/index'.format(key))

writer.close()
sink_comp.close()
buf = sink.getvalue()
client.put_object(Body=buf.to_pybytes(),
                  Bucket=bucket,
                  Key='{}/index.arrow.gz'.format(key))
