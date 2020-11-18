#!/bin/env python3

import boto3
import io
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

    # frame = table.to_pandas()
    # frame = frame * 10 + 1
    # table = pyarrow.Table.from_pandas(frame)

    batches = table.to_batches()
    sink = pyarrow.BufferOutputStream()
    sink_comp = pyarrow.output_stream(sink, compression='gzip')
    writer = pyarrow.RecordBatchStreamWriter(sink_comp, batches[0].schema)
    for batch in batches:
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
