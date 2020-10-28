#!/bin/env python3

import boto3
import os
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

index_all = []
for page in pages:
    for obj_ref in page['Contents']:
        print(obj_ref['Key'])
        obj = client.get_object(Bucket=bucket, Key=obj_ref['Key'])
        index_all.append(obj['Body'].read())

index_all = b'\n'.join(index_all) + b'\n'
client.put_object(Body=index_all, Bucket=bucket, Key='{}/index'.format(key))
