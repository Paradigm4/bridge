import boto3
import os
import pytest
import scidbpy
import shutil


base_prefix = 'bridge_tests'
base_metadata = {'version':     '1',
                 'format':      'arrow',
                 'attribute':   'ALL',
                 'compression': None}
s3_bucket = 'p4tests'
fs_base = '/tmp/{}'.format(base_prefix)

test_urls = ('s3://{}/{}'.format(s3_bucket, base_prefix),
             'file://{}'.format(fs_base))

s3_con = boto3.client('s3')


@pytest.fixture
def scidb_con():
    # FS Init
    if not os.path.exists(fs_base):
        os.makedirs(fs_base)

    yield scidbpy.connect()

    # FS Cleanup
    try:
        shutil.rmtree(fs_base)
    except PermissionError:
        pass

    # S3 Cleanup
    result = s3_con.list_objects_v2(Bucket=s3_bucket, Prefix=base_prefix)
    if 'Contents' in result.keys():
        objects = [{'Key': e['Key']} for e in result['Contents']]
        s3_con.delete_objects(Bucket=s3_bucket,
                              Delete={'Objects': objects})
