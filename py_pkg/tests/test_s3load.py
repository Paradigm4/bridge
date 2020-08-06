import boto3
import numpy
import pandas
import pytest
import scidbpy
import scidbs3

from common import *


@pytest.fixture
def scidb_con():
    return scidbpy.connect()


@pytest.fixture
def s3_con():
    return boto3.client('s3')


def test_one_chunk(scidb_con, s3_con):
    prefix = 'one_chunk'
    schema = '<v:int64> [i=0:9]'

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Load
    array = scidb_con.iquery("""
s3load(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True
    )

    assert array.equals(
        pandas.DataFrame({'i':range(10), 'v':numpy.arange(0.0, 10.0)}))

    delete_prefix(s3_con, prefix)
