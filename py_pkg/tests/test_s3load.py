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


@pytest.mark.parametrize("chunk_size", (5, 10))
def test_one_dim(scidb_con, s3_con, chunk_size):
    prefix = 'one_dim_{}'.format(chunk_size)
    schema = '<v:int64> [i=0:9:0:{}]'.format(chunk_size)

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
                             fetch=True)

    assert array.equals(
        pandas.DataFrame({'i': range(10), 'v': numpy.arange(0.0, 10.0)}))

    delete_prefix(s3_con, prefix)


@pytest.mark.parametrize("chunk_size", (5, 10))
def test_many_dim(scidb_con, s3_con, chunk_size):
    prefix = 'many_dim_{}'.format(chunk_size)
    schema = '<v:int64> [i=0:9:0:{}; j=0:9:0:{}]'.format(chunk_size,
                                                         chunk_size)

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
                             fetch=True)
    array = array.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = []
    j_lst = []
    v_lst = []
    for i in range(10):
        for j in range(10):
            i_lst.append(i)
            j_lst.append(j)
            v_lst.append(float(i))

    assert array.equals(
        pandas.DataFrame({'i': i_lst,
                          'j': j_lst,
                          'v': v_lst}))

    delete_prefix(s3_con, prefix)
