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


@pytest.mark.parametrize('chunk_size', (5, 10, 20))
def test_one_dim(scidb_con, s3_con, chunk_size):
    prefix = 'one_dim_{}'.format(chunk_size)
    schema = '<v:int64> [i=0:19:0:{}]'.format(chunk_size)

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
    array = array.sort_values(by=['i']).reset_index(drop=True)

    assert array.equals(
        pandas.DataFrame({'i': range(20), 'v': numpy.arange(0.0, 20.0)}))

    delete_prefix(s3_con, prefix)


@pytest.mark.parametrize('chunk_size', (5, 10, 20))
def test_many_dim(scidb_con, s3_con, chunk_size):
    prefix = 'many_dim_{}'.format(chunk_size)
    schema = '<v:int64> [i=0:19:0:{}; j=0:19:0:{}]'.format(chunk_size,
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
    for i in range(20):
        for j in range(20):
            i_lst.append(i)
            j_lst.append(j)
            v_lst.append(float(i))

    assert array.equals(
        pandas.DataFrame({'i': i_lst,
                          'j': j_lst,
                          'v': v_lst}))

    delete_prefix(s3_con, prefix)


@pytest.mark.parametrize('type_name,type_numpy', (('binary', numpy.object),
                                                  ('string', numpy.object),
                                                  ('char', numpy.object),
                                                  ('bool', numpy.object),
                                                  ('datetime', None),
                                                  ('float', numpy.float32),
                                                  ('double', numpy.float64),
                                                  ('int8', numpy.float16),
                                                  ('int16', numpy.float32),
                                                  ('int32', numpy.float64),
                                                  ('int64', numpy.float64),
                                                  ('uint8', numpy.float16),
                                                  ('uint16', numpy.float32),
                                                  ('uint32', numpy.float64),
                                                  ('uint64', numpy.float64)))
def test_type(scidb_con, s3_con, type_name, type_numpy):
    max_val = 5
    prefix = 'type_{}'.format(type_name)
    schema = '<v:{}> [i=0:{}:0:5]'.format(type_name, max_val - 1)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    if type_name == 'binary':
        que = scidb_con.input(
            schema.replace('binary', 'binary not null'),
            upload_data=numpy.array([bytes([i]) for i in range(max_val)],
                                    dtype='object')).redimension(
                                        schema)
    else:
        que = scidb_con.build(schema, '{}(i)'.format(type_name))
    que = que.s3save("bucket_name:'{}'".format(bucket_name),
                     "bucket_prefix:'{}'".format(bucket_prefix))
    res = que.fetch()

    # Load
    array = scidb_con.iquery("""
s3load(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i']).reset_index(drop=True)

    if type_name.startswith('datetime'):
        v = (pandas.Timestamp(i * 10 ** 9) for i in range(max_val))
    elif type_name == 'bool':
        v = pandas.Series((bool(i) for i in range(max_val)),
                          dtype=numpy.object)
    elif type_name in ('char', 'binary')bibicycle:
        v = pandas.Series((bytes([i]) if i != 0 or type_name == 'binary'
                           else bytes()
                           for i in range(max_val)),
                          dtype=numpy.object)
    elif type_name == 'string':
        v = pandas.Series((str(i) for i in range(max_val)),
                          dtype=numpy.object)
    else:
        v = type_numpy(range(max_val))

    assert array.equals(pandas.DataFrame({'i': range(max_val), 'v': v}))

    delete_prefix(s3_con, prefix)
