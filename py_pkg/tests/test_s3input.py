import boto3
import itertools
import numpy
import pandas
import pyarrow
import pytest
import requests
import scidbpy
import scidbs3

from common import *


@pytest.fixture
def scidb_con():
    return scidbpy.connect()


@pytest.fixture
def s3_con():
    con = boto3.client('s3')
    yield con
    delete_prefix(con, '')


@pytest.mark.parametrize('chunk_size', (5, 10, 20))
def test_one_dim_one_attr(scidb_con, s3_con, chunk_size):
    prefix = 'one_dim_one_attr{}'.format(chunk_size)
    schema = '<v:int64> [i=0:19:0:{}]'.format(chunk_size)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Input
    array = scidb_con.iquery("""
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i']).reset_index(drop=True)

    assert array.equals(
        pandas.DataFrame({'i': range(20), 'v': numpy.arange(0.0, 20.0)}))

    delete_prefix(s3_con, prefix)


@pytest.mark.parametrize('chunk_size', (5, 10, 20))
def test_multi_attr(scidb_con, s3_con, chunk_size):
    prefix = 'multi_attr_{}'.format(chunk_size)
    schema = '<v:int64, w:int64> [i=0:19:0:{}]'.format(chunk_size)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  redimension(
    apply(
      build({}, i),
      w, v * v),
    {}),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema.replace(', w:int64', ''),
                                schema,
                                bucket_name,
                                bucket_prefix))

    # Input
    array = scidb_con.iquery("""
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i']).reset_index(drop=True)

    assert array.equals(
        pandas.DataFrame({'i': range(20),
                          'v': numpy.arange(0.0, 20.0),
                          'w': (numpy.arange(0.0, 20.0) *
                                numpy.arange(0.0, 20.0))}))

    delete_prefix(s3_con, prefix)


@pytest.mark.parametrize('dim_start, dim_end, chunk_size',
                         ((s, e, c)
                          for s in (-21, -13, -10)
                          for e in (10, 16, 19)
                          for c in (5, 7, 10, 20)))
def test_multi_dim(scidb_con, s3_con, dim_start, dim_end, chunk_size):
    prefix = 'multi_dim_{}_{}_{}'.format(dim_start, dim_end, chunk_size)
    schema = '<v:int64> [i={s}:{e}:0:{c}; j=-15:14:0:{c}]'.format(
        s=dim_start, e=dim_end - 1, c=chunk_size)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Input
    array = scidb_con.iquery("""
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = []
    j_lst = []
    v_lst = []
    for i in range(dim_start, dim_end):
        for j in range(-15, 15):
            i_lst.append(i)
            j_lst.append(j)
            v_lst.append(float(i))

    assert array.equals(
        pandas.DataFrame({'i': i_lst,
                          'j': j_lst,
                          'v': v_lst}))

    delete_prefix(s3_con, prefix)


@pytest.mark.parametrize(
    'type_name, is_null, type_numpy, chunk_size',
    ((t, n, p, c)
     for (t, n, p) in itertools.chain(
             ((t, n, numpy.object)
              for n in (True, False)
              for t in ('binary', 'string', 'char')),

             (('datetime', n, None)
              for n in (True, False)),

             (('bool', ) + p
              for p in ((True, numpy.object),
                        (False, numpy.bool))),

             ((t, n, tn)
              for (t, tn) in (('float', numpy.float32),
                              ('double', numpy.float64))
              for n in (True, False)),

             (('{}int{}'.format(g, s),
               n,
               numpy.dtype(
                   'float{}'.format(min(s * 2, 64)) if n
                   else '{}int{}'.format(g, s)).type)
              for g in ('', 'u')
              for s in (8, 16, 32, 64)
              for n in (True, False)))
     for c in (5, 10, 20)))
def test_type(scidb_con, s3_con, type_name, is_null, type_numpy, chunk_size):
    max_val = 20
    prefix = 'type_{}_{}_{}'.format(type_name, is_null, chunk_size)
    schema = '<v:{} {}NULL> [i=0:{}:0:{}]'.format(
        type_name, '' if is_null else 'NOT ', max_val - 1, chunk_size)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    if type_name == 'binary':
        que = scidb_con.input(
            schema.replace('binary NULL', 'binary NOT NULL'),
            upload_data=numpy.array([bytes([i]) for i in range(max_val)],
                                    dtype='object'))
        if is_null:
            que = que.redimension(schema)
    else:
        que = scidb_con.build(schema, '{}(i)'.format(type_name))
    que = que.s3save("bucket_name:'{}'".format(bucket_name),
                     "bucket_prefix:'{}'".format(bucket_prefix))
    res = que.fetch()

    # Input
    array = scidb_con.iquery("""
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i']).reset_index(drop=True)

    if type_name in ('binary', 'char'):
        v = pandas.Series((bytes([i]) if i != 0 or type_name == 'binary'
                           else bytes()
                           for i in range(max_val)),
                          dtype=numpy.object)
    elif type_name == 'string':
        v = pandas.Series((str(i) for i in range(max_val)),
                          dtype=numpy.object)
    elif type_name.startswith('datetime'):
        v = (pandas.Timestamp(i * 10 ** 9) for i in range(max_val))
    elif type_name == 'bool':
        if is_null:
            v = pandas.Series((bool(i) for i in range(max_val)),
                              dtype=numpy.object)
        else:
            v = pandas.Series((bool(i) for i in range(max_val)))
    else:
        v = type_numpy(range(max_val))

    assert array.equals(pandas.DataFrame({'i': range(max_val), 'v': v}))

    delete_prefix(s3_con, prefix)


# Test for Empty Cells
@pytest.mark.parametrize('dim_start, dim_end, chunk_size',
                         ((s, e, c)
                          for s in (-13, -11, -7)
                          for e in (11, 13, 17)
                          for c in (3, 7, 11)))
def test_filter_before(scidb_con, s3_con, dim_start, dim_end, chunk_size):
    prefix = 'filter_before_{}_{}_{}'.format(dim_start, dim_end, chunk_size)
    schema = '<v:int64> [i={s}:{e}:0:{c}; j=-11:13:0:{c}]'.format(
        s=dim_start, e=dim_end - 1, c=chunk_size)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  filter(build({}, i), i % 3 = 0 and i > 7),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Input
    array = scidb_con.iquery("""
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = []
    j_lst = []
    v_lst = []
    for i in range(dim_start, dim_end):
        for j in range(-11, 14):
            if i % 3 == 0 and i > 7:
                i_lst.append(i)
                j_lst.append(j)
                v_lst.append(float(i))

    assert array.equals(
        pandas.DataFrame({'i': i_lst,
                          'j': j_lst,
                          'v': v_lst}))

    delete_prefix(s3_con, prefix)


@pytest.mark.parametrize('dim_start, dim_end, chunk_size',
                         ((s, e, c)
                          for s in (-13, -11, -7)
                          for e in (11, 13, 17)
                          for c in (3, 7, 11)))
def test_filter_after(scidb_con, s3_con, dim_start, dim_end, chunk_size):
    prefix = 'filter_after_{}_{}_{}'.format(dim_start, dim_end, chunk_size)
    schema = '<v:int64> [i={s}:{e}:0:{c}; j=-11:13:0:{c}]'.format(
        s=dim_start, e=dim_end - 1, c=chunk_size)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Input
    array = scidb_con.iquery("""
filter(
  s3input(
    bucket_name:'{}',
    bucket_prefix:'{}'),
  i % 3 = 0 and i > 7)""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = []
    j_lst = []
    v_lst = []
    for i in range(dim_start, dim_end):
        for j in range(-11, 14):
            if i % 3 == 0 and i > 7:
                i_lst.append(i)
                j_lst.append(j)
                v_lst.append(float(i))

    assert array.equals(
        pandas.DataFrame({'i': i_lst,
                          'j': j_lst,
                          'v': v_lst}))

    delete_prefix(s3_con, prefix)


def test_nulls(scidb_con, s3_con):
    prefix = 'nulls'
    schema = '<v:int64> [i=0:99:0:5]'

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, iif(i % 2 = 0, i, missing(i))),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Input
    array = scidb_con.iquery("""
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i']).reset_index(drop=True)

    i_lst = []
    v_lst = []
    for i in range(100):
        i_lst.append(i)
        if i % 2 == 0:
            v_lst.append(float(i))
        else:
            v_lst.append(numpy.nan)

    assert array.equals(
        pandas.DataFrame({'i': i_lst,
                          'v': v_lst}))

    delete_prefix(s3_con, prefix)


def test_chunk_index(scidb_con, s3_con):
    size = 300
    prefix = 'chunk_index'
    schema = '<v:int64> [i=0:{}:0:5]'.format(size - 1)

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Input
    array = scidb_con.iquery("""
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(bucket_name, bucket_prefix),
                             fetch=True)
    array = array.sort_values(by=['i']).reset_index(drop=True)

    assert array.equals(
        pandas.DataFrame({'i': range(size),
                          'v': numpy.arange(0.0, float(size))}))

    delete_prefix(s3_con, prefix)


# Test with Different Cache Sizes
@pytest.mark.parametrize('cache_size', (None, 5000, 2500, 0))
def test_cache(scidb_con, s3_con, cache_size):
    prefix = 'cache'
    schema = '<v:int64 not null, w:int64 not null> [i=0:999:0:100]'

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  redimension(
    filter(
      apply(
        build({}, i),
        w, i * i),
      i % 100 < 80 or i >= 800),
    {}),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema.replace(', w:int64 not null', ''),
                                schema,
                                bucket_name,
                                bucket_prefix))

    # Input
    que = """
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}'
  {})""".format(
      bucket_name,
      bucket_prefix,
      '' if cache_size is None else ', cache_size:{}'.format(cache_size))

    if cache_size == 2500:
        with pytest.raises(requests.exceptions.HTTPError):
            array = scidb_con.iquery(que, fetch=True)
    else:
        array = scidb_con.iquery(que, fetch=True)
        array = array.sort_values(by=['i']).reset_index(drop=True)

        assert array.equals(
            pandas.DataFrame(data=((i, i, i * i)
                                   for i in range(1000)
                                   if (i % 100 < 80 or i >= 800)),
                             columns=('i', 'v', 'w')))

    delete_prefix(s3_con, prefix)


# Test with Multiple Arrow Chunks per File
def test_arrow_chunk(scidb_con, s3_con):
    prefix = 'arrow_chunk'
    schema = '<v:int64> [i=0:999:0:1000]'

    # Store
    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    # Re-write one SciDB Chunk file to use multiple Arrow Chunks
    bucket_file = '{}/c_0'.format(bucket_prefix)
    obj = s3_con.get_object(Bucket=bucket_name, Key=bucket_file)
    reader = pyarrow.ipc.open_stream(obj['Body'].read())
    tbl = reader.read_all()
    sink = pyarrow.BufferOutputStream()
    writer = pyarrow.ipc.RecordBatchStreamWriter(sink, tbl.schema)
    for batch in tbl.to_batches(max_chunksize=200):  # 1000 / 200 = 5 chunks
        writer.write_batch(batch)
    writer.close()
    s3_con.put_object(Body=sink.getvalue().to_pybytes(),
                      Bucket=bucket_name,
                      Key=bucket_file)

    # Input
    que = """
s3input(
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(
      bucket_name,
      bucket_prefix)

    with pytest.raises(requests.exceptions.HTTPError):
        array = scidb_con.iquery(que, fetch=True)

    delete_prefix(s3_con, prefix)
