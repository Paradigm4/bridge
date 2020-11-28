import boto3
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
    con = boto3.client('s3')
    yield con
    delete_prefix(con, '')


def test_one_chunk(scidb_con, s3_con):
    prefix = 'one_chunk'
    schema = '<v:int64> [i=0:9]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.__str__() == 's3://{}/{}'.format(bucket_name, bucket_prefix)
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(
                                  schema.replace(']', ':0:1000000]'))}}
    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data={'i': range(1)}))
    pandas.testing.assert_frame_equal(
        array.get_chunk(0).to_pandas(),
        pandas.DataFrame(data={'v': range(10), 'i': range(10)}))

    delete_prefix(s3_con, prefix)


def test_multi_chunk(scidb_con, s3_con):
    prefix = 'multi_chunk'
    schema = '<v:int64> [i=0:19:0:5]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.__str__() == 's3://{}/{}'.format(bucket_name, bucket_prefix)
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema)}}
    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data={'i': range(0, 20, 5)}))
    pandas.testing.assert_frame_equal(
        array.get_chunk(0).to_pandas(),
        pandas.DataFrame(data={'v': range(5), 'i': range(5)}))
    pandas.testing.assert_frame_equal(
        array.get_chunk(10).to_pandas(),
        pandas.DataFrame(data={'v': range(10, 15), 'i': range(10, 15)}))

    delete_prefix(s3_con, prefix)


def test_multi_dim(scidb_con, s3_con):
    prefix = 'multi_dim'
    schema = '<v:int64> [i=0:9:0:5; j=10:19:0:5]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.__str__() == 's3://{}/{}'.format(bucket_name, bucket_prefix)
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema)}}
    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data=((i, j)
                               for i in range(0, 9, 5)
                               for j in range(10, 20, 5)),
                         columns=('i', 'j')))
    pandas.testing.assert_frame_equal(
        array.get_chunk(0, 10).to_pandas(),
        pandas.DataFrame(data=((i, i, j)
                               for i in range(5)
                               for j in range(10, 15)),
                         columns=('v', 'i', 'j')))

    delete_prefix(s3_con, prefix)


def test_multi_atts(scidb_con, s3_con):
    prefix = 'multi_attr'
    schema = '<v:int64,w:double> [i=0:9:0:5; j=10:19:0:5]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  apply(
    build({}, i),
    w, double(v * v)),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(
      schema.replace(',w:double', ''), bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.__str__() == 's3://{}/{}'.format(bucket_name, bucket_prefix)
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema)}}
    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data=((i, j)
                               for i in range(0, 9, 5)
                               for j in range(10, 20, 5)),
                         columns=('i', 'j')))
    pandas.testing.assert_frame_equal(
        array.get_chunk(0, 10).to_pandas(),
        pandas.DataFrame(data=((i, float(i * i), i, j)
                               for i in range(5)
                               for j in range(10, 15)),
                         columns=('v', 'w', 'i', 'j')))

    delete_prefix(s3_con, prefix)


def test_filter(scidb_con, s3_con):
    prefix = 'filter'
    schema = '<v:int64> [i=0:9:0:5; j=10:19:0:5]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  filter(
    build({}, i),
    (i < 3 or i > 5) and j > 15),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.__str__() == 's3://{}/{}'.format(bucket_name, bucket_prefix)
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema)}}
    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data=((i, 15) for i in range(0, 9, 5)),
                         columns=('i', 'j')))
    pandas.testing.assert_frame_equal(
        array.get_chunk(0, 15).to_pandas(),
        pandas.DataFrame(data=((i, i, j)
                               for i in range(3)
                               for j in range(16, 20)),
                         columns=('v', 'i', 'j')))
    pandas.testing.assert_frame_equal(
        array.get_chunk(5, 15).to_pandas(),
        pandas.DataFrame(data=((i, i, j)
                               for i in range(6, 10)
                               for j in range(16, 20)),
                         columns=('v', 'i', 'j')))

    delete_prefix(s3_con, prefix)


def test_empty_chunks(scidb_con, s3_con):
    prefix = 'empty_chunks'
    schema = '<v:int64> [i=0:19:0:5; j=10:49:0:10]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  filter(
    build({}, i + j),
    (i >= 5 and i < 10 or i >= 15) and (j < 20 or j >= 30 and j < 40)),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.__str__() == 's3://{}/{}'.format(bucket_name, bucket_prefix)
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema)}}
    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data=((i, j)
                               for i in (5, 15)
                               for j in (10, 30)),
                         columns=('i', 'j')))

    for i_st in (5, 15):
        for j_st in (10, 30):
            pandas.testing.assert_frame_equal(
                array.get_chunk(i_st, j_st).to_pandas(),
                pandas.DataFrame(data=((i + j, i, j)
                                       for i in range(i_st, i_st + 5)
                                       for j in range(j_st, j_st + 10)),
                                 columns=('v', 'i', 'j')))

    delete_prefix(s3_con, prefix)


def test_one_index(scidb_con, s3_con):
    prefix = 'one_index'
    schema = '<v:int64> [i=0:99:0:5; j=0:99:0:5]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i + j),
  bucket_name:'{}',
  bucket_prefix:'{}')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data=((i, j)
                               for i in range(0, 100, 5)
                               for j in range(0, 100, 5)),
                         columns=('i', 'j')))

    index_prefix = '{}/index/'.format(bucket_prefix)
    result = s3_con.list_objects_v2(Bucket=bucket_name,
                                    Prefix=index_prefix)
    keys = [split['Key'] for split in result['Contents']]
    assert keys == ['{}0'.format(index_prefix)]

    delete_prefix(s3_con, prefix)


def test_multi_index(scidb_con, s3_con):
    prefix = 'multi_index'
    schema = '<v:int64> [i=0:99:0:5; j=0:99:0:5]'

    for index_split in (100, 200, 400, 800):
        sub_prefix = '{}_0'.format(prefix)
        bucket_prefix = '/'.join((base_prefix, sub_prefix))
        scidb_con.iquery("""
s3save(
  build({}, i + j),
  bucket_name:'{}',
  bucket_prefix:'{}',
  index_split:{})""".format(schema, bucket_name, bucket_prefix, index_split))

        array = scidbs3.S3Array(bucket_name=bucket_name,
                                bucket_prefix=bucket_prefix)

        pandas.testing.assert_frame_equal(
            array.list_chunks(),
            pandas.DataFrame(data=((i, j)
                                   for i in range(0, 100, 5)
                                   for j in range(0, 100, 5)),
                             columns=('i', 'j')))

        index_prefix = '{}/index/'.format(bucket_prefix)
        result = s3_con.list_objects_v2(Bucket=bucket_name,
                                        Prefix=index_prefix)
        keys = [split['Key'] for split in result['Contents']]
        assert keys == ['{}{}'.format(index_prefix, i)
                        for i in range(800 // index_split)]

        delete_prefix(s3_con, sub_prefix)


def test_no_compression(scidb_con, s3_con):
    prefix = 'no_compression'
    schema = '<v:int64> [i=0:19:0:5; j=10:49:0:10]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i + j),
  bucket_name:'{}',
  bucket_prefix:'{}',
  compression:'none')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema)}}
    for i_st in (5, 15):
        for j_st in (10, 30):
            pandas.testing.assert_frame_equal(
                array.get_chunk(i_st, j_st).to_pandas(),
                pandas.DataFrame(data=((i + j, i, j)
                                       for i in range(i_st, i_st + 5)
                                       for j in range(j_st, j_st + 10)),
                                 columns=('v', 'i', 'j')))

    sz = s3_con.head_object(Bucket=bucket_name,
                            Key='{}/{}'.format(bucket_prefix,
                                               'c_0_0'))['ContentLength']
    assert sz > 1500

    delete_prefix(s3_con, prefix)


def test_gzip_compression(scidb_con, s3_con):
    prefix = 'gzip_compression'
    schema = '<v:int64> [i=0:19:0:5; j=10:49:0:10]'

    bucket_prefix = '/'.join((base_prefix, prefix))
    scidb_con.iquery("""
s3save(
  build({}, i + j),
  bucket_name:'{}',
  bucket_prefix:'{}',
  compression:'gzip')""".format(schema, bucket_name, bucket_prefix))

    array = scidbs3.S3Array(bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix)

    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema),
                                 'compression': 'gzip'}}
    for i_st in (5, 15):
        for j_st in (10, 30):
            pandas.testing.assert_frame_equal(
                array.get_chunk(i_st, j_st).to_pandas(),
                pandas.DataFrame(data=((i + j, i, j)
                                       for i in range(i_st, i_st + 5)
                                       for j in range(j_st, j_st + 10)),
                                 columns=('v', 'i', 'j')))

    sz = s3_con.head_object(Bucket=bucket_name,
                            Key='{}/{}'.format(bucket_prefix,
                                               'c_0_0'))['ContentLength']
    assert sz < 500

    delete_prefix(s3_con, prefix)
