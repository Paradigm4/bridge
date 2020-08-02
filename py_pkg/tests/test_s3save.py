import boto3
import pandas
import pytest
import scidbpy
import scidbs3


bucket_name = 'p4tests'
base_prefix = 's3bridge-tests'
base_metadata = {'version':   '1',
                 'format':    'arrow',
                 'attribute': 'ALL'}


@pytest.fixture
def scidb_con():
    return scidbpy.connect()


@pytest.fixture
def s3_con():
    return boto3.client('s3')


def delete_prefix(s3_con, prefix):
    result = s3_con.list_objects_v2(Bucket=bucket_name,
                                    Prefix='/'.join((base_prefix, prefix)))
    objects = [{'Key': e['Key']} for e in result['Contents']]
    s3_con.delete_objects(Bucket=bucket_name,
                          Delete={'Objects': objects})


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
    assert array.list_chunks() == ((0,),)
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
    assert array.list_chunks() == tuple((i,) for i in range(0, 20, 5))
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
    assert array.list_chunks() == tuple((i, j)
                                        for i in range(0, 10, 5)
                                        for j in range(10, 20, 5))
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
    assert array.list_chunks() == tuple((i, j)
                                        for i in range(0, 10, 5)
                                        for j in range(10, 20, 5))
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
    assert array.list_chunks() == tuple((i, 15)
                                        for i in range(0, 10, 5))
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
