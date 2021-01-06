# BEGIN_COPYRIGHT
#
# Copyright (C) 2020 Paradigm4 Inc.
# All Rights Reserved.
#
# scidbbridge is a plugin for SciDB, an Open Source Array DBMS
# maintained by Paradigm4. See http://www.paradigm4.com/
#
# scidbbridge is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as
# published by the Free Software Foundation.
#
# scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
# KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
# AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public
# License along with scidbbridge. If not, see
# <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

import itertools
import os.path
import pandas
import pytest
import requests
import scidbbridge

from common import *


@pytest.mark.parametrize('url', test_urls)
def test_one_chunk(scidb_con, url):
    url = '{}/one_chunk'.format(url)
    schema = '<v:int64> [i=0:9]'

    scidb_con.iquery("""
xsave(
  build({}, i),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)

    assert array.__str__() == url
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(
                                  schema.replace(']', ':0:1000000]'))}}
    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data={'i': range(1)}))
    pandas.testing.assert_frame_equal(
        array.get_chunk(0).to_pandas(),
        pandas.DataFrame(data={'v': range(10), 'i': range(10)}))


@pytest.mark.parametrize('url', test_urls)
def test_multi_chunk(scidb_con, url):
    url = '{}/multi_chunk'.format(url)
    schema = '<v:int64> [i=0:19:0:5]'

    scidb_con.iquery("""
xsave(
  build({}, i),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)

    assert array.__str__() == url
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


@pytest.mark.parametrize('url', test_urls)
def test_multi_dim(scidb_con, url):
    url = '{}/multi_dim'.format(url)
    schema = '<v:int64> [i=0:9:0:5; j=10:19:0:5]'

    scidb_con.iquery("""
xsave(
  build({}, i),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)

    assert array.__str__() == url
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


@pytest.mark.parametrize('url', test_urls)
def test_multi_atts(scidb_con, url):
    url = '{}/multi_attr'.format(url)
    schema = '<v:int64,w:double> [i=0:9:0:5; j=10:19:0:5]'

    scidb_con.iquery("""
xsave(
  apply(
    build({}, i),
    w, double(v * v)),
  '{}')""".format(
      schema.replace(',w:double', ''), url))

    array = scidbbridge.Array(url)

    assert array.__str__() == url
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


@pytest.mark.parametrize('url', test_urls)
def test_filter(scidb_con, url):
    url = '{}/filter'.format(url)
    schema = '<v:int64> [i=0:9:0:5; j=10:19:0:5]'

    scidb_con.iquery("""
xsave(
  filter(
    build({}, i),
    (i < 3 or i > 5) and j > 15),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)

    assert array.__str__() == url
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


@pytest.mark.parametrize('url', test_urls)
def test_empty_chunks(scidb_con, url):
    url = '{}/empty_chunks'.format(url)
    schema = '<v:int64> [i=0:19:0:5; j=10:49:0:10]'

    scidb_con.iquery("""
xsave(
  filter(
    build({}, i + j),
    (i >= 5 and i < 10 or i >= 15) and (j < 20 or j >= 30 and j < 40)),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)

    assert array.__str__() == url
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


@pytest.mark.parametrize('url', test_urls)
def test_one_index(scidb_con, url):
    prefix = 'one_index'
    url = '{}/{}'.format(url, prefix)
    schema = '<v:int64> [i=0:99:0:5; j=0:99:0:5]'

    scidb_con.iquery("""
xsave(
  build({}, i + j),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)

    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data=((i, j)
                               for i in range(0, 100, 5)
                               for j in range(0, 100, 5)),
                         columns=('i', 'j')))

    if url.startswith('s3://'):
        index_prefix = '{}/{}/index/'.format(base_prefix, prefix)
        result = s3_con.list_objects_v2(Bucket=s3_bucket, Prefix=index_prefix)
        keys = [split['Key'] for split in result['Contents']]
    elif url.startswith('file://'):
        index_prefix = '{}/{}/index/'.format(fs_base, prefix)
        keys = []
        for fn in os.listdir(index_prefix):
            key = index_prefix + fn
            if os.path.isfile(key):
                keys.append(key)
        keys.sort()

    assert keys == ['{}{}'.format(index_prefix, 0)]


@pytest.mark.parametrize(('url', 'index_split'),
                         itertools.product(test_urls, (100, 200, 400, 800)))
def test_multi_index(scidb_con, url, index_split):
    prefix = 'multi_index_{}'.format(index_split)
    url = '{}/{}'.format(url, prefix)
    schema = '<v:int64> [i=0:99:0:5; j=0:99:0:5]'

    scidb_con.iquery("""
xsave(
  build({}, i + j),
  '{}',
  index_split:{})""".format(schema, url, index_split))

    array = scidbbridge.Array(url)

    pandas.testing.assert_frame_equal(
        array.list_chunks(),
        pandas.DataFrame(data=((i, j)
                               for i in range(0, 100, 5)
                               for j in range(0, 100, 5)),
                         columns=('i', 'j')))

    if url.startswith('s3://'):
        index_prefix = '{}/{}/index/'.format(base_prefix, prefix)
        result = s3_con.list_objects_v2(Bucket=s3_bucket,
                                        Prefix=index_prefix)
        keys = [split['Key'] for split in result['Contents']]
    elif url.startswith('file://'):
        index_prefix = '{}/{}/index/'.format(fs_base, prefix)
        keys = []
        for fn in os.listdir(index_prefix):
            key = index_prefix + fn
            if os.path.isfile(key):
                keys.append(key)
        keys.sort()

    assert keys == ['{}{}'.format(index_prefix, i)
                    for i in range(800 // index_split)]


@pytest.mark.parametrize(('url', 'compression', 'sz_min', 'sz_max'),
                         ((url, *param)
                          for url in test_urls
                          for param in zip(('default', 'none', 'gzip'),
                                           (1500, 1500, 0),
                                           (9999, 9999, 500))))
def test_compression(scidb_con, url, compression, sz_min, sz_max):
    prefix = 'compression_{}'.format(compression)
    url = '{}/{}'.format(url, prefix)
    schema = '<v:int64> [i=0:19:0:5; j=10:49:0:10]'

    scidb_con.iquery("""
xsave(
  build({}, i + j),
  '{}'{})""".format(schema,
                    url,
                    '' if compression == 'default'
                    else ", compression:'{}'".format(compression)))

    array = scidbbridge.Array(url)

    m_compression = (None if compression in ('default', 'none')
                     else compression)
    assert array.metadata == {**base_metadata,
                              **{'schema': '{}'.format(schema),
                                 'compression': m_compression}}
    for i_st in (5, 15):
        for j_st in (10, 30):
            pandas.testing.assert_frame_equal(
                array.get_chunk(i_st, j_st).to_pandas(),
                pandas.DataFrame(data=((i + j, i, j)
                                       for i in range(i_st, i_st + 5)
                                       for j in range(j_st, j_st + 10)),
                                 columns=('v', 'i', 'j')))

    if url.startswith('s3://'):
        sz = s3_con.head_object(Bucket=s3_bucket,
                                Key='{}/{}/c_0_0'.format(
                                    base_prefix, prefix))['ContentLength']
    elif url.startswith('file://'):
        sz = os.path.getsize('{}/{}/c_0_0'.format(fs_base, prefix))

    assert sz > sz_min
    assert sz < sz_max


@pytest.mark.parametrize('url', (None,
                                 '',
                                 'foo',
                                 'foo://',
                                 'foo://bar/taz',
                                 's3',
                                 's3:',
                                 's3:/',
                                 's3://',
                                 's3:\\',
                                 's3:\\\\',
                                 'file',
                                 'file:',
                                 'file:/',
                                 'file://',
                                 'file:\\',
                                 ))
def test_bad_url(scidb_con, url):
    schema = '<v:int64> [i=0:9]'

    que = 'xsave(build({}, i){})'.format(
        schema, '' if url is None else ", '{}'".format(url))

    with pytest.raises(requests.exceptions.HTTPError):
        scidb_con.iquery(que)