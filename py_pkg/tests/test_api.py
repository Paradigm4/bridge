# BEGIN_COPYRIGHT
#
# Copyright (C) 2020-2021 Paradigm4 Inc.
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

import pandas
import pytest
import scidbbridge

from common import *


@pytest.mark.parametrize('url', test_urls)
def test_chunks_all(scidb_con, url):
    url = '{}/chunks_all'.format(url)
    schema = '<v:int64> [i=0:19:0:5]'

    scidb_con.iquery("""
xsave(
  build({}, i),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)
    chunks = array.list_chunks()

    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data={'i': range(0, 20, 5)}))

    for i in range(0, 20, 5):
        array.get_chunk(i)
    with pytest.raises(Exception):
        array.get_chunk()
    with pytest.raises(Exception):
        array.get_chunk(-1)
    with pytest.raises(Exception):
        array.get_chunk(20)
    with pytest.raises(Exception):
        array.get_chunk(17)
    with pytest.raises(Exception):
        array.get_chunk(0, 5)


@pytest.mark.parametrize('url', test_urls)
def test_chunks_holes(scidb_con, url):
    url = '{}/chunks_holes'.format(url)
    schema = '<v:int64> [i=-3:31:0:7]'

    scidb_con.iquery("""
xsave(
  filter(
    build({}, i),
    i < 4 or i >= 11 and i % 3 = 0),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)
    chunks = array.list_chunks()

    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data={'i': [-3, 11, 18, 25]}))

    for i in range(-3, 31, 7):
        array.get_chunk(i)
    with pytest.raises(Exception):
        array.get_chunk()
    with pytest.raises(Exception):
        array.get_chunk(-5)
    with pytest.raises(Exception):
        array.get_chunk(35)
    with pytest.raises(Exception):
        array.get_chunk(17)
    with pytest.raises(Exception):
        array.get_chunk(0, 5)


@pytest.mark.parametrize('url', test_urls)
def test_chunks_dim_all(scidb_con, url):
    url = '{}/chunks_dim_all'.format(url)
    schema = '<v:int64> [i=0:19:0:5; j=0:19:0:10]'

    scidb_con.iquery("""
xsave(
  build({}, i),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)
    chunks = array.list_chunks()

    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data=((i, j)
                               for i in range(0, 20, 5)
                               for j in range(0, 20, 10)),
                         columns=('i', 'j')))

    for i in range(0, 20, 5):
        for j in range(0, 20, 10):
            array.get_chunk(i, j)
    with pytest.raises(Exception):
        array.get_chunk()
    with pytest.raises(Exception):
        array.get_chunk(0)
    with pytest.raises(Exception):
        array.get_chunk(-1, 0)
    with pytest.raises(Exception):
        array.get_chunk(5, 17)
    with pytest.raises(Exception):
        array.get_chunk(5, 10, 0)


@pytest.mark.parametrize('url', test_urls)
def test_chunks_dim_holes(scidb_con, url):
    url = '{}/chunks_dim_holes'.format(url)
    schema = '<v:int64> [i=-3:31:0:7; j=11:23:0:3]'

    scidb_con.iquery("""
xsave(
  filter(
    build({}, i),
    i < 4 and j >= 20 or i >= 11 and j < 14 and i % 3 = 0 and j % 2 = 0),
  '{}')""".format(schema, url))

    array = scidbbridge.Array(url)
    chunks = array.list_chunks()

    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data=((i, j)
                               for i in range(-3, 32, 7)
                               for j in range(11, 24, 3)
                               if i < 4 and j >= 20 or i >= 11 and j < 14),
                         columns=('i', 'j')))

    for i in range(-3, 31, 7):
        for j in range(11, 24, 3):
            array.get_chunk(i, j)
    with pytest.raises(Exception):
        array.get_chunk(-3)
    with pytest.raises(Exception):
        array.get_chunk(-5, 11)
    with pytest.raises(Exception):
        array.get_chunk(11, 26)
    with pytest.raises(Exception):
        array.get_chunk(-3, 12)
    with pytest.raises(Exception):
        array.get_chunk(-3, 11, 0)


@pytest.mark.parametrize('url', test_urls)
def test_update_chunk(scidb_con, url):
    url = '{}/update_chunk'.format(url)
    schema = '<v:int64> [i=0:19:0:5; j=0:19:0:10]'

    # Create Array Using xsave
    scidb_con.iquery("""
xsave(
  filter(
    build({}, i * j),
    i % 3 = 0 and j % 2 = 0),
  '{}')""".format(schema, url))

    # Fetch Array Using xinput
    array_pd = scidb_con.iquery("xinput('{}')".format(url), fetch=True)
    array_pd = array_pd.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = []
    j_lst = []
    v_lst = []
    for i in range(0, 20):
        for j in range(0, 20):
            if i % 3 == 0 and j % 2 == 0:
                i_lst.append(i)
                j_lst.append(j)
                v_lst.append(float(i * j))
    pandas.testing.assert_frame_equal(
        array_pd,
        pandas.DataFrame({'i': i_lst,
                          'j': j_lst,
                          'v': v_lst}))

    # Fetch Chunks List Using Python API
    array = scidbbridge.Array(url)
    chunks = array.list_chunks()

    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data=((i, j)
                               for i in range(0, 20, 5)
                               for j in range(0, 20, 10)),
                         columns=('i', 'j')))

    # Fetch Chunk Using Python API
    chunk = array.get_chunk(0, 0)
    pd = chunk.to_pandas()
    pandas.testing.assert_frame_equal(
        pd,
        pandas.DataFrame(data=((i * j, i, j)
                               for i in range(0, 5)
                               for j in range(0, 10)
                               if i % 3 == 0 and j % 2 == 0),
                         columns=('v', 'i', 'j')))

    # Update Chunk Using Python API
    pd = pd.append(pandas.DataFrame({'v': (100, 200),
                                     'i': (4, 1),
                                     'j': (3, 3)}),
                   ignore_index=True)

    chunk.from_pandas(pd)
    chunk.save()

    # Insert duplicates
    pd_dup = pd.append({'v': 100, 'i': 4, 'j': 3}, ignore_index=True)
    with pytest.raises(Exception):
        chunk.from_pandas(pd_dup)
    pd_dup = pd.append({'v': 100, 'i': 0, 'j': 2}, ignore_index=True)
    with pytest.raises(Exception):
        chunk.from_pandas(pd_dup)

    # Insert coordinates outside chunk boundaries
    pd_out = pd.append({'v': 100, 'i': 0, 'j': -1}, ignore_index=True)
    with pytest.raises(Exception):
        chunk.from_pandas(pd_out)
    pd_out = pd.append({'v': 100, 'i': 5, 'j': 0}, ignore_index=True)
    with pytest.raises(Exception):
        chunk.from_pandas(pd_out)

    # Fetch Array Using xinput
    array_pd = scidb_con.iquery("xinput('{}')".format(url), fetch=True)
    array_pd = array_pd.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = []
    j_lst = []
    v_lst = []
    for i in range(0, 20):
        for j in range(0, 20):
            if i % 3 == 0 and j % 2 == 0:
                i_lst.append(i)
                j_lst.append(j)
                v_lst.append(float(i * j))
            elif i in (1, 4) and j == 3:
                i_lst.append(i)
                j_lst.append(j)
                if i == 4:
                    v_lst.append(float(100))
                else:
                    v_lst.append(float(200))
    pandas.testing.assert_frame_equal(
        array_pd,
        pandas.DataFrame({'i': i_lst,
                          'j': j_lst,
                          'v': v_lst}))


@pytest.mark.parametrize('url', test_urls)
def test_update_add_chunks(scidb_con, url):
    url = '{}/update_add_chunks'.format(url)
    schema = '<v:int64> [i=0:49:0:5; j=0:29:0:10]'

    # Create Array Using xsave
    scidb_con.iquery("""
xsave(
  redimension(
    filter(
      build({}, i * j),
      i % 3 = 0 and j % 2 = 0),
    {}),
  '{}')""".format(schema.replace('49', '19').replace('29', '19'),
                  schema,
                  url))

    # Fetch Array Using xinput
    array_pd = scidb_con.iquery("xinput('{}')".format(url), fetch=True)
    array_pd = array_pd.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = []
    j_lst = []
    v_lst = []
    for i in range(0, 20):
        for j in range(0, 20):
            if i % 3 == 0 and j % 2 == 0:
                i_lst.append(i)
                j_lst.append(j)
                v_lst.append(float(i * j))
    pandas.testing.assert_frame_equal(
        array_pd,
        pandas.DataFrame({'i': i_lst, 'j': j_lst, 'v': v_lst}))

    # Fetch Chunks List Using Python API
    array = scidbbridge.Array(url)
    chunks = array.list_chunks()

    chunks_list = [(i, j)
                   for i in range(0, 20, 5)
                   for j in range(0, 20, 10)]
    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data=chunks_list, columns=('i', 'j')))

    # Get New Chunk, Add Data to Chunk, Add Chunk to Index
    chunk = array.get_chunk(20, 10)
    chunk.from_pandas(pandas.DataFrame({'v': (100, ),
                                        'i': (20, ),
                                        'j': (10, )}))
    chunk.save()
    array.add_to_index(pandas.DataFrame({'i': (20, ),
                                         'j': (10, )}))
    array.save_index()

    # Fetch Array Using xinput
    array_pd = scidb_con.iquery("xinput('{}')".format(url), fetch=True)
    array_pd = array_pd.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst.append(20)
    j_lst.append(10)
    v_lst.append(100)
    pandas.testing.assert_frame_equal(
        array_pd,
        pandas.DataFrame({'i': i_lst, 'j': j_lst, 'v': v_lst}))

    # Add Data to Chunk
    chunk = array.get_chunk(20, 10)
    pd = chunk.to_pandas()
    pd = pd.append(pandas.DataFrame({'v': (110, 120),
                                     'i': (22, 24),
                                     'j': (11, 11)}),
                   ignore_index=True)
    chunk.from_pandas(pd)
    chunk.save()

    # Fetch Chunks List Using Python API
    chunks = array.list_chunks()

    chunks_list.append((20, 10))
    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data=chunks_list, columns=('i', 'j')))

    # Fetch Array Using xinput
    array_pd = scidb_con.iquery("xinput('{}')".format(url), fetch=True)
    array_pd = array_pd.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = i_lst + [22, 24]
    j_lst = j_lst + [11, 11]
    v_lst = v_lst + [110, 120]
    pandas.testing.assert_frame_equal(
        array_pd,
        pandas.DataFrame({'i': i_lst, 'j': j_lst, 'v': v_lst}))


    # Get Tow New Chunk, Add Data to Chunks, Add Chunks to Index
    chunk = array.get_chunk(40, 20)
    chunk.from_pandas(pandas.DataFrame({'v': (10, 10),
                                        'i': (42, 43),
                                        'j': (25, 25)}))
    chunk.save()

    chunk = array.get_chunk(45, 20)
    chunk.from_pandas(pandas.DataFrame({'v': (10, 10),
                                        'i': (48, 49),
                                        'j': (25, 25)}))
    chunk.save()

    array.add_to_index(pandas.DataFrame({'i': (40, 45),
                                         'j': (20, 20)}))
    array.save_index()

    # Fetch Chunks List Using Python API
    chunks = array.list_chunks()

    chunks_list = chunks_list + [(40, 20), (45, 20)]
    pandas.testing.assert_frame_equal(
        chunks,
        pandas.DataFrame(data=chunks_list, columns=('i', 'j')))

    # Fetch Array Using xinput
    array_pd = scidb_con.iquery("xinput('{}')".format(url), fetch=True)
    array_pd = array_pd.sort_values(by=['i', 'j']).reset_index(drop=True)

    i_lst = i_lst + [42, 43, 48, 49]
    j_lst = j_lst + [25] * 4
    v_lst = v_lst + [10] * 4
    pandas.testing.assert_frame_equal(
        array_pd,
        pandas.DataFrame({'i': i_lst, 'j': j_lst, 'v': v_lst}))

    # Get Chunk Errors
    with pytest.raises(Exception):
        ar.get_chunk(-5, 10)
    with pytest.raises(Exception):
        ar.get_chunk(0, 5)
    with pytest.raises(Exception):
        ar.get_chunk(25, 10)

    # Add Chunks to Index Errors
    with pytest.raises(Exception):
        ar.add_to_index([1, 2])
    with pytest.raises(Exception):
        ar.add_to_index(pandas.DataFrame({'i':(25, )}))
    with pytest.raises(Exception):
        ar.add_to_index(pandas.DataFrame({'i':(25, ), 'k':(20, )}))
    with pytest.raises(Exception):
        ar.add_to_index(pandas.DataFrame({'i':(25, ), 'j':(25, )}))
    with pytest.raises(Exception):
        ar.add_to_index(pandas.DataFrame({'i':(50, ), 'j':(20, )}))
    with pytest.raises(Exception):
        ar.add_to_index(pandas.DataFrame({'i':(25, ), 'j':(30, )}))
    with pytest.raises(Exception):
        ar.add_to_index(pandas.DataFrame({'i':(40, ), 'j':(20, )}))
    with pytest.raises(Exception):
        ar.add_to_index(pandas.DataFrame({'i':(25, 40), 'j':(20, 20)}))
