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

import pyarrow
import boto3
import itertools
import os
import os.path
import pandas
import pyarrow
import scidbpy

from .driver import Driver

__version__ = '19.11.1'


class Array(object):
    """Wrapper for SciDB array stored externally"""

    def __init__(self, url):
        self.url = url

        self._metadata = None
        self._schema = None

    def __iter__(self):
        return (i for i in (self.url, ))

    def __eq__(self):
        return tuple(self) == tuple(other)

    def __repr__(self):
        return ('{}(url={!r})').format(type(self).__name__, *self)

    def __str__(self):
        return self.url

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = Array.metadata_from_string(
                Driver.read_metadata(self.url))
        return self._metadata

    @property
    def schema(self):
        if self._schema is None:
            self._schema = scidbpy.Schema.fromstring(
                self.metadata['schema'])
        return self._schema

    @staticmethod
    def metadata_from_string(input):
        res = dict(ln.split('\t') for ln in input.strip().split('\n'))
        try:
            if res['compression'] == 'none':
                res['compression'] = None
        except KeyError:
            pass
        return res

    def list_chunks(self):
        batches = []
        indices_url = '{}/index'.format(self.url)
        for index_url in Driver.list(indices_url):
            reader = Driver.reader(index_url, 'gzip')
            for batch in reader:
                batches.append(batch)

        table = pyarrow.Table.from_batches(batches)
        index = table.to_pandas(split_blocks=True, self_destruct=True)
        index.sort_values(by=list(index.columns),
                          inplace=True,
                          # ignore_index=True  # Pandas >= 1.0.0
                          )
        index.reset_index(inplace=True, drop=True)  # Pandas < 1.0.0
        return index

    def get_chunk(self, *argv):
        return Chunk(self, *argv)


class Chunk(object):
    """Wrapper for SciDB array chunk stored externally"""

    def __init__(self, array, *argv):
        self.array = array

        if (len(argv) == 1 and
                type(argv[0]) is pandas.core.series.Series):
            argv = tuple(argv[0])

        dims = self.array.schema.dims
        if len(argv) != len(dims):
            raise Exception(
                ('Number of arguments, {}, does no match the number of ' +
                 'dimensions, {}. Please specify one start coordiante for ' +
                 'each dimension.').format(
                     len(argv), len(self.array.schema.dims)))

        parts = ['c']
        for (val, dim) in zip(argv, dims):
            if val < dim.low_value or val > dim.high_value:
                raise Exception(
                    ('Coordinate value, {}, is outside of dimension range, '
                     '[{}:{}]').format(
                         val, dim.low_value, dim.high_value))

            part = val - dim.low_value
            if part % dim.chunk_length != 0:
                raise Exception(
                    ('Coordinate value, {}, is not a multiple of ' +
                     'chunk size, {}').format(
                         val, dim.chunk_length))
            part = part // dim.chunk_length
            parts.append(part)

        self.url = '{}/chunks/{}'.format(self.array.url,
                                         '_'.join(map(str, parts)))
        self._table = None

    def __iter__(self):
        return (i for i in (self.array, self.url))

    def __eq__(self):
        return tuple(self) == tuple(other)

    def __repr__(self):
        return ('{}(array={!r}, url={!r})').format(
            type(self).__name__, *self)

    def __str__(self):
        return self.url

    @property
    def table(self):
        if self._table == None:
            self._table = Driver.reader(
                self.url,
                compression=self.array.metadata['compression']).read_all()
        return self._table

    def to_pandas(self):
        return pyarrow.Table.to_pandas(self.table)

    def from_pandas(self, pd):
        self._table = pyarrow.Table.from_pandas(pd)
        self._table = self._table.replace_schema_metadata()

    def save(self):
        sink = Driver.writer(self.url,
                             schema=self._table.schema,
                             compression=self.array.metadata['compression'])
        writer = next(sink)
        writer.write_table(self._table)
        sink.close()
