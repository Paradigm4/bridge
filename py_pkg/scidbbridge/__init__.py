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
        self._index = None

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

    @property
    def index(self):
        if self._index is None:
            # Read index as Arrow Table
            tables = []
            index_dir_url = '{}/index'.format(self.url)
            for index_url in Driver.list(index_dir_url):
                reader = Driver.create_reader(index_url, 'gzip')
                tables.append(reader.read_all())
            table = pyarrow.concat_tables(tables)

            self._index_schema = table.schema

            # Convert Arrow Table index to Pandas DataFrame
            self._index = table.to_pandas(split_blocks=True,
                                          self_destruct=True)
            self._index.sort_values(by=list(self._index.columns),
                                    inplace=True)
            # Pandas < 1.0.0
            self._index.reset_index(inplace=True, drop=True)
            # Pandas >= 1.0.0
            # sort_values(ignore_index=True)
        return self._index

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
        return self.index

    def add_to_index(self, extra_index):
        # Check for a DataFrame
        if not isinstance(extra_index, pandas.DataFrame):
            raise Exception("Value provided as argument " +
                            "is not a Pandas DataFrame")

        # Check for the same columns
        if not self.index.columns.equals(extra_index.columns):
            raise Exception("Existing index and extra index " +
                            "do not have the same columns")

        # Check for coordinates outside chunk boundaries
        for dim in self.schema.dims:
            vals = extra_index[dim.name]
            if any(vals < dim.low_value):
                raise Exception("Index values smaller than " +
                                "lower bound on dimension " + dim.name)
            if any(vals > dim.high_value):
                raise Exception("Index values bigger than " +
                                "upper bound on dimension " + dim.name)
            if any((vals - dim.low_value) % dim.chunk_length != 0):
                raise Exception("Index values misaligned " +
                                "with chunk size on dimension " + dim.name)

        # Check for duplicates
        new_index = self.index.append(extra_index)
        if new_index.duplicated().any():
            raise Exception("Duplicate entries found " +
                            "in the resulting index.")

        self._index = new_index.sort_values(by=list(new_index.columns),
                                            ignore_index=True)

    def save_index(self):
        index_split_size = int(self.metadata['index_split'])
        i = 0
        for offset in range(0,
                            len(self.index),
                            index_split_size // len(self.index.columns)):
            sink = Driver.create_writer('{}/index/{}'.format(self.url, i),
                                        self._index_schema,
                                        'gzip')
            writer = next(sink)
            writer.write_table(
                pyarrow.Table.from_pandas(
                    self.index.iloc[offset:offset + index_split_size]))
            sink.close()
            i += 1

    def get_chunk(self, *argv):
        return Chunk(self, *argv)


class Chunk(object):
    """Wrapper for SciDB array chunk stored externally"""

    def __init__(self, array, *argv):
        self.array = array
        self.coords = argv

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
        for (coord, dim) in zip(self.coords, dims):
            if coord < dim.low_value or coord > dim.high_value:
                raise Exception(
                    ('Coordinate value, {}, is outside of dimension range, '
                     '[{}:{}]').format(
                         coord, dim.low_value, dim.high_value))

            part = coord - dim.low_value
            if part % dim.chunk_length != 0:
                raise Exception(
                    ('Coordinate value, {}, is not a multiple of ' +
                     'chunk size, {}').format(
                         coord, dim.chunk_length))
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
        if self._table is None:
            self._table = Driver.create_reader(
                self.url,
                compression=self.array.metadata['compression']).read_all()
        return self._table

    def to_pandas(self):
        return pyarrow.Table.to_pandas(self.table)

    def from_pandas(self, pd):
        dims = [d.name for d in self.array.schema.dims]

        # Sort by dimensions
        pd = pd.sort_values(by=dims, ignore_index=True)

        # Check for duplicates
        if pd.duplicated(subset=dims).any():
            raise Exception("Duplicate coordinate pairs found.")

        # Check for coordinates outside chunk boundaries
        for (coord, dim) in zip(self.coords, self.array.schema.dims):
            vals = pd[dim.name]
            if (vals.iloc[0] < coord or
                    vals.iloc[-1] >= coord + dim.chunk_length):
                raise Exception("Coordinates outside chunk boundaries")

        self._table = pyarrow.Table.from_pandas(pd)
        self._table = self._table.replace_schema_metadata()

    def save(self):
        sink = Driver.create_writer(
            self.url,
            schema=self._table.schema,
            compression=self.array.metadata['compression'])
        writer = next(sink)
        writer.write_table(self._table)
        sink.close()
