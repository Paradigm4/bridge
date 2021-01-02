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

import boto3
import itertools
import os
import os.path
import pandas
import pyarrow
import scidbpy

__version__ = '19.11.1'


def Array(url):
    """Array Factory Function"""

    if url.startswith('s3://'):
        return S3Array(url)
    if url.startswith('file://'):
        return FSArray(url)
    raise Exception('URL {} not supported'.format(url))


class _Array(object):
    """Wrapper for SciDB array stored externally"""

    @staticmethod
    def __new__(cls, *args, **kwargs):
        """Disallow direct instantiation"""
        if cls is _Array:
            raise TypeError("_Array class cannot be instantiated directly")
        return object.__new__(cls)

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


class S3Array(_Array):
    """Wrapper for SciDB array stored in S3"""

    def __init__(self, url):
        _Array.__init__(self, url)
        prefix_len = len('s3://')
        pos = url.find('/', prefix_len)
        self.bucket_name = url[prefix_len:pos]
        self.bucket_prefix = url[pos + 1:].rstrip('/')

        self._client = None

    def __iter__(self):
        return (i for i in (self.url, self.bucket_name, self.bucket_prefix))

    def __repr__(self):
        return ('{}(url={!r}, bucket_name={!r}, bucket_prefix={!r})').format(
            type(self).__name__, *self)

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client('s3')
        return self._client

    @property
    def metadata(self):
        if self._metadata is None:
            obj = self.client.get_object(
                Bucket=self.bucket_name,
                Key='{}/metadata'.format(self.bucket_prefix))
            self._metadata = _Array.metadata_from_string(
                obj["Body"].read().decode('utf-8'))
        return self._metadata

    def list_chunks(self):
        prefix = '{}/index/'.format(self.bucket_prefix)
        result = self.client.list_objects_v2(Bucket=self.bucket_name,
                                             Prefix=prefix)
        batches = []
        for split in result['Contents']:
            obj = self.client.get_object(Bucket=self.bucket_name,
                                         Key=split['Key'])

            buf = obj['Body'].read()
            strm = pyarrow.input_stream(pyarrow.BufferReader(buf),
                                        compression='gzip')
            reader = pyarrow.RecordBatchStreamReader(strm)
            for batch in reader:
                batches.append(batch)

        table = pyarrow.Table.from_batches(batches)
        index = table.to_pandas(split_blocks=True, self_destruct=True)
        index.sort_values(by=list(index.columns),
                          inplace=True,
                          ignore_index=True)
        return index

    def get_chunk(self, *argv):
        return S3Chunk(self, *argv)


class FSArray(_Array):
    """Wrapper for SciDB array stored in a File System"""

    def __init__(self, url):
        _Array.__init__(self, url)
        self.path = url[len('file://'):].rstrip('/')

    def __iter__(self):
        return (i for i in (self.url, self.path))

    def __repr__(self):
        return ('{}(url={!r}, path={!r})').format(
            type(self).__name__, *self)

    @property
    def metadata(self):
        if self._metadata is None:
            with open(self.path + '/metadata') as f:
                self._metadata = _Array.metadata_from_string(f.read())
        return self._metadata

    def list_chunks(self):
        index_path = self.path + '/index'
        files = [fn for fn in os.listdir(index_path)
                 if os.path.isfile(index_path + '/' + fn)]

        batches = []
        for fn in files:
            strm = pyarrow.input_stream(index_path + '/' + fn,
                                        compression='gzip')
            reader = pyarrow.RecordBatchStreamReader(strm)
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
        return FSChunk(self, *argv)


class Chunk(object):
    """Wrapper for SciDB array chunk stored externally"""

    @staticmethod
    def __new__(cls, *args, **kwargs):
        """Disallow direct instantiation"""
        if cls is Chunk:
            raise TypeError("Chunk class cannot be instantiated directly")
        return object.__new__(cls)

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

        self.url_suffix = '_'.join(map(str, parts))

    def __iter__(self):
        return (i for i in (self.array, self.url_suffix))

    def __eq__(self):
        return tuple(self) == tuple(other)

    def __repr__(self):
        return ('{}(array={!r}, url_suffix={!r})').format(
            type(self).__name__, *self)

    def __str__(self):
        return '{}/{}'.format(self.array.url, self.url_suffix)


class S3Chunk(Chunk):
    """Wrapper for SciDB array chunk stored in S3"""

    def __init__(self, array, *argv):
        Chunk.__init__(self, array, *argv)

    def to_pandas(self):
        obj = self.array._client.get_object(
            Bucket=self.array.bucket_name,
            Key='{}/{}'.format(self.array.bucket_prefix, self.url_suffix))
        strm = pyarrow.input_stream(
            pyarrow.BufferReader(obj["Body"].read()),
            compression=self.array.metadata['compression'])
        return pyarrow.RecordBatchStreamReader(strm).read_pandas()


class FSChunk(Chunk):
    """Wrapper for SciDB array chunk stored in a File System"""

    def __init__(self, array, *argv):
        Chunk.__init__(self, array, *argv)

    def to_pandas(self):
        strm = pyarrow.input_stream(
            self.array.path + '/' + self.url_suffix,
            compression=self.array.metadata['compression'])
        return pyarrow.RecordBatchStreamReader(strm).read_pandas()
