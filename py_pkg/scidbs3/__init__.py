import boto3
import itertools
import pandas
import pyarrow
import scidbpy

__version__ = '19.11.1'


def metadata_from_string(input):
    res = dict(ln.split('\t') for ln in input.strip().split('\n'))
    try:
        if res['compression'] == 'none':
            res['compression'] = None
    except KeyError:
        pass
    return res


class S3Array(object):
    """Wrapper for SciDB array stored in S3"""

    def __init__(self,
                 bucket_name,
                 bucket_prefix):
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix.rstrip('/')

        self._client = None
        self._object = None
        self._metadata = None
        self._schema = None

    def __iter__(self):
        return (i for i in (self.bucket_name, self.bucket_prefix))

    def __eq__(self):
        return tuple(self) == tuple(other)

    def __repr__(self):
        return ('{}(bucket_name={!r}, bucket_prefix={!r})').format(
            type(self).__name__, *self)

    def __str__(self):
        return 's3://{}/{}'.format(self.bucket_name, self.bucket_prefix)

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client('s3')
        return self._client

    @property
    def object(self):
        if self._object is None:
            self._object = self.client.get_object(
                Bucket=self.bucket_name,
                Key='{}/metadata'.format(self.bucket_prefix))
        return self._object

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = metadata_from_string(
                self.object["Body"].read().decode('utf-8'))
        return self._metadata

    @property
    def schema(self):
        if self._schema is None:
            self._schema = scidbpy.Schema.fromstring(self.metadata['schema'])
        return self._schema

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
        return index.sort_values(by=list(index.columns))

    def get_chunk(self, *argv):
        return S3Chunk(self, *argv)


class S3Chunk(object):
    """Wrapper for SciDB array chunk stored in S3"""

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
            parts.append((val - dim.low_value) // dim.chunk_length)

        self.bucket_postfix = '_'.join(map(str, parts))
        self._object = None

    def __iter__(self):
        return (i for i in (self.array, self.bucket_postfix))

    def __eq__(self):
        return tuple(self) == tuple(other)

    def __repr__(self):
        return ('{}(array={!r}, bucket_postfix={!r})').format(
            type(self).__name__, *self)

    def __str__(self):
        return 's3://{}/{}/{}'.format(self.array.bucket_name,
                                      self.array.bucket_prefix,
                                      self.bucket_postfix)

    @property
    def object(self):
        if self._object is None:
            self._object = self.array._client.get_object(
                Bucket=self.array.bucket_name,
                Key='{}/{}'.format(self.array.bucket_prefix,
                                   self.bucket_postfix))
        return self._object

    def to_pandas(self):
        strm = pyarrow.input_stream(
            pyarrow.BufferReader(self.object["Body"].read()),
            compression=self.array.metadata['compression'])
        return pyarrow.RecordBatchStreamReader(strm).read_pandas()
