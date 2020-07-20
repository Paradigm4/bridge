import boto3
import itertools
import pyarrow
import scidbpy

__version__ = '19.11.1'


class S3Array(object):
    """Wrapper for SciDB array stored in S3"""

    def __init__(self,
                 bucket_name,
                 bucket_prefix):
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix.rstrip('/')

        self._client = None
        self._object = None
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
        return self.object["Metadata"]

    @property
    def schema(self):
        if self._schema is None:
            self._schema = scidbpy.Schema.fromstring(self.metadata['schema'])
        return self._schema

    def list_chunks(self):
        prefix = '{}/chunk_'.format(self.bucket_prefix)
        result = self.client.list_objects_v2(Bucket=self.bucket_name,
                                             Prefix=prefix)
        return tuple(sorted(tuple(map(int, e['Key'].lstrip(prefix).split('_')))
                            for e in result['Contents']))

    def get_chunk(self, *argv):
        return S3Chunk(self, *argv)


class S3Chunk(object):
    """Wrapper for SciDB array chunk stored in S3"""

    def __init__(self, array, *argv):
        self.array = array

        if len(argv) != len(self.array.schema.dims):
            raise Exception(
                ('Number of arguments, {}, does no match the number of ' +
                 'dimensions, {}. Please specify one start coordiante for ' +
                 'each dimension.').format(
                     len(argv), len(self.array.schema.dims)))
        self.bucket_postfix = '_'.join(itertools.chain(
            ('chunk', ), (str(arg) for arg in argv)))

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
        return pyarrow.ipc.open_stream(
            self.object["Body"].read()).read_all().to_pandas()
