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

import boto3
import os
import pyarrow
import urllib.parse


class Driver:
    _s3_client = None
    _s3_resource = None

    @staticmethod
    def s3_client():
        if Driver._s3_client is None:
            Driver._s3_client = boto3.client('s3')
        return Driver._s3_client

    @staticmethod
    def s3_resource():
        if Driver._s3_resource is None:
            Driver._s3_resource = boto3.resource('s3')
        return Driver._s3_resource

    @staticmethod
    def list(url):
        parts = urllib.parse.urlparse(url)

        # S3
        if parts.scheme == 's3':
            bucket = parts.netloc
            key = parts.path[1:] + '/'
            pages = Driver.s3_client().get_paginator(
                'list_objects_v2').paginate(Bucket=bucket, Prefix=key)
            for page in pages:
                if 'Contents' in page.keys():
                    for obj in page['Contents']:
                        yield 's3://{}/{}'.format(bucket, obj['Key'])

        # File System
        elif parts.scheme == 'file':
            path = os.path.join(parts.netloc, parts.path)
            for fn in os.listdir(path):
                if os.path.isfile(os.path.join(path, fn)):
                    yield 'file://' + os.path.join(path, fn)

        else:
            raise Exception('URL {} not supported'.format(url))

    @staticmethod
    def read_metadata(url):
        parts = urllib.parse.urlparse(url)

        # S3
        if parts.scheme == 's3':
            bucket = parts.netloc
            key = parts.path[1:] + '/metadata'
            obj = Driver.s3_client().get_object(Bucket=bucket, Key=key)
            return obj['Body'].read().decode('utf-8')

        # File System
        elif parts.scheme == 'file':
            path = os.path.join(parts.netloc, parts.path, 'metadata')
            return open(path).read()

        else:
            raise Exception('URL {} not supported'.format(url))

    @staticmethod
    def create_reader(url, compression=None):
        parts = urllib.parse.urlparse(url)

        # S3
        if parts.scheme == 's3':
            bucket = parts.netloc
            key = parts.path[1:]
            obj = Driver.s3_client().get_object(Bucket=bucket, Key=key)
            buf = obj['Body'].read()
            strm = pyarrow.input_stream(pyarrow.py_buffer(buf),
                                        compression=compression)
            return pyarrow.RecordBatchStreamReader(strm)

        # File System
        elif parts.scheme == 'file':
            path = os.path.join(parts.netloc, parts.path)
            strm = pyarrow.input_stream(path, compression=compression)
            return pyarrow.RecordBatchStreamReader(strm)

        else:
            raise Exception('URL {} not supported'.format(url))

    @staticmethod
    def create_writer(url, schema, compression=None):
        parts = urllib.parse.urlparse(url)

        # S3
        if parts.scheme == 's3':
            bucket = parts.netloc
            key = parts.path[1:]
            buf = pyarrow.BufferOutputStream()
            stream = pyarrow.output_stream(buf, compression=compression)
            writer = pyarrow.RecordBatchStreamWriter(stream, schema)

            try:
                yield writer
            except GeneratorExit:
                writer.close()
                stream.close()
                Driver.s3_client().put_object(Body=buf.getvalue().to_pybytes(),
                                              Bucket=bucket,
                                              Key=key)

        # File System
        elif parts.scheme == 'file':
            path = os.path.join(parts.netloc, parts.path)
            stream = pyarrow.output_stream(path, compression=compression)
            writer = pyarrow.ipc.RecordBatchStreamWriter(stream, schema)

            try:
                yield writer
            except GeneratorExit:
                writer.close()
                stream.close()

        else:
            raise Exception('URL {} not supported'.format(url))

    @staticmethod
    def delete_all(url):
        parts = urllib.parse.urlparse(url)

        # S3
        if parts.scheme == 's3':
            bucket = parts.netloc
            key = parts.path[1:]
            Driver.s3_resource().Bucket(
                bucket).objects.filter(Prefix=key).delete()

        # File System
        elif parts.scheme == 'file':
            path = os.path.join(parts.netloc, parts.path)
            for fn in os.listdir(path):
                os.unlink(os.path.join(path, fn))

        else:
            raise Exception('URL {} not supported'.format(url))


    @staticmethod
    def delete(url):
        parts = urllib.parse.urlparse(url)

        # S3
        if parts.scheme == 's3':
            bucket = parts.netloc
            key = parts.path[1:]
            Driver.s3_client().delete_object(Bucket=bucket, Key=key)

        # File System
        elif parts.scheme == 'file':
            path = os.path.join(parts.netloc, parts.path)
            os.unlink(path)

        else:
            raise Exception('URL {} not supported'.format(url))
