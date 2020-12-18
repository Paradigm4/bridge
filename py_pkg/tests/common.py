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
import os
import pytest
import scidbpy
import shutil


base_prefix = 'bridge_tests'
base_metadata = {'version':     '1',
                 'format':      'arrow',
                 'attribute':   'ALL',
                 'compression': None}
s3_bucket = 'p4tests'
fs_base = '/tmp/{}'.format(base_prefix)

test_urls = ('s3://{}/{}'.format(s3_bucket, base_prefix),
             'file://{}'.format(fs_base))

s3_con = boto3.client('s3')


@pytest.fixture
def scidb_con():
    # FS Init
    if not os.path.exists(fs_base):
        os.makedirs(fs_base)

    yield scidbpy.connect()

    # FS Cleanup
    try:
        shutil.rmtree(fs_base)
    except PermissionError:
        pass

    # S3 Cleanup
    result = s3_con.list_objects_v2(Bucket=s3_bucket, Prefix=base_prefix)
    if 'Contents' in result.keys():
        objects = [{'Key': e['Key']} for e in result['Contents']]
        s3_con.delete_objects(Bucket=s3_bucket,
                              Delete={'Objects': objects})
