#!/bin/env python3

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

import os
import sys

from scidbbridge.driver import Driver


if len(sys.argv) != 3:
    print('Missing arguments:')
    print(os.path.basename(__file__), 'url INDEX_SPLIT_SIZE')
    sys.exit(2)

url = sys.argv[1]
INDEX_SPLIT_SIZE = int(sys.argv[2])


Driver.delete_all('{}/index/'.format(url))

reader = Driver.create_reader('{}/index.arrow.gz'.format(url), 'gzip')
table = reader.read_all()

i = 0
for offset in range(0, table.num_rows, INDEX_SPLIT_SIZE // table.num_columns):
    sink = Driver.create_writer('/'.join((url, 'index/{}'.format(i))),
                                table.schema,
                                'gzip')
    writer = next(sink)
    writer.write_table(table.slice(offset, INDEX_SPLIT_SIZE))
    sink.close()
    i += 1
