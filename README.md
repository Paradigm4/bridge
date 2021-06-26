# SciDB Input/Output Using External Storage

[![SciDB 19.11](https://img.shields.io/badge/SciDB-19.11-blue.svg)](https://forum.paradigm4.com/t/scidb-release-19-11/2411)
[![arrow 3.0.0](https://img.shields.io/badge/arrow-3.0.0-blue.svg)](https://arrow.apache.org/release/3.0.0.html)

This document contains installation and usage instructions of the
`bridge` SciDB plugin.

1. [Usage](#usage)
   1. [Advanced Usage](#advanced-usage)
1. [Installation](#installation)
   1. [SciDB Plug-in](#scidb-plug-in)
   1. [Python Package](#python-package)
1. [AWS Configuration](#aws-configuration)

## Usage

1. Save SciDB array in S3:
   ```
   > iquery --afl
   AFL% xsave(
          filter(
            apply(
              build(<v:int64>[i=0:9:0:5; j=10:19:0:5], j + i),
              w, double(v*v)),
            i >= 5 and w % 2 = 0),
          's3://p4tests/bridge/foo');
   {chunk_no,dest_instance_id,source_instance_id} val
   ```
   The SciDB array is saved in the `p4tests` bucket in the `foo` object.
1. Load SciDB array from S3 in Python:
   ```
   > python
   >>> import scidbbridge
   >>> ar = scidbbridge.Array('s3://p4tests/bridge/foo')

   >>> ar.metadata
   {'attribute':   'ALL',
    'compression': None,
    'format':      'arrow',
    'index_split': '100000',
    'namespace':   'public',
    'schema':      '<v:int64,w:double> [i=0:9:0:5; j=10:19:0:5]',
    'version':     '1'}

   >>> print(ar.schema)
   <v:int64,w:double> [i=0:9:0:5; j=10:19:0:5]

   >>> ar.read_index()
      i   j
   0  5  10
   1  5  15

   >>> ch = ar.get_chunk(5, 15)
   >>> ch.to_pandas()
        v      w  i   j
   0   20  400.0  5  15
   1   22  484.0  5  17
   2   24  576.0  5  19
   ...
   ```

Note: If using the file system for storage, make sure the storage is
shared across instances and that the path used by the non-admin SciDB
users is in `io-paths-list` in SciDB `config.ini`.

### Advanced Usage

#### Permissions

When using `xinput`/`xsave` to read/write to a mounted file system,
the `io-paths-list` values are enforced. This is similar to the
`input`/`save` operators. Please see [File I/O
Restrictions](https://paradigm4.atlassian.net/l/c/uhmpNqz7) in the
SciDB documentations for more details. Note that user running the
SciDB process also needs appropriate I/O permissions to the file
system path used.

S3 paths are also supported in the `io-paths-list` value as
follows. Any paths that start with `s3/` are considered S3 paths. For
example, a value of `s3/foo/bar` in `io-paths-list` is converted to
the `s3://foo/bar` S3 URL. The S3 URLs used in the `xinput`/`xsave`
operators have to be a super-set of one of the URLS listed in
`io-paths-list`.

For example, given:
```
io-paths-list=s3/foo/bar:s3/taz/qux/
```
the following calls are accepted:
```
xinput('s3://foo/bar/1')
xinput('s3://foo/bartaz/2')     # matches s3://foo/bar
xsave(..., 's3://taz/qux/3)
```
while the following are *not*:
```
xsave(..., 's3://taz/quxfoo/4)  # does not match s3://taz/qux/
```

The S3 URLs are checked against the `io-paths-list` values for *all*
SciDB users.

#### xsave

Parameters:

* `namespace`
  * Assign the array to a specific namespace, defaults to
    `public`. The user's read permission against the assigned
    namespace is checked by `xinput` when the array is read.
  * e.g., `xsave(..., namespace:public)`
* `update`
  * Specify if a new array is created (`update:false`, default) or an
    existing array is updated (`update:true`)
* `format`
  * Specify array storage format. Currently the only and the default
    format supported is Apache Arrow
  * e.g., `xsave(..., format:'arrow')`
* `compression`
  * Specify is the array should be stored compress and what
    compression algorithm should be used. Currently only GZip
    compression algorithm is supported. By default, no compression is
    used.
  * e.g., `xsave(..., compression:'gzip')`
* `index_split`
  * Specify how the index should be split in different index
    files. The value represents the number of coordinates to store in
    each index file. The resulting number of chunks index by each file
    is equal to the number of coordinates specified divided by the
    number of dimensions. The default value is `100,000`.
  * e.g., `xsave(..., index_split:3000)` if the array has three
    dimensions, then each index split will index `1,000` chunks.
* `s3_sse`
  * Specify S3 Server-Side Encryption algorithm. By default, no
    server-side encryption algorithm is used, i.e.,
    `s3_sse:'NOT_SET'`. Accepted values are `NOT_SET`, `AES256`, and
    `aws:kms`.
  * e.g., `xsave(..., s3_sse:'AES256')`

#### xinput

Parameters:

* `cache_size`
  * Specify the size of the cache to be used for storing chunks. Once
    a chunk is read in memory, it is stored in the cache. Once a chunk
    is store while reading one attribute, it can be re-used when
    reading subsequent attributes from the same chunk. The cache uses
    a least recently used eviction policy. The value is specified in
    bytes. The default value is `268,435,456` (`256MB`). A value of
    `0` disables the cache.
  * e.g., `xinput(..., cache_size:524288000)` (`500 MB`)

#### Python API

Rebuild the index of an array stored in S3:

```
import scidbbridge
ar = scidbbridge.Array('s3://p4tests/bridge/foo')
ix = ar.build_index()
ar.write_index(ix)
```

### Troubleshoot

It is common for S3 to return _Access Denied_ for non-obvious cases
like, for example, if the bucket specified does not exist. `xsave`
includes an extended error message for this type of errors which
include a link to a troubleshooting guide. E.g.:

```
> iquery -aq "xsave(build(<v:int64>[i=0:9], i), bucket_name:'foo', object_path:'bar')"
UserException in file: PhysicalXSave.cpp function: uploadS3 line: 372 instance: s0-i1 (1)
Error id: scidb::SCIDB_SE_ARRAY_WRITER::SCIDB_LE_UNKNOWN_ERROR
Error description: Error while saving array. Unknown error: Upload to
s3://foo/bar failed. Access Denied. See
https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-403/.
```

## Installation

### SciDB Plug-in

#### Using Extra SciDB Libs

Install extra-scidb-libs following the instructions
[here](https://paradigm4.github.io/extra-scidb-libs/).

#### From Source

#### AWS C++ SDK

1. Install the required packages:
   1. Ubuntu:
      ```
      apt-get install cmake libcurl4-openssl-dev
      ```
   1. RHEL/CentOS:
      ```
      yum install libcurl-devel
      yum install https://downloads.paradigm4.com/devtoolset-3/centos/7/sclo/x86_64/rh/devtoolset-3/scidb-devtoolset-3.noarch.rpm
      yum install cmake3 devtoolset-3-runtime devtoolset-3-toolchain
      scl enable devtoolset-3 bash
      ```
1. Download and unzip the SDK:
   ```
   wget --no-verbose --output-document - https://github.com/aws/aws-sdk-cpp/archive/1.8.3.tar.gz \
   | tar --extract --gzip --directory=.
   ```
1. Configure the SDK:
   ```
   > cd aws-sdk-cpp-1.8.3
   aws-sdk-cpp-1.8.3> mkdir build
   aws-sdk-cpp-1.8.3/build> cd build
   ```
   1. Ubuntu:
       ```
       aws-sdk-cpp-1.8.3/build> cmake ..            \
           -DBUILD_ONLY=s3                          \
           -DCMAKE_BUILD_TYPE=RelWithDebInfo        \
           -DBUILD_SHARED_LIBS=ON                   \
           -DCMAKE_INSTALL_PREFIX=/opt/aws-sdk-cpp
       ```
   1. RHEL/CentOS:
       ```
       aws-sdk-cpp-1.8.3/build> cmake3 ..                               \
           -DBUILD_ONLY=s3                                              \
           -DCMAKE_BUILD_TYPE=RelWithDebInfo                            \
           -DBUILD_SHARED_LIBS=ON                                       \
           -DCMAKE_INSTALL_PREFIX=/opt/aws-sdk-cpp                      \
           -DCMAKE_C_COMPILER=/opt/rh/devtoolset-3/root/usr/bin/gcc     \
           -DCMAKE_CXX_COMPILER=/opt/rh/devtoolset-3/root/usr/bin/g++
       ```
1. Compile and install the SDK:
   ```
   aws-sdk-cpp-1.8.3/build> make
   aws-sdk-cpp-1.8.3/build> make install
   ```
   The SDK will be installed in `/opt/aws-sdk-cpp`

#### Apache Arrow

1. Apache Arrow library version `3.0.0` is required. The easiest way
   to install it is by running:
   ```
   wget -O- https://paradigm4.github.io/extra-scidb-libs/install.sh \
   | sudo sh -s -- --only-prereq
   ```
1. Install Apache Arrow development library:
   1. Ubuntu
      ```
      apt-get install libarrow-dev=3.0.0-1
      ```
   1. RHEL/CentOS
      ```
      yum install arrow-devel-3.0.0
      ```

#### cURL (RHEL/CentOS ONLY)

Compile cURL with OpenSSL (instead of NSS):
```
> curl https://curl.se/download/curl-7.72.0.tar.gz | tar xz
> cd curl*
> ./configure --prefix=/opt/curl
> make
> make install
```
More details: https://github.com/aws/aws-sdk-cpp/issues/1491


#### Compile and Load SciDB Plug-in

1. Checkout and compile the plug-in:
   ```
   > git clone https://github.com/Paradigm4/bridge.git
   bridge> make
   ```
1. Install in SciDB:
   ```
   bridge> cp libbridge.so /opt/scidb/19.11/lib/scidb/plugins
   ```
1. Restart SciDB and load the plug-in:
   ```
   scidbctl.py stop mydb
   scidbctl.py start mydb
   iquery --afl --query "load_library('bridge')"
   ```

### Python Package

#### Install Latest Release from PyPI

```
pip install scidb-bridge
```

#### Install Latest Version from GitHub

```
pip install --user git+https://github.com/Paradigm4/bridge.git#subdirectory=py_pkg`
```

## AWS Configuration

1. AWS uses two separate filed to configure the S3 client. The
   `credentials` file is required and stores the AWS credentials for
   accessing S3, e.g.:
   ```
   > cat credentials
   [default]
   aws_access_key_id = ...
   aws_secret_access_key = ...
   ```
   The `config` file is optional and stores the region for the S3
   bucket. By default the `us-east-1` region is used, e.g.:
   ```
   > cat config
   [default]
   region = us-east-1
   ```
1. In SciDB installations these two files are located in
   `/home/scidb/.aws` directory, e.g.:
   ```
   > ls /home/scidb/.aws
   config
   credentials
   ```

Note: The credentials used need to have read/write permission to the
S3 bucket used.
