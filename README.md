# SciDB Save/Load to/from Simple Storage Service (S3)

[![SciDB 19.11](https://img.shields.io/badge/SciDB-19.11-blue.svg)](https://forum.paradigm4.com/t/scidb-release-19-11/2411)
[![arrow 0.16.0](https://img.shields.io/badge/arrow-0.16.0-blue.svg)](https://arrow.apache.org/release/0.16.0.html)

This document contains installation and usage instructions of the
`s3bridge` SciDB plugin.

1. [Installation](#installation)
1. [Usage](#usage)

## Installation

### AWS C++ SDK

1. Install the required packages:
   1. Ubuntu:
      ```
      apt-get install cmake libcurl4-openssl-dev
      ```
   1. RHEL/CentOS:
      ```
      dnf install libcurl-devel
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
   aws-sdk-cpp-1.8.3/build> cmake ..            \
       -DBUILD_ONLY=s3                          \
       -DCMAKE_BUILD_TYPE=RelWithDebInfo        \
       -DBUILD_SHARED_LIBS=ON                   \
       -DCMAKE_INSTALL_PREFIX=/opt/aws
   ```
1. Compile and install the SDK:
   ```
   aws-sdk-cpp-1.8.3/build> make
   aws-sdk-cpp-1.8.3/build> make install
   ```
   The SDK will be installed in `/opt/aws`

### AWS Python Package

1. Install the `Boto3` Python package:
   ```
   pip install boto3
   ```

### AWS Configuration

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

### Apache Arrow

1. Apache Arrow library version `0.16.0` is required. The easiest way
   to install it is by running:
   ```
   wget -O- https://paradigm4.github.io/extra-scidb-libs/install.sh \
   | sudo sh -s -- --only-prereq
   ```
1. Apache Arrow Python package `PyArrow` version `0.16.0` is also
   required. It can be installed by running:
   ```
   pip install pyarrow==0.16.0
   ```

### SciDB Plug-in

1. Checkout and compile the plug-in:
   ```
   > git clone https://github.com/Paradigm4/s3bridge.git
   s3bridge> make
   ```
1. Install in SciDB:
   ```
   s3bridge> cp libs3bridge.so /opt/scidb/19.11/lib/scidb/plugins
   ```
1. Restart SciDB and load the plug-in:
   ```
   scidbctl.py stop mydb
   scidbctl.py start mydb
   iquery --afl --query "load_library('s3bridge')"
   ```

## Usage

Note: only single chunks arrays are currently supported.

1. Save SciDB array in S3:
   ```
   > iquery --afl
   AFL% s3save(apply(build(<v:int64>[i=0:9], i), w, double(v * v)), bucket_name:'p4tests', object_path:'foo');
   {chunk_no,dest_instance_id,source_instance_id} val
   ```
   The SciDB array is saved in the `p4tests` bucket in the `foo` object.
1. Load SciDB array from S3 in Python:
   ```
   > python
   >>> import boto3, pyarrow
   >>> res = boto3.client('s3').get_object(Bucket="p4tests", Key="foo")
   >>> res["Metadata"]
   {'s3bridge_version' : '1',
    'format'           : 'arrow',
    'schema'           : 'build<v:int64,w:double> [i=0:9:0:1000000]',
    'attribute_mapping': 'UNUSED'}
   >>> pyarrow.ipc.open_stream(res["Body"].read()).read_all().to_pandas()
      v     w  i
   0  0   0.0  0
   1  1   1.0  1
   2  2   4.0  2
   3  3   9.0  3
   4  4  16.0  4
   5  5  25.0  5
   6  6  36.0  6
   7  7  49.0  7
   8  8  64.0  8
   9  9  81.0  9
   ```

### Troubleshoot

It is common for S3 to return _Access Denied_ for non-obvious cases
like, for example, if the bucket specified does not exist. `s3save`
includes an extended error message for this type of errors which
include a link to a troubleshootting guide. E.g.:

```
> iquery -aq "s3save(build(<v:int64>[i=0:9], i), bucket_name:'foo', object_path:'bar')"
UserException in file: PhysicalS3Save.cpp function: uploadS3 line: 372 instance: s0-i1 (1)
Error id: scidb::SCIDB_SE_ARRAY_WRITER::SCIDB_LE_UNKNOWN_ERROR
Error description: Error while saving array. Unknown error: Upload to
s3://foo/bar failed. Access Denied. See
https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-403/.
```
