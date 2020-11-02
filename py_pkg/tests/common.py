bucket_name = 'p4tests'
base_prefix = 's3bridge-tests'
base_metadata = {'version':     '1',
                 'format':      'arrow',
                 'attribute':   'ALL',
                 'compression': 'none'}


def delete_prefix(s3_con, prefix):
    result = s3_con.list_objects_v2(Bucket=bucket_name,
                                    Prefix='/'.join((base_prefix, prefix)))
    if 'Contents' not in result.keys():
        return
    objects = [{'Key': e['Key']} for e in result['Contents']]
    s3_con.delete_objects(Bucket=bucket_name,
                          Delete={'Objects': objects})
