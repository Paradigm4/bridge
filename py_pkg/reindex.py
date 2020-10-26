import boto3
import os
import sys


INDEX_SPLIT_SIZE = 32 * 1024


if len(sys.argv) != 3:
    print('Missing arguments:')
    print(os.path.basename(__file__), 'bucket key')
    sys.exit(2)

bucket = sys.argv[1]
key = sys.argv[2]


# Delete Existing Split Index
bkt = boto3.resource('s3').Bucket(bucket)
bkt.objects.filter(Prefix='{}/index/'.format(key)).delete()


client = boto3.client('s3')
obj = client.get_object(Bucket=bucket, Key='{}/index'.format(key))
data = obj['Body'].read()
chunks = data.split(b'\n')
line_sz = len(chunks[0].split(b'\t'))
split_sz = INDEX_SPLIT_SIZE // line_sz

i = 0
for split_st in range(0, len(chunks), split_sz):
    body = b'\n'.join(chunks[split_st:(split_st+split_sz)])
    split_key = '{}/{}'.format(key, i)
    print(split_key)
    client.put_object(Body=body, Bucket=bucket, Key=split_key)
    i += 1
