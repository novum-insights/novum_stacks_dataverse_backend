import os

S3_CONN_NAME = 'my_S3_conn'
AWS_ACCOUNT_ID = os.environ['AWS_ACCOUNT_ID']

STACKS_DATA_BUCKET_NAME = 'stacks-data-bucket'
STACKS_DATA_BUCKET_PATH = f's3://{STACKS_DATA_BUCKET_NAME}'
