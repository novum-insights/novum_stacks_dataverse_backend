import json
import logging
from concurrent.futures import ThreadPoolExecutor
from csv import QUOTE_NONNUMERIC
from gzip import compress, decompress, GzipFile
from io import TextIOWrapper, BytesIO
from json import dumps

from pandas import json_normalize, read_json, read_csv, read_sql

from etl.common.paths import S3_CONN_NAME, STACKS_DATA_BUCKET_NAME


def _get_s3_hook():
    from airflow.hooks.S3_hook import S3Hook
    return S3Hook(S3_CONN_NAME)


def get_warehouse_db_engine():
    postgres_hook = get_warehouse_db_hook()
    return postgres_hook.get_sqlalchemy_engine()


def get_warehouse_db_hook():
    from airflow.hooks.postgres_hook import PostgresHook
    postgres_hook = PostgresHook('warehouse_db')
    return postgres_hook


def get_stacks_db_hook():
    from airflow.hooks.postgres_hook import PostgresHook
    postgres_hook = PostgresHook('stacks_db')
    return postgres_hook


def save_df_to_db(df, table_name, **kwargs):
    engine = get_warehouse_db_engine()
    df.to_sql(table_name, engine, **kwargs)


def save_obj(obj, key, replace=False, bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=None):
    hook = _get_s3_hook()
    bytes_data = dumps(obj).encode('UTF-8')
    logging.info('Saving object to Bucket: %s with Key: %s', bucket_name, key)
    if key.endswith('.gz'):
        bytes_data = compress(bytes_data)
    hook.load_bytes(bytes_data, key, bucket_name, replace=replace, acl_policy=acl_policy)


def get_json(key, flatten=False, raw=False, **kwargs):
    logging.info('Getting json from: %s', key)
    bytes_data = _get_object(key, kwargs.get('bucket_name', STACKS_DATA_BUCKET_NAME))
    if flatten or raw:
        data = json.loads(bytes_data)
        if raw:
            return data
        return json_normalize(data, **kwargs)
    return read_json(bytes_data, **kwargs)


def get_csv(key, **kwargs):
    logging.info('Getting csv from: %s', key)
    bytes_data = _get_object(key, kwargs.get('bucket_name', STACKS_DATA_BUCKET_NAME))
    return read_csv(BytesIO(bytes_data), encoding='utf8', **kwargs)


def _get_object(key, bucket_name=STACKS_DATA_BUCKET_NAME):
    hook = _get_s3_hook()
    obj = hook.get_key(key, bucket_name).get()
    bytes_data = obj['Body'].read()
    if key.endswith('.gz'):
        bytes_data = decompress(bytes_data)
    return bytes_data


def _read_flattened_json(key):
    return json_normalize(_get_object(key))


def save_df(df, key, bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=None):
    path = f'{bucket_name}/{key}'
    logging.info('Saving dataframe to %s', path)
    hook = _get_s3_hook()
    gz_buffer = BytesIO()
    with GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        df.to_csv(TextIOWrapper(gz_file, 'utf8'), quotechar='"', quoting=QUOTE_NONNUMERIC)
    hook.load_bytes(gz_buffer.getvalue(), key=key, bucket_name=bucket_name, replace=True, acl_policy=acl_policy)


def copy(source, destination):
    hook = _get_s3_hook()
    bucket_name = STACKS_DATA_BUCKET_NAME
    logging.info('Coping within Bucket: %s from %s to %s', bucket_name, source, destination)
    hook.copy_object(source, destination,
                     source_bucket_name=bucket_name,
                     dest_bucket_name=bucket_name)


def multi_copy(prefix, new_prefix='', source_bucket_name=None, dest_bucket_name=None, acl_policy=None):
    hook = _get_s3_hook()
    if source_bucket_name is None:
        source_bucket_name = STACKS_DATA_BUCKET_NAME
    if dest_bucket_name is None:
        dest_bucket_name = STACKS_DATA_BUCKET_NAME
    if source_bucket_name == dest_bucket_name:
        logging.info('Multi Copy within Bucket: %s from prefix: %s to %s ...', source_bucket_name, prefix, new_prefix)
    else:
        logging.info('Multi Copy from Bucket %s to Bucket %s from prefix: %s to %s ...',
                     source_bucket_name, dest_bucket_name, prefix, new_prefix)
    with ThreadPoolExecutor() as executor:
        for key in hook.list_keys(source_bucket_name, prefix):
            destination = new_prefix + key[len(prefix):] if prefix else key
            logging.info('\tObject from  %s to %s ...', key, destination)
            executor.submit(hook.copy_object, key, destination,
                            source_bucket_name=source_bucket_name,
                            dest_bucket_name=dest_bucket_name,
                            acl_policy=acl_policy)


def copy_to_latest(path_template, date, source_bucket_name=None, dest_bucket_name=None, acl_policy=None):
    dated_key = path_template.format(execution_date=date)
    latest_key = path_template.format(execution_date='latest')
    multi_copy(dated_key, latest_key, source_bucket_name, dest_bucket_name, acl_policy)


def load_from_data_warehouse(sql, params, **kwargs):
    connection = get_warehouse_db_hook().get_conn()
    data = read_sql(sql, connection, params=params, **kwargs)
    return data


