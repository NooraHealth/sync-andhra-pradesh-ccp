import google
import io
import json
import os
import oyaml as yaml
import polars as pl
import slack_sdk
import time
import uuid
import warnings
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from pathlib import Path


def get_params(source_name, params_path = 'params.yaml', envir = None):
  with open(params_path) as f:
    params = yaml.safe_load(f)

  github_ref_name = os.getenv('GITHUB_REF_NAME')
  params.update({'github_ref_name': github_ref_name})
  if envir is None:
    envir = 'prod' if github_ref_name == 'main' else 'dev'

  y = [x for x in params['sources'] if x['name'] == source_name][0]
  params['source_name'] = y['name']

  z = [x for x in y['environments'] if x['name'] == envir][0]
  params['environment'] = z.pop('name')
  params.update(z)
  del params['sources']

  key = 'service_account_key'
  if github_ref_name is None:
    with open(Path('secrets', params[key])) as f:
      params[key] = json.load(f)
    with open(Path('secrets', 'slack_token.txt')) as f:
      params['slack_token'] = f.read().strip('\n')
    with open(Path('secrets', f'{source_name}.yaml')) as f:
      params['source_params'] = yaml.safe_load(f)
  else:
    params[key] = json.loads(os.getenv('SERVICE_ACCOUNT_KEY'))
    params['slack_token'] = os.getenv('SLACK_TOKEN')
    params['source_params'] = yaml.safe_load(os.getenv('SOURCE_PARAMS'))

  params['credentials'] = Credentials.from_service_account_info(params[key])
  del params[key]
  return params


def json_dumps_list(x):
  return json.dumps(x.to_list())


def run_bigquery(query, credentials, num_tries = 5, wait_secs = 5):
  client = bigquery.Client(credentials = credentials)
  gtg = False
  idx_try = 1
  while not gtg and idx_try <= num_tries:
    try:
      query_job = client.query(query)
      result = query_job.result()
      gtg = True
    except google.api_core.exceptions.BadRequest as e:
      if 'due to concurrent update' in str(e) and (idx_try < num_tries):
        time.sleep(wait_secs)
        idx_try += 1
      else:
        raise e
  return result


def read_bigquery(query, credentials):
  with warnings.catch_warnings(action = 'ignore'):
    rows = run_bigquery(query, credentials).to_arrow()
    df = pl.from_arrow(rows)
  return df


def read_bigquery_exists(table_name, params):
  q = f"select * from `{params['dataset']}.__TABLES__` where table_id = '{table_name}'"
  df = read_bigquery(q, params['credentials'])
  return df.shape[0] > 0


def get_bigquery_schema(df):
  n = 'NUMERIC'
  i = 'INT64'
  s = 'STRING'
  dtype_mappings = {
    pl.Decimal: n, pl.Float32: n, pl.Float64: n,
    pl.Int8: i, pl.Int16: i, pl.Int32: i, pl.Int64: i,
    pl.UInt8: i, pl.UInt16: i, pl.UInt32: i, pl.UInt64: i,
    pl.String: s, pl.Categorical: s, pl.Enum: s, pl.Utf8: s,
    pl.Binary: 'BOOLEAN', pl.Boolean: 'BOOLEAN', pl.Date: 'DATE',
    pl.Datetime('us', time_zone = 'UTC'): 'TIMESTAMP',
    pl.Datetime('us', time_zone = None): 'DATETIME',  # won't work for other time zones
    pl.Time: 'TIME', pl.Duration: 'INTERVAL', pl.Null: 'STRING'
  }
  schema = []
  for j in range(df.shape[1]):
    dtype = dtype_mappings[df.dtypes[j]]
    schema.append(bigquery.SchemaField(df.columns[j], dtype))
  return schema


def write_bigquery(df, table_name, params, write_disposition = 'WRITE_EMPTY'):
  schema = get_bigquery_schema(df)
  client = bigquery.Client(credentials = params['credentials'])
  with io.BytesIO() as stream:
    df.write_parquet(stream)
    stream.seek(0)
    job_config = bigquery.LoadJobConfig(
      source_format = 'PARQUET', schema = schema, write_disposition = write_disposition)
    job = client.load_table_from_file(
      stream, destination = f"{params['dataset']}.{table_name}",
      project = params['project'], job_config = job_config)
  job.result()
  return df.shape[0]


def add_extracted_columns(df, extracted_at = None):
  if extracted_at is not None:
    df = df.with_columns(_extracted_at = pl.lit(extracted_at))
  uuids = [str(uuid.uuid4()) for _ in range(df.shape[0])]
  df = df.with_columns(pl.Series('_extracted_uuid', uuids))
  return df


def get_slack_message_text(e, source_name):
  text = (
    f':warning: Sync for *{source_name}* failed with the following error:'
    f'\n\n`{str(e)}`'
  )
  run_url = os.getenv('RUN_URL')
  if run_url is not None:
    text += f'\n\nPlease see the GitHub Actions <{run_url}|workflow run log>.'
  return text


def send_message_to_slack(text, channel_id, token):
  client = slack_sdk.WebClient(token = token)
  try:
    client.chat_postMessage(channel = channel_id, text = text)
  except Exception as e:
    raise e
