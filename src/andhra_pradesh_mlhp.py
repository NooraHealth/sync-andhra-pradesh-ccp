import argparse
import concurrent.futures
import datetime as dt
import functools
import github
import os
import polars as pl
import polars.selectors as cs
import re
import requests
import time
from pathlib import Path
from tqdm import tqdm
from . import utils


USERS_TABLE_NAME = 'users'
DEFAULT_EXTRACTED_AT = dt.datetime(2023, 4, 1).replace(tzinfo = dt.timezone.utc)


def get_try_chunk_days(range_days, try_chunk_days = (90, 45, 21, 10, 3)):
  idx = next(
    (i for i in range(len(try_chunk_days)) if try_chunk_days[i] < range_days),
    len(try_chunk_days))
  idx = max(0, idx - 1)
  return try_chunk_days[idx:]


def get_chunk_dates(fromdate, todate, chunk_days):
  chunk_dates = pl.date_range(
    fromdate, todate, f'{chunk_days}d', closed = 'left', eager = True).to_list()
  chunk_dates.append(todate + dt.timedelta(days = 1))

  fromdates = [x.strftime('%Y-%m-%d') for x in chunk_dates[:-1]]
  todates = [(x - dt.timedelta(days = 1)).strftime('%Y-%m-%d') for x in chunk_dates[1:]]
  return zip(fromdates, todates)


def get_sessions_from_api(fromdate, todate, username, api_key, api_url):
  try_chunk_days = get_try_chunk_days((todate - fromdate).days)
  base_headers = {'username': username, 'ApiKey': api_key}
  no_data = {'status': 'Failed', 'msg': 'No Data Found'}

  for chunk_days in try_chunk_days:
    try:
      if chunk_days != try_chunk_days[0]:
        print(f'Downshifting username {username} to chunk_days={chunk_days}')
        time.sleep(3)
      chunk_dates = get_chunk_dates(fromdate, todate, chunk_days)
      data = []

      for chunk_fromdate, chunk_todate in chunk_dates:
        chunk_headers = {'fromdate': chunk_fromdate, 'todate': chunk_todate}
        response = requests.get(api_url, headers = chunk_headers | base_headers)
        response.raise_for_status()
        if response.json() != no_data and 'data' in response.json().keys():
          data.extend(response.json()['data'])
      break  # if we've made it this far, we're good to go

    except Exception as e:
      if chunk_days == try_chunk_days[-1]:
        raise e

  if len(data) == 0:
    return pl.DataFrame()

  # int_cols = ['id', 'patients_trained', 'member_trained']
  df = (
    pl.from_dicts(data)
    # .with_columns(cs.by_name(int_cols, require_all = False).cast(pl.Int64))
    .with_columns(
      pl.col('id').cast(pl.Int64),
      pl.col('session_date').str.to_date('%d-%m-%Y'),
      cs.by_name('subdata', require_all = False)
      .map_elements(utils.json_dumps_list, return_dtype = pl.String))
  )
  return df


def sync_sessions_by_users(users_slice, params, extracted_at, overlap_days = 5):
  def get_where(x, colname = 'username'):
    return f"where {colname} in ('{"', '".join(x)}')"

  q_pre = f"update `{params['dataset']}.{USERS_TABLE_NAME}` set"
  query = q_pre + ' is_extracting = true ' + get_where(users_slice['username'].to_list())
  utils.run_bigquery(query, params['credentials'])

  sessions_list = []
  ok_usernames = []
  err_usernames = []
  todate = extracted_at.date() - dt.timedelta(days = 1)
  (api_key, api_url) = [params['source_params'][x] for x in ('key', 'url')]

  for user in users_slice.iter_rows(named = True):
    try:
      fromdate = user['max_todate'] - dt.timedelta(days = overlap_days - 1)
      fromdate = max(fromdate, DEFAULT_EXTRACTED_AT.date())
      sessions_now = get_sessions_from_api(
        fromdate, todate, user['username'], api_key, api_url)
      sessions_list.append(sessions_now)
      ok_usernames.append(user['username'])

    except Exception as e:
      print(f"Error for username {user['username']}: {e}")
      err_usernames.append(user['username'])

  if len(sessions_list) > 0:
    sessions = pl.concat(sessions_list, how = 'diagonal_relaxed')
    if sessions.shape[0] > 0:
      sessions = utils.add_extracted_columns(sessions, extracted_at)
      utils.write_bigquery(sessions, 'sessions', params, 'WRITE_APPEND')

  if len(ok_usernames) > 0:
    query = q_pre + (
      f" max_todate = date('{todate}'), "
      f"_extracted_at = timestamp('{extracted_at}'), "
      f"is_extracting = false "
    ) + get_where(ok_usernames)
    utils.run_bigquery(query, params['credentials'])

  if len(err_usernames) > 0:
    query = q_pre + ' is_extracting = false ' + get_where(err_usernames)
    utils.run_bigquery(query, params['credentials'])

  return users_slice.shape[0]


def sync_data_to_warehouse(params, timeout_mins, trigger_mode, max_workers = 4):
  update_users(params)

  colname = '_extracted_at'
  timeout = timeout_mins * 60 if timeout_mins > 0 else None
  num_users_per_slice = 10  # bigquery quota of 1500 table appends per 24 hours

  if trigger_mode == 'continuing':
    query = f"select max({colname}) from `{params['dataset']}.{USERS_TABLE_NAME}`"
    extracted_at = utils.read_bigquery(query, params['credentials']).item()

  if trigger_mode != 'continuing' or extracted_at is None:
    extracted_at = dt.datetime.now(dt.timezone.utc).replace(microsecond = 0)

  query = (
    f"select username, max_todate, _extracted_at "
    f"from `{params['dataset']}.{USERS_TABLE_NAME}`"
  )
  if trigger_mode == 'continuing':
    query += (
      f" where {colname} is null or {colname} != "
      f"(select max({colname}) from `{params['dataset']}.{USERS_TABLE_NAME}`)"
    )
  users = utils.read_bigquery(query, params['credentials']).sort([colname, 'username'])

  sync_sessions_by_users_p = functools.partial(
    sync_sessions_by_users, params = params, extracted_at = extracted_at)

  try:  # api is easily overwhelmed
    # this version wasn't timing out properly and wasn't updating the progress bar
    # with tqdm(total = users.shape[0]) as pbar:
    #   with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
    #     futures = [
    #       executor.submit(sync_sessions_by_users_p, users_slice)
    #       for users_slice in users.iter_slices(num_users_per_slice)]
    #     for future in concurrent.futures.as_completed(futures, timeout = timeout):
    #       num_users_this_slice = future.result()
    #       pbar.update(num_users_this_slice)

    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
      list(tqdm(
        executor.map(
          sync_sessions_by_users_p,
          users.iter_slices(num_users_per_slice),
          timeout = timeout),
        total = (users.shape[0] // num_users_per_slice) + 1))

  except TimeoutError:
    if (trigger_mode in ('oneormore', 'continuing')
        and params['github_ref_name'] is not None):  # noqa: W503
      trigger_workflow(timeout_mins)

  return True


def trigger_workflow(timeout_mins, trigger_mode = 'continuing'):
  g = github.Github(login_or_token = os.getenv('GH_PAT'))
  repo = g.get_repo(os.getenv('GITHUB_REPOSITORY'))
  pattern = '(?<=\\.github/workflows/).+\\.ya?ml'
  workflow_name = re.search(pattern, os.getenv('GITHUB_WORKFLOW_REF')).group(0)
  workflow = repo.get_workflow(workflow_name)
  inputs = {'timeout_mins': str(timeout_mins), 'trigger_mode': trigger_mode}
  response = workflow.create_dispatch(os.getenv('GITHUB_REF_NAME'), inputs = inputs)
  return response


def update_users(params, colname = 'username'):
  users_current = utils.read_bigquery(
    f"select distinct {colname} from `{params['dataset']}.{USERS_TABLE_NAME}`",
    params['credentials'])
  users_mappings = utils.read_bigquery(
    f'select distinct {colname} from andhra_pradesh_ccp_mappings.mlhp_users',
    params['credentials'])

  users_new = (
    users_mappings
    .join(users_current, on = colname, how = 'anti')
    .with_columns(
      created_at = dt.datetime.now(dt.timezone.utc).replace(microsecond = 0),
      max_todate = DEFAULT_EXTRACTED_AT.date(),
      _extracted_at = DEFAULT_EXTRACTED_AT,
      is_extracting = False
    )
  )
  utils.write_bigquery(users_new, USERS_TABLE_NAME, params, 'WRITE_APPEND')


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--params-path', default = 'params.yaml', type = Path)
  parser.add_argument('--timeout-mins', default = 5, type = int)
  parser.add_argument(
    '--trigger-mode', default = 'oneanddone',
    choices = ['oneanddone', 'oneormore', 'continuing'])
  parser.add_argument('--max-workers', default = 4, type = int)
  args = parser.parse_args()
  return args


def main():
  try:
    source_name = 'andhra_pradesh_mlhp'
    args = parse_args()
    params = utils.get_params(source_name, args.params_path)
    sync_data_to_warehouse(params, args.timeout_mins, args.trigger_mode, args.max_workers)

  except Exception as e:
    if params['environment'] == 'prod':
      text = utils.get_slack_message_text(e, source_name)
      utils.send_message_to_slack(
        text, params['slack_channel_id'], params['slack_token'])
    raise e


if __name__ == '__main__':
  main()
