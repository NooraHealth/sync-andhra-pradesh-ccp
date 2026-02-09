import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import polars as pl
import polars.selectors as cs
import requests
import xlsxwriter
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter, Retry
from . import utils


def dict_hash(dictionary):
  dictionary_json = json.dumps(dictionary, sort_keys = True)
  # Create an MD5 hash object
  hasher = hashlib.md5()
  # Encode the JSON string and update the hasher
  hasher.update(dictionary_json.encode('utf-8'))
  # Return the hexadecimal representation of the hash
  return hasher.hexdigest()


def dt_iterate(start, end, step):
  assert start <= end, 'start should be less than end'
  while start <= end:
    yield start
    start += step


def requests_retry_session(
    total = 10, backoff_factor = 0.5, status_forcelist = (500, 502, 503, 504)):
  session = requests.Session()
  retries = Retry(
    total = total, backoff_factor = backoff_factor, status_forcelist = status_forcelist)
  adapter = HTTPAdapter(max_retries = retries)
  session.mount('http://', adapter)
  session.mount('https://', adapter)
  return session


class Report:
  def __init__(self, base_url: str, username: str, password: str):
    self.url = base_url
    self.username = username
    self.password = password
    self.key = None

  def login(self):
    request_json = {'login': True, 'username': self.username, 'password': self.password}
    response = requests_retry_session().get(self.url, json = request_json)
    response.raise_for_status()
    data = response.json()

    assert data['result'] == 'success', f'Login unsuccessful: {data}'
    print('Login successful')
    self.key = data['Auth-Key']

  @staticmethod
  def has_token_expired(data):
    error_vals = ('Invalid or token expired', 'Expired token')
    return data['result'] == 'failed' and data['error'] in error_vals

  @property
  def headers(self):
    return {'Auth-Key': self.key, 'Username': self.username}

  def get_patient_training(self, date: datetime):
    request_json = {
      'get_total_ccp_class_attendancedata': True,
      'date': date.strftime('%d-%m-%Y')
    }
    response = requests_retry_session().get(
      self.url, headers = self.headers, json = request_json)
    response.raise_for_status()
    data = response.json()

    if Report.has_token_expired(data):
      self.login()
      return self.get_patient_training(date)
    else:
      print(f'Fetched patient training sessions for date {date}')
      return data['data'] if data['result'] == 'success' else []

  def get_nurse_training(self, date: datetime):
    request_json = {
      'get_total_nurse_training_sessiondata': True,
      'date': date.strftime('%d-%m-%Y')
    }
    response = requests_retry_session().get(
      self.url, headers = self.headers, json = request_json)
    response.raise_for_status()
    data = response.json()

    if Report.has_token_expired(data):
      self.login()
      return self.get_nurse_training(date)
    else:
      print(f'Fetched nurse training sessions for date {date}')
      return data['data'] if data['result'] == 'success' else []

  def get_nurse_details(self, phone_number: str):
    request_json = {'get_nurses_detailes_data': True, 'username': phone_number}
    response = requests_retry_session().get(
      self.url, headers = self.headers, json = request_json)
    response.raise_for_status()
    data = response.json()

    # check for expired token, and if expired, regenerate token
    if Report.has_token_expired(data):
      self.login()
      return self.get_nurse_details(phone_number)
    else:
      print(f'Fetched nurse details for phone ending in {phone_number[-4:]}')
      return data['data'] if data['result'] == 'success' else []


def read_sessions_data_from_api(params, dates):
  report = Report(params['url'], params['username'], params['password'])
  report.login()
  iter_dates = [*dt_iterate(dates['start'], dates['end'], timedelta(days = 1))]

  with concurrent.futures.ThreadPoolExecutor() as executor:
    patient_trainings_nested = executor.map(report.get_patient_training, iter_dates)
  patient_trainings = [x2 for x1 in patient_trainings_nested for x2 in x1]

  with concurrent.futures.ThreadPoolExecutor() as executor:
    nurse_trainings_nested = executor.map(report.get_nurse_training, iter_dates)
  nurse_trainings = [x2 for x1 in nurse_trainings_nested for x2 in x1]

  for x in patient_trainings:
    x['md5'] = dict_hash(x)

  for x in nurse_trainings:
    x['md5'] = dict_hash(x)

  data_frames = {}

  if patient_trainings:
    int_cols = ['mothers_trained', 'family_members_trained', 'total_trained']
    data_frames['patient_training_sessions'] = (
      pl.from_dicts(patient_trainings)
      .with_columns(
        pl.col('date_of_session').str.to_date('%d-%m-%Y'),
        cs.by_name(int_cols, require_all = False).cast(pl.Int64),
        cs.by_name('data1', require_all = False)
        .map_elements(utils.json_dumps_list, return_dtype = pl.String)
      )
    )
  else:
    print('No patient training sessions found')

  if nurse_trainings:
    int_cols = ['totalmaster_trainer', 'total_trainees']
    struct_cols = ['trainerdata1', 'traineesdata1']
    data_frames['nurse_training_sessions'] = (
      pl.from_dicts(nurse_trainings)
      .with_columns(
        cs.by_name('sessiondateandtime', require_all = False).str.to_date('%d-%m-%Y'),
        cs.by_name(int_cols, require_all = False).cast(pl.Int64),
        cs.by_name(struct_cols, require_all = False)
        .map_elements(utils.json_dumps_list, return_dtype = pl.String)
      )
    )
  else:
    print('No nurse training sessions found')

  return data_frames


def read_nurse_phones_from_bigquery(params):
  col_name = 'session_conducted_by'
  phones_raw = utils.read_bigquery(
    (
      f"select distinct {col_name} "
      f"from `{params['dataset']}.patient_training_sessions` "
      f"where {col_name} != ''"
    ),
    params['credentials'])
  phones = phones_raw.get_column(col_name).str.split(',').explode().unique().sort()
  return phones


def read_nurses_data_from_api(params, nurse_phones):
  report = Report(params['url'], params['username'], params['password'])
  report.login()

  with concurrent.futures.ThreadPoolExecutor() as executor:
    nurse_details_nested = executor.map(report.get_nurse_details, nurse_phones)
  nurses = [x2 for x1 in nurse_details_nested for x2 in x1]

  data_frames = {}
  if not nurses:
    return data_frames

  data_frames['nurses'] = pl.from_dicts(nurses).with_columns(
    cs.by_name('user_created_dateandtime', require_all = False)
    .str.to_datetime('%Y-%m-%d %H:%M:%S')
  )
  return data_frames


def write_data_to_excel(data_frames, filepath = 'data.xlsx'):
  with xlsxwriter.Workbook(filepath) as wb:
    for key, df in sorted(data_frames.items()):
      df.write_excel(workbook = wb, worksheet = key)


def write_data_to_bigquery(params, data_frames):
  write_dispositions = {
    'nurses': 'WRITE_TRUNCATE',
    'patient_training_sessions': 'WRITE_APPEND',
    'nurse_training_sessions': 'WRITE_APPEND',
  }
  extracted_at = datetime.now(dt.timezone.utc).replace(microsecond = 0)

  for key in data_frames:
    data_frames[key] = utils.add_extracted_columns(data_frames[key], extracted_at)
    utils.write_bigquery(data_frames[key], key, params, write_dispositions[key])


def get_dates_from_bigquery(
    params, default_start_date = dt.date(2023, 6, 1), overlap = 30):
  # earliest nurse training 2023-06-06, earliest patient training 2023-08-16
  table_name = 'patient_training_sessions'
  col_name = 'date_of_session'
  dates = dict(
    start = default_start_date, end = datetime.now().date() - timedelta(days = 1))

  if utils.read_bigquery_exists(table_name, params):
    q = f"select max({col_name}) as max_date from `{params['dataset']}.{table_name}`"
    df = utils.read_bigquery(q, params['credentials'])
    if df.item() is not None:
      dates['start'] = df.item() - timedelta(days = overlap - 1)

  return dates


def get_dates(args, params):
  today = datetime.now().date()
  if args.dest == 'bigquery':
    dates = get_dates_from_bigquery(params)
  else:
    dates = dict(start = today - timedelta(days = 7), end = today - timedelta(days = 1))

    if args.start_date is not None:
      dates['start'] = args.start_date
    if args.end_date is not None:
      dates['end'] = args.end_date

  if dates['start'] > dates['end']:
    raise Exception('Start date cannot be later than end date.')
  if dates['end'] > today:
    raise Exception('End date cannot be later than today.')

  print(
    f"Attempting to fetch data between {dates['start']} and {dates['end']}, inclusive")
  return dates


def parse_args():
  parser = argparse.ArgumentParser(
    description = 'Extract and load for the Andhra Pradesh CCP API.',
    formatter_class = argparse.RawTextHelpFormatter)
  parser.add_argument(
    '--dest',
    choices = ['bigquery', 'local'],
    default = 'bigquery',
    help = 'Destination to write the data (bigquery or local).')
  parser.add_argument(
    '--start-date',
    type = lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
    help = '(Optional) Start date in YYYY-MM-DD format. Only used if --dest=local.')
  parser.add_argument(
    '--end-date',
    type = lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
    help = '(Optional) End date in YYYY-MM-DD format. Only used if --dest=local.')

  args = parser.parse_args()
  return args


def main():
  try:
    source_name = 'andhra_pradesh_ccp'
    args = parse_args()
    params = utils.get_params(source_name)
    dates = get_dates(args, params)

    for entity in ['sessions', 'nurses']:
      if entity == 'sessions':
        data = read_sessions_data_from_api(params['source_params'], dates)
      elif entity == 'nurses':
        nurse_phones = read_nurse_phones_from_bigquery(params)
        data = read_nurses_data_from_api(params['source_params'], nurse_phones)

      if len(data) > 0:
        return None

      if args.dest == 'bigquery':
        write_data_to_bigquery(params, data)
      else:
        write_data_to_excel(data)

  except Exception as e:
    if params['environment'] == 'prod':
      text = utils.get_slack_message_text(e, source_name)
      utils.send_message_to_slack(text, params['slack_channel_id'], params['slack_token'])
    raise e


if __name__ == '__main__':
  main()
