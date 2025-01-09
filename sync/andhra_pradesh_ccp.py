import argparse
import datetime as dt
import hashlib
import json
import polars as pl
import polars.selectors as cs
import requests
import xlsxwriter
from datetime import datetime, timedelta
from multiprocessing import Pool
from .. import utils


def dict_hash(dictionary):
    dictionary_json = json.dumps(dictionary, sort_keys=True)

    # Create an MD5 hash object
    hasher = hashlib.md5()

    # Encode the JSON string and update the hasher
    hasher.update(dictionary_json.encode("utf-8"))

    # Return the hexadecimal representation of the hash
    return hasher.hexdigest()


def dt_iterate(start, end, step):
    assert start <= end, "start should be less than end"
    while start <= end:
        yield start
        start += step


class Report:
    def __init__(self, base_url: str, username: str, password: str):
        self.url = base_url
        self.username = username
        self.password = password
        self.key = None

    def login(self):
        response = requests.get(
            self.url,
            json={"login": True, "username": self.username, "password": self.password})
        response.raise_for_status()
        data = response.json()

        assert data["result"] == "success", f"Login unsuccessful: {data}"

        print("Login successful. Auth-Key:", data["Auth-Key"])
        self.key = data["Auth-Key"]

    @staticmethod
    def has_token_expired(data):
        error_vals = ("Invalid or token expired", "Expired token")
        return data["result"] == "failed" and data["error"] in error_vals

    @property
    def headers(self):
        return {"Auth-Key": self.key, "Username": self.username}

    def get_patient_training(self, date: datetime):
        response = requests.get(
            self.url,
            headers=self.headers,
            json={
                "get_total_ccp_class_attendancedata": True,
                "date": date.strftime("%d-%m-%Y"),
            },
        )
        response.raise_for_status()
        data = response.json()

        if Report.has_token_expired(data):
            self.login()
            return self.get_patient_training(date)
        else:
            print(f"Fetched patient training sessions for date {date}")
            return data["data"] if data["result"] == "success" else []

    def get_nurse_training(self, date: datetime):
        response = requests.get(
            self.url,
            headers=self.headers,
            json={
                "get_total_nurse_training_sessiondata": True,
                "date": date.strftime("%d-%m-%Y"),
            },
        )
        response.raise_for_status()
        data = response.json()

        if Report.has_token_expired(data):
            self.login()
            return self.get_nurse_training(date)
        else:
            print(f"Fetched nurse training sessions for date {date}")
            return data["data"] if data["result"] == "success" else []

    def get_nurse_details(self, phone_number: str):
        response = requests.get(
            self.url,
            headers=self.headers,
            json={"get_nurses_detailes_data": True, "username": phone_number},
        )
        response.raise_for_status()
        data = response.json()

        # check for expired token, and if expired, regenerate token
        if Report.has_token_expired(data):
            self.login()
            return self.get_nurse_details(phone_number)
        else:
            print(f"Fetched nurse details for phone ending in {phone_number[-4:]}")
            return data["data"] if data["result"] == "success" else []


def read_data_from_api(params, dates):
    report = Report(params["url"], params["username"], params["password"])
    report.login()

    with Pool() as p:
        patient_trainings_nested = p.map(
            report.get_patient_training,
            [*dt_iterate(dates["start"], dates["end"], timedelta(days=1))],
        )
    patient_trainings = [x2 for x1 in patient_trainings_nested for x2 in x1]

    with Pool() as p:
        nurse_trainings_nested = p.map(
            report.get_nurse_training,
            [*dt_iterate(dates["start"], dates["end"], timedelta(days=1))],
        )
    # nurse_trainings_nested = map(
    #     report.get_nurse_training,
    #     [*dt_iterate(dates["start"], dates["end"], timedelta(days=1))],
    # )

    nurse_trainings = [x2 for x1 in nurse_trainings_nested for x2 in x1]

    if not patient_trainings:
        print("No patient training sessions found")
        return

    for x in patient_trainings:
        x["md5"] = dict_hash(x)

    for x in nurse_trainings:
        x["md5"] = dict_hash(x)

    phones_in_patient_trainings = [
        x2 for x1 in patient_trainings for x2 in x1["session_conducted_by"].split(",")
    ]

    phones_in_nurse_trainings = []
    for i in nurse_trainings:
        if i.get("trainerdata1"):
            for trainer in i["trainerdata1"]:
                phones_in_nurse_trainings.append(trainer["phone_no"])

        if i.get("traineesdata1"):
            for trainee in i["traineesdata1"]:
                phones_in_nurse_trainings.append(trainee["phone_no"])

    all_phones = list(set(phones_in_patient_trainings + phones_in_nurse_trainings))

    with Pool() as p:
        nurse_details_nested = p.map(report.get_nurse_details, all_phones)
    # nurse_details_nested = map(report.get_nurse_details, all_phones)
    nurses = [x2 for x1 in nurse_details_nested for x2 in x1]

    nurses_df = pl.from_dicts(nurses)
    nurses_df = nurses_df.with_columns(
        cs.by_name("user_created_dateandtime", require_all=False)
        .str.to_datetime("%Y-%m-%d %H:%M:%S")
    )

    patient_trainings_df = pl.from_dicts(patient_trainings)
    int_cols = ["mothers_trained", "family_members_trained", "total_trained"]
    patient_trainings_df = (
        patient_trainings_df
        .with_columns(cs.by_name(int_cols, require_all=False).cast(pl.Int64))
        .with_columns(pl.col("date_of_session").str.to_date("%d-%m-%Y"))
        .with_columns(
            cs.by_name("data1", require_all=False)
            .map_elements(utils.json_dumps_list, return_dtype=pl.String))
    )

    nurse_trainings_df = pl.from_dicts(nurse_trainings)
    int_cols = ["totalmaster_trainer", "total_trainees"]
    struct_cols = ["trainerdata1", "traineesdata1"]
    nurse_trainings_df = (
        nurse_trainings_df
        .with_columns(cs.by_name(int_cols, require_all=False).cast(pl.Int64))
        .with_columns(
            cs.by_name("sessiondateandtime", require_all=False).str.to_date("%d-%m-%Y"))
        .with_columns(
            cs.by_name(struct_cols, require_all=False)
            .map_elements(utils.json_dumps_list, return_dtype=pl.String))
    )

    data_frames = {
        "nurses": nurses_df,
        "patient_training_sessions": patient_trainings_df,
        "nurse_training_sessions": nurse_trainings_df,
    }
    return data_frames


def write_data_to_excel(data_frames, filepath="data.xlsx"):
    with xlsxwriter.Workbook(filepath) as wb:
        for key, df in sorted(data_frames.items()):
            df.write_excel(workbook=wb, worksheet=key)


def write_data_to_bigquery(params, data_frames):
    extracted_at = datetime.now(dt.timezone.utc).replace(microsecond=0)
    for key in data_frames:
        data_frames[key] = utils.add_extracted_columns(data_frames[key], extracted_at)

    if utils.read_bigquery_exists("nurses", params):
        # keep the latest data for each username
        nurses_old = utils.read_bigquery(
            f"select * from `{params['dataset']}.nurses`", params["credentials"])
        nurses_concat = pl.concat(
            [nurses_old, data_frames["nurses"]], how="diagonal_relaxed")
        data_frames["nurses"] = nurses_concat.group_by("username").tail(1)

    write_dispositions = {
        "nurses": "WRITE_TRUNCATE",
        "patient_training_sessions": "WRITE_APPEND",
        "nurse_training_sessions": "WRITE_APPEND",
    }

    for table_name in sorted(data_frames.keys()):
        utils.write_bigquery(
            data_frames[table_name], table_name, params, write_dispositions[table_name])


def get_dates_from_bigquery(
        params, default_start_date=dt.date(2023, 6, 1), overlap=30):
    # earliest nurse training 2023-06-06, earliest patient training 2023-08-16
    table_name = "patient_training_sessions"
    col_name = "date_of_session"
    dates = dict(start=default_start_date, end=datetime.now().date() - timedelta(days=1))

    if utils.read_bigquery_exists(table_name, params):
        q = f"select max({col_name}) as max_date from `{params['dataset']}.{table_name}`"
        df = utils.read_bigquery(q, params["credentials"])
        if df.item() is not None:
            dates["start"] = df.item() - timedelta(days=overlap - 1)

    return dates


def get_dates(args, params):
    today = datetime.now().date()
    if args.dest == "bigquery":
        dates = get_dates_from_bigquery(params)
    else:
        dates = dict(start=today - timedelta(days=7), end=today - timedelta(days=1))

        if args.start_date is not None:
            dates["start"] = args.start_date
        if args.end_date is not None:
            dates["end"] = args.end_date

    if dates["start"] > dates["end"]:
        raise Exception("Start date cannot be later than end date.")
    if dates["end"] > today:
        raise Exception("End date cannot be later than today.")

    print(f"Attempting to fetch data between {dates['start']} "
          f"and {dates['end']}, inclusive.")
    return dates


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract and load for the Andhra Pradesh CCP API.",
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--dest",
        choices=["bigquery", "local"],
        default="bigquery",
        help="Destination to write the data (bigquery or local).")
    parser.add_argument(
        "--start-date",
        type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
        help="(Optional) Start date in YYYY-MM-DD format. Only used if --dest=local.")
    parser.add_argument(
        "--end-date",
        type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
        help="(Optional) End date in YYYY-MM-DD format. Only used if --dest=local.")

    args = parser.parse_args()
    return args


def main():
    try:
        source_name = "andhra_pradesh_ccp"
        args = parse_args()
        params = utils.get_params(source_name)
        dates = get_dates(args, params)

        data = read_data_from_api(params["source_secrets"], dates)
        if data is None:
            return None

        if args.dest == "bigquery":
            write_data_to_bigquery(params, data)
        else:
            write_data_to_excel(data)

    except Exception as e:
        if params["environment"] == "prod":
            text = utils.get_slack_message_text(e, source_name)
            utils.send_message_to_slack(
                text, params["slack_channel_id"], params["slack_token"])
        raise e


if __name__ == "__main__":
    main()
