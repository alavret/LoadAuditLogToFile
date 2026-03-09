from dotenv import load_dotenv
import requests
import logging
import json
import logging.handlers as handlers
import os
from pathlib import Path
import sys
import re
from dataclasses import dataclass
from datetime import datetime
from dateutil.relativedelta import relativedelta
from http import HTTPStatus
import time
import traceback

DEFAULT_360_API_URL = "https://api360.yandex.net"
NEW_360_API_URL = "https://cloud-api.yandex.net/v1"
LOG_FILE = "get_audit_logs.log"

# отфильтрованные события и почтовые ящики (не исользуется)
FILTERED_MAIL_EVENTS = []
FILTERED_MAILBOXES = []

# Количество страниц для запроса логов последовательно в одном цикле последовательного обращения к API, после чего формируется новый набор стартовой и конечных дат
OLD_LOG_MAX_PAGES = 10

# На сколько секунд сдвигается назад стартовыя дата запроса логов между последовательными обращениями к API (чтобы не потерять записи)
OVERLAPPED_SECONDS = 2
MAX_RETRIES = 3
RETRIES_DELAY_SEC = 2

# Цикл запроса логов
SLEEP_MINITS_AFTER_LAST_FETCH = 1

# Количество дней в прошлое для запроса логов, если нет никакой истории выгрузки
MAX_DAYS_AGO_FOR_API_CALLS = 90

# Время в минутах для сбора логов нового формата в одном цикле обращения к API и сброса полученных данных в файл
NEW_LOG_ONE_FETCH_CYCLE_IN_MINUTES = 180

# Время в минутах для сбора логов старого формата и сброса полученных данных в файл (внутри сбора применяется еще OLD_LOG_MAX_PAGES)
OLD_LOG_ONE_FETCH_CYCLE_IN_MINUTES = 180

# MAX value - 100 records
ALL_LOGS_MAX_RECORDS = 100

# !!! Don't modify MAIL_LOGS_MAX_RECORDS and DISK_LOGS_MAX_RECORDS values !!!
MAIL_LOGS_MAX_RECORDS = 100
DISK_LOGS_MAX_RECORDS = 100

# !!! Don't change values in LOGS_NAMES list !!!
LOGS_SOURCES = ["mail", "all"]


EXIT_CODE = 1

logger = logging.getLogger("get_audit_log")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
#file_handler = handlers.TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30, encoding='utf-8')
file_handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024,  backupCount=10, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def main():

    logger.info("--------------------------------------------------------")
    logger.info("Starting script...")

    settings = get_settings()
    runtime_data = RuntimeData(last_records={"mail": [], "all": []}, oldest_datetime={"mail": None, "all": None})

    if settings is None:
        logger.error("Settings are not set.")
        sys.exit(EXIT_CODE)

    logger.info("Constants in this run:")
    logger.info(f"MAIL_LOGS_MAX_RECORDS: {MAIL_LOGS_MAX_RECORDS}")
    logger.info(f"ALL_LOGS_MAX_RECORDS: {ALL_LOGS_MAX_RECORDS}")
    logger.info(f"NEW_LOG_ONE_FETCH_CYCLE_IN_MINUTES: {NEW_LOG_ONE_FETCH_CYCLE_IN_MINUTES}")
    logger.info(f"OLD_LOG_ONE_FETCH_CYCLE_IN_MINUTES: {OLD_LOG_ONE_FETCH_CYCLE_IN_MINUTES}")
    logger.info(f"SLEEP_MINITS_AFTER_LAST_FETCH: {SLEEP_MINITS_AFTER_LAST_FETCH}")
    logger.info(f"OVERLAPPED_SECONDS: {OVERLAPPED_SECONDS}")
    logger.info(f"FILTERED_MAIL_EVENTS: {FILTERED_MAIL_EVENTS}")
    logger.info(f"FILTERED_MAILBOXES: {FILTERED_MAILBOXES}")

    logger.info("--------------------------------------------------------")

    download_sсheduler(settings, runtime_data)
    

def fetch_and_save_old_logs_controller(settings: "SettingParams", runtime_data: "RuntimeData", oldest_datetime: str, label: str):

    try:
        fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
        parsed_oldest = _parse_utc_datetime(oldest_datetime)
        new_started_at = parsed_oldest + relativedelta(microseconds=+100)
        logger.info(f"Started mail audit logs download process from {new_started_at.strftime(fmt)}")

        progress_start_dt = _parse_utc_datetime(oldest_datetime)
        progress_end_dt = datetime.now() + relativedelta(hours=-settings.timezone_shift)
        print_progress_bar(progress_start_dt, progress_start_dt, progress_end_dt)

        exit_while = False
        while True:

            parsed_oldest = _parse_utc_datetime(oldest_datetime)
            new_started_at = parsed_oldest + relativedelta(microseconds=+1000)
            last_datetime = new_started_at.strftime(fmt)

            diff_in_minutes = (datetime.now() + relativedelta(hours=-settings.timezone_shift) - parsed_oldest).total_seconds() / 60
            if diff_in_minutes > NEW_LOG_ONE_FETCH_CYCLE_IN_MINUTES:
                ended_at = parsed_oldest + relativedelta(minutes=+NEW_LOG_ONE_FETCH_CYCLE_IN_MINUTES)
            else:
                ended_at = datetime.now() + relativedelta(hours=-settings.timezone_shift)
                exit_while = True
            str_ended_at = ended_at.strftime(fmt)

            logger.debug(f"Start downloading data from {label} audit logs from {last_datetime} to {str_ended_at}.")
            if label == "mail":
                error, records = fetch_mail_audit_logs(settings, last_datetime, str_ended_at)

            if error:
                logger.error(f"Error occured during reciving records from {label} audit logs from {last_datetime} to {str_ended_at}. Force quite cycle.")
                break

            if records:
                #logger.info(f"{len(records)} records were recived from {label} audit logs from {last_datetime} to {str_ended_at}.")
                decoded_records = [r.decode() for r in records]
                save_old_logs_to_file(settings, label, decoded_records, runtime_data)
                json_records = [json.loads(r) for r in decoded_records]
                sorted_records = sorted(json_records, key=lambda x: x["date"], reverse=True)
                occurred_at_raw = sorted_records[0]["date"]
                match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", occurred_at_raw)
                if match:
                    suggested_date = match.group(1)
                else:
                    logger.warning(f"Could not parse occurred_at field: {occurred_at_raw}")
                    suggested_date = occurred_at_raw[:19]  # fallback, though не гарантия что корректно

                # Если дата последнего события в полученных событиях совпадает до секунды с конечной датой,
                # то используем дату последнего события в полученных событиях, иначе используем конечную дату
                if suggested_date[:19] ==   str_ended_at[:19]:
                    oldest_datetime = f"{suggested_date}Z"
                else:
                    oldest_datetime = str_ended_at

            elif records == []:
                #logger.debug(f"No new logs received for period from {last_datetime} to {str_ended_at}. Next turn.")
                oldest_datetime = str_ended_at
            else:
                break

            print_progress_bar(progress_start_dt, ended_at, progress_end_dt)
            runtime_data.oldest_datetime["mail"] = oldest_datetime

            if exit_while:
                break

        print_progress_bar(progress_start_dt, progress_end_dt, progress_end_dt)
        sys.stdout.write('\n')
        sys.stdout.flush()

    except Exception as e:
        sys.stdout.write('\n')
        sys.stdout.flush()
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def get_date_of_last_record(settings: "SettingParams", runtime_data: "RuntimeData", log_source):

    fmt = '%Y-%m-%dT%H:%M:%SZ'
    date_now = datetime.now() + relativedelta(hours=-settings.timezone_shift)
    date = (date_now + relativedelta(days=-MAX_DAYS_AGO_FOR_API_CALLS)).strftime(fmt)
    existing_records = []
    if runtime_data.oldest_datetime[log_source] is None:
        all_files = Path(settings.dir_paths[log_source]).glob(f"*.{settings.ext}")
        all_names = (file_path.name.lower() for file_path in all_files)
        files = [f for f in all_names if re.match(settings.file_names[log_source] + r'_[0-9]{4}\-[0-9]{2}\-[0-9]{2}\.' + settings.ext, f)]

        if not files:
            logger.info(f"No files found in {settings.dir_paths[log_source]} catalog. Start full downloading data.")
        else:
            files.sort(reverse=True)
            for file in files:
                logger.debug(f"Check records in file {os.path.join(settings.dir_paths[log_source], file)}.")
                with open(os.path.join(settings.dir_paths[log_source], file), 'r', encoding="utf8") as f:
                    for line in f:
                        existing_records.append(line.replace('\n', ''))
                if not existing_records:
                    logger.debug(f"No records found in file {os.path.join(settings.dir_paths[log_source], file)}. Selecting previous file.")
                else:
                    temp_list = [json.loads(r) for r in existing_records]
                    if log_source == "mail" or log_source == "disk":
                        occurred_at_raw = temp_list[-1]["date"][0:19]
                    else:
                        occurred_at_raw = temp_list[-1]['event']['occurred_at']
                    # Поддержка разных форматов строки времени: YYYY-MM-DDTHH:MM:SS[.microseconds]
                    match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", occurred_at_raw)
                    if match:
                        suggested_date = match.group(1)
                    else:
                        logger.warning(f"Could not parse occurred_at field: {occurred_at_raw}")
                        suggested_date = occurred_at_raw[:19]  # fallback, though не гарантия что корректно
                    runtime_data.oldest_datetime[log_source] = suggested_date
                    date = f"{suggested_date}Z"
                    break

    else:
        date =runtime_data.oldest_datetime[log_source]

    logger.info(f"Last record date for {log_source} logs: {date}")
    
    return date

@dataclass
class SettingParams:
    oauth_token: str
    organization_id: int  
    dir_paths : dict
    ext: str
    file_names: dict
    timezone_shift: int

@dataclass
class RuntimeData:
    last_records: dict = None
    oldest_datetime: str = None

def get_settings():
    exit_flag = False
    try:
        settings = SettingParams (
            oauth_token = os.environ.get("OAUTH_TOKEN_ARG"),
            organization_id = int(os.environ.get("ORGANIZATION_ID_ARG")),
            dir_paths = {},
            file_names = {},
            ext = os.environ.get("LOG_FILE_EXTENSION"),
            timezone_shift = 3,
        )
    except ValueError:
        logger.error("ORGANIZATION_ID_ARG params must be an integer")
        exit_flag = True

    if not settings.oauth_token:
        logger.error("OAUTH_TOKEN_ARG is not set")
        exit_flag = True

    if settings.organization_id == 0:
        logger.error("ORGANIZATION_ID_ARG is not set")
        exit_flag = True
    
    mail_dir_path = Path(os.environ.get("MAIL_LOG_CATALOG_LOCATION"))
    if not mail_dir_path:
        logger.error("MAIL_LOG_CATALOG_LOCATION is not set")
        exit_flag = True
    else:
        if not mail_dir_path.exists:
            print(f"!!! ERROR !!! The path '{mail_dir_path}' does not exist. Check path and letter case. Exit.")
            exit_flag = True
        if not mail_dir_path.is_dir():
            print(f"!!! ERROR !!! The path '{mail_dir_path}' is not a directory. Exit.")
            exit_flag = True

    all_dir_path = Path(os.environ.get("NEW_LOG_CATALOG_LOCATION"))
    if not all_dir_path:
        logger.error("NEW_LOG_CATALOG_LOCATION is not set")
        exit_flag = True
    else:
        if not all_dir_path.exists:
            print(f"!!! ERROR !!! The path '{all_dir_path}' does not exist. Check path and letter case. Exit.")
            exit_flag = True
        if not all_dir_path.is_dir():
            print(f"!!! ERROR !!! The path '{all_dir_path}' is not a directory. Exit.")
            exit_flag = True

    if not settings.ext:
        logger.error("LOG_FILE_EXTENSION is not set")
        exit_flag = True
    
    mail_file_name = os.environ.get("MAIL_LOG_FILE_BASE_NAME")
    all_file_name = os.environ.get("NEW_LOG_FILE_BASE_NAME")

    temp_timezone_shift = int(os.environ.get("TIMEZONE_SHIFT_IN_HOURS"))
    if temp_timezone_shift >= 12 or temp_timezone_shift <= -12:
         logger.error("TIMEZONE_SHIFT_IN_HOURS is wrong. Exit.")
         exit_flag = True
    else:
        settings.timezone_shift = temp_timezone_shift

    if exit_flag:
        return None
    
    settings.dir_paths["mail"] = mail_dir_path
    settings.dir_paths["all"] = all_dir_path

    settings.file_names["mail"] = mail_file_name
    settings.file_names["all"] = all_file_name

    logger.info(f"Settings: ORGANIZATION_ID_ARG - {settings.organization_id}")
    logger.info(f"Settings: MAIL_LOG_CATALOG_LOCATION - {settings.dir_paths['mail']}")
    logger.info(f"Settings: NEW_LOG_CATALOG_LOCATION - {settings.dir_paths['all']}")
    logger.info(f"Settings: MAIL_LOG_FILE_BASE_NAME - {settings.file_names['mail']}")
    logger.info(f"Settings: NEW_LOG_FILE_BASE_NAME - {settings.file_names['all']}")
    logger.info(f"Settings: LOG_FILE_EXTENSION - {settings.ext}")
    logger.info(f"Settings: TIMEZONE_SHIFT_IN_HOURS - {settings.timezone_shift}")
    
    return settings

def fetch_mail_audit_logs(settings: "SettingParams", last_date: str = "", ended_at: str = ""):
  
    log_records = set()
    params = {}
    error = False
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    try:
        params["pageSize"] = MAIL_LOGS_MAX_RECORDS
        if last_date:
            params["afterDate"] = last_date
        if ended_at:
            params["beforeDate"] = ended_at
        url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/audit_log/mail"
        headers = {"Authorization": f"OAuth {settings.oauth_token}"}
        pages_count = 0
        retries = 0
        while True:           
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Error during GET request: {response.status_code}. Error message: {response.text}")
                logger.debug(f"Error during GET request. url - {url}. Params - {params}")
                logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
                if retries < MAX_RETRIES:
                    logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Forcing exit without getting data.")
                    error = True
                    return error, []
            else:
                if response.json()["events"] is not None and response.json()["events"] != []:
                    retries = 1
                    temp_list = response.json()["events"]
                    sorted_list = sorted(temp_list, key=lambda x: x["date"], reverse=True)
                    if temp_list:
                        logger.debug(f'Received {len(sorted_list)} records, from {sorted_list[-1]["date"]} to {sorted_list[0]["date"]}')
                        temp_json = [json.dumps(d, ensure_ascii=False).encode('utf8') for d in sorted_list]
                        log_records.update(temp_json)
                    
                    if response.json()["nextPageToken"] == "":
                        break
                    else:
                        if pages_count < OLD_LOG_MAX_PAGES:
                            pages_count += 1
                            params["pageToken"] = response.json()["nextPageToken"]
                        else:
                            if params.get('pageToken') : del params['pageToken']
                            if temp_list:
                                occurred_at_raw = sorted_list[-1]["date"]
                                match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", occurred_at_raw)
                                if match:
                                    suggested_date = match.group(1)
                                else:
                                    logger.warning(f"Could not parse occurred_at field: {occurred_at_raw}")
                                    suggested_date = occurred_at_raw[:19]  # fallback, though не гарантия что корректно

                                suggested_date = f'{suggested_date}Z'
                                msg_date = datetime.strptime(suggested_date, fmt) + relativedelta(microsecond=-1000)
                                params["beforeDate"] = msg_date.strftime(fmt)
                            else:
                                logger.debug("No data returned from API request. Exit from cycle.")
                                logger.debug(f"Data for GET request: url - {url}. Params - {params}")
                                logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
                                break
                            params["pageSize"] = 100
                            pages_count = 0
                elif response.json()["events"] == []:
                    logger.debug("API returned empty list of events. Exit from cycle.")
                    return False, []

    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        error = True
        return []
        
    return error, log_records


def save_old_logs_to_file(settings: "SettingParams", label: str, log_records: list, runtime_data: "RuntimeData" ):

    result = False
    existing_records = runtime_data.last_records[label]
    separated_list = {}
    for r in log_records:
        # logger.info(f"source - {r}")
        # for ex in existing_records:
        #     if r == ex:
        #         continue
        #     else:
        #         logger.info(f"existing - {ex}")
        #         diff_chars = [(i, char1, char2) for i, (char1, char2) in enumerate(zip(r, ex)) if char1 != char2]
        #         logger.info(f"diff_chars - {diff_chars}")
        if r in existing_records:
            continue

        search_result = re.search(r".+\"date\"\:\s\"(.+)\".+", r)
        if search_result:
            date_part = search_result.group(1)[0:10]
            if date_part not in separated_list.keys():
                separated_list[date_part] = []
            sorted_dict = {}
            sorted_dict["full_time"] = search_result.group(1)
            sorted_dict["data"] = r
            separated_list[date_part].append(sorted_dict)
        else:
            logger.error(f"No date found in record: {r}")
    
    for date, records in separated_list.items():
        file_path = os.path.join(settings.dir_paths[label], f"{settings.file_names[label]}_{date}.{settings.ext}")
        if len(records) > 0:
            logger.debug(f"Writing {len(records)} records to {label} audit file {file_path}")
            try:
                with open(file_path, 'a', encoding="utf8") as f:
                    for r in sorted(records, key=lambda d: d['full_time']):
                        f.write(f"{r['data']}\n")
                    result = True
            except Exception as e:
                logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            
    return result

def fetch_all_audit_logs_by_params(settings: "SettingParams", query_params: dict):
    error = False
    params = query_params.copy()
    params["count"] = ALL_LOGS_MAX_RECORDS
    if not params.get("ended_at"):
        logger.error("Param ended_at for gettion new audit logs not set.")
        return []

    log_records = []
    url = f"{NEW_360_API_URL}/auditlog/organizations/{settings.organization_id}/events"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    try:
        retries = 0
        while True:           
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Error during GET request: {response.status_code}. Error message: {response.text}")
                logger.debug(f"Error during GET request. url - {url}. Params - {params}")
                if retries < MAX_RETRIES:
                    logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Forcing exit without getting data.")
                    error = True
                    return []
            else:
                retries = 1
                temp_list = response.json()["items"]
                if temp_list:
                    sorted_list = sorted(temp_list, key=lambda d: d["event"]["occurred_at"], reverse=True)
                    logger.debug(f'Received {len(temp_list)} records, from {sorted_list[-1]["event"]["occurred_at"][0:19]} to {sorted_list[0]["event"]["occurred_at"][0:19]}')
                    log_records.extend(temp_list)
                else:
                    logger.debug("No data returned from API request.")
                    logger.debug(f"Data for GET request: url - {url}. Params - {params}")
                    logger.debug(f"Received data: {response.json()}")

                if response.json().get("iteration_key") is None or response.json()["iteration_key"] == "":
                    break
                else:
                    params["iteration_key"] = response.json()["iteration_key"]

    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        error = True
        return []
        
    return error, log_records

def print_progress_bar(start_dt, current_dt, end_dt, bar_length=40):
    total_seconds = (end_dt - start_dt).total_seconds()
    if total_seconds <= 0:
        progress = 1.0
    else:
        elapsed_seconds = (current_dt - start_dt).total_seconds()
        progress = min(max(elapsed_seconds / total_seconds, 0.0), 1.0)

    filled = int(bar_length * progress)
    bar = '█' * filled + '░' * (bar_length - filled)
    percent = progress * 100

    sys.stdout.write(
        f'\r[{bar}] {percent:5.1f}% | '
        f'{current_dt.strftime("%Y-%m-%d %H:%M")} / {end_dt.strftime("%Y-%m-%d %H:%M")}'
    )
    sys.stdout.flush()


def _parse_utc_datetime(dt_str):
    if isinstance(dt_str, datetime):
        return dt_str
    for f in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            return datetime.strptime(dt_str, f)
        except ValueError:
            continue
    raise ValueError(f"Could not parse datetime string: {dt_str}")

def fetch_and_save_new_logs_controller(settings: "SettingParams", runtime_data: "RuntimeData", oldest_datetime : str = ""):

    try:
        fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
        params = {}
        parsed_oldest = _parse_utc_datetime(oldest_datetime)
        new_started_at = parsed_oldest + relativedelta(microseconds=+1)
        logger.info(f"Started new log download process from {new_started_at.strftime(fmt)}")
        exit_while = False

        progress_start_dt = _parse_utc_datetime(oldest_datetime)
        progress_end_dt = datetime.now() + relativedelta(hours=-settings.timezone_shift)
        print_progress_bar(progress_start_dt, progress_start_dt, progress_end_dt)

        while True:
            #Добавляем микросекунду, т.к. в API запрос для начальной даты учитывет микросекунды
            parsed_oldest = _parse_utc_datetime(oldest_datetime)
            new_started_at = parsed_oldest + relativedelta(microseconds=+1)
            params["started_at"] = new_started_at.strftime(fmt)

            diff_in_minutes = (datetime.now() + relativedelta(hours=-settings.timezone_shift) - parsed_oldest).total_seconds() / 60
            if diff_in_minutes > NEW_LOG_ONE_FETCH_CYCLE_IN_MINUTES:
                ended_at = parsed_oldest + relativedelta(minutes=+NEW_LOG_ONE_FETCH_CYCLE_IN_MINUTES)
            else:
                ended_at = datetime.now() + relativedelta(hours=-settings.timezone_shift)
                exit_while = True
            params["ended_at"] = ended_at.strftime(fmt)
            logger.debug(f"Fetch new logs cycle from {params['started_at']} to {params['ended_at']}")
            error, log_records = fetch_all_audit_logs_by_params(settings, params)
            if error:
                logger.error(f"Error occured during reciving records from new audit logs from  {params['started_at']} to {params['ended_at']}. Force quite cycle.")
                break
            if log_records:
                #logger.debug(f'Received {len(log_records)} records, from {log_records[-1]["event"]["occurred_at"][0:19]} to {log_records[0]["event"]["occurred_at"][0:19]}')
                save_new_logs_to_file(log_records, settings, runtime_data)
                sorted_log_records = sorted(log_records, key=lambda d: d["event"]["occurred_at"], reverse=True)
                occurred_at_raw = sorted_log_records[0]["event"]["occurred_at"]
                match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", occurred_at_raw)
                if match:
                    suggested_date = match.group(1)
                else:
                    logger.warning(f"Could not parse occurred_at field: {occurred_at_raw}")
                    suggested_date = occurred_at_raw[:19]  # fallback, though не гарантия что корректно
                
                occurred_at_zero_milliseconds = f"{suggested_date[:19]}Z"
                # Сравнение нужно для того, чтобы понять, какую дату нужно использовать для следующего запроса
                # Если дата последнего запроса совпадает посекундно (микросекунды не учитываются) с датой последнего события в ответе,
                # то используем дату последнего события (с микросекундами), иначе используем дату окончания текущего запроса
                if occurred_at_zero_milliseconds == params["ended_at"]:
                    oldest_datetime = f"{suggested_date}Z"
                else:
                    oldest_datetime = ended_at.strftime(fmt)
                
            elif log_records == []:
                logger.debug(f"No new logs received for period from {params['started_at']} to {params['ended_at']}. Next turn.")
                oldest_datetime = ended_at.strftime(fmt)
            else:
                break

            print_progress_bar(progress_start_dt, ended_at, progress_end_dt)
            runtime_data.oldest_datetime["all"] = oldest_datetime

            if exit_while:
                break

        print_progress_bar(progress_start_dt, progress_end_dt, progress_end_dt)
        sys.stdout.write('\n')
        sys.stdout.flush()

    except Exception as e:
        sys.stdout.write('\n')
        sys.stdout.flush()
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

def save_new_logs_to_file(log_records: list, settings: "SettingParams", runtime_data: "RuntimeData"):
    result = False
    fmt = '%Y-%m-%dT%H:%M:%SZ'
    existing_records = runtime_data.last_records["all"]
    separated_list = {}
    for r in log_records:
        if r in existing_records:
            continue
        date_part = datetime.strptime(f"{r['event']['occurred_at'][0:19]}Z", fmt).strftime("%Y-%m-%d")
        if date_part not in separated_list.keys():
            separated_list[date_part] = []
        sorted_dict = {}
        sorted_dict["full_time"] = f'{r["event"]["occurred_at"][0:19]}Z'
        sorted_dict["data"] = r
        separated_list[date_part].append(sorted_dict)

    for date, records in separated_list.items():
        file_path = os.path.join(settings.dir_paths["all"], f'{settings.file_names["all"]}_{date}.{settings.ext}')
        if len(records) > 0:
            logger.debug(f"Writing {len(records)} records of new audit log format logs to file {file_path}")
            try:
                with open(file_path, 'a', encoding="utf8") as f:
                    for r in sorted(records, key=lambda d: d["full_time"]):
                        f.write(f"{json.dumps(r['data'], ensure_ascii=False).encode('utf8').decode()}\n")
                    result = True
            except Exception as e:
                logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    return result

def download_sсheduler(settings: "SettingParams", runtime_data: "RuntimeData"):
    while True:
        for log_source in LOGS_SOURCES:
            last_datetime = get_date_of_last_record(settings, runtime_data, log_source)
            if log_source == "all":
                fetch_and_save_new_logs_controller(settings, runtime_data, last_datetime)
            elif log_source == "mail":
                fetch_and_save_old_logs_controller(settings, runtime_data, last_datetime, "mail")

        logger.info(f"Start sleeping for {SLEEP_MINITS_AFTER_LAST_FETCH} minutes.")
        time.sleep(SLEEP_MINITS_AFTER_LAST_FETCH * 60)

if __name__ == "__main__":

    denv_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(denv_path):
        load_dotenv(dotenv_path=denv_path,verbose=True, override=True)

    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nCtrl+C pressed. До свидания!")
        sys.exit(EXIT_CODE)
    except Exception as exc:
        tb = traceback.extract_tb(exc.__traceback__)
        last_frame = tb[-1] if tb else None
        if last_frame:
            logger.error(f"{type(exc).__name__} at {last_frame.filename}:{last_frame.lineno} in {last_frame.name}: {exc}")
        else:
            logger.error(f"{type(exc).__name__}: {exc}")
        sys.exit(EXIT_CODE)