from dotenv import load_dotenv
import requests
import logging
import json
import logging.handlers as handlers
import os
import sys
import re
from dataclasses import dataclass
import datetime
from dateutil.relativedelta import relativedelta
from http import HTTPStatus
import time
from os import environ

DEFAULT_360_API_URL = "https://api360.yandex.net"
LOG_FILE = "get_audit_logs.log"
FILTERED_MAIL_EVENTS = []
FILTERED_MAILBOXES = []
MAIL_LOG_MAX_PAGES = 20
OVERLAPPED_MINITS = 2
MAX_RETRIES = 3
RETRIES_DELAY_SEC = 2

EXIT_CODE = 1

logger = logging.getLogger("get_audit_log")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
#file_handler = handlers.TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30, encoding='utf-8')
file_handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024,  backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def main():
    settings = get_settings()
    if settings is None:
        logger.error("Settings are not set.")
        sys.exit(EXIT_CODE)

    logger.info("Starting script...")

    logger.debug("Collect existing files names in log catalog.")

    log_params = []
    d = {}
    d["dir"] = settings.mail_dir_path
    d["file"] = settings.mail_file
    d["label"] = "mail"
    log_params.append(d)
    d = {}
    d["dir"] = settings.disk_dir_path
    d["file"] = settings.disk_file
    d["label"] = "disk"
    log_params.append(d)

    for log_param in log_params:

        existing_records = []  
        files = [f for f in os.listdir(log_param['dir']) if re.match(log_param['file'] + r'_[0-9]{4}\-[0-9]{2}\-[0-9]{2}\.' + settings.ext, f)]

        if not files:
            logger.info(f"No files found in {log_param['dir']} catalog. Start full downloading data.")
        else:
            files.sort(reverse=True)
            for file in files:
                logger.debug(f"Check records in file {os.path.join(log_param['dir'], file)}.")
                with open(os.path.join(log_param['dir'], file), 'r', encoding="utf8") as f:
                    for line in f:
                        existing_records.append(line.replace('\n', ''))
                if not existing_records:
                    logger.debug(f"No records found in file {os.path.join(log_param['dir'], log_param['file'])}. Selecting previous file.")
                else:
                    break

        records = []
        if existing_records:
            last_record = existing_records[-1]
            date = json.loads(last_record)["date"]
            logger.info(f"Last record date for {log_param['label']} logs: {date}")
            logger.info(f"Start downloading data from {log_param['label']} audit logs.")
            if log_param["label"] == "mail":
                records = fetch_mail_audit_logs(settings, last_date = date)
            elif log_param["label"] == "disk":
                records = fetch_disk_audit_logs(settings, last_date = date)
        else:
            if log_param["label"] == "mail":
                records = fetch_mail_audit_logs(settings)
            elif log_param["label"] == "disk":
                records = fetch_disk_audit_logs(settings)

        if not records:
            logger.error(f"No records were recived from {log_param['label']} audit logs.")
            sys.exit(EXIT_CODE)
        else:
            logger.info(f"{len(records)} records were recived from {log_param['label']} audit logs.")

        decoded_records = [r.decode() for r in records]
        
        separated_list = {}
        for r in decoded_records:
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
            file_path = os.path.join(log_param['dir'], f"{log_param['file']}_{date}.{settings.ext}")
            if len(records) > 0:
                logger.info(f"Writing {len(records)} records to {log_param['label']} audit file {file_path}")
                with open(file_path, 'a', encoding="utf8") as f:
                    for r in sorted(records, key=lambda d: d['full_time']):
                        f.write(f"{r['data']}\n")
           

    logger.info("Sript finished.")

@dataclass
class SettingParams:
    oauth_token: str
    organization_id: int  
    mail_dir_path : str
    disk_dir_path : str
    ext: str
    mail_file: str
    disk_file: str

def get_settings():
    exit_flag = False
    try:
        settings = SettingParams (
            oauth_token = os.environ.get("OAUTH_TOKEN_ARG"),
            organization_id = int(os.environ.get("ORGANIZATION_ID_ARG")),
            mail_dir_path = os.environ.get("MAIL_LOG_CATALOG_LOCATION"),
            disk_dir_path = os.environ.get("DISK_LOG_CATALOG_LOCATION"),
            ext = os.environ.get("LOG_FILE_EXTENSION"),
            mail_file = os.environ.get("MAIL_LOG_FILE_BASE_NAME"),
            disk_file = os.environ.get("DISK_LOG_FILE_BASE_NAME")
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

    if not settings.mail_dir_path:
        logger.error("MAIL_LOG_CATALOG_LOCATION is not set")
        exit_flag = True
    else:
        if not os.path.isdir(settings.mail_dir_path):
            logger.error(f"Catalog {settings.mail_dir_path} is not exist.")
            exit_flag = True

    if not settings.disk_dir_path:
        logger.error("DISK_LOG_CATALOG_LOCATION is not set")
        exit_flag = True
    else:
        if not os.path.isdir(settings.disk_dir_path):
            logger.error(f"Catalog {settings.disk_dir_path} is not exist.")
            exit_flag = True

    if settings.mail_dir_path.endswith("/") or settings.mail_dir_path.endswith("\\"):
        settings.mail_dir_path = settings.mail_dir_path[:-1]

    if settings.disk_dir_path.endswith("/") or settings.disk_dir_path.endswith("\\"):
        settings.mail_dir_path = settings.mail_dir_path[:-1]

    if not settings.ext:
        logger.error("LOG_FILE_EXTENSION is not set")
        exit_flag = True

    if not settings.mail_file:
        logger.error("MAIL_LOG_FILE_BASE_NAME is not set")
        exit_flag = True

    if not settings.disk_file:
        logger.error("DISK_LOG_FILE_BASE_NAME is not set")
        exit_flag = True

    if exit_flag:
        return None
    
    return settings

def fetch_mail_audit_logs(settings: "SettingParams", last_date: str = ""):
  
    log_records = set()
    params = {}
    try:
        params["pageSize"] = 100
        if last_date:
            msg_date = datetime.datetime.strptime(last_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            shifted_date = msg_date + relativedelta(minutes=-OVERLAPPED_MINITS)
            params["afterDate"] = shifted_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/audit_log/mail"
        headers = {"Authorization": f"OAuth {settings.oauth_token}"}
        pages_count = 0
        retries = 0
        while True:           
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Error during GET request: {response.status_code}. Error message: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Forcing exit without getting data.")
                    return []
            else:
                retries = 1
                temp_list = response.json()["events"]
                logger.debug(f'Received {len(temp_list)} records, from {temp_list[-1]["date"]} to {temp_list[0]["date"]}')
                temp_json = [json.dumps(d, ensure_ascii=False).encode('utf8') for d in temp_list]
                log_records.update(temp_json)
                if response.json()["nextPageToken"] == "":
                    break
                else:
                    if pages_count < MAIL_LOG_MAX_PAGES:
                        pages_count += 1
                        params["pageToken"] = response.json()["nextPageToken"]
                    else:
                        if params.get('pageToken') : del params['pageToken']
                        params["beforeDate"] = temp_list[-10]["date"]
                        params["pageSize"] = 100
                        pages_count = 0

    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return []
        
    return list(log_records)[::-1]

def fetch_disk_audit_logs(settings: "SettingParams", last_date: str = ""):
  
    log_records = set()
    params = {}
    try:
        params["pageSize"] = 100
        if last_date:
            msg_date = datetime.datetime.strptime(last_date, "%Y-%m-%dT%H:%M:%SZ")
            shifted_date = msg_date + relativedelta(minutes=-OVERLAPPED_MINITS)
            params["afterDate"] = shifted_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/audit_log/disk"
        headers = {"Authorization": f"OAuth {settings.oauth_token}"}
        pages_count = 0
        retries = 0
        while True:           
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Error during GET request: {response.status_code}. Error message: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Forcing exit without getting data.")
                    return []
            else:
                retries = 1
                temp_list = response.json()["events"]
                logger.debug(f'Received {len(temp_list)} records, from {temp_list[-1]["date"]} to {temp_list[0]["date"]}')
                temp_json = [json.dumps(d, ensure_ascii=False).encode('utf8') for d in temp_list]
                log_records.update(temp_json)
                if response.json()["nextPageToken"] == "":
                    break
                else:
                    if pages_count < MAIL_LOG_MAX_PAGES:
                        pages_count += 1
                        params["pageToken"] = response.json()["nextPageToken"]
                    else:
                        if params.get('pageToken') : del params['pageToken']
                        params["beforeDate"] = temp_list[-10]["date"]
                        params["pageSize"] = 100
                        pages_count = 0

    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return []
        
    return list(log_records)[::-1]

if __name__ == "__main__":

    denv_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(denv_path):
        load_dotenv(dotenv_path=denv_path,verbose=True, override=True)

    try:
        main()
    except Exception as exp:
        logging.exception(exp)
        sys.exit(EXIT_CODE)