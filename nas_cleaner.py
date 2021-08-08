# -*- coding: utf8-*-
##############################
# v20200806 for python3.7
# NAS 파일 정리 툴; RD-66
##############################

import swiftclient
from swiftclient.service import SwiftService
from keystoneauth1 import session
from keystoneauth1.identity import v3
from swiftclient.exceptions import ClientException
import pprint
import logging
import logging.handlers
import subprocess
import os
import sys
import pytz
from pytz import timezone, utc
from datetime import datetime,timedelta
from imp import reload
import time
import mimetypes
import zipfile
import hashlib
import json
import shutil
import requests
reload(sys)

# DB 정보
db_host = '127.0.0.1' #'db-svmk.pub-cdb.ntruss.com' #'db-1ea6m.cdb.ntruss.com'
mysql_exe = None
try:
    mysql_exe = subprocess.check_output('which mysql', shell=True)
    mysql_exe = mysql_exe.strip() # 줄바꿈 (if any) 제거
finally:
    if mysql_exe is None:
        print('ERROR: mysql not found')
        sys.exit(1)

# 환경 변수에서 DB, 계정정보 가져오기
db_host_env = os.environ.get('ASSIGN_SYNC_DB_HOST')
if db_host_env is not None:
    db_host = db_host_env

db_user = os.environ.get('ASSIGN_SYNC_DB_USER')
dbu = None
dbp = None
if db_user is None:
    print('ERROR: ASSIGN_SYNC_DB_USER is None')
    sys.exit(1)
else:
    tmp_pos = db_user.find(':')
    if tmp_pos > 0:
        dbu = db_user[0:tmp_pos]
        dbp = db_user[tmp_pos + 1:]
if not dbu or not dbp:
    print('ERROR: ASSIGN_SYNC_DB_USER is invalid')
    sys.exit(1)

# 어제 날짜; 한국시간 기준
today = datetime.now(timezone('Asia/Seoul'))
yesterday = today - timedelta(days=1)
yesterday_ymd = yesterday.strftime('%Y%m%d')
yesterday_ymd_sql = yesterday.strftime('%Y-%m-%d')

time_now = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))

# 환경 변수에서 username, password 가져오기
username = os.environ.get('NAS_CLEAN_USER')
if username is  None:
    print('ERROR: NAS_CLEAN_USER is None')
    sys.exit(1)

password = os.environ.get('NAS_CLEAN_PASS')
if password is None:
    print('ERROR: NAS_CLEAN_PASS is None')
    sys.exit(1)

##############################
# cli 옵션 처리; project_id
##############################
if len(sys.argv) != 2:
    print('Usage: python {} {{month}}'.format(os.path.basename(__file__)))
    sys.exit(1)

month = sys.argv[1]

try:
    month = int(month)
except:
    print('Invalid task ID: {}'.format(month))
    sys.exit(1)

##############################
# dir , ps
##############################
current_dir = os.path.dirname(os.path.realpath(__file__))
current_file = os.path.basename(__file__)
current_file_name = current_file[:-3] #xxxx.py
current_pid = os.getpid()

##############################
# 시간 관련 처리
# timezone to local
##############################
local_tz = pytz.timezone('Asia/Seoul')
def localTime(*args):
        utc_dt = utc.localize(datetime.utcnow())
        converted = utc_dt.astimezone(local_tz)
        return converted.timetuple()

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

##############################
# init mkdirs
##############################
log_dir = '{}/logs'.format(current_dir)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

##############################
# logging
##############################  
# Variables from log

last_run_date = '' # Last process date
last_run_smonth = 0 # Last process standard month
emergency = False
is_first = False

# Get run dates in one file
LOG_FILENAME2_1 = '{}/log_access_{}_1'.format(log_dir, current_file_name)
LOG_FILENAME2_2 = '{}/log_access_{}_2'.format(log_dir, current_file_name)

def add_checkpoint(log_filename):
    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.handlers.TimedRotatingFileHandler(log_filename, when='midnight', interval=1, backupCount=14)
    file_handler.suffix = 'log-%Y%m%d'
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(process)d - [%(filename)s:%(lineno)d] %(message)s')
    formatter.converter = localTime
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

if not os.path.exists(LOG_FILENAME2_1) and not os.path.exists(LOG_FILENAME2_2):
    print('First run')
    is_first = True

    logger2_1 = add_checkpoint(LOG_FILENAME2_1)
    logger2_1.info('start')
    logger2_1.info('{} {}'.format(month, time.strftime('%Y-%m-%d', time.localtime(time.time()))))

    logger2_2 = add_checkpoint(LOG_FILENAME2_2)
    logger2_2.info('start')
    logger2_2.info('{} {}'.format(month, time.strftime('%Y-%m-%d', time.localtime(time.time()))))

else:
    if os.path.exists(LOG_FILENAME2_1) and not os.path.exists(LOG_FILENAME2_2):
        LOG_FILENAME2 = LOG_FILENAME2_2
        GET_LOG_FILENAME2 = LOG_FILENAME2_1
    elif not os.path.exists(LOG_FILENAME2_1) and os.path.exists(LOG_FILENAME2_2):
        LOG_FILENAME2 = LOG_FILENAME2_1
        GET_LOG_FILENAME2 = LOG_FILENAME2_1
    else:
        last_log_mod_1 = os.path.getmtime(LOG_FILENAME2_1)
        last_log_mod_2 = os.path.getmtime(LOG_FILENAME2_2)

        if last_log_mod_1 >= last_log_mod_2:
            LOG_FILENAME2 = LOG_FILENAME2_2
            GET_LOG_FILENAME2 = LOG_FILENAME2_1
        else:
            LOG_FILENAME2 = LOG_FILENAME2_1
            GET_LOG_FILENAME2 = LOG_FILENAME2_2


    last_run_data_error = subprocess.check_output('tail -1 {}'.format(GET_LOG_FILENAME2), shell=True).decode('utf-8').split()

    emergency = (str(last_run_data_error[-2] + ' ' + last_run_data_error[-1]) != 'End successful')

    if emergency:   

        print('This is a retry process - previous run had an error!')
        print('Running emergency error fix process')

        last_run_data = subprocess.check_output('head -1 {}'.format(GET_LOG_FILENAME2), shell=True).decode('utf-8').split()

        last_pid_info = subprocess.check_output('tail -1 {}'.format(GET_LOG_FILENAME2), shell=True).decode('utf-8').split()

        try:
            last_pid = int(last_pid_info[-1])
            last_folder = last_pid_info[-2]
        except:
            last_folder = ''
            print('Didn\'t even start the upload process')

        is_first = (str(last_run_data[-1]) == 'start')

        if not is_first:

            print('Not starting from DB')
            try:
                last_run_date = last_run_data[-1] # Last process date -> string

                last_run_smonth = int(last_run_data[-2]) # Last standard month -> int

                last_run_year = int(last_run_date[:4])
                last_run_month = int(last_run_date[5:7])
                last_run_day = int(last_run_date[8:])

                if os.path.exists(LOG_FILENAME2):
                    subprocess.call('rm {}'.format(LOG_FILENAME2), shell=True)

                logger2 = add_checkpoint(LOG_FILENAME2)
                
                logger2.info('{} {}'.format(last_run_smonth, last_run_date))
                logger2.info('{} {}'.format(month, time.strftime('%Y-%m-%d', time.localtime(time.time()))))

            except:
                print(last_run_data)
                print('Error occurred - ending process...')
                sys.exit(1)
        else:
            print('Starting from DB')

            if os.path.exists(LOG_FILENAME2):
                subprocess.call('rm {}'.format(LOG_FILENAME2), shell=True)

            logger2 = add_checkpoint(LOG_FILENAME2)
                
            logger2.info('start')
            logger2.info('{} {}'.format(month, time.strftime('%Y-%m-%d', time.localtime(time.time()))))


    else:
        print('Not an emergency')

        last_run_data = subprocess.check_output('head -2 {} | tail -1'.format(GET_LOG_FILENAME2), shell=True).decode('utf-8').split()

        try:
            last_run_date = last_run_data[-1] # Last process date -> string

            last_run_smonth = int(last_run_data[-2]) # Last standard month -> int

            last_run_year = int(last_run_date[:4])
            last_run_month = int(last_run_date[5:7])
            last_run_day = int(last_run_date[8:])

            if os.path.exists(LOG_FILENAME2):
                subprocess.call('rm {}'.format(LOG_FILENAME2), shell=True)

            logger2 = add_checkpoint(LOG_FILENAME2)

            logger2.info('{} {}'.format(last_run_smonth, last_run_date))
            logger2.info('{} {}'.format(month, time.strftime('%Y-%m-%d', time.localtime(time.time()))))

        except:
            print(last_run_data)
            print('Error occurred - ending process...')
            sys.exit(1)

#logging.basicConfig(level=logging.DEBUG)
LOG_FILENAME = '{}/log_info_{}'.format(log_dir, current_file_name)
logger = logging.getLogger(LOG_FILENAME)
logger.setLevel(logging.DEBUG)
file_handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when='midnight', interval=1, backupCount=14)
file_handler.suffix = 'log-%Y%m%d'
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(process)d - [%(filename)s:%(lineno)d] %(message)s')
formatter.converter = localTime
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

###############################
# 중복 실행 방지
###############################
result = subprocess.check_output('ps -ef | grep {} | wc -l'.format(os.path.basename(__file__)), shell=True)
if int(result.strip()) > 3:
    logger.info('There is a previous run; I \'m exiting')
    sys.exit(0)

###############################
# 실행 함수들
###############################
# DB 쿼리 실행 커맨드 구성
def get_db_cmd(qry):
    cmd = '{} -h {} -u{} -p"{}" cwaidata -N -e "{}"'
    cmd = cmd.format(mysql_exe.decode('utf-8'), db_host, dbu, dbp, qry.replace('"','\\"'))
    return cmd

# DB 쿼리 결과를 이중 배열로 구성해서 리턴
def results_to_array(result):
    rev = []
    for row in result.split(b'\n'):
        if row:
            rev.append(row.split(b'\t'))
    return rev

def get_dir_size(file_name):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(file_name):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += (os.path.getsize(fp) if os.path.isfile(fp) else 0)
    return total_size

def get_md5(file_name, hash_factory=hashlib.md5, chunk_num_blocks=128):
    h = hash_factory()
    with open(file_name,'rb') as f: 
        for chunk in iter(lambda: f.read(chunk_num_blocks*h.block_size), b''): 
            h.update(chunk)
    return h.hexdigest()

def send_to_slack(text):
    url = 'https://hooks.slack.com/services/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'

    payload = {"text": text}

    requests.post(url, json=payload)
############################################################################
logger.info('======================== START ==========================')

logger.info('{} {}'.format(month, time.strftime('%Y-%m-%d', time.localtime(time.time()))))

logger.info('Selecting target files for cleaning process ended at least {} months before standard time {}'.format(month, datetime.today()))

send_to_slack("Starting NAS-Clean operation - {}".format(datetime.now()))

###################################################
## 인증 및 스토리지 연결
###################################################

endpoint = 'https://kr.archive.ncloudstorage.com:5000/v3'
# username = 'oVe5zmArwOnezJdiCMl3'
# password = 'iLHZwMhqs2mvcZUJYYeePzPzgDn9DkQtXzRhsLVa'
domain_id = 'default'
project_id = '64f16e672bba47a0a2f871e2727fe3dc'
bucket_name = 'nas-storage'

try:
    auth = v3.Password(auth_url=endpoint, username=username, password=password, project_id=project_id, user_domain_id=domain_id)
    auth_session = session.Session(auth=auth)

    swift_connection = swiftclient.Connection(retries=5, session=auth_session)
except:
    print('Couldn\'t connect to NCP Archive Storage')
    sys.exit(1)

logger.info('Granted access to NCP Archive Storage successfully')

###################################################
## 옮기고싶은 파일 지정
###################################################

# Common variables
total_size = 0
time_now = datetime.today()
year_now = time_now.year
month_now = time_now.month
day_now = time_now.day

def check_files_upload():

    ##################
    # Upload
    ##################

    if is_first:

        if month_now > month:
            print('Getting data from DB start date to {}-{}-{}'.format(year_now, month_now - month, day_now))
            logger.info('Getting data from DB start date to {}-{}-{}'.format(year_now, month_now - month, day_now))
        else:

            print('Getting data from DB start date to {}-{}-{}'.format(int(year_now - month // 12), int(month_now - month % 12), day_now))
            logger.info('Getting data from DB start date to {}-{}-{}'.format(int(year_now - month // 12), int(month_now - month % 12), day_now))

        if emergency and last_folder == 'upload':
            print('Starting from project ID {}'.format(last_pid))
            logger.info('Starting from project ID {}'.format(last_pid))
            qry_upload = "SELECT project_id, mod_date FROM CW_PROJECT WHERE project_progress_cd = 'ENDED' AND project_id >= {} AND mod_date <= '{}-{}-{}' ORDER BY project_id ASC".format(last_pid, int(year_now - month // 12), int(month_now - month % 12), day_now)
        else:
            qry_upload = "SELECT project_id, mod_date FROM CW_PROJECT WHERE project_progress_cd = 'ENDED' AND mod_date <= '{}-{}-{}' ORDER BY project_id ASC".format(int(year_now - month // 12), int(month_now - month % 12), day_now)

    else:

        if last_run_month > last_run_smonth:
            new_sdate = '{}-{}-{}'.format(last_run_year, last_run_month - last_run_smonth, last_run_day)

        else:
            new_sdate = '{}-{}-{}'.format(int(last_run_year - last_run_smonth // 12), int(last_run_month - last_run_smonth % 12), last_run_day)
        
        if month_now > month:
            print('Getting data from date {} to {}-{}-{}'.format(new_sdate, year_now, month_now - month, day_now))
            logger.info('Getting data from date {} to {}-{}-{}'.format(new_sdate, year_now, month_now - month, day_now))
        else:
            print('Getting data from date {} to {}-{}-{}'.format(new_sdate, int(year_now - month // 12), int(month_now - month % 12), day_now))
            logger.info('Getting data from date {} to {}-{}-{}'.format(new_sdate, int(year_now - month // 12), int(month_now - month % 12), day_now))
        
        if emergency and last_folder == 'upload':
            print('Starting from project ID {}'.format(last_pid))
            logger.info('Starting from project ID {}'.format(last_pid))
            qry_upload = "SELECT project_id, mod_date FROM CW_PROJECT WHERE project_progress_cd = 'ENDED' AND mod_date > '{}' AND project_id >= {} AND mod_date <= '{}-{}-{}' ORDER BY project_id ASC".format(new_sdate, last_pid, int(year_now - month // 12), int(month_now - month % 12), day_now) 
        else:
            qry_upload = "SELECT project_id, mod_date FROM CW_PROJECT WHERE project_progress_cd = 'ENDED' AND mod_date > '{}' AND mod_date <= '{}-{}-{}' ORDER BY project_id ASC".format(new_sdate, int(year_now - month // 12), int(month_now - month % 12), day_now) 
        
    cmd_upload = get_db_cmd(qry_upload)
    logger.info(cmd_upload.replace(dbp, 'xxxxx'))
    result_upload = subprocess.check_output(cmd_upload, shell=True)

    if result_upload:
        result_list_upload = results_to_array(result_upload)
        logger.info('fetched: {}'.format(len(result_list_upload)))
    else :
        print('There are no results - Upload')
        result_list_upload = []
        logger.info('No results from query - Upload')

    # Upload file
    clean_list_upload = []
    total_size_upload = 0

    for pid, edate in result_list_upload:
        pid = pid.decode('utf-8')
        year_date = int(edate[:4])
        month_date = int(edate[5:7])
        day_date = int(edate[8:10])

        if os.path.exists('/homecw/cw/nas/files/upload/projectFile/{}'.format(pid)):

            size_list_upload = subprocess.check_output('du --max-depth=0 /homecw/cw/nas/files/upload/projectFile/{}'.format(pid), shell=True).decode('utf-8')
            size_list_upload = list(size_list_upload.split('\t'))
            size_upload = int(size_list_upload[0])

            if year_date < int(year_now - month // 12):
                clean_list_upload.append(pid)
                total_size_upload += size_upload
                print('Upload - project : {} size : {} date : ({}, {}, {})'.format(pid, size_upload, year_date, month_date, day_date))
                logger.info('Upload - project : {} size : {} date : ({}, {}, {})'.format(pid, size_upload, year_date, month_date, day_date))

            elif year_date == int(year_now - month // 12):
                if month_date < int(month_now - month % 12):
                    clean_list_upload.append(pid)
                    total_size_upload += size_upload
                    print('Upload - project : {} size : {} date : ({}, {}, {})'.format(pid, size_upload, year_date, month_date, day_date))
                    logger.info('Upload - project : {} size : {} date : ({}, {}, {})'.format(pid, size_upload, year_date, month_date, day_date))

                elif month_date == int(month_now - month % 12):
                    if day_date <= day_now:
                        clean_list_upload.append(pid)
                        total_size_upload += size_upload
                        print('Upload - project : {} size : {} date : ({}, {}, {})'.format(pid, size_upload, year_date, month_date, day_date))
                        logger.info('Upload - project : {} size : {} date : ({}, {}, {})'.format(pid, size_upload, year_date, month_date, day_date))
            else:
                print('{} does not match the date criteria : {}-{}-{}'.format(pid, year_date, month_date, day_date))
            
        else:
            print('{} does not exist in any of the directories'.format(pid))

    return clean_list_upload, total_size_upload

def check_files_result():

    ##################
    # Result
    ##################

    if is_first:

        if month_now > month:
            print('Getting data from DB start date to {}-{}-{}'.format(year_now, month_now - month, day_now))
            logger.info('Getting data from DB start date to {}-{}-{}'.format(year_now, month_now - month, day_now))
        else:
            print('Getting data from DB start date to {}-{}-{}'.format(int(year_now - month // 12), int(month_now - month % 12), day_now))
            logger.info('Getting data from DB start date to {}-{}-{}'.format(int(year_now - month // 12), int(month_now - month % 12), day_now))

        if emergency and last_folder == 'result':
            print('Starting from prj_idx {}'.format(last_pid))
            logger.info('Starting from prj_idx {}'.format(last_pid))
            qry_result = "SELECT prj_idx, CW_PROJECT.mod_date FROM TB_PRJ_MST INNER JOIN CW_PROJECT ON TB_PRJ_MST.project_id = CW_PROJECT.project_id WHERE CW_PROJECT.project_progress_cd = 'ENDED' AND prj_idx >= {} AND CW_PROJECT.mod_date <= '{}-{}-{}' ORDER BY prj_idx ASC".format(last_pid, int(year_now - month // 12), int(month_now - month % 12), day_now)
        else:
            qry_result = "SELECT prj_idx, CW_PROJECT.mod_date FROM TB_PRJ_MST INNER JOIN CW_PROJECT ON TB_PRJ_MST.project_id = CW_PROJECT.project_id WHERE CW_PROJECT.project_progress_cd = 'ENDED' AND CW_PROJECT.mod_date <= '{}-{}-{}' ORDER BY prj_idx ASC" .format(int(year_now - month // 12), int(month_now - month % 12), day_now)

    else:

        if last_run_month > last_run_smonth:
            new_sdate = '{}-{}-{}'.format(last_run_year, last_run_month - last_run_smonth, last_run_day)

        else:
            new_sdate = '{}-{}-{}'.format(int(last_run_year - last_run_smonth // 12), int(last_run_month - last_run_smonth % 12), last_run_day)

        if month_now > month:
            print('Getting data from date {} to {}-{}-{}'.format(new_sdate, year_now, month_now - month, day_now))
            logger.info('Getting data from date {} to {}-{}-{}'.format(new_sdate, year_now, month_now - month, day_now))
        else:
            print('Getting data from date {} to {}-{}-{}'.format(new_sdate, int(year_now - month // 12), int(month_now - month % 12), day_now))
            logger.info('Getting data from date {} to {}-{}-{}'.format(new_sdate, int(year_now - month // 12), int(month_now - month % 12), day_now))

        if emergency and last_folder == 'result':
            print('Starting from prj_idx {}'.format(last_pid))
            logger.info('Starting from prj_idx {}'.format(last_pid))
            qry_result = "SELECT prj_idx, CW_PROJECT.mod_date FROM TB_PRJ_MST INNER JOIN CW_PROJECT ON TB_PRJ_MST.project_id = CW_PROJECT.project_id WHERE CW_PROJECT.project_progress_cd = 'ENDED' AND CW_PROJECT.mod_date > '{}' AND prj_idx >= {} AND CW_PROJECT.mod_date <= '{}-{}-{}' ORDER BY prj_idx ASC".format(new_sdate, last_pid, int(year_now - month // 12), int(month_now - month % 12), day_now)
        else:
            qry_result = "SELECT prj_idx, CW_PROJECT.mod_date FROM TB_PRJ_MST INNER JOIN CW_PROJECT ON TB_PRJ_MST.project_id = CW_PROJECT.project_id WHERE CW_PROJECT.project_progress_cd = 'ENDED' AND CW_PROJECT.mod_date > '{}' AND CW_PROJECT.mod_date <= '{}-{}-{}' ORDER BY prj_idx ASC".format(new_sdate, int(year_now - month // 12), int(month_now - month % 12), day_now)
    
    cmd_result = get_db_cmd(qry_result)
    logger.info(cmd_result.replace(dbp, 'xxxxx'))
    result_result = subprocess.check_output(cmd_result, shell=True)

    if result_result:
        result_list_result = results_to_array(result_result)
        logger.info('fetched: {}'.format(len(result_list_result)))
    else :
        print('There are no results - Result')
        logger.info('No results from query - Result')
        sys.exit(1)

    # Result file
    clean_list_result = []
    total_size_result = 0

    for tid, edate in result_list_result:
        tid = tid.decode('utf-8')
        year_date = int(edate[:4])
        month_date = int(edate[5:7])
        day_date = int(edate[8:10])

        if os.path.exists('/homecw/cw/nas/files/result/{}'.format(tid)):

            size_list_result = subprocess.check_output('du --max-depth=0 /homecw/cw/nas/files/result/{}'.format(tid), shell=True).decode('utf-8')
            size_list_result = list(size_list_result.split('\t'))
            size_result = int(size_list_result[0])

            if year_date < int(year_now - month // 12):
                clean_list_result.append(tid)
                total_size_result += size_result
                print('Result - task : {} size : {} date : ({}, {}, {})'.format(tid, size_result, year_date, month_date, day_date))
                logger.info('Result - task : {} size : {} date : ({}, {}, {})'.format(tid, size_result, year_date, month_date, day_date))

            elif year_date == int(year_now - month // 12):
                if month_date < int(month_now - month % 12):
                    clean_list_result.append(tid)
                    total_size_result += size_result
                    print('Result - task : {} size : {} date : ({}, {}, {})'.format(tid, size_result, year_date, month_date, day_date))
                    logger.info('Result - task : {} size : {} date : ({}, {}, {})'.format(tid, size_result, year_date, month_date, day_date))

                elif month_date == int(month_now - month % 12):
                    if day_date <= day_now:
                        clean_list_result.append(tid)
                        total_size_result += size_result
                        print('Result - task : {} size : {} date : ({}, {}, {})'.format(tid, size_result, year_date, month_date, day_date))
                        logger.info('Result - task : {} size : {} date : ({}, {}, {})'.format(tid, size_result, year_date, month_date, day_date))
            else:
                print('{} does not match the date criteria : {}-{}-{}'.format(tid, year_date, month_date, day_date))
        
        else:
            print('{} does not exist in any of the directories'.format(tid))

    return clean_list_result, total_size_result

###################################################
## 오브젝트 업로드
###################################################

def obj_upload_upload(clean_list_upload, clean_list_result, run_type):

    ############################
    # Upload
    ############################
    
    total_dtime = 0

    # Super Directory - Upload
    if run_type != 'd':
        try:
            swift_connection.put_object(bucket_name, 'Upload', contents='', content_type='application/directory')
        except:
            print('Couldn\'t make directory')
            logger.info('Couldn\'t make directory')
            sys.exit(1)

        try:
            swift_connection.head_object(bucket_name, 'Upload')
        except ClientException as e:
            if e.http_status == '404':
                print('The object was not found')
            else:
                print('An error occurred checking for the existence of the object')
                
            print('Creation was not successful - something wrong : {}'.format('Upload'))
            logger.info('Creation was not successful - something wrong : {}'.format('Upload'))
            sys.exit(1)
    
    print('Created directory {} successfully'.format('Upload'))
    logger.info('Created directory {} successfully'.format('Upload'))

    # Directory - projectFile
    if run_type != 'd':
        try:
            swift_connection.put_object('{}/{}'.format(bucket_name, 'Upload'), 'projectFile', contents='', content_type='application/directory')
        except:
            print('Couldn\'t make directory')
            logger.info('Couldn\'t make directory')
            sys.exit(1)

        try:
            swift_connection.head_object('{}/{}'.format(bucket_name, 'Upload'), 'projectFile')
        except ClientException as e:
            if e.http_status == '404':
                print('The object was not found')
            else:
                print('An error occurred checking for the existence of the object')
            
            print('Creation was not successful - something wrong : {}'.format('projectFile'))
            logger.info('Creation was not successful - something wrong : {}'.format('projectFile'))
            sys.exit(1)
    
    print('Created directory {} successfully'.format('projectFile'))
    logger.info('Created directory {} successfully'.format('projectFile'))
        
    total_file_num = 0

    for c in clean_list_upload:
        total_project_size = 0
        prj_file_num = 0


        if not is_first or emergency:
            logger2.info('{} {}'.format('upload', c))
        elif is_first and not emergency:
            logger2_1.info('{} {}'.format('upload', c))
            logger2_2.info('{} {}'.format('upload', c))

        file_name = '/homecw/cw/nas/files/upload/projectFile/{}'.format(c)

        stime = time.perf_counter()

        # Directory
        if run_type != 'd':
            try:
                swift_connection.put_object('{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile'), c, contents='', content_type='application/directory')
            except:
                print('Couldn\'t make directory {}'.format(c))
                logger.info('Couldn\'t make directory {}'.format(c))
                sys.exit(1)

            try:
                swift_connection.head_object('{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile'), c)

            except ClientException as e:
                if e.http_status == '404':
                    print('The object was not found')
                else:
                    print('An error occurred checking for the existence of the object')
                    
                print('Creation was not successful - something wrong : {}'.format(c))
                logger.info('Creation was not successful - something wrong : {}'.format(c))
                sys.exit(1)
            
        print('Created directory {} successfully'.format(c))
        logger.info('Created directory {} successfully'.format(c))


        # Actual file
        for folder, subfolders, files in os.walk(file_name):

            c_index = folder.split('/').index(c)

            cur_folder_path = ''

            for p in folder.split('/')[c_index + 1:]:
                cur_folder_path += p + '/'

            # Inner directory
            if run_type != 'd':
                try:
                    swift_connection.put_object('{}/{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile', c), cur_folder_path, contents='', content_type='application/directory')
                except:
                    print('Couldn\'t make directory : {} pid : {}'.format(folder, c))
                    logger.info('Couldn\'t make directory : {} pid : {}'.format(folder, c))
                    sys.exit(1)

                try:
                    swift_connection.head_object('{}/{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile', c), cur_folder_path)

                except ClientException as e:
                    if e.http_status == '404':
                        print('The object was not found')
                    else:
                        print('An error occurred checking for the existence of the object')
                        
                    print('Creation was not successful - something wrong : {} pid : {}'.format(folder, c))
                    logger.info('Creation was not successful - something wrong : {} pid : {}'.format(folder, c))
                    sys.exit(1)
                
            if cur_folder_path != '':
                print('Created directory {} successfully'.format(cur_folder_path[:-1]))
                logger.info('Created directory {} successfully'.format(cur_folder_path[:-1]))

            for fname in files:
                file_path = folder + os.sep + fname
                prj_file_num += 1
                size_file = os.path.getsize(file_path)
                # Existance check
                exist = False
                try:
                    hash_file = get_md5(file_path)

                    obj = swift_connection.head_object(bucket_name, '{}/{}/{}/{}/{}'.format('Upload', 'projectFile', c, cur_folder_path, fname))

                    if int(obj['content-length']) == size_file and obj['etag'] == hash_file: 
                        print('There already exists the file {} in the bucket'.format(fname))
                        total_project_size += size_file
                        logger.info('There already exists the file {} in the bucket'.format(fname))
                        exist = True

                except:
                    exist = False
                
                if not exist:
                    print('uploading {}'.format(fname))
                    if run_type != 'd':
                        if size_file > 5000000000:
                            print('{} is a large file -> segmentation needed'.format(fname))
                            segment_object_prefix = 'segments-{}/segment-'.format(fname)

                            segment_size = 10 * 1024 * 1024  # 10MB
                            manifest = []

                            with open(file_path, 'rb') as f:
                                segment_number = 1
                                while True:
                                    data = f.read(segment_size)
                                    if not len(data):
                                        break

                                    segment_object_name = '%s%d' % (segment_object_prefix, segment_number)

                                    try:
                                        etag = swift_connection.put_object('{}/{}/{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile', c, cur_folder_path), segment_object_name, contents=data)
                                    except ClientException as e:
                                        print("Error: %s" % e)
                                        if e.http_status == '404':
                                            print('The PUT request failed')
                                        else:
                                            print('An error occurred uploading the object')
                                
                                        print('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                        logger.info('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                        logger.info("Error: %s" % e)
                                        sys.exit(1)

                                    manifest.append({
                                        "path": '%s/%s' % ('{}/{}/{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile', c, cur_folder_path), segment_object_name),
                                        "etag": etag
                                    })
                                    print('Segmentation number {}'.format(segment_number))
                                    segment_number += 1

                            body = json.dumps(manifest)
                            
                            try:
                                swift_connection.put_object('{}/{}/{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile', c, cur_folder_path), fname, contents=body, query_string='multipart-manifest=put')
                            except ClientException as e:
                                print("Error: %s" % e)
                                if e.http_status == '404':
                                    print('The object was not uploaded')
                                else:
                                    print('An error occurred uploading the object')

                                print('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                logger.info('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                sys.exit(1)

                            try:
                                obj = swift_connection.head_object('{}/{}/{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile', c, cur_folder_path), fname)
                            except ClientException as e:
                                print("Error: %s" % e)
                                if e.http_status == '404':
                                    print('The object was not found')
                                else:
                                    print('An error occurred checking for the existence of the object')

                                print('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info("Error: %s" % e)
                                sys.exit(1)

                            if int(obj['content-length']) != size_file: 
                                print('The file sizes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {} -> Difference : {}'.format(int(obj['content-length']), size_file, int(obj['content-length']) / 1000 - size_file))
                                logger.info('The file sizes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)
                            
                            print('Sizes match! - successful : {}'.format(file_path))
                            logger.info('Sizes match! - successful : {}'.format(file_path))

                            hash_check_echo = ''

                            with open(file_path,'rb') as f: 
                                while True:
                                    data = f.read(segment_size)
                                    
                                    if not len(data):
                                        break

                                    h = hashlib.md5(data)

                                    hash_check_echo += h.hexdigest()

                            full_hash = subprocess.check_output('echo -n \'{}\' | md5sum'.format(hash_check_echo), shell=True).decode('utf-8').split()[0]

                            if str(full_hash) != obj['etag'][1:-1]:
                                print('The file hashes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {}'.format(obj['etag'], full_hash))
                                logger.info('The file hashes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)

                            print('Hashes match! - successful : {}'.format(file_path))
                            logger.info('Hashes match! - successful : {}'.format(file_path))


                        else:
                            hash_file = get_md5(file_path)

                            with open(file_path, 'rb') as f:
                                try:
                                    swift_connection.put_object('{}/{}/{}/{}/{}'.format(bucket_name, 'Upload', 'projectFile', c, cur_folder_path), fname, contents=f)
                                except ClientException as e:
                                    print("Error: %s" % e)
                                    if e.http_status == '404':
                                        print('The PUT request failed')
                                    else:
                                        print('An error occurred checking for the existence of the object')

                                    print('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                    logger.info('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                    logger.info("Error: %s" % e)
                                    sys.exit(1)

                            # Upload check
                            try:
                                obj = swift_connection.head_object(bucket_name, '{}/{}/{}/{}/{}'.format('Upload', 'projectFile', c, cur_folder_path, fname))
                                
                            except ClientException as e:
                                print("Error: %s" % e)
                                if e.http_status == '404':
                                    print('The object was not found')
                                else:
                                    print('An error occurred checking for the existence of the object')
                                    
                                print('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info("Error: %s" % e)
                                sys.exit(1)

                            if int(obj['content-length']) != size_file: 
                                print('The file sizes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {} -> Difference : {}'.format(int(obj['content-length']), size_file, int(obj['content-length']) / 1000 - size_file))
                                logger.info('The file sizes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)
                            
                            print('Sizes match! - successful : {}'.format(file_path))
                            logger.info('Sizes match! - successful : {}'.format(file_path))
                            
                            if obj['etag'] != hash_file:
                                print('The file hashes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {}'.format(obj['etag'], hash_file))
                                logger.info('The file hashes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)

                            print('Hashes match! - successful : {}'.format(file_path))
                            logger.info('Hashes match! - successful : {}'.format(file_path))

                    total_project_size += size_file

        etime = time.perf_counter()

        total_file_num += prj_file_num

        size_prj = get_dir_size(file_name)

        if total_project_size == size_prj or run_type == 'd':

            print('The total size of the folder {} was {}'.format(c, total_project_size))

            logger.info('The total size of the folder {} was {}'.format(c, total_project_size))

            print('There were {} files in the project {}'.format(prj_file_num, c))

            logger.info('There were {} files in the project {}'.format(prj_file_num, c))

            print('Uploaded file {} into bucket {} successfully'.format(file_name, bucket_name))

            logger.info('Uploaded file {} into bucket {} successfully'.format(file_name, bucket_name))

            dtime = etime - stime

            total_dtime += dtime

            print('Took {} for {}'.format(dtime, c))
            logger.info('Took {} for {}'.format(dtime, c))

            if run_type != 'd':

                try:
                    shutil.rmtree('/homecw/cw/nas/files/upload/projectFile/{}'.format(c))
                    print('Deleted file {}'.format(c))
                except:
                    print('Coudln\'t remove the file {}'.format(c))
                    logger.info('Coudln\'t remove the file {}'.format(c))
                    sys.exit(1)

                logger.info('Removed file {} successfully in upload file'.format(c))

        
        else:

            print('Total size in storage does not match total size of file in server - ERROR : Bucket {} NAS {}'.format(total_project_size, size_prj))
            logger.info('Total size in storage does not match total size of file in server - ERROR : Bucket {} NAS {}'.format(total_project_size, size_prj))
            sys.exit(1)

    return total_dtime, total_file_num

def obj_upload_result(clean_list_upload, clean_list_result, run_type):

    ############################
    # Result
    ############################

    total_dtime = 0

    # Super Directory - Result
    if run_type != 'd':
        try:
            swift_connection.put_object(bucket_name, 'Result', contents='', content_type='application/directory')
        except:
            print('Couldn\'t make directory')
            logger.info('Couldn\'t make directory')
            sys.exit(1)

        try:
            swift_connection.head_object(bucket_name, 'Result')

        except ClientException as e:
            if e.http_status == '404':
                print('The object was not found')
            else:
                print('An error occurred checking for the existence of the object')
                
            print('Creation was not successful - something wrong : {}'.format('Result'))
            logger.info('Creation was not successful - something wrong : {}'.format('Result'))
            sys.exit(1)
        
    print('Created directory {} successfully'.format('Result'))
    logger.info('Created directory {} successfully'.format('Result'))

    total_file_num = 0

    for c in clean_list_result:
        total_project_size = 0
        prj_file_num = 0

        if not is_first or emergency:
            logger2.info('{} {}'.format('result', c))
        elif is_first and not emergency:
            logger2_1.info('{} {}'.format('result', c))
            logger2_2.info('{} {}'.format('result', c))

        file_name = '/homecw/cw/nas/files/result/{}'.format(c)

        stime = time.perf_counter()

        # Directory
        if run_type != 'd':
            try:
                swift_connection.put_object('{}/{}'.format(bucket_name, 'Result'), c, contents='', content_type='application/directory')
            except:
                print('Couldn\'t make directory {}'.format(c))
                logger.info('Couldn\'t make directory {}'.format(c))
                sys.exit(1)

            try:
                swift_connection.head_object('{}/{}'.format(bucket_name, 'Result'), c)

            except ClientException as e:
                if e.http_status == '404':
                    print('The object was not found')
                else:
                    print('An error occurred checking for the existence of the object')
                    
                print('Upload was not successful - something wrong : {}'.format(c))
                logger.info('Upload was not successful - something wrong : {}'.format(c))
                sys.exit(1)

        print('Created directory {} successfully'.format(c))
        logger.info('Created directory {} successfully'.format(c))

        # Actual file
        for folder, subfolders, files in os.walk(file_name):

            c_index = folder.split('/').index(c)

            cur_folder_path = ''

            for p in folder.split('/')[c_index + 1:]:
                cur_folder_path += p + '/'

            # Inner directory
            if run_type != 'd':
                try:
                    swift_connection.put_object('{}/{}/{}'.format(bucket_name, 'Result', c), cur_folder_path, contents='', content_type='application/directory')
                except:
                    print('Couldn\'t make directory : {} pid : {}'.format(folder, c))
                    logger.info('Couldn\'t make directory : {} pid : {}'.format(folder, c))
                    sys.exit(1)

                try:
                    swift_connection.head_object('{}/{}/{}'.format(bucket_name, 'Result', c), cur_folder_path)

                except ClientException as e:
                    if e.http_status == '404':
                        print('The object was not found')
                    else:
                        print('An error occurred checking for the existence of the object')
                        
                    print('Creation was not successful - something wrong : {} pid : {}'.format(folder, c))
                    logger.info('Creation was not successful - something wrong : {} pid : {}'.format(folder, c))
                    sys.exit(1)
            
            if cur_folder_path != '':
                print('Created directory {} successfully'.format(cur_folder_path[:-1]))
                logger.info('Created directory {} successfully'.format(cur_folder_path[:-1]))

            for fname in files:
                file_path = folder + os.sep + fname
                prj_file_num += 1

                size_file = os.path.getsize(file_path)

                # Existance check
                exist = False
                try:
                    hash_file = get_md5(file_path)

                    obj = swift_connection.head_object(bucket_name, '{}/{}/{}/{}'.format('Result', c, cur_folder_path, fname))

                    if int(obj['content-length']) == size_file and obj['etag'] == hash_file: 
                        print('There already exists the file {} in the bucket'.format(fname))
                        total_project_size += size_file
                        logger.info('There already exists the file {} in the bucket'.format(fname))
                        exist = True

                except:
                    exist = False

                if not exist:
                    print('uploading {}'.format(fname))
                    if run_type != 'd':
                        if size_file > 5000000000:
                            print('{} is a large file -> segmentation needed'.format(fname))
                            segment_object_prefix = 'segments-{}/segment-'.format(fname)

                            segment_size = 50 * 1024 * 1024  # 50MB
                            manifest = []

                            with open(file_path, 'rb') as f:
                                segment_number = 1
                                while True:
                                    data = f.read(segment_size)
                                    if not len(data):
                                        break
                                    
                                    segment_object_name = '%s%d' % (segment_object_prefix, segment_number)

                                    try:
                                        etag = swift_connection.put_object('{}/{}/{}/{}'.format(bucket_name, 'Result', c, cur_folder_path), segment_object_name, contents=data)
                                    except ClientException as e:
                                        print("Error: %s" % e)
                                        if e.http_status == '404':
                                            print('The PUT request failed')
                                        else:
                                            print('An error occurred uploading the object')
                                
                                        print('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                        logger.info('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                        sys.exit(1)

                                    print('%s/%s' % ('{}/{}/{}/{}'.format(bucket_name, 'Result', c, cur_folder_path), segment_object_name))
                                    manifest.append({
                                        "path": '%s/%s' % ('{}/{}/{}/{}'.format(bucket_name, 'Result', c, cur_folder_path), segment_object_name),
                                        "etag": etag
                                    })
                                    print('Segmentation number {}'.format(segment_number))
                                    segment_number += 1

                            body = json.dumps(manifest)
                            
                            try:
                                swift_connection.put_object('{}/{}/{}/{}'.format(bucket_name, 'Result', c, cur_folder_path), fname, contents=body, query_string='multipart-manifest=put')
                            except ClientException as e:
                                print("Error: %s" % e)
                                if e.http_status == '404':
                                    print('The PUT request failed')
                                else:
                                    print('An error occurred uploading the object')
                        
                                print('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                logger.info('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                logger.info("Error: %s" % e)
                                sys.exit(1)

                            try:
                                obj = swift_connection.head_object('{}/{}/{}/{}'.format(bucket_name, 'Result', c, cur_folder_path), fname)
                            except ClientException as e:
                                print("Error: %s" % e)
                                if e.http_status == '404':
                                    print('The object was not found')
                                else:
                                    print('An error occurred checking for the existence of the object')

                                print('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info("Error: %s" % e)
                                sys.exit(1)

                            if int(obj['content-length']) != size_file: 
                                print('The file sizes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {} -> Difference : {}'.format(int(obj['content-length']), size_file, int(obj['content-length']) / 1000 - size_file))
                                logger.info('The file sizes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)
                            
                            print('Sizes match! - successful : {}'.format(file_path))
                            logger.info('Sizes match! - successful : {}'.format(file_path))
                                    
                            hash_check_echo = ''

                            with open(file_path, 'rb') as f: 
                                while True:
                                    data = f.read(segment_size)
                                    
                                    if not len(data):
                                        break

                                    h = hashlib.md5(data)

                                    hash_check_echo += h.hexdigest()


                            full_hash = subprocess.check_output('echo -n \'{}\' | md5sum'.format(hash_check_echo), shell=True).decode('utf-8').split()[0]

                            if str(full_hash) != obj['etag'][1:-1]:
                                print('The file hashes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {}'.format(obj['etag'], full_hash))
                                logger.info('The file hashes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)

                            print('Hashes match! - successful : {}'.format(file_path))
                            logger.info('Hashes match! - successful : {}'.format(file_path))

                        else:
                            hash_file = get_md5(file_path)

                            with open(file_path, 'rb') as f:
                                try:
                                    swift_connection.put_object('{}/{}/{}/{}'.format(bucket_name, 'Result', c, cur_folder_path), fname, contents=f)
                        
                                except ClientException as e:
                                    print("Error: %s" % e)
                                    if e.http_status == '404':
                                        print('The PUT request failed')
                                    else:
                                        print('An error occurred uploading the object')
                                    
                                    print('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                    logger.info('Couldn\'t open or upload the file : {} pid : {}'.format(fname, c))
                                    logger.info("Error: %s" % e)
                                    sys.exit(1)

                            # Upload check
                            try:
                                obj = swift_connection.head_object(bucket_name, '{}/{}/{}/{}'.format('Result', c, cur_folder_path, fname))
                                
                            except ClientException as e:
                                print("Error: %s" % e)
                                if e.http_status == '404':
                                    print('The object was not found')
                                else:
                                    print('An error occurred checking for the existence of the object')

                                print('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info('Upload was not successful - something wrong : {} pid : {}'.format(fname, c))
                                logger.info("Error: %s" % e)
                                sys.exit(1)

                            if int(obj['content-length']) != size_file: 
                                print('The file sizes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {} -> Difference : {}'.format(int(obj['content-length']), size_file, int(obj['content-length']) / 1000 - size_file))
                                logger.info('The file sizes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)
                            
                            print('Sizes match! - successful : {}'.format(file_path))
                            logger.info('Sizes match! - successful : {}'.format(file_path))
                            
                            if obj['etag'] != hash_file:
                                print('The file hashes don\'t match - something wrong :{}'.format(file_path))
                                print('Storage : {} File : {}'.format(obj['etag'], hash_file))
                                logger.info('The file hashes don\'t match - something wrong : {} pid : {}'.format(file_path, c))
                                sys.exit(1)

                            print('Hashes match! - successful : {}'.format(file_path))
                            logger.info('Hashes match! - successful : {}'.format(file_path))

                    total_project_size += size_file

        etime = time.perf_counter()

        total_file_num += prj_file_num

        size_prj = get_dir_size(file_name)

        if total_project_size == size_prj or run_type == 'd':
            
            print('The total size of the folder {} was {}'.format(c, total_project_size))

            logger.info('The total size of the folder {} was {}'.format(c, total_project_size))

            print('There were {} files in the task {}'.format(prj_file_num, c))

            logger.info('There were {} files in the task {}'.format(prj_file_num, c))

            print('Uploaded file {} into bucket {} successfully'.format(file_name, bucket_name))

            logger.info('Uploaded file {} into bucket {} successfully'.format(file_name, bucket_name))

            dtime = etime - stime

            total_dtime += dtime

            print('Took {} for {} upload'.format(dtime, c))
            logger.info('Took {} for {} upload'.format(dtime, c))

            if run_type != 'd':
            
                try:
                    shutil.rmtree('/homecw/cw/nas/files/result/{}'.format(c))
                    print('Deleted file {}'.format(c))
                except:
                    print('Coudln\'t remove the file {}'.format(c))
                    logger.info('Coudln\'t remove the file {}'.format(c))
                    sys.exit(1)

                logger.info('Removed file {} successfully in upload file'.format(c))
        
        else:

            print('Total size in storage does not match total size of file in server - ERROR : Bucket {} NAS {}'.format(total_project_size, size_prj))
            logger.info('Total size in storage does not match total size of file in server - ERROR : Bucket {} NAS {}'.format(total_project_size, size_prj))
            sys.exit(1)

    return total_dtime, total_file_num

def obj_upload(clean_list_upload, clean_list_result, run_type):
        
    ###### Output #######
    total_dtime_upload, total_file_num_upload = obj_upload_upload(clean_list_upload, clean_list_result, run_type)

    total_dtime_result, total_file_num_result = obj_upload_result(clean_list_upload, clean_list_result, run_type)

    total_dtime = total_dtime_upload + total_dtime_result
    print('Took {} in total for upload folder'.format(total_dtime_upload))
    print('Took {} in total for result folder'.format(total_dtime_result))
    print('Took {} in total, {} in average'.format(total_dtime, total_dtime / (len(clean_list_upload) + len(clean_list_result))))

    logger.info('Took {} in total, {} in average'.format(total_dtime, total_dtime / (len(clean_list_upload) + len(clean_list_result)))) 

    print('Upload folder has {} files in total'.format(total_file_num_upload))
    print('Result folder has {} files in total'.format(total_file_num_result))
    print('There are total {} files uploaded'.format(total_file_num_upload + total_file_num_result))

    logger.info('There are total {} files uploaded'.format(total_file_num_upload + total_file_num_result))

    send_to_slack('There were total {} files uploaded - {}'.format(total_file_num_upload + total_file_num_result, datetime.now()))

###################################################
## 업로드 후 해쉬/파일 사이즈 체크
###################################################

def check_hash_size(clean_list_upload, clean_list_result):

    # Upload
    container = swift_connection.get_container(bucket_name)

    for obj in container[1]:
        
        if obj['content_type'] != 'application/directory':
            print(obj['name'])
            obj_sdir = obj['name'].split('/')[0]
            obj_dir = obj['name'].split('/')[1]
            obj_name = obj['name'].split('/')[-1]

            if obj_sdir == 'Upload':

                file_name = '/homecw/cw/nas/files/upload/projectFile/{}'.format(obj_dir)

            elif obj_sdir == 'Result':

                file_name = '/homecw/cw/nas/files/result/{}'.format(obj_dir)
            
            for folder, subfolders, files in os.walk(file_name):
                for fname in files:
                    if fname == obj_name:
                        file_path = folder + os.sep + fname

            size_file = os.path.getsize(file_path)

            hash_file = get_md5(file_path)


            if int(obj['bytes']) != size_file: 
                print('The file sizes don\'t match - something wrong :{}'.format(file_path))
                print('Storage : {} File : {}'.format(int(obj['bytes']), size_file))
                logger.info('The file sizes don\'t match - something wrong :{}'.format(file_path))
                # sys.exit(1)
            else:
                print('Sizes match! - successful : {}'.format(file_path))
                logger.info('Sizes match! - successful : {}'.format(file_path))
            
            if obj['hash'] != hash_file:
                print('The file hashes don\'t match - something wrong :{}'.format(file_path))
                print('Storage : {} File : {}'.format(obj['hash'], hash_file))
                logger.info('The file hashes don\'t match - something wrong :{}'.format(file_path))
                # sys.exit(1)

            else:
                print('Hashes match! - successful : {}'.format(file_path))
                logger.info('Hashes match! - successful : {}'.format(file_path))

###################################################
## 업로드 후 파일 삭제
###################################################

def rm_after_upload(clean_list_upload, clean_list_result):
    del_cmd = input("Do you want to erase the files? [Y/N] : ").strip().lower()

    if del_cmd == 'y':

        ######## Upload #########

        for c in clean_list_upload:
            try:
                shutil.rmtree('/homecw/cw/nas/files/upload/projectFile/{}'.format(c))
                print('Deleted file {}'.format(c))
            except:
                print('Coudln\'t remove the file {}'.format(c))
                logger.info('Coudln\'t remove the file {}'.format(c))
                sys.exit(1)

            logger.info('Removed file {} successfully in upload file'.format(c))

        print('Successfully removed all target files in Upload')

        ######## Result #########

        for c in clean_list_result:
            try:
                shutil.rmtree('/homecw/cw/nas/files/result/{}'.format(c))
                print('Deleted file {}'.format(c))
            except:
                print('Coudln\'t remove the file')
                logger.info('Coudln\'t remove the file {}'.format(c))
                sys.exit(1)

            logger.info('Removed file {} successfully in result file'.format(c))

        print('Successfully removed all target files in Result')

    elif del_cmd == 'n':
        logger.info('Chose not to remove the files')
        print('Chose not to remove the files')

###################################################
## 오브젝트 목록 조회
###################################################

def obj_list_check():
    list_cmd = input('Do you want to get the list of files in your bucket [{}]? [Y/N] : '.format(bucket_name)).strip().lower()

    if list_cmd == 'y':
        container = swift_connection.get_container(bucket_name)

        for obj in container[1]:
            pprint.pprint(obj)

    elif list_cmd == 'n':
        logger.info('Not listing the files - Bye!')
        print('Not listing the files - Bye!')

###################################################
## user command section
###################################################

clean_list_upload = []
total_size_upload = 0

clean_list_result = []
total_size_result = 0

print('Last process run date was : {}'.format(last_run_date))
logger.info('Last process run date was : {}'.format(last_run_date))

print('Uploading to bucket {}'.format(bucket_name))
logger.info('Uploading to bucket {}'.format(bucket_name))

# Check Process
clean_list_upload, total_size_upload = check_files_upload()

clean_list_result, total_size_result = check_files_result()

# Printing and logging
total_size = total_size_result + total_size_upload
print('\nThe projects that should be cleaned in the upload file are : {}, \nThere are total {} files to clean in upload :)'.format(clean_list_upload, len(clean_list_upload)))
print('\nThe projects that should be cleaned in the result file are : {}, \nThere are total {} files to clean in result :)'.format(clean_list_result, len(clean_list_result)))
print('The total size of all the files in upload file are {} kb'.format(total_size_upload))
print('The total size of all the files in result file are {} kb'.format(total_size_result))
print('The total size of all files are {} kb -> {} gb -> {} tb'.format(total_size, total_size / 1000000, total_size / 1000000000))
logger.info('The projects that should be cleaned in the upload file are : {}, \nThere are total {} files to clean in upload :)'.format(clean_list_upload, len(clean_list_upload)))
logger.info('The projects that should be cleaned in the result file are : {}, \nThere are total {} files to clean in result :)'.format(clean_list_result, len(clean_list_result)))
logger.info('The total size of all files are {} kb -> {} gb -> {} tb'.format(total_size, total_size / 1000000, total_size / 1000000000))
logger.info('Found wanted projects successfully')
send_to_slack('The total size of all files are {} kb -> {} gb -> {} tb - {}'.format(total_size, total_size / 1000000, total_size / 1000000000, datetime.now()))

done = False

while not done:

    user_cmd = input('Do you want to upload selected files? [Y/N/D] : ').strip().lower()
    
    if user_cmd == 'y':

        # Upload Process
        obj_upload(clean_list_upload, clean_list_result, 'r')

        # After-Check Process
        # hs_check_cmd = input('Do you want to check if the size/hashes match between storage & files? [Y/N] : ').strip().lower()

        # if hs_check_cmd == 'y':
        #     check_hash_size(clean_list_upload, clean_list_result)
        # elif hs_check_cmd == 'n':
        #     print('Skipping size/hash recheck process')

        # Remove Process
        # rm_after_upload(clean_list_upload, clean_list_result)

        # List objects in storage
        # obj_list_check()

        done = True

    elif user_cmd == 'd':
        logger.info('Started dry run')
        print('Dry run process begin ------------------')
        obj_upload(clean_list_upload, clean_list_result, 'd')
        print('Dry run process end --------------------')
        logger.info('Ended dry run successfully')

    elif user_cmd == 'n':
        print('Ending process')
        done = True
    else:
        print('couldn\'t understand command. Please type again')


if emergency:
    logger.info('Successfully ended emergency error run...')

logger.info('======================== END ==========================')
if not is_first or emergency:
    logger2.info('End successful')
elif is_first and not emergency:
    logger2_1.info('End successful')
    logger2_2.info('End successful')
send_to_slack("Upload successful - {}".format(datetime.now()))
sys.exit(0)