# -*- coding: utf8-*-
##############################
# v20200806 for python3.7
# NAS 파일 정리 툴; RD-66
##############################

import swiftclient
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
import shutil
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
# dir , ps
##############################
current_dir = os.path.dirname(os.path.realpath(__file__))
current_file = os.path.basename(__file__)
current_file_name = current_file[:-3] #xxxx.py
current_pid = os.getpid()

###############################
# 중복 실행 방지
###############################
result = subprocess.check_output('ps -ef | grep {} | wc -l'.format(os.path.basename(__file__)), shell=True)
if int(result.strip()) > 3:
    sys.exit(0)

###################################################
## 인증 및 스토리지 연결
###################################################

endpoint = 'https://kr.archive.ncloudstorage.com:5000/v3'
# username = 'oVe5zmArwOnezJdiCMl3'
# password = 'iLHZwMhqs2mvcZUJYYeePzPzgDn9DkQtXzRhsLVa'
domain_id = 'default'
project_id = '64f16e672bba47a0a2f871e2727fe3dc'
bucket_name = 'nas-test'

try:
    auth = v3.Password(auth_url=endpoint, username=username, password=password, project_id=project_id, user_domain_id=domain_id)
    auth_session = session.Session(auth=auth)

    swift_connection = swiftclient.Connection(retries=5, session=auth_session)
except:
    print('Couldn\'t connect to NCP Archive Storage')
    sys.exit(1)

container = swift_connection.get_container(bucket_name)

for obj in container[1]:
    obj_name = obj['name']

    if obj_name != 'Result':
        prj_num = int(obj_name.split('/')[1])
    else:
        prj_num = 1000

    if prj_num >= 688:

        try:
            swift_connection.head_object('nas-storage', obj_name)
            print('{} already exists'.format(obj_name))
        except:
            try:
                swift_connection.copy_object('nas-test', obj_name, destination='nas-storage/{}'.format(obj_name))
                print('Copying {}...'.format(obj_name))
            except:
                print('Error in {}'.format(obj_name))
                sys.exit(1)
    

print('Ending')
sys.exit(0)

