"""
 - 구글 스프레드시트에서 읽기를 쉽게 해주는 모듈입니다. 아직은 쓰는 기능은 없습니다만 쉽게 추가 가능합니다.

 - 메인 함수는 get_google_sheet_to_csv입니다.
  - 이는 google sheet API를 통해 구글 스프레드시트를 읽고 쓰는 것이 가능하게 해줍니다.
  - 읽으려는 시트(탭)가 있는 스프레드시트 파일이 구글 서비스 어카운트 이메일과 공유가 되어있어야 합니다.
  - Airflow 상에서는 서비스어카운트 JSON 파일의 내용이 google_sheet_access_token이라는 이름의 Variable로 저장되어 있어야 합니다.
    - 이 이메일은 iam.gserviceaccount.com로 끝납니다.
    - 이 Variable의 내용이 매번 파일로 쓰여지고 그 파일이 구글에 권한 체크를 하는데 사용되는데 이 파일은 local_data_dir Variable로 지정된 로컬 파일 시스템에 저장된다. 이 Variable은 보통 /var/lib/airflow/data/로 설정되며 이를 먼저 생성두어야 한다 (airflow 사용자)
  - JSON 기반 서비스 어카운트를 만들려면 이 링크를 참고하세요: https://denisluiz.medium.com/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e

 - 아래 2개의 모듈 설치가 별도로 필요합니다.
  - pip3 install oauth2client
  - pip3 install gspread

 - get_google_sheet_to_csv 함수:
  - 첫 번째 인자로 스프레드시트 링크를 제공. 이 시트를 service account 이메일과 공유해야합니다.
  - 두 번째 인자로 데이터를 읽어올 tab의 이름을 지정합니다.
  - 세 번째 인자로 지정된 test.csv로 저장합니다.
gsheet.get_google_sheet_to_csv(
    'https://docs.google.com/spreadsheets/d/1hW-_16OqgctX-_lXBa0VSmQAs98uUnmfOqvDYYjuE50/',
    'Test',
    'test.csv',
)

 - 여기 예제에서는 아래와 같이 테이블을 만들어두고 이를 구글스프레드시트로부터 채운다
CREATE TABLE keeyong.spreadsheet_copy_testing (
    col1 int,
    col2 int,
    col3 int,
    col4 int
);
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from plugins import gsheet
from plugins import s3

import requests
import logging
import psycopg2
import json


def download_tab_in_gsheet(**context):
    url = context["params"]["url"]
    tab = context["params"]["tab"]
    table = context["params"]["table"]
    data_dir = Variable.get("local_data_dir")

    gsheet.get_google_sheet_to_csv(
        url,
        tab,
        data_dir+'{}.csv'.format(table)
    )
     

def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_conn_id = "aws_conn_id"
    s3_bucket = "grepp-data-engineering"
    s3_key = table
    data_dir = Variable.get("local_data_dir")
    local_files_to_upload = [ data_dir+'{}.csv'.format(table) ]
    replace = True

    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)


dag = DAG(
    dag_id = 'Gsheet_to_Redshift',
    start_date = datetime(2021,11,27), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    concurrency = 2,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

sheets = [
    {
        "url": "https://docs.google.com/spreadsheets/d/1hW-_16OqgctX-_lXBa0VSmQAs98uUnmfOqvDYYjuE50/",
        "tab": "Test",
        "schema": "keeyong",
        "table": "spreadsheet_copy_testing"
    }
]

for sheet in sheets:
    download_tab_in_gsheet = PythonOperator(
        task_id = 'download_{}_in_gsheet'.format(sheet["table"]),
        python_callable = download_tab_in_gsheet,
        params = sheet,
        provide_context=True,
        dag = dag)

    copy_to_s3 = PythonOperator(
        task_id = 'copy_{}_to_s3'.format(sheet["table"]),
        python_callable = copy_to_s3,
        params = {
            "table": sheet["table"]
        },
        provide_context=True,
        dag = dag)

    run_copy_sql = S3ToRedshiftOperator(
        task_id = 'run_copy_sql_{}'.format(sheet["table"]),
        s3_bucket = "grepp-data-engineering",
        s3_key = sheet["table"],
        schema = sheet["schema"],
        table = sheet["table"],
        copy_options=['csv', 'IGNOREHEADER 1'],
        truncate_table = True,
        redshift_conn_id = "redshift_dev_db",
        dag = dag
    )

    download_tab_in_gsheet >> copy_to_s3 >> run_copy_sql
