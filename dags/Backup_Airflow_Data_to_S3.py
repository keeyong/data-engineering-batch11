from airflow import DAG
from airflow.macros import *

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.models import Variable

from plugins import file_ops
from datetime import datetime

import json
import os
import logging

"""
data_s3_bucket: 백업 데이터가 저장될 S3 버켓을 지정

This is a job to copy the content of a local PostgreSQL database to S3
- https://www.postgresql.org/docs/9.1/backup-dump.html
"""


def main(**context):
    s3_bucket = Variable.get("data_s3_bucket")  # 백업 데이터가 저장될 S3 버켓을 
    folder = 'airflow_backup'

    # for now dbname can be airflow or superset
    # the latter is optional since superset isn't used everywhere
    dbname = context["params"]["dbname"]

    # get the current month in local time
    datem = datetime.datetime.today().strftime("%Y-%m-%d")
    year_month_day = str(datem)
    logging.info(year_month_day)
    
    filename = dbname + "-" + year_month_day + ".sql"
    local_filename = Variable.get('local_data_dir') + filename
    s3_key = "{folder}/{filename}".format(
        filename=filename,
        folder=folder
    )
    
    # run pg_dump to get a backup
    cmd = "pg_dump {dbname}".format(
        dbname=dbname
    )
    logging.info(cmd)
    file_ops.run_cmd_with_direct(cmd, local_filename)
    
    if os.stat(local_filename).st_size == 0:
        raise AirflowException(local_filename + " is empty")

    # upload it to S3
    s3_hook = S3Hook('aws_s3_default')
    s3_hook.load_file(
        filename=local_filename,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

    os.remove(local_filename)


DAG_ID = 'Backup_Airflow_Data_to_S3'
with DAG(
    DAG_ID,
    schedule_interval="0 8 * * *",
    start_date = datetime(2022,3,17), # 날짜가 미래인 경우 실행이 안됨
    catchup=False,
    tags=["medium", "backup"],
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    main = PythonOperator(
        dag=dag,
        task_id='Backup_Postgresql_Airflow_ToS3',
        python_callable=main,
        params = {
            "dbname": 'airflow'
        },
        provide_context=True
    )
