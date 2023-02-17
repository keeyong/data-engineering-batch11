from airflow import DAG
from airflow.macros import *

import os
from glob import glob
import logging
import subprocess

from plugins import redshift_summary
from plugins import slack


DAG_ID = "Build_Summary_v2"
dag = DAG(
    DAG_ID,
    schedule_interval="25 13 * * *",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    start_date=datetime(2021, 9, 17),
    default_args= {
        'on_failure_callback': slack.on_failure_callback,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
)

# this should be listed in dependency order (all in analytics)
tables_load = [
    'nps_summary'
]

dag_root_path = os.path.dirname(os.path.abspath(__file__))
redshift_summary.build_summary_table(dag_root_path, dag, tables_load, "redshift_dev_db")
