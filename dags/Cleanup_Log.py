from airflow import DAG
from airflow.macros import *

from airflow.operators.bash_operator import BashOperator

import logging

def return_bash_cleanup_command(log_dir, depth, days):
    return  "find {log_dir} -mindepth {depth} -mtime +{days} -delete && find {log_dir} -type d -empty -delete".format(
        log_dir=log_dir,
        days=days,
        depth=depth
    )


def return_bash_cleanup_for_scheduler_command(log_dir, depth, days):
    return "find {log_dir} -mindepth {depth} -mtime +{days} -exec rm -rf '{{}}' \;".format(
        log_dir=log_dir,
        days=days,
        depth=depth
    )


DAG_ID = "Cleanup_Logdir"
dag = DAG(
    DAG_ID,
    start_date=datetime(2022,3,17),
    schedule_interval="0 0,12 * * *",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

base_log = "/var/lib/airflow/logs" 
bash_command=return_bash_cleanup_for_scheduler_command(base_log, 2, 1)
# logging.info(bash_command)
task1 = BashOperator(
    task_id='cleanup_base_logdir',
    bash_command=bash_command,
    retries=1,
    dag=dag
)

scheduler_log = "/var/lib/airflow/logs/scheduler"
bash_command=return_bash_cleanup_for_scheduler_command(scheduler_log, 2, 1)
# logging.info(bash_command)
task2 = BashOperator(
    task_id='cleanup_scheduler_logdir',
    bash_command=bash_command,
    retries=1,
    dag=dag
)
