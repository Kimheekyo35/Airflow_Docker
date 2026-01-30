from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import sys
from pathlib import Path
import Slackwebhook

def start():
    print("start")

default_args = {
    'owner':'Airflow',
    'depends_on_past':False,
    'on_failure_callback':Slackwebhook.airflow_failed_callback,
    'on_success_callback':Slackwebhook.airflow_success_message,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id = "bf_mw_runner",
    default_args = default_args,
    start_date = datetime(2026,1,31),
    schedule = "0 0 * * *",
    catchup = False,
    tags = ["25_4Q_BF_MW"]
) as dag:

    start_dag = PythonOperator(
        task_id = "start_alarm",
        python_callable = start,
        on_success_callback = None
    )

    task_1 = BashOperator(
        task_id = "bf_mw_runner",
        bash_command = "python3 /opt/airflow/app/25_4Q_BF_MW/bf_mw_runner.py"
    )
