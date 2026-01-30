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
    dag_id = "first_수집코드_20분_DAG",
    default_args = default_args,
    start_date = datetime(2026,1,31),
    schedule = "20 3 * * *",
    catchup = False,
    tags = ["메타광고현황수집"]
) as dag:

    start_dag = PythonOperator(
        task_id = "start_alarm",
        python_callable = start,
        on_success_callback = None
    )

    task_1 = BashOperator(
        task_id = "first_수집코드",
        bash_command = "python3 /opt/airflow/app/메타광고현황수집/first_수집_코드.py"
    )