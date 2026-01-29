from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
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
    dag_id="test_dag",
    default_args=default_args,
    start_date=datetime(2026,1,28)
) as dag:

    start_dag = BashOperator(
        task_id ="test",
        bash_command = "python3 /opt/airflow/app/japan_mc4_daily_copy/mc4_daily.py"
    )

    start_dag