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
    dag_id="X_crawling_makeup_daily",
    default_args=default_args,
    start_date=datetime(2026,1,27),
    schedule="0 23 * * *",
    catchup=False,
    tags=["X_crawling"],
) as dag:

    start_dag = PythonOperator(
        task_id = 'start_alarm',
        python_callable = start,
        on_success_callback = None
    )

    task_1 = PythonOperator(
        task_id = "makeup_crawling",
        bash_command = "python3 /opt/airflow/app/japantwitter_makeup_crawling/makeup_daily.py"
    )

    task_2 = PythonOperator(
        task_id = "mc4_daily",
        bash_command = "python3 /opt/airflow/app/japan_mc4_daily_copy/mc4_daily.py"
    )

    start >> task_1
    start >> task_2

