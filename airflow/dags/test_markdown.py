from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import Slackwebhook
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR / "/opt/airflow/app/비나우"))

def start():
    print("start")

def run_yesstyle_and_return_md(**context):
    import yesstyle_crawling_slack_table
    table_block = yesstyle_crawling_slack_table.main() 
    return table_block

default_args = {
    'owner':'Airflow',
    'depends_on_past':False,
    'on_success_callback':Slackwebhook.send_df_message,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id = "test_markdown",
    default_args = default_args,
    start_date = datetime(2026,1,28),
    tags=["TEST"]
) as dag:

    start_dag = PythonOperator(
        task_id = 'start_alarm',
        python_callable = start,
        on_success_callback = None
    )

    task_1 = PythonOperator(
        task_id="yesstyle_test",
        python_callable=run_yesstyle_and_return_md
    )

    start_dag >> task_1