import os
import requests
from datetime import datetime
from pathlib import Path
import sys
import pytz


SEOUL_TZ = pytz.timezone("Asia/Seoul")
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR / "/opt/airflow/app/비나우"))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def _post_to_slack(payload):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set; skipping Slack notification.", file=sys.stderr)
        return
    requests.post(SLACK_WEBHOOK_URL, json=payload)

def send_messages(name, message):
    icon_emoji = ":crying_cat_face:"
    channel = '#프로젝트-테스트'
    payload = {"channel":channel, "username":name, "text":message, "icon_emoji":icon_emoji}

    _post_to_slack(payload)

def send_success_message(name, message):
    icon_emoji = ":pizza:"
    channel = '#프로젝트-테스트'
    payload = {"channel":channel, "username":name, "text":message, "icon_emoji":icon_emoji}

    _post_to_slack(payload)


def airflow_failed_callback(context):
    ti = context.get("task_instance")
    message = (
        f":red_circle: Task Failed.\n"
        f"`DAG` : {ti.dag_id}\n"
        f"`Task` : {ti.task_id}\n"
        f"`Run ID` : {context.get('run_id')}\n"
        f"`Date` : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"`URL` : {ti.log_url}"
    )
    send_messages("Airflow", message)

def airflow_success_message(context):
    ti = context.get("task_instance")
    message = (
        f":partying_face: Task Success.\n"  
        f"`DAG` : {ti.dag_id}\n"
        f"`Task` : {ti.task_id}\n"
        f"`Run ID` : {context.get('run_id')}\n"
        f"`Date` : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"`URL` : {ti.log_url}"
    )
    send_success_message("Airflow", message)


def send_df_message(context):
    ti = context["ti"]
    task_id = context["task"].task_id
    table_block = ti.xcom_pull(task_ids=task_id)
    icon_emoji = ":smirk_cat:"
    channel = "#프로젝트_테스트_md"
    payload = {"channel":channel, 
                "username":"Airflow_YESSTYLE",
                "icon_emoji":icon_emoji,
                "blocks":table_block["blocks"]}

    _post_to_slack(payload)

def send_excel(context):
    ti = context["ti"]
    task_id = context["task"].task_id
    excel = ti.xcom_pull(task_ids=task_id)
    icon_emoji = ":smirk_cat:"
    channel = "#프로젝트_테스트_md"
    payload = {"channel":channel, 
                "username":"Airflow_YESSTYLE",
                "icon_emoji":icon_emoji,
                "excel":excel}
                
    _post_to_slack(payload)
