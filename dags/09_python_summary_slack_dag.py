from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import datetime
import sys
import pkg_resources
import platform
import requests

# Get Slack Webhook URL from Airflow Variable
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL")

default_args = {
    'start_date': datetime(2024, 1, 1),
}

def get_python_summary(**context):
    """Collect Python and package information."""
    python_info = {
        'version': sys.version,
        'implementation': platform.python_implementation(),
        'compiler': platform.python_compiler(),
        'executable': sys.executable
    }

    important_packages = [
        'apache-airflow', 'pyspark', 'pandas', 'numpy', 'psycopg2',
        'sqlalchemy', 'kubernetes', 'boto3', 'requests'
    ]
    found_packages = {}
    missing_packages = []

    for pkg in important_packages:
        try:
            package = pkg_resources.get_distribution(pkg)
            found_packages[pkg] = package.version
        except pkg_resources.DistributionNotFound:
            missing_packages.append(pkg)

    installed = sorted([f"{d.project_name}=={d.version}" for d in pkg_resources.working_set])[:10]

    summary = f"""
📌 *Python Environment Summary*

*✅ Python Version:*
- {python_info['version']}
- {python_info['implementation']}
- {python_info['compiler']}
- {python_info['executable']}

*✅ Found Packages:*
{', '.join([f"{k} ({v})" for k, v in found_packages.items()]) or 'None'}

*❌ Missing Packages:*
{', '.join(missing_packages) or 'None'}

*📦 Top 10 Installed Packages:*
{chr(10).join(installed)}
"""
    print(summary)
    context['ti'].xcom_push(key='python_summary', value=summary.strip())

def notify_slack(**context):
    """Send notification to Slack channel."""
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    status = dag_run.get_state()
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url

    summary = context['ti'].xcom_pull(key='python_summary', task_ids='get_python_summary')

    message = f"""
🚀 *DAG `{dag_id}` Notification*

*Status:* {status.upper()}
*Execution Time:* {execution_date}
*Log URL:* {log_url}

{summary}
"""

    response = requests.post(SLACK_WEBHOOK_URL, json={"text": message})

    if response.status_code != 200:
        raise ValueError(f"Slack webhook error {response.status_code}: {response.text}")
    else:
        print("Slack notification sent!")

with DAG(
    dag_id='09_python_summary_slack_dag',
    default_args=default_args,
    description='Collect Python summary and notify via Slack',
    schedule_interval=None,
    catchup=False,
    tags=['python', 'slack'],
) as dag:

    collect_summary = PythonOperator(
        task_id='get_python_summary',
        python_callable=get_python_summary,
        provide_context=True,
    )

    slack_notify = PythonOperator(
        task_id='slack_notify',
        python_callable=notify_slack,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
    )

    collect_summary >> slack_notify
