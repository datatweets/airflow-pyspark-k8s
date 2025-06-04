# Set Up Slack Notifications in Airflow Using Airflow Variables

This short guide walks you through:

1. Creating a Slack webhook
2. Storing it in Airflow Variables
3. Sending Slack messages using your DAG

---

## ✅ Step 1: Create a Slack Webhook

Follow these steps **once** to generate your webhook URL and send messages to a Slack channel.

### 1.1. Go to Slack API

<img width="1512" alt="Screenshot 2025-06-01 at 4 49 42 PM" src="https://github.com/user-attachments/assets/8f24c44d-ef36-4153-b245-6f4f7c6c0c86" />

Visit [https://api.slack.com/apps](https://api.slack.com/apps) and click **"Create New App"**.

---

### 1.2. Choose "From scratch"

<img width="1512" alt="slack00" src="https://github.com/user-attachments/assets/1a1722fc-1c0e-4f24-940b-14f7abaa8c11" />

Select **"From scratch"** to start creating a new app manually.

---

### 1.3. Name Your App & Select Workspace

<img width="1512" alt="slack01" src="https://github.com/user-attachments/assets/30072604-06ae-4e8d-bfc9-5fb661437039" />

* Give it a name like `Airflow Notifications`
* Select your Slack workspace
* Click **Create App**

---

### 1.4. Enable Incoming Webhooks

<img width="1512" alt="slack02" src="https://github.com/user-attachments/assets/c2ae3098-5653-4a83-979c-6879a18a6e3e" />

Go to **Incoming Webhooks** in the sidebar and click **ON** to enable it.

---

### 1.5. Add Webhook to Workspace

<img width="1512" alt="slack05" src="https://github.com/user-attachments/assets/1fa1426f-6def-4711-90b7-704201a034e2" />

Click **“Add New Webhook to Workspace”**, choose your target channel, and click **Allow**.

---

### 1.6. Copy Webhook URL

<img width="1512" alt="slack06" src="https://github.com/user-attachments/assets/da5ff40f-6d51-4050-80c6-bafe5c8b31fc" />

Copy the generated URL. It will look like:

```
https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

---

## ✅ Step 2: Store Webhook URL in Airflow

1. Go to **Admin → Variables** in the Airflow UI.
2. Create a new variable:

   * **Key**: `SLACK_WEBHOOK_URL`
   * **Value**: paste your Slack webhook URL

> ✅ This keeps the webhook secret and out of your code.

---

## ✅ Step 3: Use the Code in Your DAG

Below is the DAG that only sends a Slack message:

```python
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
    dag_id='python_summary_slack_dag',
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
```

---

## What You’ve Learned

* How to create a Slack app and get a webhook
* How to store it securely in Airflow Variables
* How to notify Slack from your DAG without exposing secrets

