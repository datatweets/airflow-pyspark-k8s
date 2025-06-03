# Complete Guide to Setting Up Slack Notifications for Apache Airflow DAGs with Secure Environment Variables

## Table of Contents

1. Introduction
2. Understanding Environment Variables and .env Files
3. Setting Up .env Files Securely
4. Prerequisites
5. Creating a Slack App
6. Configuring Incoming Webhooks
7. Implementing Slack Notifications with .env
8. Testing and Troubleshooting
9. Best Practices

------

## 1. Introduction

This tutorial will guide you through setting up Slack notifications for your Apache Airflow DAGs using secure environment variable management with .env files. When your data pipelines run, you'll receive real-time updates in your Slack channel about their status, execution times, and any errors that occur.

### Why Slack Notifications for Airflow?

- **Real-time monitoring**: Get instant updates when DAGs succeed or fail
- **Team collaboration**: Share pipeline status with your entire team
- **Error tracking**: Quickly identify and respond to failures
- **Audit trail**: Keep a searchable history of all pipeline executions

------

## 2. Understanding Environment Variables and .env Files

### What is a .env File?

A `.env` file is a simple text file that stores environment variables as key-value pairs:

```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
DB_PASSWORD=your-secure-password
API_KEY=your-api-key
```

### Why Use .env Files?

1. **Security**: Keep sensitive data (webhooks, passwords, API keys) out of your code
2. **Flexibility**: Different configurations for dev/staging/production without changing code
3. **Version Control Safety**: .env files are gitignored, preventing accidental commits of secrets
4. **Portability**: Easy to share configuration templates without exposing actual credentials

### How .env Files Work

The `python-dotenv` library reads the .env file at startup and loads the variables into the environment, making them accessible via `os.getenv()` in your Python code.

------

## 3. Setting Up .env Files Securely

### Step 3.1: Create Your .env File

In your Airflow project root directory, create a file named `.env`:

```bash
# Slack Configuration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Environment
AIRFLOW_ENV=production

# Optional: Other configurations
DB_HOST=your-database-host
DB_PORT=5432
DB_NAME=airflow_db
DB_USER=airflow_user
DB_PASSWORD=your-secure-password
```

### Step 3.2: Create .env.example Template

Create `.env.example` for other developers (without actual secrets):

```bash
# Slack Configuration
SLACK_WEBHOOK_URL=your-slack-webhook-url-here

# Environment
AIRFLOW_ENV=development

# Optional: Other configurations
DB_HOST=localhost
DB_PORT=5432
DB_NAME=airflow_db
DB_USER=airflow_user
DB_PASSWORD=changeme
```

### Step 3.3: Configure .gitignore

Create or update `.gitignore` to exclude sensitive files:

```gitignore
# Environment variables
.env
.env.local
.env.production

# Keep the example
!.env.example

# Python
__pycache__/
*.pyc

# Airflow
logs/
airflow.db
airflow-webserver.pid
*.log

# IDE
.vscode/
.idea/

# OS
.DS_Store
```

### Step 3.4: Verify .env is Not Tracked

```bash
# Check if .env is already tracked by Git
git ls-files | grep .env

# If it shows up, remove from tracking
git rm --cached .env
git commit -m "Remove .env from tracking"
```

### Step 3.5: Commit Your Security Files

```bash
git add .gitignore .env.example
git commit -m "Add environment configuration templates"
```

------

## 4. Prerequisites

Before starting with Slack setup, ensure you have:

- Apache Airflow installed and running
- A Slack workspace where you have permissions to create apps
- Python 3.7 or higher
- The `requests` and `python-dotenv` libraries installed
- Admin access to create Slack apps in your workspace

Project structure:

```
airflow-pyspark-k8s/
├── .env                    # Your actual secrets (not committed)
├── .env.example           # Template for others (committed)
├── .gitignore            # Excludes .env (committed)
├── dags/
│   └── slack_notification_dag.py
├── requirements.txt
└── README.md
```

------

## 5. Creating a Slack App

### Step 5.1: Navigate to Slack API

<img width="1512" alt="Screenshot 2025-06-01 at 4 49 42 PM" src="https://github.com/user-attachments/assets/8f24c44d-ef36-4153-b245-6f4f7c6c0c86" />

Go to [api.slack.com/apps](https://api.slack.com/apps) and click on "Create New App". You'll see a page similar to the image above showing your existing apps (if any) and the option to create a new one.

### Step 5.2: Choose App Creation Method

<img width="1512" alt="slack00" src="https://github.com/user-attachments/assets/1a1722fc-1c0e-4f24-940b-14f7abaa8c11" />

When you click "Create New App", you'll see two options:

- **From scratch**: Start with a blank app (we'll use this)
- **From a manifest**: Use a pre-configured template

Select "From scratch" as we want to configure everything step by step.

### Step 5.3: Name Your App and Select Workspace

<img width="1512" alt="slack01" src="https://github.com/user-attachments/assets/30072604-06ae-4e8d-bfc9-5fb661437039" />

In this dialog:

1. **App Name**: Enter a descriptive name like "Airflow DAG Notifications" or "Data Pipeline Status"
2. **Pick a workspace**: Select your Slack workspace from the dropdown (in the example, it's "bigeeks")
3. Click "Create App"

------

## 6. Configuring Incoming Webhooks

### Step 6.1: Access App Settings

<img width="1512" alt="slack02" src="https://github.com/user-attachments/assets/c2ae3098-5653-4a83-979c-6879a18a6e3e" />

After creating your app, you'll land on the Basic Information page. Here you can see:

- **App ID**: A unique identifier for your app
- **Client ID**: Used for OAuth authentication
- **Client Secret**: Keep this confidential
- **Signing Secret**: Used to verify requests from Slack

### Step 6.2: Navigate to Incoming Webhooks

<img width="1512" alt="slack03" src="https://github.com/user-attachments/assets/d1832a65-ad24-40cc-8fda-8d225ee587b5" />

From the left sidebar, click on "Incoming Webhooks". You'll see:

- A description of what incoming webhooks do
- An activation toggle (currently OFF)
- Information about how webhooks work

### Step 6.3: Activate Incoming Webhooks

<img width="1512" alt="slack04" src="https://github.com/user-attachments/assets/fdfa9540-81b6-4a9e-915c-3e15278d1236" />

Click the toggle to turn it ON. The page will update to show:

- Webhook URLs section
- Sample curl request
- Option to add new webhooks

### Step 6.4: Add Webhook to Workspace

<img width="1512" alt="slack05" src="https://github.com/user-attachments/assets/1fa1426f-6def-4711-90b7-704201a034e2" />

Click "Add New Webhook" and you'll see an authorization page where you:

1. Select the channel where notifications will be posted
2. The dropdown shows all available channels (public and private you have access to)
3. Click "Allow" to authorize the app

### Step 6.5: Copy Your Webhook URL

<img width="1512" alt="slack06" src="https://github.com/user-attachments/assets/da5ff40f-6d51-4050-80c6-bafe5c8b31fc" />

After authorization, you'll see:

- Your webhook URL (format: `https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/XXXXXXXXXXXXXXXX`)
- The channel it's connected to
- When it was added and by whom
- A "Copy" button to copy the URL

**Important**:

1. Copy this webhook URL
2. Add it to your `.env` file (NOT in your code!)
3. Never commit this URL to Git

------

## 7. Implementing Slack Notifications with .env

### Step 7.1: Update Your .env File

Add the webhook URL you copied to your `.env` file:

```bash
# Slack Configuration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/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
AIRFLOW_ENV=production
```

### Step 7.2: Create the DAG with Environment Variables

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import requests
import json
import os
import traceback
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'development')

# Validate that webhook URL is configured
if not SLACK_WEBHOOK_URL:
    raise ValueError("SLACK_WEBHOOK_URL not found in environment variables. Please check your .env file.")

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def send_slack_notification(context, status, custom_message=None):
    """
    Send formatted notification to Slack
    
    Parameters:
    - context: Airflow context dictionary
    - status: 'success', 'failure', or custom status
    - custom_message: Optional additional message
    """
    # Extract context information
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id if 'task' in context else 'N/A'
    execution_date = context['execution_date']
    
    # Determine emoji and color based on status
    status_config = {
        'success': {'emoji': '✅', 'color': 'good'},
        'failure': {'emoji': '❌', 'color': 'danger'},
        'warning': {'emoji': '⚠️', 'color': 'warning'},
        'info': {'emoji': 'ℹ️', 'color': '#439FE0'}
    }
    
    config = status_config.get(status, {'emoji': '❓', 'color': '#808080'})
    
    # Build the Slack message
    slack_message = {
        "attachments": [{
            "color": config['color'],
            "title": f"{config['emoji']} {dag_id} - {status.upper()}",
            "fields": [
                {
                    "title": "Task",
                    "value": task_id,
                    "short": True
                },
                {
                    "title": "Execution Date",
                    "value": execution_date.strftime('%Y-%m-%d %H:%M:%S'),
                    "short": True
                },
                {
                    "title": "Status",
                    "value": status.upper(),
                    "short": True
                },
                {
                    "title": "Environment",
                    "value": AIRFLOW_ENV,
                    "short": True
                }
            ],
            "footer": "Airflow Notification System",
            "ts": int(datetime.now().timestamp())
        }]
    }
    
    # Add custom message if provided
    if custom_message:
        slack_message["attachments"][0]["fields"].append({
            "title": "Message",
            "value": custom_message,
            "short": False
        })
    
    # Add log URL if available
    if 'task_instance' in context and hasattr(context['task_instance'], 'log_url'):
        slack_message["attachments"][0]["fields"].append({
            "title": "Logs",
            "value": f"<{context['task_instance'].log_url}|View Logs>",
            "short": False
        })
    
    # Send the notification
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'},
            timeout=10  # 10 second timeout
        )
        
        if response.status_code != 200:
            print(f"Failed to send Slack notification: {response.status_code} - {response.text}")
        else:
            print(f"Successfully sent Slack notification for {dag_id}")
            
    except requests.exceptions.RequestException as e:
        print(f"Error sending Slack notification: {str(e)}")

# Callback functions
def task_success_callback(context):
    """Called when a task succeeds"""
    send_slack_notification(context, 'success', 'Task completed successfully')

def task_failure_callback(context):
    """Called when a task fails"""
    # Get exception details if available
    exception = context.get('exception')
    error_message = str(exception) if exception else 'Unknown error occurred'
    
    # Include truncated traceback for debugging
    if exception:
        tb_lines = traceback.format_exc().split('\n')
        # Take last 5 lines of traceback (usually most relevant)
        error_message += '\n\nTraceback (last 5 lines):\n' + '\n'.join(tb_lines[-5:])
    
    send_slack_notification(context, 'failure', error_message)

def dag_success_callback(context):
    """Called when entire DAG succeeds"""
    # Calculate total duration
    dag_run = context['dag_run']
    duration = (datetime.now() - dag_run.execution_date).total_seconds()
    duration_str = f"{int(duration // 60)}m {int(duration % 60)}s"
    
    send_slack_notification(
        context, 
        'success', 
        f'All tasks completed successfully. Total duration: {duration_str}'
    )

def dag_failure_callback(context):
    """Called when DAG fails"""
    send_slack_notification(
        context, 
        'failure', 
        'One or more tasks failed. Check logs for details.'
    )

# Example processing functions
def extract_data(**context):
    """Example extract task"""
    print("Extracting data from source...")
    # Simulate data extraction
    import random
    record_count = random.randint(1000, 5000)
    
    # Push data to XCom for next task
    context['ti'].xcom_push(key='record_count', value=record_count)
    
    # Send custom notification
    send_slack_notification(
        context, 
        'info', 
        f'Extracted {record_count} records from source database'
    )
    
    return record_count

def transform_data(**context):
    """Example transform task"""
    # Pull data from previous task
    ti = context['ti']
    record_count = ti.xcom_pull(task_ids='extract_data', key='record_count')
    
    print(f"Transforming {record_count} records...")
    # Simulate transformation
    import time
    time.sleep(2)
    
    # Simulate occasional warnings
    if record_count > 4000:
        send_slack_notification(
            context,
            'warning',
            f'Large dataset detected: {record_count} records. Processing may take longer.'
        )
    
    return record_count

def load_data(**context):
    """Example load task"""
    # Pull data from previous task
    ti = context['ti']
    record_count = ti.xcom_pull(task_ids='extract_data', key='record_count')
    
    print(f"Loading {record_count} records to destination...")
    # Simulate loading
    import time
    time.sleep(1)
    
    # Simulate occasional failures for testing
    import random
    if random.random() < 0.1:  # 10% chance of failure
        raise Exception("Simulated database connection error")
    
    return f"Successfully loaded {record_count} records"

# Create the DAG
with DAG(
    dag_id='etl_pipeline_with_slack',
    default_args=default_args,
    description='ETL pipeline with comprehensive Slack notifications',
    schedule_interval='0 2 * * *' if AIRFLOW_ENV == 'production' else None,  # Daily at 2 AM in production
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'notifications', 'slack'],
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
) as dag:
    
    # Start task - simple bash command
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting ETL pipeline..."'
    )
    
    # Extract task with custom notifications
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        on_failure_callback=task_failure_callback,
        provide_context=True
    )
    
    # Transform task
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        on_failure_callback=task_failure_callback,
        provide_context=True
    )
    
    # Load task
    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        on_failure_callback=task_failure_callback,
        on_success_callback=task_success_callback,
        provide_context=True
    )
    
    # End task - runs regardless of previous task status
    end = BashOperator(
        task_id='end',
        bash_command='echo "ETL pipeline finished"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Define task dependencies
    start >> extract >> transform >> load >> end
```

### Step 7.3: Key Changes Explained

**1. Loading Environment Variables:**

```python
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
```

This loads all variables from your .env file into the environment.

**2. Getting Values from Environment:**

```python
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'development')
```

`os.getenv()` retrieves values from environment variables, with optional defaults.

**3. Validation:**

```python
if not SLACK_WEBHOOK_URL:
    raise ValueError("SLACK_WEBHOOK_URL not found in environment variables. Please check your .env file.")
```

This ensures the webhook URL is configured before the DAG runs.

**4. Environment-Based Configuration:**

```python
schedule_interval='0 2 * * *' if AIRFLOW_ENV == 'production' else None
```

Different behavior based on environment (scheduled in production, manual in development).

------

## 8. Testing and Troubleshooting

### Step 8.1: Test Your Configuration

Create a test script `test_slack_config.py`:

```python
import os
from dotenv import load_dotenv
import requests
import json

# Load environment variables
load_dotenv()

# Get webhook URL
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

if not SLACK_WEBHOOK_URL:
    print("❌ SLACK_WEBHOOK_URL not found in .env file")
    exit(1)

# Send test message
test_message = {
    "text": "🧪 Test notification from Airflow setup"
}

response = requests.post(
    SLACK_WEBHOOK_URL,
    data=json.dumps(test_message),
    headers={'Content-Type': 'application/json'}
)

if response.status_code == 200:
    print("✅ Test message sent successfully! Check your Slack channel.")
else:
    print(f"❌ Failed to send message: {response.status_code} - {response.text}")
```

Run the test:

```bash
python test_slack_config.py
```

### Step 8.2: Testing Your DAG

1. **Verify DAG appears in Airflow UI**:

   ```bash
   airflow dags list | grep etl_pipeline_with_slack
   ```

2. **Test the DAG**:

   ```bash
   airflow dags test etl_pipeline_with_slack 2024-01-01
   ```

3. **Trigger manually**:

   ```bash
   airflow dags trigger etl_pipeline_with_slack
   ```

### Step 8.3: Common Issues and Solutions

**Issue 1: "SLACK_WEBHOOK_URL not found"**

- Check if .env file exists in the correct location
- Verify the variable name matches exactly
- Ensure python-dotenv is installed

**Issue 2: "No module named 'dotenv'"**

```bash
pip install python-dotenv
```

**Issue 3: Notifications not appearing in Slack**

- Verify the webhook URL is complete and correct
- Check if the Slack channel still exists
- Look at Airflow logs for error messages

### Step 8.4: Debugging Tips

Add debug logging:

```python
def send_slack_notification(context, status, custom_message=None):
    print(f"Loading from environment: AIRFLOW_ENV={AIRFLOW_ENV}")
    print(f"Webhook URL configured: {'Yes' if SLACK_WEBHOOK_URL else 'No'}")
    print(f"Sending {status} notification to Slack...")
    
    # ... rest of function
```

------

## 9. Best Practices

### 9.1: Security Best Practices

**1. Never commit .env files:**

```bash
# Always check before committing
git status
git diff --cached

# Should never see .env in the output
```

**2. Use different webhooks for different environments:**

```bash
# .env.development
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/dev/webhook
SLACK_CHANNEL=#dev-notifications

# .env.production
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/prod/webhook
SLACK_CHANNEL=#prod-alerts
```

**3. Rotate webhooks regularly:**

- Create new webhooks periodically
- Update .env files
- Revoke old webhooks in Slack

### 9.2: Production Deployment

**1. Upload .env file securely (not via Git):**

```bash
# Use secure file transfer
scp .env.production user@server:/path/to/airflow/.env

# Or use secrets management service
aws secretsmanager create-secret --name airflow-slack-webhook --secret-string file://.env
```

**2. Set proper file permissions:**

```bash
chmod 600 .env  # Only owner can read/write
```

**3. Alternative: Use Airflow Variables:**

```python
from airflow.models import Variable

# Set in Airflow UI or CLI
# airflow variables set SLACK_WEBHOOK_URL "https://hooks.slack.com/..."

# Use in code
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var=os.getenv('SLACK_WEBHOOK_URL'))
```

### 9.3: Team Collaboration

**1. Document in README.md:**

1. Copy environment template:

   ```bash
   cp .env.example .env
   ```

2. Edit .env with your Slack webhook URL


**2. Keep .env.example updated:**

- Add new variables to both .env and .env.example
- Document what each variable does

### 9.4: Message Formatting Best Practices

- Keep messages concise and actionable
- Include relevant links (logs, dashboards)
- Use consistent emoji patterns
- Consider rate limiting for high-volume pipelines

### 9.5: Channel Strategy

Consider using different channels:

- `#data-pipeline-status`: All DAG executions
- `#data-pipeline-alerts`: Only failures
- `#data-pipeline-metrics`: Performance statistics

---

## Conclusion

You now have a complete Slack notification system for your Airflow DAGs with secure environment variable management! This setup:

1. **Keeps secrets secure** - Webhook URLs never appear in code
2. **Prevents accidental commits** - .env is gitignored
3. **Supports multiple environments** - Easy dev/staging/prod configuration
4. **Enables team collaboration** - .env.example shows required configuration
5. **Provides comprehensive monitoring** - Real-time pipeline status in Slack

### Quick Checklist

- [ ] Ensure `python-dotenv` installed.
- [ ] Create `.env` file with your Slack webhook URL
- [ ] Create `.env.example` as a template
- [ ] Update `.gitignore` to exclude `.env`
- [ ] Add `load_dotenv()` to your DAG
- [ ] Test with `test_slack_config.py`
- [ ] Deploy DAG to Airflow
- [ ] Monitor your Slack channel!

Remember: **Never commit your .env file to Git!** Always use .env.example as a template for other developers.

Happy monitoring! 🚀
