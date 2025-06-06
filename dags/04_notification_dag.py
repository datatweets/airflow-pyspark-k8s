from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='04_parallel_notifications',
    description='Send notifications to multiple channels in parallel',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['parallel', 'notifications'],
) as dag:
    
    # Generate a report
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command='echo "Generating daily report..."; sleep 3; echo "Report complete!"',
    )
    
    # Send notifications to different channels - all in parallel
    email_notification = BashOperator(
        task_id='send_email',
        bash_command='echo "Sending email notification..."; sleep 2; echo "Email sent!"',
    )
    
    slack_notification = BashOperator(
        task_id='send_slack_message',
        bash_command='echo "Posting to Slack..."; sleep 1; echo "Slack message posted!"',
    )
    
    teams_notification = BashOperator(
        task_id='send_teams_message',
        bash_command='echo "Posting to Teams..."; sleep 1; echo "Teams message posted!"',
    )
    
    update_dashboard = BashOperator(
        task_id='update_dashboard',
        bash_command='echo "Updating dashboard..."; sleep 3; echo "Dashboard updated!"',
    )
    
    # After report is generated, send all notifications in parallel
    generate_report >> [email_notification, slack_notification, 
                       teams_notification, update_dashboard]
