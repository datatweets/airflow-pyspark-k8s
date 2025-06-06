from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments define the basic parameters that all tasks in this DAG will inherit
# Think of these as the "house rules" that apply to everyone unless specifically overridden
default_args = {
    'owner': 'data-team',  # Who is responsible for this DAG
    'retries': 1,          # How many times to retry a failed task
    'retry_delay': timedelta(minutes=5),  # Wait time between retries
}

# The DAG object represents your entire workflow
# It's like the blueprint that tells Airflow what to do and when
dag = DAG(
    dag_id='01_simple_hello_world',  # Unique identifier for your DAG
    description='Our first "Hello World" DAG!',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),  # When the DAG becomes active
    schedule_interval=None,  # No automatic scheduling (manual trigger only)
    catchup=False,  # Don't run for past dates
)

# A task is a single unit of work within your DAG
# BashOperator executes bash commands - perfect for our hello world example
hello_task = BashOperator(
    task_id='hello_world_task',  # Unique identifier within the DAG
    bash_command='echo "Hello from Kubernetes pod!"',
    dag=dag  # Associate this task with our DAG
)

# By referencing the task at the end, we ensure Airflow registers it
hello_task
