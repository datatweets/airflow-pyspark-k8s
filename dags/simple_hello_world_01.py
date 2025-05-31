from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'Mehdi',
}

with DAG(
    dag_id='simple_hello_world',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = None,
    schedule_interval = None,
) as dag:

    # Define a simple task that prints "Hello world!" to the logs

    task = BashOperator(
        task_id = 'simple_hello_world_task',
        bash_command = 'echo Hello world!',
)

task