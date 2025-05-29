from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 0,
    'email_on_failure': False,
}

with DAG(
    dag_id='one_task_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    task = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello, World!"',
        dag=dag
    )