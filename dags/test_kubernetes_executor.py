from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import socket

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'test_kubernetes_executor',
    default_args=default_args,
    description='Test KubernetesExecutor - each task runs in separate pod',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'kubernetes'],
)

def get_pod_info():
    """Get information about the current pod"""
    hostname = socket.gethostname()
    print(f"Running on pod: {hostname}")
    return hostname

# Task 1: Simple bash command
task1 = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello from KubernetesExecutor Pod: $(hostname)" && sleep 10',
    dag=dag,
)

# Task 2: Python task to show pod isolation
task2 = PythonOperator(
    task_id='python_task',
    python_callable=get_pod_info,
    dag=dag,
)

# Task 3: Resource intensive task
task3 = BashOperator(
    task_id='resource_task',
    bash_command='echo "CPU info:" && cat /proc/cpuinfo | grep processor | wc -l && echo "Memory info:" && free -m',
    dag=dag,
)

# Set task dependencies
task1 >> task2 >> task3