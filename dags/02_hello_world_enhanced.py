from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Using the context manager syntax (with statement) is more Pythonic
# It ensures proper cleanup and makes the code cleaner
with DAG(
    dag_id='02_hello_world_enhanced',
    description='Enhanced Hello World with multiple tasks',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # Run once per day
    catchup=False,
    tags=['beginner', 'tutorial', 'kubernetes'],  # Tags for organization
) as dag:

    # First task: Print a message
    hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from pod: $HOSTNAME"'
    )

    # Second task: Print the current date
    date_task = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # Third task: A Python function
    def print_context(**context):
        """This function receives the Airflow context and prints execution details"""
        print(f"Running in Kubernetes pod: {context['task_instance'].hostname}")
        print(f"Execution date: {context['execution_date']}")
        print(f"Task: {context['task'].task_id}")
        return "Python task completed successfully!"

    python_task = PythonOperator(
        task_id='python_info',
        python_callable=print_context,
        provide_context=True
    )

    # Define task dependencies using the bit shift operator
    # This creates a pipeline: hello -> date -> python
    hello_task >> date_task >> python_task
