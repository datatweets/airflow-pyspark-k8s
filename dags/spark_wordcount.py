from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='spark_wordcount_dag',
    default_args=default_args,
    description='Run PySpark wordcount job via Airflow',
    schedule_interval=None,  # Only runs manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pyspark', 'airflow'],
) as dag:

    # Task: Run PySpark wordcount
    spark_wordcount_task = BashOperator(
        task_id='spark_wordcount_task',
        bash_command="""
        rm -rf /opt/airflow/scripts/output &&
        spark-submit /opt/airflow/scripts/wordcount.py /opt/airflow/scripts/sample_text.txt /opt/airflow/scripts/output
        """,
    )

    # Set task dependencies (only one task here)
    spark_wordcount_task
