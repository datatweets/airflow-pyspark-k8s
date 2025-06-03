from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'analytics-setup',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='create_analytics_database',
    description='Create separate database for analytics data',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=False,
    tags=['setup', 'database']
) as dag:

    # Create analytics database
    create_db = PostgresOperator(
        task_id='create_analytics_db',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE DATABASE analytics;
        """,
        autocommit=True
    )