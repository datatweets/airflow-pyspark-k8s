from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define default arguments for all tasks in the DAG
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['lotfinejad@gmail.com']
}

def process_daily_sales(**context):
    """Process sales data for a specific date"""
    # The execution_date tells us which day's data we're processing
    execution_date = context['execution_date']
    
    print(f"Processing sales data for: {execution_date.strftime('%Y-%m-%d')}")
    print(f"Current time (when task runs): {datetime.now()}")
    
    # Simulate processing
    # In reality, you'd query data WHERE date = execution_date
    sales_total = 10000 + (execution_date.day * 100)  # Dummy calculation
    
    print(f"Total sales for {execution_date.strftime('%Y-%m-%d')}: ${sales_total}")
    
    # Push result to XCom for downstream tasks
    return {'date': execution_date.strftime('%Y-%m-%d'), 'total': sales_total}

def generate_report(**context):
    """Generate a report based on processed data"""
    ti = context['task_instance']
    
    # Pull data from previous task
    sales_data = ti.xcom_pull(task_ids='process_sales')
    
    print(f"Generating report for {sales_data['date']} with total: ${sales_data['total']}")
    
    # In practice, this might create a PDF, send an email, or update a dashboard
    report_name = f"sales_report_{sales_data['date']}.pdf"
    print(f"Report generated: {report_name}")

# Create the DAG
dag = DAG(
    dag_id='10_daily_sales_pipeline',
    description='Process daily sales data and generate reports',
    default_args=default_args,
    start_date=days_ago(7),  # Start 7 days ago
    schedule_interval='0 15 * * *',  # Run daily at 3 PM UTC
    catchup=True,  # We'll explore this shortly
    max_active_runs=1,  # Only one run at a time
    tags=['sales', 'daily', 'reports']
)

# Define tasks
with dag:
    # Task 1: Check if data is ready
    check_data = BashOperator(
        task_id='check_data_availability',
        bash_command='echo "Checking if sales data is ready for {{ ds }}"'
        # {{ ds }} is a template variable for execution_date in YYYY-MM-DD format
    )
    
    # Task 2: Process the sales data
    process_sales = PythonOperator(
        task_id='process_sales',
        python_callable=process_daily_sales
    )
    
    # Task 3: Generate report
    create_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )
    
    # Task 4: Notify completion
    notify = BashOperator(
        task_id='send_notification',
        bash_command='echo "Sales pipeline completed for {{ ds }}"'
    )
    
    # Define task dependencies
    check_data >> process_sales >> create_report >> notify
