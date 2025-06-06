from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Define our default arguments
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create our DAG using the context manager
with DAG(
    dag_id='03_parallel_data_processing',
    description='Demonstrates parallel task execution',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    tags=['parallel', 'tutorial'],
) as dag:
    
    # Start task - this represents the beginning of our pipeline
    # DummyOperator is perfect for creating structure without actual processing
    start = DummyOperator(
        task_id='start_pipeline',
    )
    
    # Three parallel extraction tasks
    # Each simulates extracting data from a different source
    extract_sales = BashOperator(
        task_id='extract_sales_data',
        bash_command='echo "Extracting sales data..."; sleep 5; echo "Sales data extracted!"',
    )
    
    extract_customers = BashOperator(
        task_id='extract_customer_data',
        bash_command='echo "Extracting customer data..."; sleep 7; echo "Customer data extracted!"',
    )
    
    extract_inventory = BashOperator(
        task_id='extract_inventory_data',
        bash_command='echo "Extracting inventory data..."; sleep 3; echo "Inventory data extracted!"',
    )
    
    # Process the extracted data - also in parallel
    process_sales = BashOperator(
        task_id='process_sales_data',
        bash_command='echo "Processing sales data..."; sleep 2; echo "Sales processing complete!"',
    )
    
    process_customers = BashOperator(
        task_id='process_customer_data',
        bash_command='echo "Processing customer data..."; sleep 3; echo "Customer processing complete!"',
    )
    
    process_inventory = BashOperator(
        task_id='process_inventory_data',
        bash_command='echo "Processing inventory data..."; sleep 2; echo "Inventory processing complete!"',
    )
    
    # Combine all processed data
    combine_data = BashOperator(
        task_id='combine_all_data',
        bash_command='echo "Combining all processed data..."; sleep 2; echo "Data combination complete!"',
    )
    
    # End task
    end = DummyOperator(
        task_id='end_pipeline',
    )
    
    # Define the task dependencies
    # This is where we create the parallel execution pattern
    start >> [extract_sales, extract_customers, extract_inventory]
    
    # Each extract task leads to its corresponding process task
    extract_sales >> process_sales
    extract_customers >> process_customers
    extract_inventory >> process_inventory
    
    # All process tasks must complete before combining
    [process_sales, process_customers, process_inventory] >> combine_data
    
    # Finally, end the pipeline
    combine_data >> end
