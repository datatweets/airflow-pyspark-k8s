from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

# Define default arguments for our DAG
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# This is our first Python function - it creates sample sales data
def create_sales_data():
    """
    Generate sample sales data for our pipeline.
    In a real scenario, this might read from a database or file.
    """
    # Creating a sample dataset embedded in our code
    data = {
        'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', 
                 '2024-01-03', '2024-01-03', '2024-01-04', '2024-01-04'],
        'product': ['Laptop', 'Mouse', 'Laptop', 'Keyboard', 
                    'Monitor', 'Mouse', 'Laptop', 'Monitor'],
        'category': ['Electronics', 'Accessories', 'Electronics', 'Accessories',
                     'Electronics', 'Accessories', 'Electronics', 'Electronics'],
        'quantity': [2, 5, 1, 3, 2, 10, 3, 1],
        'price': [1200, 25, 1200, 75, 450, 25, 1200, 450],
        'store': ['Store_A', 'Store_A', 'Store_B', 'Store_B',
                  'Store_A', 'Store_B', 'Store_A', 'Store_B']
    }
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Calculate total sales for each row
    df['total_sales'] = df['quantity'] * df['price']
    
    print(f"Created sales data with {len(df)} records")
    print("\nFirst few rows:")
    print(df.head())
    
    # Return the data so other tasks can use it
    return df.to_dict('records')

# Define our DAG
with DAG(
    dag_id='05_python_sales_analysis',
    description='Analyze sales data using Python operators',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['python-operator', 'tutorial'],
) as dag:
    
    # Task 1: Create the sales data
    create_data_task = PythonOperator(
        task_id='create_sales_data',
        python_callable=create_sales_data,  # The function to run
    )
