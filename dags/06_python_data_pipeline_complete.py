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
 # Task 2: Filter high-value sales
def filter_high_value_sales(**context):
    """
    Filter for sales transactions above a certain threshold.
    This demonstrates how to receive data from a previous task.
    """
    # Pull data from the previous task using XCom
    # The context parameter gives us access to Airflow's internal state
    task_instance = context['task_instance']
    sales_data = task_instance.xcom_pull(task_ids='create_sales_data')
    
    # Convert back to DataFrame
    df = pd.DataFrame(sales_data)
    
    print(f"Received {len(df)} sales records")
    
    # Filter for high-value transactions (total_sales > 1000)
    high_value_sales = df[df['total_sales'] > 1000]
    
    print(f"\nFound {len(high_value_sales)} high-value transactions:")
    print(high_value_sales)
    
    # Return filtered data for next task
    return high_value_sales.to_dict('records')

def calculate_store_totals(**context):
    """
    Group sales by store and calculate totals.
    This shows how to perform aggregations on the data.
    """
    # Get the original sales data
    task_instance = context['task_instance']
    sales_data = task_instance.xcom_pull(task_ids='create_sales_data')
    
    # Convert to DataFrame
    df = pd.DataFrame(sales_data)
    
    # Group by store and calculate total sales
    store_totals = df.groupby('store')['total_sales'].agg(['sum', 'count', 'mean'])
    store_totals = store_totals.round(2)  # Round for cleaner display
    
    print("\nStore Performance Summary:")
    print(store_totals)
    
    # Convert to dictionary for XCom
    return store_totals.to_dict()

def analyze_product_categories(**context):
    """
    Analyze sales by product category.
    Another example of grouping and aggregation.
    """
    # Get the original sales data
    task_instance = context['task_instance']
    sales_data = task_instance.xcom_pull(task_ids='create_sales_data')
    
    # Convert to DataFrame
    df = pd.DataFrame(sales_data)
    
    # Group by category
    category_analysis = df.groupby('category').agg({
        'quantity': 'sum',
        'total_sales': 'sum',
        'product': 'count'  # Count number of transactions
    })
    category_analysis.columns = ['total_quantity', 'total_revenue', 'transaction_count']
    
    print("\nCategory Analysis:")
    print(category_analysis)
    
    return category_analysis.to_dict()
# Continuing in the same file...

def create_summary_report(**context):
    """
    Create a final summary report combining insights from all analyses.
    This demonstrates pulling data from multiple upstream tasks.
    """
    task_instance = context['task_instance']
    
    # Pull data from multiple previous tasks
    high_value_sales = task_instance.xcom_pull(task_ids='filter_high_value')
    store_totals = task_instance.xcom_pull(task_ids='calculate_store_totals')
    category_analysis = task_instance.xcom_pull(task_ids='analyze_categories')
    
    print("=" * 50)
    print("SALES ANALYSIS SUMMARY REPORT")
    print("=" * 50)
    
    print(f"\n1. High-Value Transactions: {len(high_value_sales)} transactions over $1000")
    
    print("\n2. Store Performance:")
    for store, metrics in store_totals.items():
        if store == 'sum':
            print(f"   - Total Sales: ${metrics}")
        elif store == 'count':
            print(f"   - Total Transactions: {metrics}")
    
    print("\n3. Category Insights:")
    for metric, categories in category_analysis.items():
        print(f"   {metric}:")
        for category, value in categories.items():
            print(f"     - {category}: {value}")
    
    print("\n" + "=" * 50)
    
    return "Summary report generated successfully"

# Now, within our DAG context, let's create all the tasks
with DAG(
    dag_id='06_python_sales_analysis_complete',
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
        python_callable=create_sales_data,
    )
    
    # Task 2: Filter high-value sales
    filter_task = PythonOperator(
        task_id='filter_high_value',
        python_callable=filter_high_value_sales,
        provide_context=True,  # This enables the **context parameter
    )
    
    # Task 3: Calculate store totals
    store_totals_task = PythonOperator(
        task_id='calculate_store_totals',
        python_callable=calculate_store_totals,
        provide_context=True,
    )
    
    # Task 4: Analyze product categories
    category_task = PythonOperator(
        task_id='analyze_categories',
        python_callable=analyze_product_categories,
        provide_context=True,
    )
    
    # Task 5: Create summary report
    summary_task = PythonOperator(
        task_id='create_summary',
        python_callable=create_summary_report,
        provide_context=True,
    )
    
    # Define task dependencies
    # First, create the data
    create_data_task >> [filter_task, store_totals_task, category_task]
    
    # Then, all analysis tasks must complete before the summary
    [filter_task, store_totals_task, category_task] >> summary_task
