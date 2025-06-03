from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import os

# Configuration
default_args = {
    'owner': 'file-sensor-tutorial',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False
}

# File paths (as seen from inside the Airflow container)
WATCH_FILE = '/opt/airflow/scripts/input/laptops_morning.csv'
OUTPUT_DIR = '/opt/airflow/scripts/output'
SQL_DIR = '/opt/airflow/scripts/sql'

def check_file_and_create_sql():
    """
    Check if file exists, read it, and create SQL statements for PostgresOperator
    """
    if not os.path.exists(WATCH_FILE):
        raise FileNotFoundError(f"File not found: {WATCH_FILE}")
    
    # Read the CSV file
    print(f"Reading file: {WATCH_FILE}")
    df = pd.read_csv(WATCH_FILE)
    
    # Display basic information
    print(f"File processed successfully!")
    print(f"Total records: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Create summary statistics
    summary = {
        'total_laptops': len(df),
        'avg_price': df['Price_euros'].mean(),
        'min_price': df['Price_euros'].min(),
        'max_price': df['Price_euros'].max(),
        'laptop_types': df['TypeName'].value_counts().to_dict()
    }
    
    print("=== LAPTOP SUMMARY ===")
    print(f"Total laptops: {summary['total_laptops']}")
    print(f"Average price: €{summary['avg_price']:.2f}")
    print(f"Price range: €{summary['min_price']:.2f} - €{summary['max_price']:.2f}")
    print("Laptop types breakdown:")
    for laptop_type, count in summary['laptop_types'].items():
        print(f"  - {laptop_type}: {count}")
    
    # Create SQL INSERT statements
    os.makedirs(SQL_DIR, exist_ok=True)
    insert_sql_file = f"{SQL_DIR}/insert_morning_data.sql"
    
    with open(insert_sql_file, 'w') as f:
        f.write("-- Auto-generated SQL from CSV file\n")
        f.write(f"-- Source: {WATCH_FILE}\n")
        f.write(f"-- Generated: {datetime.now()}\n\n")
        
        for _, row in df.iterrows():
            sql = f"""INSERT INTO laptops (id, company, product, type_name, price_euros, sale_date) 
VALUES ({row['Id']}, '{row['Company']}', '{row['Product']}', '{row['TypeName']}', {row['Price_euros']}, '{row['Sale_Date']}')
ON CONFLICT (id) DO UPDATE SET
    company = EXCLUDED.company,
    product = EXCLUDED.product,
    type_name = EXCLUDED.type_name,
    price_euros = EXCLUDED.price_euros,
    sale_date = EXCLUDED.sale_date;
"""
            f.write(sql + "\n")
    
    print(f"SQL insert statements created: {insert_sql_file}")
    
    # Create summary file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary_file = f"{OUTPUT_DIR}/morning_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(summary_file, 'w') as f:
        f.write("MORNING LAPTOP SALES SUMMARY\n")
        f.write("===========================\n\n")
        f.write(f"Processing time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Source file: {WATCH_FILE}\n")
        f.write(f"Total laptops: {summary['total_laptops']}\n")
        f.write(f"Average price: €{summary['avg_price']:.2f}\n")
        f.write(f"Price range: €{summary['min_price']:.2f} - €{summary['max_price']:.2f}\n\n")
        f.write("Laptop types breakdown:\n")
        for laptop_type, count in summary['laptop_types'].items():
            f.write(f"  - {laptop_type}: {count}\n")
    
    print(f"Summary saved to: {summary_file}")
    return {"records": len(df), "summary_file": summary_file}

def archive_processed_file():
    """Move the processed file to archive directory"""
    processed_dir = '/opt/airflow/scripts/processed'
    os.makedirs(processed_dir, exist_ok=True)
    
    if os.path.exists(WATCH_FILE):
        # Create new filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = os.path.basename(WATCH_FILE)
        name, ext = os.path.splitext(file_name)
        new_name = f"{name}_{timestamp}{ext}"
        
        dest_path = os.path.join(processed_dir, new_name)
        os.rename(WATCH_FILE, dest_path)
        print(f"File archived: {WATCH_FILE} -> {dest_path}")
        return dest_path
    else:
        print("File already processed or moved")
        return None

# Define the DAG
with DAG(
    dag_id='01_simple_file_sensor',
    description='Basic file sensor that waits for a single CSV file and processes it',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',  # Run only when triggered
    catchup=False,
    tags=['tutorial', 'file-sensor', 'basic'],
    template_searchpath='/opt/airflow/scripts/sql'  # Tell Airflow where to find SQL files
) as dag:

    # Task 1: Create database tables
    create_tables = PostgresOperator(
        task_id='create_database_tables',
        postgres_conn_id='postgres_laptop_db',
        sql='create_tables.sql'
    )

    # Task 2: Wait for the file to appear
    wait_for_file = FileSensor(
        task_id='wait_for_morning_file',
        filepath=WATCH_FILE,
        poke_interval=10,     # Check every 10 seconds
        timeout=300,          # Give up after 5 minutes
        mode='poke',          # Keep checking in the same task instance
        soft_fail=False       # Fail the task if timeout is reached
    )
    
    # Task 3: Process the file and create SQL
    process_file = PythonOperator(
        task_id='process_file_create_sql',
        python_callable=check_file_and_create_sql
    )
    
    # Task 4: Insert data using PostgresOperator
    insert_data = PostgresOperator(
        task_id='insert_laptop_data',
        postgres_conn_id='postgres_laptop_db',
        sql='insert_morning_data.sql'
    )
    
    # Task 5: Log the processing
    log_processing = PostgresOperator(
        task_id='log_file_processing',
        postgres_conn_id='postgres_laptop_db',
        sql="""
        INSERT INTO file_processing_log (file_name, records_processed, status)
        VALUES ('laptops_morning.csv', 
                (SELECT COUNT(*) FROM laptops WHERE sale_date = '2024-01-15'), 
                'SUCCESS');
        """
    )
    
    # Task 6: Create summary report using database
    create_summary = PostgresOperator(
        task_id='create_database_summary',
        postgres_conn_id='postgres_laptop_db',
        sql="""
        -- Display summary of processed data
        SELECT 
            'Total Laptops' as metric,
            COUNT(*)::text as value
        FROM laptops
        UNION ALL
        SELECT 
            'Average Price',
            '€' || ROUND(AVG(price_euros), 2)::text
        FROM laptops
        UNION ALL
        SELECT 
            'Gaming Laptops',
            COUNT(*)::text
        FROM laptops WHERE type_name = 'Gaming'
        UNION ALL
        SELECT 
            'Business Laptops', 
            COUNT(*)::text
        FROM laptops WHERE type_name IN ('Notebook', 'Ultrabook');
        """
    )
    
    # Task 7: Archive the processed file
    archive_file = PythonOperator(
        task_id='archive_processed_file',
        python_callable=archive_processed_file
    )
    
    # Define task sequence
    create_tables >> wait_for_file >> process_file >> insert_data >> log_processing >> create_summary >> archive_file