from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
import sys
import os

# Add scripts directory to Python path so we can import our processing functions
sys.path.append('/opt/airflow/scripts')
from file_processor import process_morning_file

# DAG Configuration
default_args = {
    'owner': 'file-sensor-tutorial',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False
}

# File paths (as seen from inside Airflow container)
MORNING_FILE = '/opt/airflow/scripts/input/sales_morning.csv'

with DAG(
    dag_id='01_basic_file_sensor',
    description='Basic file sensor example - waits for morning sales file',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',  # Only run when manually triggered
    catchup=False,
    max_active_runs=1,  # Prevent multiple runs at the same time
    tags=['tutorial', 'file-sensor', 'basic', 'beginner']
) as dag:

    # Task 1: Check directory structure
    check_setup = BashOperator(
        task_id='check_directory_setup',
        bash_command="""
        echo "=== CHECKING DIRECTORY SETUP ==="
        echo "Input directory:"
        ls -la /opt/airflow/scripts/input/ || echo "Input directory not found"
        
        echo -e "\nOutput directory:"
        ls -la /opt/airflow/scripts/output/ || echo "Output directory not found"
        
        echo -e "\nProcessed directory:"
        ls -la /opt/airflow/scripts/processed/ || echo "Processed directory not found"
        
        echo -e "\nLooking for morning sales file..."
        if [ -f "/opt/airflow/scripts/input/sales_morning.csv" ]; then
            echo "✅ Morning sales file found"
            echo "File details:"
            ls -la /opt/airflow/scripts/input/sales_morning.csv
            echo "File preview:"
            head -3 /opt/airflow/scripts/input/sales_morning.csv
        else
            echo "❌ Morning sales file NOT found"
            echo "Expected location: /opt/airflow/scripts/input/sales_morning.csv"
        fi
        """
    )
    
    # Task 2: File Sensor - Wait for morning sales file
    wait_for_morning_file = FileSensor(
        task_id='wait_for_morning_sales',
        filepath=MORNING_FILE,
        poke_interval=10,      # Check every 10 seconds
        timeout=300,           # Give up after 5 minutes (300 seconds)
        mode='poke',           # Keep checking in the same task instance
        soft_fail=False,       # Fail the DAG if file doesn't arrive in time
        retries=0              # Don't retry the sensor itself
    )
    
    # Task 3: Verify file is readable
    verify_file = BashOperator(
        task_id='verify_file_readable',
        bash_command=f"""
        echo "=== VERIFYING FILE ==="
        echo "File path: {MORNING_FILE}"
        
        if [ ! -f "{MORNING_FILE}" ]; then
            echo "❌ File does not exist!"
            exit 1
        fi
        
        echo "✅ File exists"
        echo "File size: $(stat -f%z "{MORNING_FILE}" 2>/dev/null || stat -c%s "{MORNING_FILE}")"
        echo "File permissions: $(ls -la "{MORNING_FILE}")"
        
        # Check if file is readable and has content
        if [ ! -r "{MORNING_FILE}" ]; then
            echo "❌ File is not readable!"
            exit 1
        fi
        
        line_count=$(wc -l < "{MORNING_FILE}")
        if [ "$line_count" -lt 2 ]; then
            echo "❌ File appears empty or has no data rows!"
            exit 1
        fi
        
        echo "✅ File is readable with $line_count lines"
        echo "First few lines:"
        head -5 "{MORNING_FILE}"
        """
    )
    
    # Task 4: Process the file using our separated processing script
    process_file = PythonOperator(
        task_id='process_morning_sales',
        python_callable=process_morning_file,
        doc_md="""
        This task processes the morning sales file using the separated 
        file_processor.py script. It will:
        1. Read and validate the CSV file
        2. Calculate sales statistics
        3. Create summary reports (JSON and text)
        4. Create category-specific reports
        5. Archive the processed file
        """
    )
    
    # Task 5: Show processing results
    show_results = BashOperator(
        task_id='show_processing_results',
        bash_command="""
        echo "=== PROCESSING RESULTS ==="
        
        echo "Output files created:"
        if [ -d "/opt/airflow/scripts/output" ]; then
            ls -la /opt/airflow/scripts/output/
        else
            echo "No output directory found"
        fi
        
        echo -e "\nArchived files:"
        if [ -d "/opt/airflow/scripts/processed" ]; then
            ls -la /opt/airflow/scripts/processed/
        else
            echo "No processed directory found"
        fi
        
        echo -e "\nLatest summary (if available):"
        latest_json=$(ls -t /opt/airflow/scripts/output/summary_morning_*.json 2>/dev/null | head -1)
        if [ -f "$latest_json" ]; then
            echo "Summary file: $latest_json"
            cat "$latest_json"
        else
            echo "No summary file found"
        fi
        """
    )

    # Define task dependencies
    # This creates a linear workflow: check → wait → verify → process → show results
    check_setup >> wait_for_morning_file >> verify_file >> process_file >> show_results