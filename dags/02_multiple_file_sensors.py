from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
import sys

# Add scripts directory to Python path
sys.path.append('/opt/airflow/scripts')
from file_processor import process_afternoon_file, process_all_sales_files, check_files_exist

# DAG Configuration
default_args = {
    'owner': 'file-sensor-tutorial',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# File paths
AFTERNOON_FILE = '/opt/airflow/scripts/input/sales_afternoon.csv'
EVENING_FILE = '/opt/airflow/scripts/input/sales_evening.csv'

def check_multiple_files():
    """Check if both afternoon and evening files exist"""
    exists, files = check_files_exist('sales_*.csv')
    
    print(f"=== MULTIPLE FILE CHECK ===")
    print(f"Pattern check result: {exists}")
    print(f"Files found: {len(files)}")
    
    for file_path in files:
        print(f"  - {file_path}")
    
    # Specifically check for our target files
    afternoon_exists = any('afternoon' in f for f in files)
    evening_exists = any('evening' in f for f in files)
    
    print(f"\nTarget files status:")
    print(f"  Afternoon file: {'✅' if afternoon_exists else '❌'}")
    print(f"  Evening file: {'✅' if evening_exists else '❌'}")
    
    if not afternoon_exists or not evening_exists:
        missing = []
        if not afternoon_exists:
            missing.append('sales_afternoon.csv')
        if not evening_exists:
            missing.append('sales_evening.csv')
        
        raise Exception(f"Missing required files: {missing}")
    
    return {'files_found': len(files), 'afternoon_ready': afternoon_exists, 'evening_ready': evening_exists}

with DAG(
    dag_id='02_multiple_file_sensors',
    description='Multiple file sensors waiting for different files in parallel',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=False,
    max_active_runs=1,
    tags=['tutorial', 'file-sensor', 'multiple', 'parallel']
) as dag:

    # Task 1: Initial setup check
    setup_check = BashOperator(
        task_id='initial_setup_check',
        bash_command="""
        echo "=== MULTIPLE FILE SENSOR SETUP ==="
        echo "Checking for required input files..."
        
        echo "Expected files:"
        echo "  - /opt/airflow/scripts/input/sales_afternoon.csv"
        echo "  - /opt/airflow/scripts/input/sales_evening.csv"
        
        echo -e "\nCurrent input directory:"
        ls -la /opt/airflow/scripts/input/ || echo "Input directory not found"
        
        echo -e "\nFile availability:"
        for file in sales_afternoon.csv sales_evening.csv; do
            if [ -f "/opt/airflow/scripts/input/$file" ]; then
                echo "  ✅ $file found"
            else
                echo "  ❌ $file missing"
            fi
        done
        """
    )
    
    # Task 2a: Wait for afternoon file
    wait_for_afternoon = FileSensor(
        task_id='wait_for_afternoon_sales',
        filepath=AFTERNOON_FILE,
        poke_interval=15,      # Check every 15 seconds
        timeout=600,           # Wait up to 10 minutes
        mode='poke'
    )
    
    # Task 2b: Wait for evening file (runs in parallel with afternoon sensor)
    wait_for_evening = FileSensor(
        task_id='wait_for_evening_sales',
        filepath=EVENING_FILE,
        poke_interval=15,      # Check every 15 seconds
        timeout=600,           # Wait up to 10 minutes
        mode='poke'
    )
    
    # Task 3: Verify both files are ready (only runs after both sensors complete)
    verify_both_files = PythonOperator(
        task_id='verify_both_files_ready',
        python_callable=check_multiple_files,
        doc_md="""
        This task verifies that both afternoon and evening sales files 
        are present and ready for processing. It runs only after both
        file sensors have successfully detected their respective files.
        """
    )
    
    # Task 4a: Process afternoon file
    process_afternoon = PythonOperator(
        task_id='process_afternoon_sales',
        python_callable=process_afternoon_file
    )
    
    # Task 4b: Process evening file 
    process_evening = PythonOperator(
        task_id='process_evening_sales',
        python_callable=lambda: process_single_evening_file()
    )
    
    # Task 5: Create combined analysis of all files
    create_combined_analysis = PythonOperator(
        task_id='create_combined_analysis',
        python_callable=process_all_sales_files,
        doc_md="""
        This task processes all sales files together to create a 
        comprehensive daily analysis. It combines data from all
        processed files and generates consolidated reports.
        """
    )
    
    # Task 6: Generate final reports
    generate_final_reports = BashOperator(
        task_id='generate_final_reports',
        bash_command="""
        echo "=== FINAL PROCESSING SUMMARY ==="
        
        echo "All output files:"
        find /opt/airflow/scripts/output -name "*.csv" -o -name "*.json" -o -name "*.txt" | sort
        
        echo -e "\nArchived files:"
        find /opt/airflow/scripts/processed -name "*.csv" | sort
        
        echo -e "\nProcessing statistics:"
        json_files=$(find /opt/airflow/scripts/output -name "summary_*.json" | wc -l)
        csv_files=$(find /opt/airflow/scripts/output -name "*.csv" | wc -l)
        archived_files=$(find /opt/airflow/scripts/processed -name "*.csv" | wc -l)
        
        echo "  - Summary files created: $json_files"
        echo "  - Report files created: $csv_files"
        echo "  - Files archived: $archived_files"
        
        echo -e "\nLatest combined summary:"
        latest_combined=$(ls -t /opt/airflow/scripts/output/summary_combined_*.json 2>/dev/null | head -1)
        if [ -f "$latest_combined" ]; then
            echo "File: $latest_combined"
            cat "$latest_combined"
        else
            echo "No combined summary found"
        fi
        """
    )

    # Define task dependencies
    # Setup check first
    setup_check >> [wait_for_afternoon, wait_for_evening]
    
    # Both sensors must complete before verification
    [wait_for_afternoon, wait_for_evening] >> verify_both_files
    
    # Process files in parallel after verification
    verify_both_files >> [process_afternoon, process_evening]
    
    # Combined analysis only after both files are processed
    [process_afternoon, process_evening] >> create_combined_analysis
    
    # Final reports after everything is complete
    create_combined_analysis >> generate_final_reports


# Helper function for evening file processing
def process_single_evening_file():
    """Process evening sales file - similar to afternoon but for evening data"""
    import sys
    sys.path.append('/opt/airflow/scripts')
    from file_processor import SalesFileProcessor
    
    processor = SalesFileProcessor(
        input_dir='/opt/airflow/scripts/input',
        output_dir='/opt/airflow/scripts/output',
        processed_dir='/opt/airflow/scripts/processed'
    )
    
    file_path = '/opt/airflow/scripts/input/sales_evening.csv'
    stats, df = processor.process_single_file(file_path)
    processor.create_summary_report(stats, "evening")
    processor.create_category_reports(df, "evening")
    processor.archive_file(file_path)
    
    return stats