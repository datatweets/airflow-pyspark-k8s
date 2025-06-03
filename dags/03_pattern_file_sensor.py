from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
import sys
import os
import glob

# Add scripts directory to Python path
sys.path.append('/opt/airflow/scripts')
from file_processor import SalesFileProcessor, process_all_sales_files  # ← Import the function

# DAG Configuration
default_args = {
    'owner': 'file-sensor-tutorial',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# File pattern to watch for any sales CSV files
SALES_FILE_PATTERN = '/opt/airflow/scripts/input/sales_*.csv'

def scan_and_analyze_pattern():
    """Scan for all files matching the sales pattern and provide detailed analysis"""
    import os
    
    files = glob.glob(SALES_FILE_PATTERN)
    
    print(f"=== PATTERN SCANNING RESULTS ===")
    print(f"Pattern: {SALES_FILE_PATTERN}")
    print(f"Files found: {len(files)}")
    
    if not files:
        raise Exception(f"No files found matching pattern: {SALES_FILE_PATTERN}")
    
    file_analysis = []
    total_size = 0
    
    for file_path in sorted(files):
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        
        # Try to get a preview of the file content
        try:
            with open(file_path, 'r') as f:
                first_line = f.readline().strip()
                line_count = sum(1 for _ in f) + 1  # +1 for the first line we already read
        except Exception as e:
            first_line = f"Error reading file: {str(e)}"
            line_count = 0
        
        file_info = {
            'name': file_name,
            'path': file_path,
            'size': file_size,
            'lines': line_count,
            'modified': mod_time.strftime('%Y-%m-%d %H:%M:%S'),
            'preview': first_line
        }
        
        file_analysis.append(file_info)
        total_size += file_size
        
        print(f"\n📁 {file_name}")
        print(f"   Size: {file_size} bytes")
        print(f"   Lines: {line_count}")
        print(f"   Modified: {file_info['modified']}")
        print(f"   Preview: {first_line[:50]}..." if len(first_line) > 50 else f"   Preview: {first_line}")
    
    summary = {
        'pattern': SALES_FILE_PATTERN,
        'files_found': len(files),
        'total_size_bytes': total_size,
        'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'files': file_analysis
    }
    
    print(f"\n📊 SUMMARY:")
    print(f"   Total files: {len(files)}")
    print(f"   Total size: {total_size} bytes")
    print(f"   Ready for processing: {'✅ YES' if len(files) > 0 else '❌ NO'}")
    
    return summary

def validate_all_files():
    """Validate that all found files are properly formatted sales files"""
    import pandas as pd
    
    files = glob.glob(SALES_FILE_PATTERN)
    validation_results = []
    
    print(f"=== FILE VALIDATION ===")
    
    required_columns = ['Product_ID', 'Product_Name', 'Category', 'Price', 'Quantity_Sold']
    
    for file_path in files:
        file_name = os.path.basename(file_path)
        print(f"\n🔍 Validating: {file_name}")
        
        try:
            # Try to read the CSV
            df = pd.read_csv(file_path)
            
            # Check required columns
            missing_cols = [col for col in required_columns if col not in df.columns]
            extra_cols = [col for col in df.columns if col not in required_columns + ['Sale_Time']]
            
            # Check data types and content
            issues = []
            
            if len(df) == 0:
                issues.append("File is empty")
            
            if missing_cols:
                issues.append(f"Missing columns: {missing_cols}")
            
            # Check for numeric columns
            try:
                pd.to_numeric(df['Price'], errors='raise')
                pd.to_numeric(df['Quantity_Sold'], errors='raise')
            except Exception as e:
                issues.append(f"Invalid numeric data: {str(e)}")
            
            # Check for negative values
            if (df['Price'] < 0).any():
                issues.append("Negative prices found")
            
            if (df['Quantity_Sold'] < 0).any():
                issues.append("Negative quantities found")
            
            validation_result = {
                'file': file_name,
                'valid': len(issues) == 0,
                'records': len(df),
                'columns': list(df.columns),
                'missing_columns': missing_cols,
                'extra_columns': extra_cols,
                'issues': issues
            }
            
            validation_results.append(validation_result)
            
            if validation_result['valid']:
                print(f"   ✅ VALID - {len(df)} records")
            else:
                print(f"   ❌ INVALID - Issues: {', '.join(issues)}")
                
        except Exception as e:
            validation_result = {
                'file': file_name,
                'valid': False,
                'records': 0,
                'columns': [],
                'missing_columns': required_columns,
                'extra_columns': [],
                'issues': [f"Failed to read file: {str(e)}"]
            }
            validation_results.append(validation_result)
            print(f"   ❌ FAILED - Cannot read file: {str(e)}")
    
    # Summary
    valid_files = [r for r in validation_results if r['valid']]
    invalid_files = [r for r in validation_results if not r['valid']]
    
    print(f"\n📊 VALIDATION SUMMARY:")
    print(f"   Total files checked: {len(validation_results)}")
    print(f"   Valid files: {len(valid_files)}")
    print(f"   Invalid files: {len(invalid_files)}")
    
    if invalid_files:
        print(f"\n❌ Invalid files:")
        for result in invalid_files:
            print(f"   - {result['file']}: {', '.join(result['issues'])}")
        
        raise Exception(f"Found {len(invalid_files)} invalid files. Please fix before processing.")
    
    print(f"\n✅ All files are valid and ready for processing!")
    return validation_results

with DAG(
    dag_id='03_pattern_file_sensor',
    description='Pattern-based file sensor that processes any matching sales files',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=False,
    max_active_runs=1,
    tags=['tutorial', 'file-sensor', 'pattern', 'advanced']
) as dag:

    # Task 1: Initial environment check
    environment_check = BashOperator(
        task_id='check_environment',
        bash_command="""
        echo "=== PATTERN FILE SENSOR ENVIRONMENT CHECK ==="
        echo "Looking for sales files matching pattern: sales_*.csv"
        echo "Search directory: /opt/airflow/scripts/input/"
        
        echo -e "\nDirectory contents:"
        ls -la /opt/airflow/scripts/input/ || echo "Input directory not found"
        
        echo -e "\nFiles matching pattern 'sales_*.csv':"
        find /opt/airflow/scripts/input -name "sales_*.csv" -type f || echo "No matching files found"
        
        echo -e "\nCounting potential files:"
        file_count=$(find /opt/airflow/scripts/input -name "sales_*.csv" -type f | wc -l)
        echo "Files found: $file_count"
        
        if [ "$file_count" -eq 0 ]; then
            echo "⚠️  WARNING: No files found matching pattern!"
            echo "Expected files like: sales_morning.csv, sales_afternoon.csv, sales_evening.csv"
        else
            echo "✅ Found $file_count file(s) ready for processing"
        fi
        """
    )
    
    # Task 2: Pattern-based file sensor
    wait_for_sales_files = FileSensor(
        task_id='wait_for_sales_pattern',
        filepath=SALES_FILE_PATTERN,
        poke_interval=20,      # Check every 20 seconds (longer for pattern matching)
        timeout=900,           # Wait up to 15 minutes
        mode='poke',
        doc_md="""
        This sensor waits for ANY file matching the pattern 'sales_*.csv' 
        in the input directory. It will trigger as soon as at least one 
        file matching the pattern is found.
        
        Pattern: /opt/airflow/scripts/input/sales_*.csv
        Examples: sales_morning.csv, sales_afternoon.csv, sales_evening.csv
        """
    )
    
    # Task 3: Scan and analyze all matching files
    scan_pattern_files = PythonOperator(
        task_id='scan_pattern_files',
        python_callable=scan_and_analyze_pattern,
        doc_md="""
        Scans for all files matching the pattern and provides detailed 
        analysis including file sizes, modification times, and content previews.
        """
    )
    
    # Task 4: Validate file formats and content
    validate_files = PythonOperator(
        task_id='validate_file_formats',
        python_callable=validate_all_files,
        doc_md="""
        Validates that all found files are properly formatted sales CSV files
        with the required columns and valid data types. This prevents 
        processing errors downstream.
        """
    )
    
    # Task 5: Process all matching files
    process_all_files = PythonOperator(
        task_id='process_all_matching_files',
        python_callable=process_all_sales_files,  # ← Use the function directly
        doc_md="""
        Processes all valid sales files found by the pattern sensor.
        Creates individual reports for each file and a combined analysis
        of all data together.
        """
    )
    
    # Task 6: Create comprehensive summary
    create_comprehensive_summary = BashOperator(
        task_id='create_comprehensive_summary',
        bash_command="""
        echo "=== COMPREHENSIVE PROCESSING SUMMARY ==="
        echo "Pattern-based file sensor processing complete!"
        
        echo -e "\n📁 INPUT FILES PROCESSED:"
        find /opt/airflow/scripts/processed -name "sales_*_processed_*.csv" -exec basename {} \; | sort
        
        echo -e "\n📊 OUTPUT REPORTS GENERATED:"
        find /opt/airflow/scripts/output -name "*.csv" -o -name "*.json" -o -name "*.txt" | sort
        
        echo -e "\n💾 STORAGE SUMMARY:"
        input_size=$(find /opt/airflow/scripts/processed -name "sales_*.csv" -exec stat -f%z {} \; 2>/dev/null | awk '{sum+=$1} END {print sum}' || echo "0")
        output_size=$(find /opt/airflow/scripts/output -name "*.*" -exec stat -f%z {} \; 2>/dev/null | awk '{sum+=$1} END {print sum}' || echo "0")
        
        echo "  Original files processed: ${input_size:-0} bytes"
        echo "  Reports generated: ${output_size:-0} bytes"
        
        echo -e "\n📈 FINAL STATISTICS:"
        
        # Try to extract statistics from the latest combined summary
        latest_summary=$(ls -t /opt/airflow/scripts/output/summary_combined_*.json 2>/dev/null | head -1)
        if [ -f "$latest_summary" ]; then
            echo "  Latest combined analysis:"
            echo "  $(cat "$latest_summary" | grep -o '"total_revenue": [0-9.]*' | cut -d':' -f2 | tr -d ' ')"
            echo "  $(cat "$latest_summary" | grep -o '"total_records": [0-9]*' | cut -d':' -f2 | tr -d ' ')"
            echo "  $(cat "$latest_summary" | grep -o '"files_processed": [0-9]*' | cut -d':' -f2 | tr -d ' ')"
        else
            echo "  No combined summary available"
        fi
        
        echo -e "\n✅ PATTERN FILE SENSOR TUTORIAL COMPLETE!"
        echo "All files matching 'sales_*.csv' have been successfully processed."
        """
    )
    
    # Task 7: Clean up demonstration
    cleanup_demo = BashOperator(
        task_id='cleanup_demonstration',
        bash_command="""
        echo "=== CLEANUP DEMONSTRATION ==="
        echo "This task demonstrates how you might clean up after processing"
        echo "(In this tutorial, we'll just show what would be cleaned up)"
        
        echo -e "\nFiles that could be cleaned up:"
        echo "Temporary processing files:"
        find /opt/airflow/scripts/output -name "*.tmp" 2>/dev/null || echo "  (none found)"
        
        echo -e "\nOld archived files (older than 7 days):"
        find /opt/airflow/scripts/processed -name "*.csv" -mtime +7 2>/dev/null || echo "  (none found)"
        
        echo -e "\nDisk space summary:"
        du -sh /opt/airflow/scripts/* 2>/dev/null || echo "  Unable to calculate"
        
        echo -e "\n💡 TIP: In production, you might:"
        echo "  - Compress old files"
        echo "  - Move files to long-term storage"
        echo "  - Send notifications about processing completion"
        echo "  - Update monitoring dashboards"
        
        echo -e "\n🎯 TUTORIAL COMPLETE!"
        echo "You've successfully learned about file sensors in Apache Airflow!"
        """
    )

    # Define task dependencies - linear flow for pattern processing
    environment_check >> wait_for_sales_files >> scan_pattern_files
    scan_pattern_files >> validate_files >> process_all_files
    process_all_files >> create_comprehensive_summary >> cleanup_demo