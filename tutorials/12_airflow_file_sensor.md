# Apache Airflow File Sensors Tutorial

## What Are Sensors?

Sensors in Apache Airflow are special operators that **wait** for something to happen before allowing the workflow to continue. Think of them as "checkpoint guards" that pause your pipeline until a specific condition is met.

**Key characteristics of sensors:**

- They don't process data - they just wait and watch
- They check conditions repeatedly until met or timeout
- They coordinate timing between different parts of your pipeline
- They make workflows robust by handling unpredictable timing

## What Are File Sensors?

File sensors specifically watch the filesystem for files to appear. They're essential when:

- Waiting for data files from external systems
- Processing files that arrive at unpredictable times
- Coordinating with other processes that create files
- Building ETL pipelines that depend on file arrival

**Real-world examples:**

- Waiting for daily sales reports from retail systems
- Processing log files from web servers
- Handling data uploads from mobile apps
- Coordinating with external vendor file drops

## Tutorial Setup

### Step 1: Directory Structure

Create a clean directory structure separating DAGs from scripts:

```bash
cd /Users/lotfinejad/airflow-pyspark-k8s

# Create directory structure
mkdir -p scripts/input
mkdir -p scripts/output
mkdir -p scripts/processed
```

Your structure:

```
airflow-pyspark-k8s/
├── dags/              # Only DAG files
├── scripts/           # All scripts and data
│   ├── input/         # Where files arrive
│   ├── output/        # Processed results
│   └── processed/     # Archived files
```

### Step 2: Create Test Data Files

First, let's create some sample CSV files to simulate file arrivals.

**File: `scripts/input/sales_morning.csv`**

```bash
Product_ID,Product_Name,Category,Price,Quantity_Sold,Sale_Time
101,Laptop Dell,Electronics,799.99,3,09:15
102,Office Chair,Furniture,149.50,7,09:30
103,Coffee Maker,Appliances,89.99,12,09:45
104,Smartphone,Electronics,599.00,5,10:00
105,Desk Lamp,Furniture,29.99,15,10:15
```

**File: `scripts/input/sales_afternoon.csv`**

```bash
Product_ID,Product_Name,Category,Price,Quantity_Sold,Sale_Time
201,Gaming Laptop,Electronics,1299.99,2,14:20
202,Standing Desk,Furniture,399.00,4,14:35
203,Blender,Appliances,69.99,8,14:50
204,Tablet,Electronics,349.00,6,15:05
205,Bookshelf,Furniture,129.99,3,15:20
```

**File: `scripts/input/sales_evening.csv`**	

```bash
Product_ID,Product_Name,Category,Price,Quantity_Sold,Sale_Time
301,Monitor 4K,Electronics,449.99,4,19:10
302,Ergonomic Chair,Furniture,299.50,2,19:25
303,Air Fryer,Appliances,129.99,6,19:40
304,Wireless Mouse,Electronics,39.99,12,19:55
305,Filing Cabinet,Furniture,199.00,1,20:10
```



### Step 3: Create Python Processing Scripts

These scripts will be separate from the DAGs and handle the actual file processing.

**File: `scripts/file_processor.py`**

```python
#!/usr/bin/env python3
"""
File processing utilities for Airflow File Sensor tutorial
This script contains all the file processing logic separate from DAG definitions
"""

import pandas as pd
import os
import json
from datetime import datetime
import glob

class SalesFileProcessor:
    """Handles processing of sales CSV files"""
    
    def __init__(self, input_dir, output_dir, processed_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.processed_dir = processed_dir
        
        # Ensure directories exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
    
    def process_single_file(self, file_path):
        """Process a single CSV file and return statistics"""
        try:
            print(f"📖 Reading file: {file_path}")
            
            # Read CSV file
            df = pd.read_csv(file_path)
            
            print(f"✅ File loaded successfully: {len(df)} rows, {len(df.columns)} columns")
            print(f"📊 Columns: {list(df.columns)}")
            
            # Validate required columns (FIXED - match actual CSV structure)
            required_columns = ['Product_Name', 'Category', 'Price', 'Quantity_Sold']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Clean and validate data
            df = df.dropna()  # Remove rows with missing values
            
            # Ensure numeric columns are properly typed
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
            df['Quantity_Sold'] = pd.to_numeric(df['Quantity_Sold'], errors='coerce')
            
            # Remove rows where conversion failed
            df = df.dropna()
            
            if len(df) == 0:
                raise ValueError("No valid data rows after cleaning")
            
            # Calculate statistics (FIXED - use correct column name)
            stats = {
                'file_name': os.path.basename(file_path),
                'total_records': len(df),
                'total_products': df['Product_Name'].nunique(),  # ← Changed from 'Product'
                'total_categories': df['Category'].nunique(),
                'total_revenue': (df['Price'] * df['Quantity_Sold']).sum(),
                'average_price': df['Price'].mean(),
                'total_quantity': df['Quantity_Sold'].sum(),
                'processed_at': datetime.now().isoformat()
            }
            
            # Round numeric values for cleaner output
            stats['total_revenue'] = round(float(stats['total_revenue']), 2)
            stats['average_price'] = round(float(stats['average_price']), 2)
            stats['total_quantity'] = int(stats['total_quantity'])
            
            print(f"💰 Total Revenue: ${stats['total_revenue']:,.2f}")
            print(f"📦 Total Products: {stats['total_products']}")
            print(f"🏷️  Total Categories: {stats['total_categories']}")
            
            return stats, df
            
        except Exception as e:
            print(f"❌ Error processing file {file_path}: {str(e)}")
            raise
    
    def create_summary_report(self, stats, file_suffix=""):
        """Create a detailed summary report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # JSON summary
        json_file = os.path.join(self.output_dir, f'summary_{file_suffix}_{timestamp}.json')
        with open(json_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        # Text summary - handle both single file and combined stats
        txt_file = os.path.join(self.output_dir, f'summary_{file_suffix}_{timestamp}.txt')
        with open(txt_file, 'w') as f:
            f.write("SALES DATA PROCESSING SUMMARY\n")
            f.write("============================\n\n")
            
            # Handle different stat structures
            if 'file_name' in stats:
                # Single file stats
                f.write(f"File: {stats['file_name']}\n")
                f.write(f"Processing Time: {stats['processed_at']}\n")
                f.write(f"Total Records: {stats['total_records']}\n")
                f.write(f"Total Revenue: ${stats['total_revenue']:.2f}\n")
                f.write(f"Total Items Sold: {stats['total_quantity']}\n")
                f.write(f"Average Price: ${stats['average_price']:.2f}\n")
                f.write(f"Total Products: {stats['total_products']}\n")
                f.write(f"Total Categories: {stats['total_categories']}\n")
            else:
                # Combined file stats
                f.write(f"Combined Analysis Report\n")
                f.write(f"Processing Time: {stats.get('processing_time', 'Unknown')}\n")
                f.write(f"Files Processed: {stats.get('files_processed', 0)}\n")
                f.write(f"Total Records: {stats.get('total_records', 0)}\n")
                f.write(f"Total Revenue: ${stats.get('total_revenue', 0):.2f}\n")
                f.write(f"Total Items Sold: {stats.get('total_items_sold', 0)}\n")
                f.write(f"Average Price: ${stats.get('avg_price', 0):.2f}\n")
            
            # Add category breakdown if available
            if 'categories' in stats:
                f.write("\nCategories Breakdown:\n")
                for category, count in stats.get('categories', {}).items():
                    f.write(f"  - {category}: {count} items\n")
            
            # Add file breakdown for combined reports
            if 'file_breakdown' in stats:
                f.write("\nFile Processing Details:\n")
                for file_stat in stats['file_breakdown']:
                    f.write(f"  - {file_stat['file_name']}: ${file_stat['total_revenue']:.2f} revenue\n")
    
        print(f"  📊 Reports saved: {json_file}, {txt_file}")
        return json_file, txt_file
    
    def create_category_reports(self, df, file_suffix=""):
        """Create separate reports for each category"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        category_files = []
        
        for category in df['Category'].unique():
            category_df = df[df['Category'] == category]
            category_file = os.path.join(
                self.output_dir, 
                f'{category.lower()}_{file_suffix}_{timestamp}.csv'
            )
            category_df.to_csv(category_file, index=False)
            category_files.append(category_file)
            
            revenue = (category_df['Price'] * category_df['Quantity_Sold']).sum()
            print(f"  📁 {category}: {len(category_df)} products, ${revenue:.2f} revenue")
        
        return category_files
    
    def archive_file(self, file_path):
        """Move processed file to archive directory"""
        if not os.path.exists(file_path):
            print(f"  ⚠️  File not found for archiving: {file_path}")
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = os.path.basename(file_path)
        name, ext = os.path.splitext(file_name)
        archived_name = f"{name}_processed_{timestamp}{ext}"
        
        dest_path = os.path.join(self.processed_dir, archived_name)
        os.rename(file_path, dest_path)
        
        print(f"  📦 File archived: {archived_name}")
        return dest_path
    
    def check_file_pattern(self, pattern):
        """Check for files matching a pattern"""
        full_pattern = os.path.join(self.input_dir, pattern)
        files = glob.glob(full_pattern)
        
        print(f"🔍 Checking pattern: {pattern}")
        print(f"   Found {len(files)} matching files:")
        
        for file_path in files:
            file_size = os.path.getsize(file_path)
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            print(f"   - {os.path.basename(file_path)} ({file_size} bytes, modified: {mod_time})")
        
        return files
    
    def process_multiple_files(self, file_pattern):
        """Process multiple files matching a pattern"""
        files = self.check_file_pattern(file_pattern)
        
        if not files:
            raise Exception(f"No files found matching pattern: {file_pattern}")
        
        all_stats = []
        combined_data = []
        
        print(f"\n🚀 Processing {len(files)} files...")
        
        for file_path in files:
            try:
                stats, df = self.process_single_file(file_path)
                all_stats.append(stats)
                combined_data.append(df)
            except Exception as e:
                print(f"  ❌ Error processing {file_path}: {str(e)}")
                continue
        
        # Create combined analysis
        if combined_data:
            combined_df = pd.concat(combined_data, ignore_index=True)
            
            combined_stats = {
                'processing_time': datetime.now().isoformat(),
                'files_processed': len(all_stats),
                'total_records': len(combined_df),
                'total_revenue': float((combined_df['Price'] * combined_df['Quantity_Sold']).sum()),
                'total_items_sold': int(combined_df['Quantity_Sold'].sum()),
                'avg_price': float(combined_df['Price'].mean()),
                'categories': combined_df['Category'].value_counts().to_dict(),
                'file_breakdown': all_stats
            }
            
            print(f"\n📊 COMBINED RESULTS:")
            print(f"   Files processed: {combined_stats['files_processed']}")
            print(f"   Total records: {combined_stats['total_records']}")
            print(f"   Total revenue: ${combined_stats['total_revenue']:.2f}")
            
            return combined_stats, combined_df, files
        
        return None, None, files

# Convenience functions for Airflow tasks
def process_morning_file():
    """Process morning sales file"""
    processor = SalesFileProcessor(
        input_dir='/opt/airflow/scripts/input',
        output_dir='/opt/airflow/scripts/output',
        processed_dir='/opt/airflow/scripts/processed'
    )
    
    file_path = '/opt/airflow/scripts/input/sales_morning.csv'
    stats, df = processor.process_single_file(file_path)
    processor.create_summary_report(stats, "morning")
    processor.create_category_reports(df, "morning")
    # Don't archive yet - let combined analysis happen first
    # processor.archive_file(file_path)
    
    return stats

def process_afternoon_file():
    """Process afternoon sales file"""
    processor = SalesFileProcessor(
        input_dir='/opt/airflow/scripts/input',
        output_dir='/opt/airflow/scripts/output',
        processed_dir='/opt/airflow/scripts/processed'
    )
    
    file_path = '/opt/airflow/scripts/input/sales_afternoon.csv'
    stats, df = processor.process_single_file(file_path)
    processor.create_summary_report(stats, "afternoon")
    processor.create_category_reports(df, "afternoon")
    # Don't archive yet - let combined analysis happen first
    # processor.archive_file(file_path)
    
    return stats

def process_all_sales_files():
    """Process all sales files matching pattern"""
    processor = SalesFileProcessor(
        input_dir='/opt/airflow/scripts/input',
        output_dir='/opt/airflow/scripts/output',
        processed_dir='/opt/airflow/scripts/processed'
    )
    
    combined_stats, combined_df, files = processor.process_multiple_files('sales_*.csv')
    
    if combined_stats:
        processor.create_summary_report(combined_stats, "combined")
        processor.create_category_reports(combined_df, "combined")
        
        # NOW archive all processed files
        for file_path in files:
            processor.archive_file(file_path)
    
    return combined_stats

def check_files_exist(pattern='sales_*.csv'):
    """Check if files exist - utility function for sensors"""
    processor = SalesFileProcessor(
        input_dir='/opt/airflow/scripts/input',
        output_dir='/opt/airflow/scripts/output',
        processed_dir='/opt/airflow/scripts/processed'
    )
    
    files = processor.check_file_pattern(pattern)
    return len(files) > 0, files
```





## Part 1: Basic File Sensor

Now let's create our first DAG that demonstrates a simple file sensor.

### Step 4: Simple File Sensor DAG

**File: `dags/01_basic_file_sensor.py`**

```python
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
```



## Part 2: Multiple File Sensors

Let's create a more advanced example with multiple file sensors running in parallel.

### Step 5: Multiple File Sensors DAG

**File: `dags/02_multiple_file_sensors.py`**

```python
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
```



## Part 3: Pattern-Based File Sensor

Now let's create the most advanced example using file patterns.

### Step 6: Pattern-Based File Sensor DAG

**File: `dags/03_pattern_file_sensor.py`**

```python
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
```



## Testing Your File Sensor Tutorial

### Step 7: Test All Three DAGs

Now let's test each DAG to understand how file sensors work:

#### Test 1: Basic File Sensor

1. **Create the test file**:

   ```bash
   # Make sure the morning sales file exists
   cp /Users/lotfinejad/airflow-pyspark-k8s/scripts/input/sales_morning.csv /Users/lotfinejad/airflow-pyspark-k8s/scripts/input/
   ```

2. **Run the DAG**:

   - Go to Airflow UI (`http://localhost:30080`)
   - Find "01_basic_file_sensor"
   - Trigger it manually
   - Watch the sensor wait and then process the file

#### Test 2: Multiple File Sensors

1. **Ensure both files exist**:

   ```bash
   cp /Users/lotfinejad/airflow-pyspark-k8s/scripts/input/sales_afternoon.csv /Users/lotfinejad/airflow-pyspark-k8s/scripts/input/
   cp /Users/lotfinejad/airflow-pyspark-k8s/scripts/input/sales_evening.csv /Users/lotfinejad/airflow-pyspark-k8s/scripts/input/
   ```

2. **Run the DAG**:

   - Find "02_multiple_file_sensors"
   - Trigger it
   - Observe how both sensors run in parallel

#### Test 3: Pattern-Based File Sensor

1. **Create additional test files**:

   ```bash
   # Create a special sales file to demonstrate pattern matching
   echo "Product_ID,Product_Name,Category,Price,Quantity_Sold,Sale_Time
   401,Weekend Special,Electronics,199.99,10,22:00
   402,Night Sale Item,Furniture,99.50,5,22:30" > /Users/lotfinejad/airflow-pyspark-k8s/scripts/input/sales_weekend.csv
   ```

2. **Run the DAG**:

   - Find "03_pattern_file_sensor"
   - Trigger it
   - See how it processes ALL files matching `sales_*.csv`

## Understanding File Sensor Behavior

### Key Concepts Demonstrated

1. **Basic File Sensor**:
   - Waits for a specific file
   - Simple linear processing
   - File archiving after processing
2. **Multiple File Sensors**:
   - Parallel waiting for different files
   - Coordination between sensors
   - Combined processing after all files arrive
3. **Pattern-Based Sensors**:
   - Flexible file matching
   - Processing unknown numbers of files
   - Comprehensive batch analysis

### File Sensor Parameters Explained

- **`filepath`**: The file or pattern to watch for

- **`poke_interval`**: How often to check (in seconds)

- **`timeout`**: Maximum time to wait before giving up

- `mode`

  :

  - `'poke'`: Keep checking in the same task instance
  - `'reschedule'`: Release worker slot between checks (better for long waits)

### Real-World Applications

**Use Case 1: Daily Reports**

```python
FileSensor(
    task_id='wait_for_daily_report',
    filepath='/data/reports/daily_sales_{{ ds }}.csv',
    poke_interval=300,  # Check every 5 minutes
    timeout=3600        # Wait up to 1 hour
)
```

**Use Case 2: Batch Processing**

```python
FileSensor(
    task_id='wait_for_batch_files',
    filepath='/data/incoming/batch_*.csv',
    poke_interval=60,   # Check every minute
    timeout=7200        # Wait up to 2 hours
)
```

**Use Case 3: External System Coordination**

```python
FileSensor(
    task_id='wait_for_trigger_file',
    filepath='/shared/triggers/process_ready.flag',
    poke_interval=10,
    timeout=1800
)
```

## Summary

You now have a complete understanding of file sensors in Apache Airflow:

✅ **Basic file sensors** - Wait for specific files
 ✅ **Multiple file sensors** - Coordinate multiple file arrivals
 ✅ **Pattern-based sensors** - Handle flexible file matching
 ✅ **Separated code architecture** - DAGs separate from processing logic
 ✅ **Real-world patterns** - File validation, archiving, and reporting

### Key Takeaways

1. **File sensors make workflows robust** by handling unpredictable file timing
2. **Multiple sensors can run in parallel** for coordination
3. **Pattern matching provides flexibility** for batch processing
4. **Separating processing logic** makes code maintainable
5. **Always validate files** before processing
6. **Archive processed files** to avoid reprocessing

This foundation will help you build reliable data pipelines that can handle real-world file arrival scenarios in production environments.