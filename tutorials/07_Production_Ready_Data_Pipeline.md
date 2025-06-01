# Building a Production-Ready Data Pipeline with Separated Scripts and DAGs

In production Airflow environments, it's a best practice to separate your DAG definitions from your business logic. This tutorial will show you how to build a data pipeline following this pattern, with DAGs in the `dags/` folder and Python scripts in the `scripts/` folder.

## Understanding the Folder Structure

In our setup, we maintain a clean separation of concerns:

```
airflow-pyspark-k8s/
├── dags/
│   └── insurance_pipeline_dag.py    # Only DAG definitions
├── scripts/
│   ├── datasets/
│   │   └── insurance.csv            # Input data
│   ├── output/                      # Processing results
│   └── insurance_processing.py      # Business logic
```

This structure provides several benefits:

- **Modularity**: Business logic can be tested independently of Airflow
- **Reusability**: Scripts can be used outside of Airflow if needed
- **Clarity**: DAG files remain focused on orchestration, not implementation
- **Maintenance**: Easier to update business logic without touching DAG structure

## Setting Up the Environment

First, let's prepare our workspace:

```bash
# Create the necessary directories in your scripts folder
cd /path/to/your/scripts
mkdir -p datasets
mkdir -p output

# Place the insurance.csv file in the datasets directory
cp insurance.csv datasets/
```

The `insurance.csv` file contains customer data including age, BMI, smoking status, region, and insurance charges. Some rows have missing values that we'll handle during processing.

## Step 1: Creating the Processing Scripts

Let's start by creating our data processing functions in a separate Python file. This keeps our business logic independent from Airflow.

Create `/path/to/your/scripts/insurance_processing.py`:

```python
"""
Insurance Data Processing Module

This module contains all the business logic for processing insurance data.
By keeping this separate from the DAG, we can:
1. Test these functions independently
2. Reuse them in other contexts
3. Keep our DAG files clean and focused on orchestration
"""

import pandas as pd
import os
import json
from datetime import datetime

# Define base paths relative to the scripts directory
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS_DIR = os.path.join(SCRIPTS_DIR, 'datasets')
OUTPUT_DIR = os.path.join(SCRIPTS_DIR, 'output')

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def read_insurance_data():
    """
    Read the insurance CSV file and perform initial data exploration.
    
    Returns:
        str: JSON representation of the dataframe for XCom
    """
    # Construct the full path to the CSV file
    csv_path = os.path.join(DATASETS_DIR, 'insurance.csv')
    
    print(f"Reading data from: {csv_path}")
    
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Log basic information about the dataset
    print("=== Dataset Overview ===")
    print(f"Total records: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print(f"Columns: {', '.join(df.columns.tolist())}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nMissing values per column:\n{df.isnull().sum()}")
    print(f"\nFirst 5 records:\n{df.head()}")
    
    # Return as JSON for XCom compatibility
    return df.to_json(orient='records', date_format='iso')

def clean_insurance_data(ti):
    """
    Remove rows with missing values from the insurance dataset.
    
    Args:
        ti: Airflow task instance for accessing XCom data
        
    Returns:
        str: JSON representation of cleaned dataframe
    """
    # Pull data from the previous task
    json_data = ti.xcom_pull(task_ids='read_data')
    
    # Convert JSON back to DataFrame
    df = pd.read_json(json_data, orient='records')
    
    # Store original shape for reporting
    original_shape = df.shape
    rows_with_nulls = df.isnull().any(axis=1).sum()
    
    # Remove rows with any missing values
    df_cleaned = df.dropna()
    
    # Log cleaning results
    print("=== Data Cleaning Summary ===")
    print(f"Original dataset: {original_shape[0]} rows, {original_shape[1]} columns")
    print(f"Rows with missing values: {rows_with_nulls}")
    print(f"Cleaned dataset: {df_cleaned.shape[0]} rows, {df_cleaned.shape[1]} columns")
    print(f"Rows removed: {original_shape[0] - df_cleaned.shape[0]}")
    print(f"Data retention rate: {(df_cleaned.shape[0] / original_shape[0]) * 100:.1f}%")
    
    return df_cleaned.to_json(orient='records', date_format='iso')

def analyze_by_smoking_status(ti):
    """
    Analyze insurance charges based on smoking status.
    
    This analysis helps understand the impact of smoking on insurance premiums.
    
    Args:
        ti: Airflow task instance for accessing XCom data
        
    Returns:
        str: JSON string containing the analysis summary
    """
    # Get cleaned data from previous task
    json_data = ti.xcom_pull(task_ids='clean_data')
    df = pd.read_json(json_data, orient='records')
    
    print("=== Smoking Status Analysis ===")
    
    # Calculate statistics by smoking status
    smoking_stats = df.groupby('smoker').agg({
        'charges': ['count', 'mean', 'std', 'min', 'max'],
        'age': 'mean',
        'bmi': 'mean'
    }).round(2)
    
    # Flatten column names for easier reading
    smoking_stats.columns = ['_'.join(col).strip() for col in smoking_stats.columns.values]
    smoking_stats = smoking_stats.reset_index()
    
    # Calculate the premium difference
    smoker_avg = smoking_stats[smoking_stats['smoker'] == 'yes']['charges_mean'].values[0]
    non_smoker_avg = smoking_stats[smoking_stats['smoker'] == 'no']['charges_mean'].values[0]
    premium_increase = ((smoker_avg - non_smoker_avg) / non_smoker_avg) * 100
    
    print(f"\nSmoking Premium Impact:")
    print(f"Average charges for smokers: ${smoker_avg:,.2f}")
    print(f"Average charges for non-smokers: ${non_smoker_avg:,.2f}")
    print(f"Smoking increases premiums by: {premium_increase:.1f}%")
    
    # Save detailed analysis
    output_path = os.path.join(OUTPUT_DIR, 'smoking_analysis.csv')
    smoking_stats.to_csv(output_path, index=False)
    print(f"\nAnalysis saved to: {output_path}")
    
    # Create a summary dictionary for the report
    summary = {
        'smoker_count': int(smoking_stats[smoking_stats['smoker'] == 'yes']['charges_count'].values[0]),
        'non_smoker_count': int(smoking_stats[smoking_stats['smoker'] == 'no']['charges_count'].values[0]),
        'smoker_avg_charge': float(smoker_avg),
        'non_smoker_avg_charge': float(non_smoker_avg),
        'premium_increase_pct': float(premium_increase)
    }
    
    # Return summary as JSON string
    return json.dumps(summary)

def analyze_by_region(ti):
    """
    Analyze insurance charges by geographical region.
    
    This analysis reveals regional variations in insurance costs.
    
    Args:
        ti: Airflow task instance for accessing XCom data
        
    Returns:
        str: JSON string containing the analysis summary
    """
    # Get cleaned data from previous task
    json_data = ti.xcom_pull(task_ids='clean_data')
    df = pd.read_json(json_data, orient='records')
    
    print("=== Regional Analysis ===")
    
    # Calculate statistics by region
    regional_stats = df.groupby('region').agg({
        'charges': ['count', 'mean', 'std', 'min', 'max'],
        'age': 'mean',
        'bmi': 'mean',
        'children': 'mean'
    }).round(2)
    
    # Flatten column names
    regional_stats.columns = ['_'.join(col).strip() for col in regional_stats.columns.values]
    regional_stats = regional_stats.reset_index()
    
    # Sort by average charges (descending)
    regional_stats = regional_stats.sort_values('charges_mean', ascending=False)
    
    # Print key findings
    print("\nRegional Insurance Charges (sorted by average):")
    for _, row in regional_stats.iterrows():
        print(f"{row['region']:12} - Avg: ${row['charges_mean']:>10,.2f} | "
              f"Count: {int(row['charges_count']):>4} | "
              f"Avg Age: {row['age_mean']:>5.1f} | "
              f"Avg BMI: {row['bmi_mean']:>5.1f}")
    
    # Calculate regional variations
    max_region = regional_stats.iloc[0]
    min_region = regional_stats.iloc[-1]
    variation = ((max_region['charges_mean'] - min_region['charges_mean']) / 
                 min_region['charges_mean']) * 100
    
    print(f"\nRegional Variation:")
    print(f"Highest: {max_region['region']} (${max_region['charges_mean']:,.2f})")
    print(f"Lowest: {min_region['region']} (${min_region['charges_mean']:,.2f})")
    print(f"Variation: {variation:.1f}%")
    
    # Save detailed analysis
    output_path = os.path.join(OUTPUT_DIR, 'regional_analysis.csv')
    regional_stats.to_csv(output_path, index=False)
    print(f"\nAnalysis saved to: {output_path}")
    
    # Create a summary dictionary for the report
    summary = {
        'highest_region': str(max_region['region']),
        'highest_avg': float(max_region['charges_mean']),
        'lowest_region': str(min_region['region']),
        'lowest_avg': float(min_region['charges_mean']),
        'regional_variation_pct': float(variation)
    }
    
    # Return summary as JSON string
    return json.dumps(summary)

def generate_executive_summary(ti):
    """
    Generate an executive summary combining all analyses.
    
    This creates a human-readable report summarizing key findings.
    
    Args:
        ti: Airflow task instance for accessing XCom data
        
    Returns:
        str: Success message
    """
    # Get analysis results from previous tasks - now as JSON strings
    smoking_summary_json = ti.xcom_pull(task_ids='analyze_smoking')
    regional_summary_json = ti.xcom_pull(task_ids='analyze_region')
    
    # Parse JSON strings back to dictionaries
    smoking_summary = json.loads(smoking_summary_json)
    regional_summary = json.loads(regional_summary_json)
    
    # Get the cleaned dataset for overall statistics
    json_data = ti.xcom_pull(task_ids='clean_data')
    df = pd.read_json(json_data, orient='records')
    
    print("=== Generating Executive Summary ===")
    
    # Create the report content
    report_lines = [
        "=" * 60,
        "INSURANCE DATA ANALYSIS - EXECUTIVE SUMMARY",
        "=" * 60,
        f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "DATASET OVERVIEW",
        "-" * 40,
        f"Total Customers Analyzed: {len(df):,}",
        f"Average Customer Age: {df['age'].mean():.1f} years",
        f"Average BMI: {df['bmi'].mean():.1f}",
        f"Average Insurance Charge: ${df['charges'].mean():,.2f}",
        "",
        "KEY FINDINGS",
        "-" * 40,
        "",
        "1. SMOKING IMPACT ON PREMIUMS",
        f"   • Smokers pay {smoking_summary['premium_increase_pct']:.1f}% more than non-smokers",
        f"   • Average smoker premium: ${smoking_summary['smoker_avg_charge']:,.2f}",
        f"   • Average non-smoker premium: ${smoking_summary['non_smoker_avg_charge']:,.2f}",
        f"   • Customer breakdown: {smoking_summary['smoker_count']} smokers, "
        f"{smoking_summary['non_smoker_count']} non-smokers",
        "",
        "2. REGIONAL VARIATIONS",
        f"   • Highest cost region: {regional_summary['highest_region']} "
        f"(${regional_summary['highest_avg']:,.2f})",
        f"   • Lowest cost region: {regional_summary['lowest_region']} "
        f"(${regional_summary['lowest_avg']:,.2f})",
        f"   • Regional price variation: {regional_summary['regional_variation_pct']:.1f}%",
        "",
        "RECOMMENDATIONS",
        "-" * 40,
        "1. Consider targeted smoking cessation programs to reduce claim costs",
        "2. Investigate factors driving regional variations",
        "3. Develop region-specific pricing strategies",
        "",
        "=" * 60,
        "End of Report"
    ]
    
    # Save the report
    report_path = os.path.join(OUTPUT_DIR, 'executive_summary.txt')
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))
    
    print(f"Executive summary saved to: {report_path}")
    print("\nReport Preview:")
    print('\n'.join(report_lines[:20]))  # Show first 20 lines
    
    return f"Executive summary generated successfully at {report_path}"
```

## Step 2: Creating the DAG

Now let's create the DAG that orchestrates these functions. The DAG file will be minimal and focused only on defining the workflow.

Create `/path/to/your/dags/insurance_pipeline_dag.py`:

```python
"""
Insurance Data Pipeline DAG

This DAG orchestrates the insurance data processing pipeline.
All business logic is contained in the separate scripts/insurance_processing.py module.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the scripts directory to Python path so we can import our module
scripts_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts')
sys.path.insert(0, scripts_path)

# Import our processing functions
from insurance_processing import (
    read_insurance_data,
    clean_insurance_data,
    analyze_by_smoking_status,
    analyze_by_region,
    generate_executive_summary
)

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Create the DAG
with DAG(
    dag_id='insurance_data_pipeline',
    default_args=default_args,
    description='Process insurance data to analyze impact of smoking and regional factors',
    schedule_interval=None,  # Manual trigger for now
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['insurance', 'analysis', 'data-pipeline'],
) as dag:
    
    # Add documentation
    dag.doc_md = """
    ## Insurance Data Processing Pipeline
    
    This pipeline processes insurance customer data to provide insights about:
    - The impact of smoking on insurance premiums
    - Regional variations in insurance costs
    - Overall customer demographics and charges
    
    ### Pipeline Steps:
    1. **Read Data**: Load insurance data from CSV
    2. **Clean Data**: Remove records with missing values
    3. **Analyze by Smoking Status**: Calculate premium differences for smokers vs non-smokers
    4. **Analyze by Region**: Identify regional cost variations
    5. **Generate Summary**: Create an executive summary report
    
    ### Output Files:
    All outputs are saved in the `scripts/output/` directory:
    - `smoking_analysis.csv`: Detailed statistics by smoking status
    - `regional_analysis.csv`: Detailed statistics by region
    - `executive_summary.txt`: High-level summary for executives
    
    ### Data Quality:
    The pipeline includes data quality checks and logs the number of records 
    removed during cleaning. Check task logs for detailed information.
    """
    
    # Task 1: Read the insurance data
    read_data_task = PythonOperator(
        task_id='read_data',
        python_callable=read_insurance_data,
        doc_md="""
        Read the insurance CSV file from scripts/datasets/insurance.csv.
        Performs initial data exploration and logs basic statistics.
        """
    )
    
    # Task 2: Clean the data
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_insurance_data,
        doc_md="""
        Remove rows with missing values.
        Logs the number of rows removed and data retention rate.
        """
    )
    
    # Task 3: Analyze by smoking status
    analyze_smoking_task = PythonOperator(
        task_id='analyze_smoking',
        python_callable=analyze_by_smoking_status,
        doc_md="""
        Analyze insurance charges based on smoking status.
        Calculates average premiums and the smoking premium increase percentage.
        """
    )
    
    # Task 4: Analyze by region
    analyze_region_task = PythonOperator(
        task_id='analyze_region',
        python_callable=analyze_by_region,
        doc_md="""
        Analyze insurance charges by geographical region.
        Identifies highest/lowest cost regions and calculates variations.
        """
    )
    
    # Task 5: Generate executive summary
    generate_summary_task = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_executive_summary,
        doc_md="""
        Create a comprehensive executive summary combining all analyses.
        Generates a human-readable report with key findings and recommendations.
        """
    )
    
    # Define the pipeline flow
    read_data_task >> clean_data_task >> [analyze_smoking_task, analyze_region_task] >> generate_summary_task
```

## Understanding the Architecture

### Why Separate Scripts from DAGs?

1. **Testing**: You can test `insurance_processing.py` functions independently without running Airflow
2. **Reusability**: Other DAGs or scripts can import and use these functions
3. **Version Control**: Changes to business logic don't affect DAG structure
4. **Debugging**: Easier to debug Python functions outside of Airflow's context

### How the Separation Works

1. **Scripts Directory**: Contains all business logic, data files, and outputs
   - Self-contained and portable
   - Can be run independently of Airflow if needed
2. **DAGs Directory**: Contains only DAG definitions
   - Imports functions from scripts
   - Focuses on orchestration, not implementation
   - Defines task dependencies and scheduling

### Path Management

Notice how we handle paths in `insurance_processing.py`:

```python
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS_DIR = os.path.join(SCRIPTS_DIR, 'datasets')
OUTPUT_DIR = os.path.join(SCRIPTS_DIR, 'output')
```

This ensures paths work correctly regardless of where Airflow is running from.

## Running the Pipeline

1. **Verify your file structure**:

   ```bash
   /learn-airflow/
   ├── dags/
   │   └── insurance_pipeline_dag.py
   └── scripts/
       ├── datasets/
       │   └── insurance.csv
       ├── output/           # Will be created automatically
       └── insurance_processing.py
   ```

2. **Access the Airflow UI** at `http://localhost:30080`

3. **Find the DAG**: Look for `insurance_data_pipeline` in the DAG list

4. **Trigger the DAG**: Click the play button to run it manually

5. **Monitor Progress**:

   - Click on the DAG name to see the Graph view
   - Watch tasks turn from light blue (running) to green (success)
   - Click on any task to view its logs

6. **Check the Results**:

   ```bash
   # View the generated files
   ls -la /path/to/your/scripts/output/
   
   # Read the executive summary
   cat /path/to/your/scripts/output/executive_summary.txt
   ```

## Debugging and Troubleshooting

### Common Issues and Solutions

1. **Import Errors**:
   - The DAG adds the scripts directory to the Python path
   - If you still get import errors, check the path manipulation in the DAG
2. **File Not Found**:
   - Verify the CSV file is in `scripts/datasets/`
   - Check the logs for the actual path being used
3. **Permission Errors**:
   - Ensure Airflow has read/write permissions in the scripts directory
   - The output directory is created automatically with proper permissions

### Viewing Task Logs

To debug issues:

1. Click on the failed task in the Graph view
2. Click "Log" to see detailed error messages
3. Look for the specific error and the line number

### Testing Functions Independently

You can test the processing functions without Airflow:

```python
# Create a test script in the scripts directory
import insurance_processing as ip

# Test reading data
json_data = ip.read_insurance_data()
print("Data read successfully!")

# For functions that need task instance, you can mock it
class MockTI:
    def xcom_pull(self, task_ids):
        # Return your test data
        return json_data

ti = MockTI()
cleaned_data = ip.clean_insurance_data(ti)
```

## Extending the Pipeline

Here are some ways you can extend this pipeline:

### 1. Add Data Validation

Create a new function in `insurance_processing.py`:

```python
def validate_data_quality(ti):
    """Check for data quality issues"""
    json_data = ti.xcom_pull(task_ids='clean_data')
    df = pd.read_json(json_data, orient='records')
    
    issues = []
    
    # Check for negative ages
    if (df['age'] < 0).any():
        issues.append("Found negative ages")
    
    # Check for unrealistic BMI values
    if (df['bmi'] > 60).any() or (df['bmi'] < 10).any():
        issues.append("Found unrealistic BMI values")
    
    # Check for negative charges
    if (df['charges'] < 0).any():
        issues.append("Found negative charges")
    
    if issues:
        raise ValueError(f"Data quality issues: {', '.join(issues)}")
    
    return "Data validation passed"
```

### 2. Add Email Notifications

Add this to your DAG after the summary task:

```python
from airflow.operators.email import EmailOperator

send_report = EmailOperator(
    task_id='send_report',
    to=['stakeholders@company.com'],
    subject='Insurance Analysis Report - {{ ds }}',
    html_content="""
    <h2>Insurance Analysis Complete</h2>
    <p>The daily insurance analysis has been completed.</p>
    <p>Please find the reports in the output directory.</p>
    <p>Run date: {{ ds }}</p>
    """,
    trigger_rule='all_success'
)

generate_summary_task >> send_report
```

### 3. Add Conditional Processing

Add logic to handle different scenarios:

```python
from airflow.operators.python import BranchPythonOperator

def check_data_size(ti):
    """Decide processing path based on data size"""
    json_data = ti.xcom_pull(task_ids='read_data')
    df = pd.read_json(json_data, orient='records')
    
    if len(df) > 10000:
        return 'heavy_processing'
    else:
        return 'standard_processing'

branch_task = BranchPythonOperator(
    task_id='check_data_size',
    python_callable=check_data_size
)
```

## Best Practices Summary

1. **Keep DAGs Simple**: DAG files should focus on orchestration, not implementation
2. **Modular Scripts**: Business logic in separate files makes testing easier
3. **Clear Logging**: Add informative print statements for debugging
4. **Error Handling**: Include try-except blocks for production pipelines
5. **Documentation**: Document both the DAG and the functions
6. **Path Management**: Use absolute paths constructed from the script location
7. **Data Validation**: Always validate data quality before processing
8. **XCom Wisely**: Use XCom for small data; for large datasets, pass file paths instead

## Conclusion

You've now built a production-ready data pipeline that follows best practices for separating business logic from orchestration. This architecture makes your pipelines:

- **Testable**: Functions can be tested independently
- **Maintainable**: Clear separation of concerns
- **Scalable**: Easy to add new processing steps
- **Reusable**: Processing functions can be used in multiple DAGs

As you build more complex pipelines, remember to maintain this separation. Your DAGs should read like a high-level description of your workflow, while the detailed implementation lives in your scripts folder. This approach will serve you well as your data infrastructure grows.