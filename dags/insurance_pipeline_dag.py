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