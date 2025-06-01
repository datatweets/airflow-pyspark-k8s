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