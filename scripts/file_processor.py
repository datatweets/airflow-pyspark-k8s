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