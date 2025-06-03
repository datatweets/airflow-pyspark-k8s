from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
default_args = {
    'owner': 'simple-etl',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def create_sample_data():
    """Create sample CSV data for our ETL"""
   
    
    # Create input directory
    os.makedirs('/opt/airflow/scripts/input', exist_ok=True)
    
    # Sample sales data
    sales_data = """product_id,product_name,category,price,quantity,sale_date
1,Laptop,Electronics,999.99,2,2024-01-15
2,Chair,Furniture,149.99,5,2024-01-15
3,Coffee Maker,Appliances,79.99,1,2024-01-16
4,Monitor,Electronics,299.99,3,2024-01-16
5,Desk,Furniture,199.99,2,2024-01-17"""
    
    with open('/opt/airflow/scripts/input/sales.csv', 'w') as f:
        f.write(sales_data)
    
    print("✅ Sample data created")

def spark_etl_process():
    """Main ETL process using PySpark"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum, count, avg
    
    print("🚀 Starting Spark ETL...")
    
    # 1. EXTRACT - Initialize Spark and read data
    spark = SparkSession.builder \
        .appName("Simple_ETL") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Read CSV file
    df = spark.read.csv('/opt/airflow/scripts/input/sales.csv', header=True, inferSchema=True)
    print(f"📊 Extracted {df.count()} records")
    
    # 2. TRANSFORM - Calculate revenue and create aggregations
    # Add revenue column
    df_with_revenue = df.withColumn("revenue", col("price") * col("quantity"))
    
    # Create category summary
    category_summary = df_with_revenue.groupBy("category").agg(
        spark_sum("revenue").alias("total_revenue"),
        count("product_id").alias("product_count"),
        avg("price").alias("avg_price")
    )
    
    # Create daily summary
    daily_summary = df_with_revenue.groupBy("sale_date").agg(
        spark_sum("revenue").alias("daily_revenue"),
        count("product_id").alias("daily_sales")
    )
    
    print("✅ Data transformed")
    
    # 3. LOAD - Save processed data to temporary location
    os.makedirs('/opt/airflow/scripts/temp', exist_ok=True)
    
    # Convert to list for database loading
    category_data = category_summary.collect()
    daily_data = daily_summary.collect()
    
    spark.stop()
    
    # Return processed data
    return {
        'category_data': [(row.category, float(row.total_revenue), row.product_count, float(row.avg_price)) 
                         for row in category_data],
        'daily_data': [(str(row.sale_date), float(row.daily_revenue), row.daily_sales) 
                      for row in daily_data]
    }

def create_tables():
    """Create tables in analytics database using PostgresHook"""
    
    # Initialize PostgresHook for analytics database
    pg_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    
    # Create tables
    create_tables_sql = """
    -- Category analytics table
    CREATE TABLE IF NOT EXISTS category_analytics (
        category VARCHAR(100) PRIMARY KEY,
        total_revenue DECIMAL(10,2),
        product_count INTEGER,
        avg_price DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Daily sales table
    CREATE TABLE IF NOT EXISTS daily_sales (
        sale_date DATE PRIMARY KEY,
        daily_revenue DECIMAL(10,2),
        daily_sales INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    pg_hook.run(create_tables_sql)
    print("✅ Tables created in analytics database")

def load_to_postgres(**context):
    """Load processed data to PostgreSQL using PostgresHook"""
    
    # Get processed data from previous task
    processed_data = context['task_instance'].xcom_pull(task_ids='spark_etl_processing')
    
    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    
    # Clear existing data
    pg_hook.run("DELETE FROM category_analytics")
    pg_hook.run("DELETE FROM daily_sales")
    
    # Load category data
    category_insert_sql = """
    INSERT INTO category_analytics (category, total_revenue, product_count, avg_price)
    VALUES (%s, %s, %s, %s)
    """
    
    for record in processed_data['category_data']:
        pg_hook.run(category_insert_sql, parameters=record)
    
    print(f"✅ Loaded {len(processed_data['category_data'])} category records")
    
    # Load daily data
    daily_insert_sql = """
    INSERT INTO daily_sales (sale_date, daily_revenue, daily_sales)
    VALUES (%s, %s, %s)
    """
    
    for record in processed_data['daily_data']:
        pg_hook.run(daily_insert_sql, parameters=record)
    
    print(f"✅ Loaded {len(processed_data['daily_data'])} daily records")

def verify_results():
    """Verify data was loaded correctly using PostgresHook"""
    
    pg_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    
    # Check category data
    category_results = pg_hook.get_records("SELECT * FROM category_analytics ORDER BY total_revenue DESC")
    print("📊 Category Analytics:")
    for category, revenue, count, avg_price, created in category_results:
        print(f"  {category}: ${revenue:.2f} revenue, {count} products, avg ${avg_price:.2f}")
    
    # Check daily data
    daily_results = pg_hook.get_records("SELECT * FROM daily_sales ORDER BY sale_date")
    print("\n📅 Daily Sales:")
    for date, revenue, sales, created in daily_results:
        print(f"  {date}: ${revenue:.2f} revenue, {sales} sales")

with DAG(
    dag_id='simple_spark_postgres_etl',
    description='Simple ETL: Extract CSV → Transform with Spark → Load to Postgres',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=False,
    tags=['etl', 'spark', 'postgres', 'simple']
) as dag:

    # Step 1: Create sample data
    create_data = PythonOperator(
        task_id='create_sample_data',
        python_callable=create_sample_data
    )
    
    # Step 2: Create database tables
    setup_tables = PythonOperator(
        task_id='create_database_tables',
        python_callable=create_tables
    )
    
    # Step 3: Run Spark ETL processing
    spark_processing = PythonOperator(
        task_id='spark_etl_processing',
        python_callable=spark_etl_process
    )
    
    # Step 4: Load results to PostgreSQL
    load_data = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )
    
    # Step 5: Verify results
    verify_data = PythonOperator(
        task_id='verify_results',
        python_callable=verify_results
    )
    
    # Step 6: Show completion
    show_completion = BashOperator(
        task_id='show_completion',
        bash_command="""
        echo "🎉 ETL PIPELINE COMPLETED!"
        echo "✅ Data extracted from CSV"
        echo "✅ Data transformed with Spark"
        echo "✅ Data loaded to PostgreSQL"
        echo "✅ Results verified"
        echo ""
        echo "📊 Check your analytics database for results!"
        """
    )
    
    # Task flow
    create_data >> setup_tables >> spark_processing >> load_data >> verify_data >> show_completion