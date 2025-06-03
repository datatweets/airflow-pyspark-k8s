from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys

# Ensure the "scripts" folder is on the PYTHONPATH so that any utility modules can be imported
sys.path.append('/opt/airflow/scripts')

default_args = {
    'owner': 'spark-etl-demo',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


def extract_sales_data():
    """
    EXTRACT Phase: Read raw sales data from multiple sources.

    In a real-world scenario, this would read from:
    - Database tables
    - API endpoints  
    - File systems
    - Streaming sources
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    print("=== EXTRACT PHASE ===")

    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName("ETL_Extract_Phase")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Define schemas for better performance and data quality
    sales_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DoubleType(), False),
        StructField("transaction_date", StringType(), False)
    ])

    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("registration_date", StringType(), False)
    ])

    # Extract: Read from CSV files
    print("📥 Reading sales transactions...")
    sales_df = (
        spark.read
        .csv("/opt/airflow/scripts/input/sales_data.csv", header=True, schema=sales_schema)
    )

    print("📥 Reading customer data...")
    customers_df = (
        spark.read
        .csv("/opt/airflow/scripts/input/customer_data.csv", header=True, schema=customer_schema)
    )

    # Data quality checks during extraction
    sales_count = sales_df.count()
    customer_count = customers_df.count()
    print(f"✅ Sales records extracted: {sales_count}")
    print(f"✅ Customer records extracted: {customer_count}")

    print("\n🔍 Data Quality Checks:")
    sales_nulls = sales_df.filter(sales_df.customer_id.isNull()).count()
    customer_nulls = customers_df.filter(customers_df.customer_id.isNull()).count()
    print(f"Sales records with null customer_id: {sales_nulls}")
    print(f"Customer records with null customer_id: {customer_nulls}")

    if sales_nulls > 0 or customer_nulls > 0:
        print("⚠️  Data quality issues detected!")
    else:
        print("✅ Data quality checks passed")

    # Save intermediate results for the transform phase
    print("\n💾 Saving extracted data for transformation...")
    sales_df.coalesce(1).write.mode("overwrite").parquet("/opt/airflow/scripts/temp/extracted_sales")
    customers_df.coalesce(1).write.mode("overwrite").parquet("/opt/airflow/scripts/temp/extracted_customers")

    spark.stop()
    print("✅ Extract phase completed successfully")


def transform_sales_data():
    """
    TRANSFORM Phase: Clean, enrich, and aggregate the extracted data.

    This phase demonstrates:
    - Data cleaning and validation
    - Complex transformations and aggregations
    - Data enrichment through joins
    - Business logic implementation
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        when,
        count,
        sum as spark_sum,
        avg,
        max as spark_max,
        upper,
        trim,
        to_date,
        datediff,
        current_date,
        round as spark_round,
        desc
    )

    print("=== TRANSFORM PHASE ===")

    spark = (
        SparkSession.builder
        .appName("ETL_Transform_Phase")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read extracted data
    print("📖 Loading extracted data...")
    sales_df = spark.read.parquet("/opt/airflow/scripts/temp/extracted_sales")
    customers_df = spark.read.parquet("/opt/airflow/scripts/temp/extracted_customers")

    total_sales = sales_df.count()
    total_customers = customers_df.count()
    print(f"Loaded {total_sales} sales records and {total_customers} customer records")

    # 1. DATA CLEANING AND STANDARDIZATION
    print("\n🧹 Data Cleaning Phase...")

    # Clean customer data
    customers_cleaned = (
        customers_df
        .withColumn("customer_name", trim(upper(col("customer_name"))))
        .withColumn("email", trim(col("email")))
        .withColumn("city", trim(upper(col("city"))))
        .withColumn("state", trim(upper(col("state"))))
    )

    # Clean sales data and add revenue column
    sales_cleaned = (
        sales_df
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(upper(col("category"))))
        .withColumn("revenue", col("quantity") * col("price"))
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    )

    print("✅ Data cleaning completed")

    # 2. DATA ENRICHMENT
    print("\n📈 Data Enrichment Phase...")

    # Join sales with customer data
    enriched_sales = sales_cleaned.join(customers_cleaned, "customer_id", "inner")

    # Add customer tenure calculation (in days)
    enriched_sales = enriched_sales.withColumn(
        "customer_tenure_days",
        datediff(current_date(), to_date(col("registration_date"), "yyyy-MM-dd"))
    )

    # Customer segmentation based on tenure
    enriched_sales = enriched_sales.withColumn(
        "customer_segment",
        when(col("customer_tenure_days") >= 365, "Loyal")
        .when(col("customer_tenure_days") >= 180, "Regular")
        .otherwise("New")
    )

    enriched_count = enriched_sales.count()
    print(f"✅ Data enrichment completed: {enriched_count} enriched records")

    # 3. BUSINESS TRANSFORMATIONS AND AGGREGATIONS
    print("\n📊 Business Transformations...")

    # Customer Analytics
    customer_analytics = (
        enriched_sales.groupBy(
            "customer_id", "customer_name", "city", "state", "customer_segment"
        )
        .agg(
            count("transaction_id").alias("total_transactions"),
            spark_sum("revenue").alias("total_revenue"),
            avg("revenue").alias("avg_transaction_value"),
            spark_max("transaction_date").alias("last_purchase_date"),
            count("transaction_date").alias("purchase_frequency")
        )
        .withColumn("total_revenue", spark_round("total_revenue", 2))
        .withColumn("avg_transaction_value", spark_round("avg_transaction_value", 2))
    )

    # Product Analytics
    product_analytics = (
        enriched_sales.groupBy("product_name", "category")
        .agg(
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum("revenue").alias("total_revenue"),
            count("transaction_id").alias("transaction_count"),
            avg("price").alias("avg_price")
        )
        .withColumn("total_revenue", spark_round("total_revenue", 2))
        .withColumn("avg_price", spark_round("avg_price", 2))
    )

    # Geographic Analytics
    geographic_analytics = (
        enriched_sales.groupBy("state", "city")
        .agg(
            count("customer_id").alias("unique_customers"),
            spark_sum("revenue").alias("total_revenue"),
            count("transaction_id").alias("total_transactions")
        )
        .withColumn("total_revenue", spark_round("total_revenue", 2))
    )

    # Daily Sales Trends
    daily_trends = (
        enriched_sales.groupBy("transaction_date")
        .agg(
            count("transaction_id").alias("daily_transactions"),
            spark_sum("revenue").alias("daily_revenue"),
            count("customer_id").alias("unique_customers")
        )
        .withColumn("daily_revenue", spark_round("daily_revenue", 2))
        .orderBy("transaction_date")
    )

    print("✅ Business transformations completed")

    # 4. DATA VALIDATION
    print("\n🔍 Data Validation...")

    total_original_revenue = sales_cleaned.agg(spark_sum("revenue")).collect()[0][0]
    total_transformed_revenue = customer_analytics.agg(spark_sum("total_revenue")).collect()[0][0]

    print(f"Original total revenue: ${total_original_revenue:,.2f}")
    print(f"Transformed total revenue: ${total_transformed_revenue:,.2f}")
    reconciliation = (
        "✅ PASS" if abs(total_original_revenue - total_transformed_revenue) < 0.01 else "❌ FAIL"
    )
    print(f"Revenue reconciliation: {reconciliation}")

    # 5. SAVE TRANSFORMED DATA
    print("\n💾 Saving transformed data...")

    customer_analytics.coalesce(1).write.mode("overwrite").parquet(
        "/opt/airflow/scripts/output/customer_analytics"
    )
    product_analytics.coalesce(1).write.mode("overwrite").parquet(
        "/opt/airflow/scripts/output/product_analytics"
    )
    geographic_analytics.coalesce(1).write.mode("overwrite").parquet(
        "/opt/airflow/scripts/output/geographic_analytics"
    )
    daily_trends.coalesce(1).write.mode("overwrite").parquet(
        "/opt/airflow/scripts/output/daily_trends"
    )
    enriched_sales.coalesce(2).write.mode("overwrite").parquet(
        "/opt/airflow/scripts/output/enriched_sales"
    )

    # Show sample results
    print("\n📊 Sample Transformed Data:")
    print("\nTop 5 Customers by Revenue:")
    customer_analytics.orderBy(desc("total_revenue")).show(5)

    print("\nTop 5 Products by Revenue:")
    product_analytics.orderBy(desc("total_revenue")).show(5)

    spark.stop()
    print("✅ Transform phase completed successfully")


def load_analytics_data():
    """
    LOAD Phase: Write processed data to final destinations.

    In production, this would write to:
    - Data warehouses (Snowflake, BigQuery, Redshift)
    - OLAP databases
    - Business intelligence tools
    - Real-time dashboards
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, desc

    print("=== LOAD PHASE ===")

    spark = (
        SparkSession.builder
        .appName("ETL_Load_Phase")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read transformed data
    print("📖 Loading transformed analytics data...")
    customer_analytics = spark.read.parquet("/opt/airflow/scripts/output/customer_analytics")
    product_analytics = spark.read.parquet("/opt/airflow/scripts/output/product_analytics")
    geographic_analytics = spark.read.parquet("/opt/airflow/scripts/output/geographic_analytics")
    daily_trends = spark.read.parquet("/opt/airflow/scripts/output/daily_trends")

    # 1. EXPORT TO CSV FOR BUSINESS USERS
    print("\n📤 Exporting to CSV for business stakeholders...")
    customer_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        "/opt/airflow/scripts/output/csv/customer_analytics"
    )
    product_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        "/opt/airflow/scripts/output/csv/product_analytics"
    )

    # 2. SIMULATE DATABASE LOADING
    print("\n💾 Simulating database loading...")
    # (In real scenarios, you would write via JDBC, e.g. customer_analytics.write.format("jdbc") ...)

    # 3. CREATE SUMMARY STATISTICS
    print("\n📈 Generating executive summary...")
    total_customers = customer_analytics.count()
    total_revenue = customer_analytics.agg({"total_revenue": "sum"}).collect()[0][0]
    avg_customer_value = customer_analytics.agg({"total_revenue": "avg"}).collect()[0][0]

    segment_performance = (
        customer_analytics.groupBy("customer_segment")
        .agg({"total_revenue": "sum", "customer_id": "count"})
        .orderBy(desc("sum(total_revenue)"))
    )

    print(f"\n📊 EXECUTIVE SUMMARY:")
    print(f"Total Customers: {total_customers:,}")
    print(f"Total Revenue: ${total_revenue:,.2f}")
    print(f"Average Customer Value: ${avg_customer_value:,.2f}")
    print(f"\nCustomer Segment Performance:")
    segment_performance.show()

    # 4. DATA QUALITY REPORT
    print("\n🔍 Final Data Quality Report:")
    null_revenues = customer_analytics.filter(col("total_revenue").isNull()).count()
    zero_revenues = customer_analytics.filter(col("total_revenue") == 0).count()

    print(f"Records with null revenue: {null_revenues}")
    print(f"Records with zero revenue: {zero_revenues}")
    quality_status = "✅ EXCELLENT" if (null_revenues == 0 and zero_revenues == 0) else "⚠️ REVIEW NEEDED"
    print(f"Data quality status: {quality_status}")

    spark.stop()
    print("✅ Load phase completed successfully")


with DAG(
    dag_id='spark_etl_comprehensive_demo',
    description='Comprehensive Spark ETL pipeline demonstrating Extract, Transform, Load',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=False,
    tags=['spark', 'etl', 'comprehensive', 'demo']
) as dag:

    # 1. Prepare environment (directories, sample data)
    setup_environment = BashOperator(
        task_id='setup_etl_environment',
        bash_command="""
        echo "=== SETTING UP ETL ENVIRONMENT ==="
        
        # Create required directories
        mkdir -p /opt/airflow/scripts/temp
        mkdir -p /opt/airflow/scripts/output/csv
        mkdir -p /opt/airflow/scripts/input
        
        echo "✅ Directories created"
        
        # Verify input data exists; if missing, create sample files
        echo "📋 Checking input data files..."
        if [ -f "/opt/airflow/scripts/input/sales_data.csv" ]; then
            echo "✅ Sales data found ($(wc -l < /opt/airflow/scripts/input/sales_data.csv) lines)"
        else
            echo "❌ Sales data missing - creating sample data..."
            cat > /opt/airflow/scripts/input/sales_data.csv << 'EOF'
transaction_id,customer_id,product_name,category,quantity,price,transaction_date
1001,C001,Laptop Dell Inspiron,Electronics,1,799.99,2024-01-15
1002,C002,Office Chair,Furniture,2,149.50,2024-01-15
1003,C003,Coffee Maker,Appliances,1,89.99,2024-01-15
1004,C001,Wireless Mouse,Electronics,3,29.99,2024-01-15
1005,C004,Standing Desk,Furniture,1,399.00,2024-01-16
1006,C002,Smartphone,Electronics,1,599.00,2024-01-16
1007,C005,Blender,Appliances,2,69.99,2024-01-16
1008,C003,Monitor 4K,Electronics,1,449.99,2024-01-16
1009,C006,Ergonomic Chair,Furniture,1,299.50,2024-01-17
1010,C001,Air Fryer,Appliances,1,129.99,2024-01-17
1011,C004,Gaming Laptop,Electronics,1,1299.99,2024-01-17
1012,C002,Bookshelf,Furniture,1,129.99,2024-01-17
1013,C007,Tablet,Electronics,2,349.00,2024-01-18
1014,C005,Desk Lamp,Furniture,3,29.99,2024-01-18
1015,C008,Microwave,Appliances,1,159.99,2024-01-18
EOF
        fi
        
        if [ -f "/opt/airflow/scripts/input/customer_data.csv" ]; then
            echo "✅ Customer data found ($(wc -l < /opt/airflow/scripts/input/customer_data.csv) lines)"
        else
            echo "❌ Customer data missing - creating sample data..."
            cat > /opt/airflow/scripts/input/customer_data.csv << 'EOF'
customer_id,customer_name,email,city,state,registration_date
C001,John Smith,john.smith@email.com,New York,NY,2023-03-15
C002,Jane Doe,jane.doe@email.com,Los Angeles,CA,2023-04-20
C003,Mike Johnson,mike.johnson@email.com,Chicago,IL,2023-05-10
C004,Sarah Wilson,sarah.wilson@email.com,Houston,TX,2023-06-05
C005,David Brown,david.brown@email.com,Phoenix,AZ,2023-07-12
C006,Lisa Davis,lisa.davis@email.com,Philadelphia,PA,2023-08-18
C007,Tom Miller,tom.miller@email.com,San Antonio,TX,2023-09-22
C008,Emma Garcia,emma.garcia@email.com,San Diego,CA,2023-10-30
EOF
        fi
        
        echo "🚀 Environment setup complete"
        """
    )

    # 2. Extract Phase
    extract_phase = PythonOperator(
        task_id='extract_sales_data',
        python_callable=extract_sales_data
    )

    # 3. Transform Phase
    transform_phase = PythonOperator(
        task_id='transform_sales_data',
        python_callable=transform_sales_data
    )

    # 4. Load Phase
    load_phase = PythonOperator(
        task_id='load_analytics_data',
        python_callable=load_analytics_data
    )

    # 5. Final Summary
    etl_summary = BashOperator(
        task_id='etl_pipeline_summary',
        bash_command="""
        echo "=== ETL PIPELINE EXECUTION SUMMARY ==="
        echo ""
        echo "🎯 PIPELINE COMPLETED SUCCESSFULLY!"
        echo ""
        echo "📊 Processed Data Summary:"
        
        # Count output files
        echo "Generated Files:"
        find /opt/airflow/scripts/output -name "*.parquet" -o -name "*.csv" | wc -l | xargs echo "  - Parquet/CSV files:"
        
        echo ""
        echo "📁 Output Structure:"
        find /opt/airflow/scripts/output -type d | sed 's/^/  /'
        
        echo ""
        echo "💾 Storage Usage:"
        du -sh /opt/airflow/scripts/output/* 2>/dev/null | sed 's/^/  /' || echo "  Unable to calculate"
        
        echo ""
        echo "✅ ETL PHASES COMPLETED:"
        echo "  1. ✅ EXTRACT - Raw data ingestion and validation"
        echo "  2. ✅ TRANSFORM - Data cleaning, enrichment, and aggregation"
        echo "  3. ✅ LOAD - Analytics data export and quality checks"
        echo ""
        echo "🔍 KEY LEARNING OUTCOMES:"
        echo "  📈 Spark DataFrame operations for large-scale data processing"
        echo "  🔄 ETL pipeline orchestration with Airflow"
        echo "  📊 Business analytics and customer segmentation"
        echo "  💾 Data quality validation and reconciliation"
        echo "  🏗️  Production-ready data pipeline patterns"
        echo ""
        echo "🚀 Ready for production deployment!"
        """
    )

    # Set up task dependencies
    setup_environment >> extract_phase >> transform_phase >> load_phase >> etl_summary
