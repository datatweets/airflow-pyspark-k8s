# ETL: PySpark + PostgresHook Tutorial

## What Are Airflow Hooks?

Hooks in Apache Airflow are interfaces to external systems and services. They provide a consistent way to interact with databases, APIs, cloud services, and other external resources without exposing connection details in your DAG code.

Think of hooks as **specialized connectors** that:

- Abstract away connection complexity
- Provide reusable connection management
- Handle authentication and credentials securely
- Offer standardized methods for common operations

### Hook Architecture in Your Infrastructure

```
┌─────────────────────────────────────────────────────────────────┐
│                      AIRFLOW ENVIRONMENT                        │
│                                                                 │
│  ┌─────────────────┐                                            │
│  │   DAG TASK      │                                            │
│  │                 │                                            │
│  │  def my_task(): │                                            │
│  │    hook = PostgresHook(conn_id='my_db')                      │
│  │    hook.run("SELECT * FROM table")                           │
│  │                 │                                            │
│  └─────────────────┘                                            │
│           │                                                     │
│           ↓                                                     │
│  ┌─────────────────┐                                            │
│  │  POSTGRES HOOK  │                                            │
│  │                 │                                            │
│  │ • Connection    │                                            │
│  │   Management    │                                            │
│  │ • Query         │                                            │
│  │   Execution     │                                            │
│  │ • Error         │                                            │
│  │   Handling      │                                            │
│  └─────────────────┘                                            │
│           │                                                     │
│           ↓                                                     │
│  ┌─────────────────┐                                            │
│  │  CONNECTION     │                                            │
│  │  METADATA       │                                            │
│  │                 │                                            │
│  │ • Host: postgres-postgresql                                  │
│  │ • Port: 5432    │                                            │
│  │ • Database: airflow                                          │
│  │ • Credentials   │                                            │
│  └─────────────────┘                                            │
│           │                                                     │
│           ↓                                                     │
│  ┌─────────────────┐                                            │
│  │  ACTUAL         │                                            │
│  │  DATABASE       │                                            │
│  │                 │                                            │
│  │ PostgreSQL Pod  │                                            │
│  │ in Kubernetes   │                                            │
│  └─────────────────┘                                            │
└─────────────────────────────────────────────────────────────────┘
```

### 

**Why use Hooks?**

- **Security**: Store credentials securely, not in your code
- **Reusability**: One connection, use everywhere
- **Consistency**: Same way to connect to similar systems
- **Error Handling**: Built-in retry and error management

**Common Hook Types:**

- `PostgresHook` - Connect to PostgreSQL databases
- `MySqlHook` - Connect to MySQL databases
- `S3Hook` - Connect to Amazon S3
- `HttpHook` - Connect to REST APIs

## PostgresHook Explained

PostgresHook is your tool for working with PostgreSQL databases in Airflow.

**What it does:**

- Manages database connections
- Executes SQL queries
- Handles errors automatically
- Keeps credentials secure

**Basic Usage:**

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Create hook (uses stored connection)
pg_hook = PostgresHook(postgres_conn_id='my_database')

# Run queries
pg_hook.run("INSERT INTO table VALUES (1, 'data')")

# Get data
results = pg_hook.get_records("SELECT * FROM table")
```

**Key Methods:**

- `run()` - Execute SQL (INSERT, UPDATE, DELETE)
- `get_records()` - Get all query results
- `get_first()` - Get first row only
- `get_pandas_df()` - Get results as DataFrame

## Create a Separate Database for Analytics

First, let's create a dedicated database for our analytics data (not touching Airflow's metadata).

**Updating the `postgres_default` Connection in the Airflow UI**

1. In the Airflow web interface, click on **Admin → Connections**.
2. Locate the connection whose **Conn Id** is **postgres\_default**.
3. Click **Edit** (the pencil icon) next to **postgres\_default**.
4. Update the **Login** field to `airflow` and the **Password** field to `airflow`.
5. Verify that **Host** is set to your PostgreSQL service (e.g., `airflow-postgresql`) and **Schema** (or **Database**) is set to `airflow`.
6. Click **Save** to apply your changes.

---

### Why This Change Is Necessary

When the PostgresOperator runs `CREATE DATABASE analytics;`, it uses the credentials stored in the `postgres_default` connection. By default, Airflow attempted to connect with the `postgres` user (which did not match our Helm chart’s configuration). Changing **Login/Password** to `airflow/airflow` ensures that Airflow can authenticate successfully against the running PostgreSQL instance—and therefore be able to create the new `analytics` database from within the DAG.



**File: `dags/create_analytics_db.py`**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'analytics-setup',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='create_analytics_database',
    description='Create separate database for analytics data',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=False,
    tags=['setup', 'database']
) as dag:

    # Create analytics database
    create_db = PostgresOperator(
        task_id='create_analytics_db',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE DATABASE analytics;
        """,
        autocommit=True
    )
```

## Set Up Database Connection

1. **Go to Airflow UI** → Admin → Connections

2. Create new connection:

   - **Connection Id**: `analytics_postgres`
   - **Connection Type**: `Postgres`
   - **Host**: `airflow-postgresql`
   - **Schema**: `analytics`
   - **Login**: `airflow`
   - **Password**: `airflow`
   - **Port**: `5432`

## Simple ETL Pipeline

**File: `dags/simple_spark_etl.py`**

```python
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
```

## How It Works

1. **Extract**: Read CSV data with Spark
2. **Transform**: Calculate revenue and create summaries
3. **Load**: Use PostgresHook to store in analytics database
4. **Verify**: Check the results

## Key Points

- **Separate Database**: Analytics data goes to dedicated database, not Airflow metadata
- **PostgresHook**: Handles database connections securely
- **Simple Flow**: CSV → Spark → PostgreSQL
- **Real ETL**: Extract, Transform, Load pattern

## Run the Pipeline

1. First run: `create_analytics_database` (creates database)
2. Then run: `simple_spark_postgres_etl` (runs ETL)

Your analytics data will be stored in the separate `analytics` database!