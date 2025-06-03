# Working with PostgreSQL in Apache Airflow on Kubernetes

## Introduction: Why PostgreSQL Matters in Data Pipelines

In the world of data engineering, databases are the foundation of everything we do. PostgreSQL, often called "Postgres," is one of the most powerful and reliable open-source databases available. When combined with Apache Airflow, it becomes a powerhouse for building robust data pipelines.

In this tutorial, we'll explore how to work with PostgreSQL in your Kubernetes-based Airflow environment. You'll learn how to:

- Connect to the PostgreSQL pod running in your Kubernetes cluster
- Use PostgreSQL operators to build data pipelines
- Create a complete ETL pipeline that extracts, transforms, and loads data
- Understand best practices for database operations in Airflow

By the end, you'll be comfortable building production-ready data pipelines that interact with PostgreSQL databases.

## Understanding the Architecture

Before we dive in, let's understand what we're working with. In your Kubernetes Airflow setup, you have:

1. **Airflow Metadata Database**: A PostgreSQL database that stores all Airflow's internal data (DAG runs, task instances, connections, etc.)
2. **Application Databases**: Separate databases for your business data
3. **PostgreSQL Pod**: A Kubernetes pod running PostgreSQL that hosts both types of databases

The key principle: **Never mix Airflow metadata with business data**. We'll create separate databases for our data pipelines.

## Part 1: Connecting to PostgreSQL in Kubernetes

### Step 1: Finding Your PostgreSQL Pod

First, let's locate the PostgreSQL pod in your Kubernetes cluster:

```bash
# List all pods in the airflow namespace
kubectl get pods -n airflow
```

You should see something like:

```
NAME                                                   READY   STATUS    RESTARTS   AGE
airflow-postgresql-645854547c-mhqsw                    1/1     Running   0          3d
airflow-scheduler-68754dd668-4plk2                     1/1     Running   376        3d
airflow-webserver-74cb6f4f9c-x9sxk                     1/1     Running   0          3d
```

The PostgreSQL pod is typically named `airflow-postgresql-<pod-id>`.

### Step 2: Connecting to the PostgreSQL Pod

Now, let's connect to the PostgreSQL pod:

```bash
# Connect to the PostgreSQL pod
kubectl exec -it airflow-postgresql-645854547c-mhqsw -n airflow -- /bin/bash
```

Once inside the pod, we need to identify the available PostgreSQL roles. Run:

```bash
psql -U postgres
```

If you encounter the error:

```
psql: error: FATAL:  role "postgres" does not exist
```

This indicates that the default `postgres` role was not created during initialization. To find existing roles, you can inspect the environment variables used during the pod's initialization:

```bash
env | grep POSTGRES
```

Look for `POSTGRES_USER`. This will indicate the superuser role created. For example, if `POSTGRES_USER=airflow`, then you can connect using:

```bash
psql -U airflow
```

Assuming the role is `airflow`, proceed:

```bash
psql -U airflow
```

You should see the PostgreSQL prompt:

```
psql (13.x)
Type "help" for help.

airflow=#
```

### Step 3: Basic PostgreSQL Commands

Let's explore some essential PostgreSQL commands:

```sql
-- List all databases
\l

-- Clear the screen (useful for keeping your work organized)
\! clear

-- Show current database
SELECT current_database();

-- List all tables in the current database
\dt

-- Get help on SQL commands
\h

-- Get help on psql commands
\?

-- Show connection info
\conninfo
```

**Important**: The `airflow` database contains Airflow's metadata. We'll create separate databases for our data pipelines.

### Step 4: Creating a Database for Our Pipeline

Let's create a dedicated database for our customer data pipeline:

```sql
-- Create a new database
CREATE DATABASE customers_db;

-- List databases to confirm creation
\l

-- Connect to the new database
\c customers_db

-- Verify we're in the right database
SELECT current_database();

-- List tables (should be empty)
\dt
```

### Step 5: Creating a Dedicated User

For security, let's create a specific user for our data pipeline:

```sql
-- Create a new user with password
CREATE USER dataengineer WITH PASSWORD 'datapass123';

-- Grant all privileges on customers_db to the new user
GRANT ALL PRIVILEGES ON DATABASE customers_db TO dataengineer;

-- Grant schema permissions (important for creating tables)
\c customers_db
GRANT ALL ON SCHEMA public TO dataengineer;

-- Verify the user was created
\du
```

You should see:

```
                                    List of roles
 Role name    |                         Attributes                         | Member of 
--------------+------------------------------------------------------------+-----------
 dataengineer |                                                            | {}
 airflow      | Superuser, Create role, Create DB, Replication, Bypass RLS | {}
```

### Step 6: Testing the Connection

Let's verify our new user can connect:

```bash
# Exit current psql session
\q

# Connect as the new user
psql -U dataengineer -d customers_db
```

If prompted for a password, enter: `datapass123`

Test creating a table:

```sql
CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50));

-- Verify table creation
\dt

-- Clean up
DROP TABLE test_table;

-- Exit
\q
```

## Part 2: Setting Up Airflow Connection

Now that we have our database ready, let's configure Airflow to connect to it.

### Step 1: Creating a Kubernetes Service (if needed)

First, ensure PostgreSQL is accessible within the cluster. Check if a service exists:

```bash
kubectl get services -n airflow | grep postgresql
```

You should see something like:

```
airflow-postgresql    ClusterIP   10.96.123.45   <none>        5432/TCP   3d
```

### Step 2: Adding Connection in Airflow UI

1. Access your Airflow UI ([http://localhost:30007](http://localhost:30007/))
2. Navigate to **Admin → Connections**
3. Click the **+** button to add a new connection
4. Fill in the details:

```
Connection Id: postgres_customers
Connection Type: Postgres
Description: Customer database for data pipelines
Host: airflow-postgresql.airflow.svc.cluster.local
Schema: customers_db
Login: dataengineer
Password: datapass123
Port: 5432
```

**Note**: The host uses Kubernetes DNS format: `<service-name>.<namespace>.svc.cluster.local`

1. Click **Test** to verify the connection
2. Click **Save**

### Alternative: Creating Connection via CLI

You can also create the connection using Airflow CLI:

```bash
# From outside the cluster
kubectl exec -it deployment/airflow-scheduler -n airflow -- airflow connections add \
    postgres_customers \
    --conn-type postgres \
    --conn-host airflow-postgresql.airflow.svc.cluster.local \
    --conn-schema customers_db \
    --conn-login dataengineer \
    --conn-password datapass123 \
    --conn-port 5432
```

## Part 3: Building Your First PostgreSQL Pipeline

Now let's build a complete data pipeline using PostgreSQL operators.

### Step 1: Creating the Project Structure

First, create the necessary directories in your Airflow project:

```bash
# Create directories for SQL statements and output
mkdir -p dags/sql_statements
mkdir -p dags/output

# Create SQL files
touch dags/sql_statements/create_table_customers.sql
touch dags/sql_statements/create_table_customer_purchases.sql
touch dags/sql_statements/insert_customers.sql
touch dags/sql_statements/insert_customer_purchases.sql
```

### Step 2: Writing SQL Scripts

Let's create our SQL scripts:

**dags/sql_statements/create_table_customers.sql:**

```sql
-- Create customers table with proper constraints
CREATE TABLE IF NOT EXISTS customers(
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
```

**dags/sql_statements/create_table_customer_purchases.sql:**

```sql
-- Create purchases table with foreign key relationship
CREATE TABLE IF NOT EXISTS customer_purchases(
    id INTEGER PRIMARY KEY,
    product VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
    customer_id INTEGER NOT NULL,
    purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_purchases_customer ON customer_purchases(customer_id);
CREATE INDEX IF NOT EXISTS idx_purchases_date ON customer_purchases(purchase_date);
```

**dags/sql_statements/insert_customers.sql:**

```sql
-- Insert customer data
-- Using INSERT ... ON CONFLICT to handle duplicates gracefully
INSERT INTO customers (id, name, email) VALUES 
    (1, 'Sarah Johnson', 'sarah.j@email.com'),
    (2, 'Michael Chen', 'mchen@email.com'),
    (3, 'Emma Williams', 'emma.w@email.com'),
    (4, 'James Rodriguez', 'jrodriguez@email.com'),
    (5, 'Lisa Anderson', 'landerson@email.com'),
    (6, 'David Kim', 'dkim@email.com')
ON CONFLICT (id) DO NOTHING;
```

**dags/sql_statements/insert_customer_purchases.sql:**

```sql
-- Insert purchase data with realistic e-commerce products
INSERT INTO customer_purchases (id, product, price, quantity, customer_id) VALUES 
    (1, 'Wireless Bluetooth Headphones', 79.99, 1, 1),
    (2, 'Organic Green Tea (50 bags)', 12.99, 2, 2),
    (3, 'Yoga Mat - Premium Quality', 45.50, 1, 1),
    (4, 'Smart Watch Fitness Tracker', 199.99, 1, 6),
    (5, 'Stainless Steel Water Bottle', 24.99, 3, 3),
    (6, 'Programming Books Bundle', 89.99, 1, 2),
    (7, 'Indoor Plant Collection', 55.00, 1, 1),
    (8, 'Laptop Stand Adjustable', 32.99, 1, 3),
    (9, 'Gourmet Coffee Beans (2lb)', 28.50, 2, 4),
    (10, 'Mechanical Keyboard', 120.00, 1, 2),
    (11, 'Essential Oils Set', 39.99, 1, 5),
    (12, 'Portable Phone Charger', 29.99, 2, 6),
    (13, 'Running Shoes', 85.00, 1, 4),
    (14, 'Kitchen Knife Set', 150.00, 1, 2),
    (15, 'Meditation Cushion', 35.99, 1, 1),
    (16, 'USB-C Hub Adapter', 45.00, 1, 1),
    (17, 'Organic Honey (Raw)', 18.99, 3, 3),
    (18, 'Noise Cancelling Earbuds', 159.99, 1, 5),
    (19, 'Bamboo Cutting Board', 22.50, 1, 1),
    (20, 'Smart Home Speaker', 89.99, 1, 2)
ON CONFLICT (id) DO NOTHING;
```

### Step 3: Creating the DAG

Now let's create our PostgreSQL pipeline DAG:

**dags/postgres_customer_pipeline.py:**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import csv
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@company.com']
}

def export_filtered_data(**context):
    """
    Export filtered customer data to CSV file
    This function demonstrates how to work with XCom data from SQL queries
    """
    # Pull data from the previous task via XCom
    ti = context['task_instance']
    filtered_data = ti.xcom_pull(task_ids='filter_high_value_customers')
    
    if not filtered_data:
        print("No data to export")
        return
    
    # Ensure output directory exists
    output_dir = '/opt/airflow/dags/output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    output_file = f'{output_dir}/high_value_customers_{timestamp}.csv'
    
    # Write data to CSV
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        
        # Write header
        writer.writerow(['Customer Name', 'Email', 'Total Purchases', 'Total Spent'])
        
        # Write data rows
        for row in filtered_data:
            writer.writerow(row)
    
    print(f"Exported {len(filtered_data)} rows to {output_file}")
    
    # Return file path for downstream tasks
    return output_file

def validate_data_quality(**context):
    """
    Perform data quality checks on the database
    """
    # This would typically connect to the database and run quality checks
    # For demo purposes, we'll use the task instance to check previous results
    ti = context['task_instance']
    
    # Get customer count from XCom
    customer_count = ti.xcom_pull(task_ids='count_customers')
    purchase_count = ti.xcom_pull(task_ids='count_purchases')
    
    print(f"Data Quality Check Results:")
    print(f"- Total Customers: {customer_count}")
    print(f"- Total Purchases: {purchase_count}")
    
    # Validate data quality
    if customer_count and customer_count[0][0] < 5:
        raise ValueError("Data quality check failed: Too few customers")
    
    if purchase_count and purchase_count[0][0] < 10:
        raise ValueError("Data quality check failed: Too few purchases")
    
    print("✓ All data quality checks passed!")

# Create the DAG
with DAG(
    dag_id='postgres_customer_pipeline',
    description='Complete customer data pipeline with PostgreSQL',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['postgres', 'etl', 'customers'],
    template_searchpath='/opt/airflow/dags/sql_statements'  # Where to find SQL files
) as dag:
    
    # Task 1: Create customers table
    create_customers_table = PostgresOperator(
        task_id='create_customers_table',
        postgres_conn_id='postgres_customers',
        sql='create_table_customers.sql'
    )
    
    # Task 2: Create purchases table
    create_purchases_table = PostgresOperator(
        task_id='create_purchases_table',
        postgres_conn_id='postgres_customers',
        sql='create_table_customer_purchases.sql'
    )
    
    # Task 3: Insert customer data
    insert_customers = PostgresOperator(
        task_id='insert_customers',
        postgres_conn_id='postgres_customers',
        sql='insert_customers.sql'
    )
    
    # Task 4: Insert purchase data
    insert_purchases = PostgresOperator(
        task_id='insert_purchases',
        postgres_conn_id='postgres_customers',
        sql='insert_customer_purchases.sql'
    )
    
    # Task 5: Create summary view
    create_summary_view = PostgresOperator(
        task_id='create_customer_summary',
        postgres_conn_id='postgres_customers',
        sql="""
            CREATE OR REPLACE VIEW customer_purchase_summary AS
            SELECT 
                c.id as customer_id,
                c.name,
                c.email,
                COUNT(cp.id) as total_purchases,
                COALESCE(SUM(cp.price * cp.quantity), 0) as total_spent,
                COALESCE(AVG(cp.price), 0) as avg_purchase_price,
                MAX(cp.purchase_date) as last_purchase_date
            FROM customers c
            LEFT JOIN customer_purchases cp ON c.id = cp.customer_id
            GROUP BY c.id, c.name, c.email
            ORDER BY total_spent DESC;
        """
    )
    
    # Task 6: Count customers (for data quality check)
    count_customers = PostgresOperator(
        task_id='count_customers',
        postgres_conn_id='postgres_customers',
        sql="SELECT COUNT(*) FROM customers;",
        do_xcom_push=True  # Push result to XCom
    )
    
    # Task 7: Count purchases (for data quality check)
    count_purchases = PostgresOperator(
        task_id='count_purchases',
        postgres_conn_id='postgres_customers',
        sql="SELECT COUNT(*) FROM customer_purchases;",
        do_xcom_push=True
    )
    
    # Task 8: Data quality validation
    validate_quality = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        provide_context=True
    )
    
    # Task 9: Filter high-value customers
    filter_customers = PostgresOperator(
        task_id='filter_high_value_customers',
        postgres_conn_id='postgres_customers',
        sql="""
            SELECT name, email, total_purchases, total_spent
            FROM customer_purchase_summary
            WHERE total_spent >= %(min_spent)s
                AND total_purchases >= %(min_purchases)s
            ORDER BY total_spent DESC;
        """,
        parameters={
            'min_spent': 100.00,
            'min_purchases': 2
        },
        do_xcom_push=True
    )
    
    # Task 10: Export filtered data to CSV
    export_data = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_filtered_data,
        provide_context=True
    )
    
    # Task 11: Generate analytics
    generate_analytics = PostgresOperator(
        task_id='generate_analytics',
        postgres_conn_id='postgres_customers',
        sql="""
            -- Create analytics table with business insights
            CREATE TABLE IF NOT EXISTS customer_analytics AS
            WITH purchase_analytics AS (
                SELECT 
                    DATE_TRUNC('day', purchase_date) as purchase_day,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    COUNT(*) as total_transactions,
                    SUM(price * quantity) as daily_revenue,
                    AVG(price) as avg_transaction_value
                FROM customer_purchases
                GROUP BY DATE_TRUNC('day', purchase_date)
            ),
            customer_segments AS (
                SELECT 
                    CASE 
                        WHEN total_spent >= 200 THEN 'VIP'
                        WHEN total_spent >= 100 THEN 'Regular'
                        ELSE 'Occasional'
                    END as segment,
                    COUNT(*) as customer_count,
                    AVG(total_spent) as avg_spent
                FROM customer_purchase_summary
                GROUP BY 1
            )
            SELECT 
                'summary' as report_type,
                CURRENT_TIMESTAMP as generated_at,
                (SELECT COUNT(*) FROM customers) as total_customers,
                (SELECT COUNT(*) FROM customer_purchases) as total_purchases,
                (SELECT SUM(price * quantity) FROM customer_purchases) as total_revenue,
                (SELECT json_agg(row_to_json(customer_segments)) FROM customer_segments) as segments
            ;
        """
    )
    
    # Define task dependencies
    # Create tables in parallel, then insert data
    [create_customers_table, create_purchases_table] >> insert_customers >> insert_purchases
    
    # After data is inserted, create summary and run counts in parallel
    insert_purchases >> [create_summary_view, count_customers, count_purchases]
    
    # Validate data quality after counts are complete
    [count_customers, count_purchases] >> validate_quality
    
    # After summary is created and quality is validated, filter and export
    [create_summary_view, validate_quality] >> filter_customers >> export_data
    
    # Generate analytics after export
    export_data >> generate_analytics
```

## Part 4: Advanced PostgreSQL Operations

Let's explore more advanced patterns for working with PostgreSQL in Airflow.

### Dynamic SQL Generation

Sometimes you need to generate SQL dynamically based on conditions:

```python
from airflow.decorators import task

@task
def generate_dynamic_sql(**context):
    """Generate SQL based on execution date"""
    execution_date = context['execution_date']
    
    # Generate date range for the query
    start_date = execution_date.strftime('%Y-%m-%d')
    end_date = (execution_date + timedelta(days=1)).strftime('%Y-%m-%d')
    
    sql = f"""
        SELECT 
            customer_id,
            COUNT(*) as daily_purchases,
            SUM(price * quantity) as daily_spent
        FROM customer_purchases
        WHERE purchase_date >= '{start_date}'
          AND purchase_date < '{end_date}'
        GROUP BY customer_id
        HAVING COUNT(*) > 1;
    """
    
    return sql

# Use in your DAG
dynamic_analysis = PostgresOperator(
    task_id='analyze_daily_patterns',
    postgres_conn_id='postgres_customers',
    sql="{{ ti.xcom_pull(task_ids='generate_dynamic_sql') }}",
)
```

### Handling Transactions

For operations that must succeed or fail together:

```python
transaction_example = PostgresOperator(
    task_id='update_customer_status',
    postgres_conn_id='postgres_customers',
    sql="""
        BEGIN;
        
        -- Update customer status based on spending
        UPDATE customers 
        SET status = CASE 
            WHEN customer_id IN (
                SELECT customer_id 
                FROM customer_purchase_summary 
                WHERE total_spent > 500
            ) THEN 'VIP'
            ELSE 'Regular'
        END
        WHERE status IS DISTINCT FROM CASE...;  -- Only update if different
        
        -- Log the update
        INSERT INTO customer_status_log (updated_at, updated_count)
        VALUES (CURRENT_TIMESTAMP, @@ROWCOUNT);
        
        COMMIT;
    """,
    autocommit=False  # Handle transaction manually
)
```

### Parallel Processing Pattern

For large datasets, process in parallel:

```python
from airflow.operators.dummy import DummyOperator

# Split processing by customer segments
start_parallel = DummyOperator(task_id='start_parallel_processing')
end_parallel = DummyOperator(task_id='end_parallel_processing')

# Create parallel tasks for each segment
segments = ['A-F', 'G-M', 'N-S', 'T-Z']

for segment in segments:
    process_segment = PostgresOperator(
        task_id=f'process_segment_{segment.replace("-", "_")}',
        postgres_conn_id='postgres_customers',
        sql=f"""
            INSERT INTO processed_customers
            SELECT * FROM customers
            WHERE UPPER(SUBSTRING(name, 1, 1)) 
                BETWEEN '{segment.split('-')[0]}' AND '{segment.split('-')[1]}'
            ON CONFLICT (id) DO UPDATE
            SET processed_at = CURRENT_TIMESTAMP;
        """,
        pool='postgres_pool',  # Use connection pooling
        pool_slots=1
    )
    
    start_parallel >> process_segment >> end_parallel
```

## Part 5: Monitoring and Troubleshooting

### Checking Task Logs

When a PostgreSQL task fails, check the logs:

```bash
# View logs for a specific task
kubectl logs -n airflow deployment/airflow-scheduler | grep "task_id"

# Or from the Airflow UI:
# Click on the task → View Log
```

### Common Issues and Solutions

**1. Connection Refused**

```python
# Error: connection to server at "airflow-postgresql", port 5432 failed

# Solution: Verify the service name
kubectl get svc -n airflow | grep postgresql

# Update your connection host accordingly
```

**2. Permission Denied**

```sql
-- Error: permission denied for schema public

-- Solution: Grant proper permissions
GRANT ALL ON SCHEMA public TO dataengineer;
GRANT ALL ON ALL TABLES IN SCHEMA public TO dataengineer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO dataengineer;
```

**3. Table Lock Issues**

```python
# Add timeout to prevent hanging
postgres_task = PostgresOperator(
    task_id='update_data',
    postgres_conn_id='postgres_customers',
    sql="UPDATE customers SET ...",
    parameters={'lock_timeout': '5s'}
)
```

### Performance Monitoring

Create a monitoring task to track database performance:

```python
monitor_performance = PostgresOperator(
    task_id='monitor_db_performance',
    postgres_conn_id='postgres_customers',
    sql="""
        -- Check table sizes
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
        
        -- Check for slow queries
        SELECT 
            query,
            mean_exec_time,
            calls,
            total_exec_time
        FROM pg_stat_statements
        WHERE mean_exec_time > 1000  -- Queries taking more than 1 second
        ORDER BY mean_exec_time DESC
        LIMIT 10;
    """
)
```

## Part 6: Best Practices

### 1. Use Connection Pooling

Configure your PostgreSQL connection with pooling:

```python
# In your Airflow connection extras field:
{
    "sslmode": "require",
    "connect_timeout": 10,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5
}
```

### 2. Implement Idempotency

Make your SQL operations idempotent:

```sql
-- Good: Idempotent insert
INSERT INTO customers (id, name, email)
VALUES (1, 'John Doe', 'john@email.com')
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    email = EXCLUDED.email;

-- Good: Idempotent table creation
CREATE TABLE IF NOT EXISTS customers (...);

-- Good: Idempotent index
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
```

### 3. Use Schemas for Organization

Organize your data using schemas:

```python
create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id='postgres_customers',
    sql="""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS analytics;
        
        -- Grant permissions
        GRANT USAGE ON SCHEMA staging TO dataengineer;
        GRANT CREATE ON SCHEMA staging TO dataengineer;
    """
)
```

### 4. Implement Data Versioning

Track data changes over time:

```sql
-- Add audit columns to your tables
ALTER TABLE customers 
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1;

-- Create trigger for automatic updates
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();
```

### 5. Clean Up Resources

Always clean up test data and temporary tables:

```python
cleanup_task = PostgresOperator(
    task_id='cleanup_old_data',
    postgres_conn_id='postgres_customers',
    sql="""
        -- Remove old temporary tables
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN 
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename LIKE 'temp_%'
                AND (NOW() - to_timestamp(substring(tablename from 'temp_(\d+)'), 'YYYYMMDDHH24MISS')) > INTERVAL '7 days'
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename);
                RAISE NOTICE 'Dropped table: %', r.tablename;
            END LOOP;
        END $$;
        
        -- Vacuum analyze for performance
        VACUUM ANALYZE;
    """,
    trigger_rule='none_failed'  # Run even if some tasks failed
)
```

## Conclusion

You've now mastered working with PostgreSQL in your Kubernetes-based Airflow environment. You've learned:

1. **How to connect** to PostgreSQL pods in Kubernetes
2. **How to create** dedicated databases and users for your pipelines
3. **How to build** complete ETL pipelines using PostgreSQL operators
4. **How to handle** advanced scenarios like transactions and parallel processing
5. **How to monitor** and troubleshoot your PostgreSQL operations
6. **Best practices** for production-ready data pipelines

Remember these key principles:

- Always separate your business data from Airflow metadata
- Make your operations idempotent for reliable reruns
- Use proper error handling and monitoring
- Clean up resources to prevent database bloat
- Document your SQL code for team collaboration

With these skills, you're ready to build robust, scalable data pipelines that leverage the full power of PostgreSQL and Apache Airflow. 

Happy data engineering!