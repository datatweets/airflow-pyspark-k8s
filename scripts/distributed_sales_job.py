from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Initialize SparkSession - settings mostly provided by spark-submit
spark = SparkSession.builder.appName("distributed-sales-sum").getOrCreate()

# Read all sales csv files to demonstrate distributed read
sales_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/opt/airflow/scripts/datasets/sales_*.csv")
)

# Perform a simple distributed computation: total sales per category
result_df = (
    sales_df
    .withColumn("total", col("Price") * col("Quantity_Sold"))
    .groupBy("Category")
    .agg(spark_sum("total").alias("category_sales"))
)

result_df.show()

# Write output back to shared volume so it can be inspected later
(
    result_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv("/opt/airflow/scripts/output/sales_category_totals")
)

spark.stop()
