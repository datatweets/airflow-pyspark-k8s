from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCountExample").getOrCreate()

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Read the text file
    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])

    # Split lines into words
    words = lines.flatMap(lambda x: x.split(" "))

    # Count words
    counts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

    # Save results
    counts.saveAsTextFile(output_path)

    spark.stop()
