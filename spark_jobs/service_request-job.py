# Analysis 5: The top 1 category of 311 service request every year from Year 2008
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, expr, row_number, substring
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

mongodb_uri = ""

# Create a SparkSession for MongoDB Altlas
spark = (
    SparkSession.builder.appName("CrimeAnalysis")
    .config("spark.mongodb.input.uri", mongodb_uri)
    .config("spark.mongodb.output.uri", mongodb_uri)
    .getOrCreate()
)

reader = (
    spark.read.format("mongodb")
    .option("spark.mongodb.read.connection.uri", mongodb_uri)
    .option("spark.mongodb.write.connection.uri", mongodb_uri)
    .option("database", "SanFrancisco")
)

# Load service_requests DataFrame from MongoDB
service_requests_df = reader.option("collection", "service_requests").load()

# Add a new column 'year' by extracting the year from the 'created_date' field
service_requests_df = service_requests_df.withColumn(
    "year", substring("created_date", 0, 4)
)

# Group by 'year' and 'category', count occurrences of each category in each year
grouped_df = service_requests_df.groupBy("year", "category").agg(
    spark_sum(expr("1")).alias("count")
)

# Find the most common category for each year
window_spec = Window.partitionBy("year").orderBy(col("count").desc())
ranked_df = (
    grouped_df.withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") == 1)
    .drop("rank")
)

# Sum the total count of documents for each year
total_count_df = grouped_df.groupBy("year").agg(spark_sum("count").alias("total_count"))

# Calculate the percentage of the most common category for each year
result_df = ranked_df.join(total_count_df, "year").withColumn(
    "percentage", (col("count") / col("total_count")) * 100
)

# Select the desired fields and show the result
result_df.select("year", "category", "count", "percentage").orderBy("year").show(
    truncate=False
)

# Write our outputs to MongoDB
(
    result_df.write.format("mongodb")
    .mode("append")
    .option("connection.uri", mongodb_uri)
    .option("database", "SanFrancisco")
    .option("collection", "service_request_aggregations")
    .save()
)

# Stop the SparkSession
spark.stop()
