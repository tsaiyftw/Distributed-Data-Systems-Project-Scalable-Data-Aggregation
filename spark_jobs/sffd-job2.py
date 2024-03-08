from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, first, sum, col, lit, concat, to_timestamp, expr


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


# Analysis 4: The top 1 call type every month from Year 2018
# Load the data from MongoDB
df = reader.option("collection", "sffd_service_calls").load()

# Filter out documents where call_date is not a valid date
df = df.filter(df["call_date"].isNotNull() & (df["call_date"] != ""))

# Convert call_date to timestamp type
df = df.withColumn("call_date", to_timestamp(df["call_date"]))

# Filter documents with call_date after 2018-01-01
df = df.filter(df["call_date"] >= lit("2018-01-01"))

# Create year and month columns
df = df.withColumn("year", year("call_date"))
df = df.withColumn("month", month("call_date"))

# Group by year, month, and call_type, and calculate count
df = df.groupBy("year", "month", "call_type").agg(sum(lit(1)).alias("count"))

# Sort by year, month, and count in descending order
df = df.orderBy("year", "month", col("count").desc())

# Group by year and month, and calculate top_call_type, top_call_type_count, and total_calls
df = df.groupBy("year", "month").agg(
    first("call_type").alias("top_call_type"),
    first("count").alias("top_call_type_count"),
    sum("count").alias("total_calls")
)

# Sort by year and month
df = df.orderBy("year", "month")

# Calculate percentage
df = df.withColumn("percentage", expr(
    "top_call_type_count / total_calls * 100"))

# Select the required columns and show the DataFrame
df.select(
    "year", "month", "top_call_type", "top_call_type_count", "percentage").show(truncate=False)

# Write our outputs to MongoDB
(
    df.write.format("mongodb")
    .mode("append")
    .option("connection.uri", mongodb_uri)
    .option("database", "SanFrancisco")
    .option("collection", "sffd_aggregations_2")
    .save()
)

# Stop SparkSession
spark.stop()
