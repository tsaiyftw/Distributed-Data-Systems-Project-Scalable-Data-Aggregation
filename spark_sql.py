from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a SparkSession for MongoDB Altlas
spark = SparkSession.builder \
    .appName("CrimeAnalysis") \
    .config("spark.mongodb.input.uri", "mongodb+srv://<username>:<password>@<cluster>/<database>.<collection>") \
    .config("spark.mongodb.output.uri", "mongodb+srv://<username>:<password>@<cluster>/<database>.<collection>") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Load the data from MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Analysis 1: Yearly stolen percentage and theft percentage
# Filter the data
filtered_df = df.filter((col("category") != "NON-CRIMINAL") &
                        (col("category") != "SECONDARY CODES") &
                        (col("category") != "WARRANTS"))

# Extract year and month from timestamp
filtered_df = filtered_df.withColumn("year", year("timestamp"))
filtered_df = filtered_df.withColumn("month", month("timestamp"))

# Group by year and month, and calculate counts for each category
yearly_monthly_counts_df = filtered_df.groupBy("year", "month") \
    .agg(count(expr("case when descript like '%STOLEN AUTOMOBILE%' then 1 end")).alias("stolen_count"),
         count(expr("case when descript like '%THEFT FROM LOCKED AUTO%' then 1 end")).alias(
             "theft_count"),
         count("*").alias("total_count"))

# Calculate percentages for each year and month
yearly_monthly_percentages_df = yearly_monthly_counts_df.withColumn("stolen_pct", (yearly_monthly_counts_df["stolen_count"] * 100) / yearly_monthly_counts_df["total_count"]) \
    .withColumn("theft_pct", (yearly_monthly_counts_df["theft_count"] * 100) / yearly_monthly_counts_df["total_count"])

# Display the results
yearly_monthly_percentages_df.show()


# Analysis 2: Top 5 streets with theft from loked cars
# Filter the data
filtered_df = df.filter((col("category") == "LARCENY/THEFT") &
                        (col("descript").rlike("THEFT FROM LOCKED AUTO")) &
                        (col("timestamp").between("2016-01-01", "2017-01-01")))

# Extract day of week and street from timestamp and address
filtered_df = filtered_df.withColumn("dayOfWeek", dayofweek("timestamp"))
filtered_df = filtered_df.withColumn(
    "street", regexp_extract("address", "^(\\d+\\s[A-Z0-9 ]+)", 1))

# Group by day of week and street, and calculate count
grouped_df = filtered_df.groupBy(
    "dayOfWeek", "street").agg(count("*").alias("count"))

# Sort by count in descending order
sorted_df = grouped_df.orderBy("count", ascending=False)

# Define a window function to partition by day of week
windowSpec = Window.partitionBy("dayOfWeek").orderBy(col("count").desc())

# Create a rank column for each street within each day of week
ranked_df = sorted_df.withColumn("rank", row_number().over(windowSpec))

# Filter to get top 5 streets for each day of week
top_streets_df = ranked_df.filter(col("rank") <= 5)

# Group by day of week and aggregate total thefts
final_df = top_streets_df.groupBy("dayOfWeek").agg(sum("count").alias("totalThefts"),
                                                   collect_list(struct("street", "count")).alias("topStreets"))

# Project and sort the final result
result_df = final_df.select(
    "dayOfWeek", "topStreets", "totalThefts").orderBy("dayOfWeek")

# Show the result
result_df.show()


# Analysis 3: Report of theft from lock cars by hour in recent one year

# Calculate the starting date for the recent one year
start_date = date_sub(current_date(), 365)

# Filter the data for the recent one year
filtered_df = df.filter((col("category") == "LARCENY/THEFT") &
                        (col("descript").rlike("THEFT FROM LOCKED AUTO")) &
                        ((col("timestamp") >= start_date)))

# Group by hour and calculate count
grouped_df = filtered_df.groupBy(
    hour("timestamp").alias("hour")).agg(count("*").alias("count"))

# Sort by hour
sorted_df = grouped_df.orderBy("hour")

# Show the result
sorted_df.show()


# Stop the SparkSession
spark.stop()
