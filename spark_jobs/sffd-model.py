import matplotlib.pyplot as plt
import pandas as pd
from prophet import Prophet
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from statsmodels.tsa.arima.model import ARIMA

mongodb_uri = ""

# Create a SparkSession for MongoDB Altlas
spark = (
    SparkSession.builder.appName("Forecasting Hospital Calls")
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

# Load the data from MongoDB
sffd_df = reader.option("collection", "sffd_service_calls").load()

sffd_filtered_df = sffd_df.filter(
    (sffd_df["hospital_timestamp"].isNotNull()) & (sffd_df["unit_type"] == "MEDIC")
)
result_df = (
    sffd_filtered_df.groupBy(
        date_format("hospital_timestamp", "yyyy-MM").alias("month_year")
    )
    .agg(count("unit_type").alias("count"))
    .orderBy("month_year")
)

result_df.show()

month_year_values = result_df.select("month_year", "count").rdd
months = month_year_values.map(lambda x: x[0]).collect()
numbers = month_year_values.map(lambda x: x[1]).collect()
df = pd.DataFrame({"ds": months, "y": numbers})

plt.figure(figsize=(10, 6))
plt.plot(months[50:-2], numbers[50:-2], marker="o")
plt.title("Number of Calls Over Time")
plt.xlabel("Month-Year")
plt.ylabel("Calls")
plt.grid(True)
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

model = ARIMA(numbers, order=(1, 1, 1))
model_fit = model.fit()

# Forecast the next quarter (3 months)
forecast = model_fit.forecast(steps=3)
print("Forecast for the next quarter:")
print(forecast)

model = Prophet()

# Fit the model to the data
model.fit(df)

# Create a DataFrame for future predictions (next quarter)
future = model.make_future_dataframe(periods=3, freq="M")

# Make predictions
forecast = model.predict(future)

# Print the forecast for the next quarter
forecast["ds"] = forecast["ds"].astype(str).str[:-3]
forecast["yhat"] = forecast["yhat"].astype(int)
store = forecast[["ds", "yhat"]]
schema = StructType(
    [
        StructField("month_year", StringType(), nullable=False),
        StructField("count", LongType(), nullable=False),
    ]
)
df_spark = spark.createDataFrame(store, schema=schema)
print(store)
(
    df_spark.write.format("mongodb")
    .mode("overwrite")
    .option("connection.uri", mongodb_uri)
    .option("database", "SanFrancisco")
    .option("collection", "sffd_aggregations")
    .save()
)
# Stop the SparkSession
spark.stop()
