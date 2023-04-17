# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/historic_weather"))
display(weather_df.count)


# COMMAND ----------

# Load the data file into a DataFrame
df = spark.read.format("csv").option("header", True).load(BIKE_TRIP_DATA_PATH)

# Print the schema of the DataFrame
df.printSchema()

# COMMAND ----------

# History Bike trip
from pyspark.sql.types import StructType, StructField, StringType

bike_schema=StructType([
StructField("ride_id", StringType(), True),
StructField("rideable_type", StringType(), True),
StructField("started_at", StringType(), True),
StructField("ended_at", StringType(), True),
StructField("start_station_name", StringType(), True),
StructField("start_station_id", StringType(), True),
StructField("end_station_name", StringType(), True),
StructField("end_station_id", StringType(), True),
StructField("start_lat", StringType(), True),
StructField("start_lng", StringType(), True),
StructField("end_lat", StringType(), True),
StructField("end_lng", StringType(), True),
StructField("member_casual", StringType(), True)
])

import os
# Read data from a CSV file in batch mode
history_bike_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(BIKE_TRIP_DATA_PATH)

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "history_bike_trips"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

history_bike_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

history_bike_df.write.format("delta").mode("overwrite").saveAsTable("history_bike_trips")

# verify the write
display(history_bike_df.count())

# COMMAND ----------

# History Weather trip
from pyspark.sql.types import StructType, StructField, StringType
weather_schema=StructType([
    StructField("dt", StringType(), True),
    StructField("temp", StringType(), True),
    StructField("feels_like", StringType(), True),
    StructField("pressure", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("dew_point", StringType(), True),
    StructField("uvi", StringType(), True),
    StructField("clouds", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("wind_deg", StringType(), True),
    StructField("pop", StringType(), True),
    StructField("snow_1h", StringType(), True),
    StructField("id", StringType(), True),
    StructField("main", StringType(), True),
    StructField("description", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("loc", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_offset", StringType(), True),
    StructField("rain_1h", StringType(), True),
])


import os
# Read data from a CSV file in batch mode
weather_df = spark.read \
    .format("csv") \
    .option("header", "true") \
.option("Schema","weather_schema") \
    .load(NYC_WEATHER_FILE_PATH)

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "historic_weather"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

weather_df.write.format("delta").mode("overwrite").saveAsTable("historic_weather_info")

# verify the write
display(weather_df.count())

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/historic_weather"))

# COMMAND ----------

import shutil

folder_path = "/dbfs/FileStore/tables/G06"

# Delete the folder and its contents
# shutil.rmtree(folder_path)


# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/raw/bike_trips/", header=True, inferSchema=True)
df.write.format("parquet").option("compression", "snappy").mode("overwrite").save("dbfs:/FileStore/tables/G06/historic_bike_trips")

# COMMAND ----------

csv_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/tables/raw/bike_trips/mnt/source") if f.path.endswith(".csv")]


# COMMAND ----------

print(csv_files)

# COMMAND ----------


    # Read the Delta table as a DataFrame
df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/historic_bike_trips/")

# Display the first 10 rows of the DataFrame
display(df.limit(10))



# COMMAND ----------



checkpoint_path = "dbfs:/FileStore/tables/G06/historic_bike_trips/checkpoint"
