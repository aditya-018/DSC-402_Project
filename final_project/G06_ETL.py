# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------

# readstream Bike trip history
from pyspark.sql.types import *

# Define the path to read from and the path to write the output
input_path = "dbfs:/FileStore/tables/raw/bike_trips/"
output_path = "dbfs:/FileStore/tables/G06/bronze/bike_trip_history"

# Define the schema for the data
bike_schema = StructType([
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

# Define the query to read from the CSV file source and write to Delta table
query = (
    spark
    .readStream
    .format("csv")
    .schema(bike_schema)  # specify the schema for the data
    .option("header", "true")  # specify if the file has a header row
    .load(input_path)
    .writeStream
    .format("delta")
    .option("path", output_path)
    .option("checkpointLocation", output_path + "/checkpoint")
    .start()
)

# Wait for the stream to finish
query.awaitTermination()


# COMMAND ----------

# readstream weather history


from pyspark.sql.types import *

# Define the path to read from and the path to write the output
input_path = "dbfs:/FileStore/tables/raw/weather/"
output_path = "dbfs:/FileStore/tables/G06/bronze/weather_history"

# Define the schema for the data
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


# Define the query to read from the CSV file source and write to Delta table
query = (
    spark
    .readStream
    .format("csv")
    .schema(weather_schema)  # specify the schema for the data
    .option("header", "true")  # specify if the file has a header row
    .load(input_path)
    .writeStream
    .format("delta")
    .option("path", output_path)
    .option("checkpointLocation", output_path + "/checkpoint")
    .start()
)

# Wait for the stream to finish
query.awaitTermination()


# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/bronze/bike_trip_history"

# Register Delta table as temporary view
spark.read.format("delta").load(delta_path).createOrReplaceTempView("bike_trip_history_delta")

# Filter data using SQL query
df_g06 = spark.sql("""
  SELECT * 
  FROM bike_trip_history_delta 
  WHERE start_station_name = 'Broadway & E 14 St'
""")

# Display filtered data
display(df_g06)  

# Display count of dataframe
df_g06.count()

# COMMAND ----------

delta_table_name = 'historic_bike_trip_g06'
df_g06.write.format("delta").mode("overwrite").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------


