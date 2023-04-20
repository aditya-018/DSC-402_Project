# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/"))

# COMMAND ----------


    # Read the Delta table as a DataFrame
df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/weather_history")

# Display the first 10 rows of the DataFrame

count = df.count()

# Print the count
print("Number of records: ", count)



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


