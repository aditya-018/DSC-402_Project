# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/" 

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/silver/"))
# dbutils.fs.rm("dbfs:/FileStore/tables/G06/silver/nyc_bike_trip_history_selected",True)

# COMMAND ----------

# readstream Bike trip history
from pyspark.sql.types import *
from pyspark.sql.functions import *
# Define the path to read from and the path to write the output
input_path = "dbfs:/FileStore/tables/raw/bike_trips/"
output_path = "dbfs:/FileStore/tables/G06/bronze/nyc_bike_trip_history"

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
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True)
])

# Define the query to read from the CSV file source and write to Delta table
df=spark.readStream.format("csv").schema(bike_schema).option("header","true").load(input_path)
df = df.withColumn("date", date_format("started_at", "yyyy-MM-dd").cast("date"))
df = df.withColumn("rounded_time", date_trunc("hour", "started_at")) \
       .withColumn("hour", lpad(hour("rounded_time").cast("string"), 2, "0")) \
       .withColumn("time", concat("hour", lit(":00:00"))) \
       .drop("rounded_time", "hour")
df = df.withColumn("year", year("date"))
df = df.withColumn("month", month("date"))
display(df.limit(5))
# for i in df.columns:
#     s=df.filter(col(i).isNull())
#     display(df)
#     if s.count()==0:
#         print("column",i,"has no null values")
#     else:
#         print("column",i,"has NULL values")
#         display(df.filter(col(i).isNull()))


# query = (
#     spark
#     .readStream
#     .format("csv")
#     .schema(bike_schema)  # specify the schema for the data
#     .option("header", "true")  # specify if the file has a header row
#     .load(input_path)
#     .writeStream
#     .format("delta")
#     .option("path", output_path)
#     .option("checkpointLocation", output_path + "/checkpoint")
#     .option("partitionBy", "start_station_name")
#     .start()
# )
query = df.writeStream \
           .format("delta") \
           .option("checkpointLocation", output_path + "/checkpoint") \
           .option("path", output_path) \
           .option("mode", "append") \
           .partitionBy("year", "month") \
           .start()

# Wait for the stream to finish
query.awaitTermination()


# COMMAND ----------

# readstream weather history

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the path to read from and the path to write the output
input_path = "dbfs:/FileStore/tables/raw/weather/"
output_path = "dbfs:/FileStore/tables/G06/bronze/nyc_weather_history"

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
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True)
])


# Define the query to read from the CSV file source and write to Delta table
# df=spark.read.csv("dbfs:/FileStore/tables/raw/weather/",header=True,schema=weather_schema)
df=spark.readStream.format("csv").schema(weather_schema).option("header","true").load(input_path)
# for i in df.columns:
#     s=df.filter(col(i).isNull())
#     display(df)
#     if s.count()==0:
#         print("column",i,"has no null values")
#     else:
#         print("column",i,"has NULL values")
#         display(df.filter(col(i).isNull()))

df = df.withColumn("datetime", to_timestamp(from_unixtime(df["dt"])))

df = df.withColumn("date", df["datetime"].cast("date"))
df = df.withColumn("time", split(df["datetime"].cast("string"), " ")[1])
df = df.withColumn("year", year("date"))
df = df.withColumn("month", month("date"))
df=df.drop("datetime")
display(df.limit(5))

query = df.writeStream \
           .format("delta") \
           .option("checkpointLocation", output_path + "/checkpoint") \
           .option("path", output_path) \
           .option("mode", "append") \
           .partitionBy("year", "month") \
           .start()

query.awaitTermination()




# COMMAND ----------

# check for null values in the data

for i in df.columns:
    s=df.filter(col(i).isNull())
    display(df)
    if s.count()==0:
        print("column",i,"has no null values")
    else:
        print("column",i,"has NULL values")

# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/bronze/nyc_bike_trip_history"

# Register Delta table as temporary view
spark.read.format("delta").load(delta_path).createOrReplaceTempView("bike_trip_history_delta")

# Filter data using SQL query
df_g06 = spark.sql("""
  SELECT * 
  FROM bike_trip_history_delta 
  WHERE start_station_name = 'Broadway & E 14 St'
""")

# Display filtered data
display(df_g06.head(5))  

# Display count of dataframe
df_g06.count()

# COMMAND ----------

# from pyspark.sql.functions import orderby

display(df_g06.orderBy("started_at").tail(5))

# COMMAND ----------

delta_table_name = 'historic_bike_trip_g06'
df_g06.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/bronze/bike_trip_history"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/bronze/weather_history"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/bronze/weather_history"))

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_weather_history")
display(df.limit(5))

# COMMAND ----------

display(df.select('snow_1h').distinct())

# COMMAND ----------

df=df.drop("datetime")
df=display

# COMMAND ----------

display(df.printSchema())

# COMMAND ----------

# Silver Table for weather

from pyspark.sql.functions import col

# Read the original Delta table
df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_weather_history")

# Select the desired columns
new_df = df.select(col("dt"), col("temp"), col("feels_like"),col("humidity"),col("main"),col("date"),col("time"))


weather_schema=StructType([
    StructField("dt", StringType(), True),
    StructField("temp", StringType(), True),
    StructField("feels_like", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("main", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True)
    
])


# Write the new DataFrame as a Delta table in a new path
new_df.write.format("delta").mode("append").save("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected")


# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/historic_bike_trip_g06/")
display(df.printSchema())

# COMMAND ----------

# Silver Table for bike ride

from pyspark.sql.functions import col

# Read the original Delta table
df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/historic_bike_trip_g06/")

# Select the desired columns
new_df = df.select(col("ride_id"), col("rideable_type"), col("started_at"),col("ended_at"),col("start_station_name"),col("start_station_id"),col("end_station_name"),col("end_station_id"),col("member_casual"),col("date"),col("time"))



# Write the new DataFrame as a Delta table in a new path
new_df.write.format("delta").mode("append").save("dbfs:/FileStore/tables/G06/silver/nyc_bike_trip_history_selected")


# COMMAND ----------

display(new_df.limit(5))

# COMMAND ----------


