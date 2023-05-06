# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/" 

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06"))
# dbutils.fs.rm("dbfs:/FileStore/tables/G06/bronze/weather_history/",True)

# COMMAND ----------

# readstream Bike trip history
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# Define the path to read from and the path to write the output
input_path = "dbfs:/FileStore/tables/raw/bike_trips/"
output_path = "dbfs:/FileStore/tables/G06/bronze/nyc_raw_bike_trip_history"

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
    .option("mode", "append")
    .option("zOrderByCol","started_at")
    .partitionBy("start_station_name")
    .trigger(availableNow=True)
    .start()
)
# Wait for the stream to finish
query.awaitTermination()


# COMMAND ----------

# readstream Bike trip history

df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_raw_bike_trip_history")
df=df.filter(df['start_station_name']=='Broadway & E 14 St')
df = df.withColumn("date", date_format("started_at", "yyyy-MM-dd").cast("date"))
df = df.withColumn("rounded_time", date_trunc("hour", "started_at")) \
       .withColumn("hour", lpad(hour("rounded_time").cast("string"), 2, "0")) \
       .withColumn("time", concat("hour", lit(":00:00"))) \
       .drop("rounded_time", "hour")
       
df = df.withColumn("year", year("date"))
df = df.withColumn("month", month("date"))

for column in df.columns:
    mode_value = df.select(column).groupBy(column).count().orderBy(F.desc("count")).first()[0]
    df = df.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))

for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    print(f"Column '{column}' has {null_count} null values.")

counts = df.groupBy(df.columns).count()

# filter only the rows with count > 1
duplicates = counts.filter("count > 1")

# show the resulting duplicates
duplicates.show()
df = df.dropDuplicates()
# save DataFrame as Delta table
df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save("dbfs:/FileStore/tables/G06/bronze/nyc_bike_trip_history")



# COMMAND ----------

sample=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_bike_trip_history")
display(sample)

# COMMAND ----------

# readstream weather history
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

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
    StructField("rain_1h", StringType(), True)
])


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
    .option("mode", "append")
    .option("zOrderByCol","dt")
    .partitionBy("main")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

 


# COMMAND ----------

df=spark.read.format("delta").load(output_path)
display(df.count())
df = df.withColumn("datetime", to_timestamp(from_unixtime(df["dt"])))
df = df.withColumn("date", df["datetime"].cast("date"))
df = df.withColumn("time", split(df["datetime"].cast("string"), " ")[1])
df = df.withColumn("year", year("date"))
df = df.withColumn("month", month("date"))
df=df.drop("datetime")
for column in df.columns:
    mode_value = df.select(column).groupBy(column).count().orderBy(F.desc("count")).first()[0]
    df = df.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))

for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    print(f"Column '{column}' has {null_count} null values.")

counts = df.groupBy(df.columns).count()

# filter only the rows with count > 1
duplicates = counts.filter("count > 1")

# show the resulting duplicates
duplicates.show()
df = df.dropDuplicates()
# save DataFrame as Delta table
df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_path)

# COMMAND ----------

# delta table for ML model
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

delta_path = "dbfs:/FileStore/tables/G06/bronze/nyc_raw_bike_trip_history"

ml_df_g06=spark.read.format("delta").load(delta_path)

# Read Delta table and filter rows
ml_df_g06 = ml_df_g06.filter((ml_df_g06.start_station_name == 'Broadway & E 14 St') | (ml_df_g06.end_station_name == 'Broadway & E 14 St'))

ml_df_g06 = ml_df_g06.withColumn("date", date_format("started_at", "yyyy-MM-dd").cast("date"))
ml_df_g06 = ml_df_g06.withColumn("rounded_time", date_trunc("hour", "started_at")) \
       .withColumn("hour", lpad(hour("rounded_time").cast("string"), 2, "0")) \
       .withColumn("time", concat("hour", lit(":00:00"))) \
       .drop("rounded_time", "hour")
       
ml_df_g06 = ml_df_g06.withColumn("year", year("date"))
ml_df_g06 = ml_df_g06.withColumn("month", month("date"))

# Write filtered DataFrame to a Delta table partitioned by start_station_name and end_station_name
delta_table_name = 'ml_historic_bike_trip_g06'
ml_df_g06.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GROUP_DATA_PATH + delta_table_name)



# COMMAND ----------

# Silver Table for weather

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read the original Delta table
df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_weather_history")

# Select the desired columns
new_df = df.select(col("dt"), col("temp"), col("feels_like"),col("humidity"),col("main"),col("date"),col("time"),col("wind_speed"),col("visibility"),col("pressure"),col("rain_1h"))

# Write the new DataFrame as a Delta table in a new path
new_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected")


# COMMAND ----------

# Silver Table for bike ride

from pyspark.sql.functions import col

# Read the original Delta table
df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_bike_trip_history")



# Loop through each column and count null values
for column in df.columns:
  null_count = df.filter(col(column).isNull()).count()
  print(f"Column '{column}' has {null_count} null values.")


# Select the desired columns
new_df = df.select(col("ride_id"), col("rideable_type"), col("started_at"),col("ended_at"),col("start_station_name"),col("start_station_id"),col("end_station_name"),col("end_station_id"),col("member_casual"),col("date"),col("time"))



# Write the new DataFrame as a Delta table in a new path
new_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/G06/silver/nyc_bike_trip_history_selected")


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

gold_path = GROUP_DATA_PATH + "gold"
model_information=gold_path+"/model_information"
schema = StructType([
    StructField("ds", StringType(), True),
    StructField("y", DoubleType(), True),
    StructField("yhat", DoubleType(), True),
    StructField("tag", StringType(), True),
    StructField("residual", DoubleType(), True),
    StructField("mae", DoubleType(), True)
])

gold_df = spark.createDataFrame([], schema=schema)

gold_df.write.format("delta").mode("overwrite").save(model_information)

# COMMAND ----------


