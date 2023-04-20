# Databricks notebook source
# DBTITLE 1,Variables to be used within your project
# MAGIC %run "../includes/includes"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table sample (ID int);

# COMMAND ----------

# DBTITLE 1,Display Historic Trip Files
display(dbutils.fs.ls('dbfs:/FileStore/tables/raw/bike_trips/'))

# COMMAND ----------

csv_files = dbutils.fs.ls('dbfs:/FileStore/tables/raw/bike_trips/')
csv_files1 = dbutils.fs.ls('dbfs:/FileStore/tables/raw/weather/')


# COMMAND ----------

dfs = []
for file in csv_files1:
    if file.isFile() and file.name.endswith(".csv"):
        df = spark.read.csv(file.path, header=True, inferSchema=True)
        dfs.append(df)

# COMMAND ----------

combined_df1 = dfs[0]
for df in dfs[1:]:
      combined_df1 = combined_df1.union(df)

# COMMAND ----------

combined_df = dfs[0]
for df in dfs[1:]:
      combined_df = combined_df.union(df)

# COMMAND ----------

combined_df1.write.mode("overwrite").saveAsTable("historic_NYC_weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(dt) 
# MAGIC FROM historic_NYC_weather

# COMMAND ----------

bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load('dbfs:/FileStore/tables/raw/bike_trips/'+'202111_citibike_tripdata.csv')
display(bdf)

# COMMAND ----------

bdf.printSchema()

# COMMAND ----------

path = 'dbfs:/FileStore/tables/raw/bike_trips/'+'202111_citibike_tripdata.csv'

df = spark.read.csv(path)

df.show()

# COMMAND ----------

spark.sql(
    CREATE TABLE historic_bike_trips (
    ride_id varchar,
    rideable_type varchar,
    started_at date,
    ended_at date,
    start_station_name varchar,
      start_station_id 
    ) location 'dbfs:/FileStore/tables/G06/'
)

# COMMAND ----------

CREATE TABLE historic_bike_trips (
ride_id varchar,
rideable_type varchar,
started_at date,
    
)

# COMMAND ----------

# DBTITLE 1,Display Historic Weather Data
wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load('dbfs:/FileStore/tables/raw/weather/'+'NYC_Weather_Data.csv')
display(wdf)

# COMMAND ----------

wdf.printSchema()

# COMMAND ----------

# DBTITLE 1,Display Bike Station Information
display(spark.read.format('delta').load('dbfs:/FileStore/tables/bronze_station_info.delta'))

# COMMAND ----------

# DBTITLE 1,Display the Bike Station Status Information
display(spark.read.format('delta').load('dbfs:/FileStore/tables/bronze_station_status.delta'))

# COMMAND ----------

# DBTITLE 1,Display the current (within the hour) NYC Weather Information
display(spark.read.format('delta').load('dbfs:/FileStore/tables/bronze_nyc_weather.delta'))
