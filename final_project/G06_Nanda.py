# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------


display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/"))

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/tables/G06/bronze/history_bike_trips/", True)

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
output_path = GROUP_DATA_PATH + "/bronze/history_bike_trips"

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
output_path = GROUP_DATA_PATH + "/bronze/historic_weather"

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

# loading the bike trip history from group data path to group database

table_name = "bike_trip_history"
database_name = "G06_db"

# Define path to Delta table
delta_path = "dbfs:/FileStore/tables/G06/history_bike_trip/"

# Create SQL table using Delta as data source
spark.sql(f"""
  CREATE TABLE G06_db.historic_bike_trip
  USING DELTA
  LOCATION "{delta_path}"
""")

# Write data to Delta table with mergeSchema option
bike_trip_history.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(delta_path)


# COMMAND ----------

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

# import shutil
# folder_path = "dbfs:/FileStore/tables/G06/history_bike_trip/"
# shutil.rmtree(folder_path)

dbutils.fs.rm("dbfs:/FileStore/tables/G06/", True)



# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/raw/bike_trips/", header=True, inferSchema=True)
df.write.format("parquet").option("compression", "snappy").mode("overwrite").save("dbfs:/FileStore/tables/G06/historic_bike_trips")

# COMMAND ----------

csv_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/tables/raw/bike_trips/mnt/source") if f.path.endswith(".csv")]


# COMMAND ----------

print(csv_files)

# COMMAND ----------


    # Read the Delta table as a DataFrame
df = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/historic_weather/")

# Display the first 10 rows of the DataFrame
display(df.limit(10))



# COMMAND ----------



checkpoint_path = "dbfs:/FileStore/tables/G06/historic_bike_trips/checkpoint"

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;
# MAGIC use g06_db;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC use g06_db;
# MAGIC show tables;

# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/bronze/history_bike_trips/"

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

# montly trip trends

# Set the path to the Delta table
delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)

# Display the DataFrame
# display(df["rideable_type"].unique())

from pyspark.sql.functions import col

# Assuming you have a DataFrame named "df" and a column named "col_name"
count = df.count()

display(count)



# COMMAND ----------

display(df.printSchema())

# COMMAND ----------

# Question 1 Monthly Trips

from pyspark.sql.functions import year, month, count

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)

# Convert the 'started_at' column to a date type
df = df.withColumn("started_at", df["started_at"].cast("date"))

# Calculate the monthly trips
monthly_trips = df.groupBy(year("started_at").alias("year"), month("started_at").alias("month")) \
                  .agg(count("*").alias("trips")) \
                  .orderBy("year", "month")

# Show the monthly trips DataFrame
display(monthly_trips)


# COMMAND ----------

# Line chart (1)

from pyspark.sql.functions import concat, lit
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

# Concatenate the year and month columns to create the 'year_month' column
monthly_trips = monthly_trips.withColumn("year_month", 
                    concat(monthly_trips["year"], lit("-"), monthly_trips["month"]))

# Create a line chart showing the monthly trips over time
sns.lineplot(x="year_month", y="trips", data=monthly_trips.toPandas())

plt.title("Monthly Trips Over Time")
plt.xlabel("Year-Month")
plt.ylabel("Trips")
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------

# Bar Chart

# Create a bar chart showing the number of trips for each month
sns.catplot(x="month", y="trips", hue="year", kind="bar", data=monthly_trips.toPandas(), height=6, aspect=2)

plt.title("Monthly Trips by Year")
plt.xlabel("Month")
plt.ylabel("Trips")
plt.show()


# COMMAND ----------

# Heat Map

# Create a pivot table with the number of trips for each month and year
pivot_table = monthly_trips.toPandas().pivot("month", "year", "trips")

# Create a heatmap showing the number of trips for each month and year
sns.heatmap(pivot_table, cmap="YlGnBu", annot=True, fmt=".0f")

plt.title("Monthly Trips by Year")
plt.xlabel("Year")
plt.ylabel("Month")
plt.show()

# COMMAND ----------

# Question 2 Daily Trip Trends

from pyspark.sql.functions import date_format

# Convert the 'started_at' column to a DateType column and extract the date
delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

# Load the Delta table as a DataFrame
df_q2 = spark.read.format("delta").load(delta_path)
df_q2 = df_q2.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

# Group by start date and count the number of rides per day
daily_trips = df_q2.groupBy("start_date").count()
daily_trips = daily_trips.orderBy("start_date")

# Display the resulting DataFrame
display(daily_trips)


# COMMAND ----------

# line chart (2)

import matplotlib.dates as mdates
import plotly.express as px

# Create line chart of daily trips

# Create a line chart showing the daily trips over time

fig = px.line(daily_trips.toPandas(), x="start_date", y="count", title="Daily Bike Trip Trends")
fig.show()



# COMMAND ----------

import plotly.graph_objs as go

# Create bar chart of total trips by day
fig = go.Figure(data=[go.Bar(x=daily_trips.toPandas()['start_date'], y=daily_trips.toPandas()['count'], 
                             marker=dict(color='blue'))])
fig.update_layout(title='Total Trips by Day', xaxis_title='Day', yaxis_title='Trips')
fig.show()


# COMMAND ----------

# Question 3 holiday affect the daily (non-holiday) system use trend



# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)

print(df.printSchema)

# COMMAND ----------

import pandas as pd
import holidays

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)
# Get list of US holidays for the year
us_holidays = holidays.US(years=2022)

# Create new column indicating whether or not each day is a holiday
df_daily_trips_pd['is_holiday'] = df_daily_trips_pd['start_date'].apply(lambda x: x in us_holidays)
pivot = pd.pivot_table(
    df_daily_trips_pd,
    values='ride_id',
    index=['year', 'month'],
    columns=['is_holiday'],
    aggfunc='count',
    fill_value=0
)

# calculate the mean number of rides for each month and year
pivot['mean_rides'] = pivot.mean(axis=1)

# calculate the mean number of rides for each month and year, separated by holiday and non-holiday
pivot['mean_holiday_rides'] = pivot[True] / pivot[True].sum()
pivot['mean_non_holiday_rides'] = pivot[False] / pivot[False].sum()

# create a stacked bar chart to compare the average number of rides per month and year, by holiday and non-holiday
pivot[['mean_non_holiday_rides', 'mean_holiday_rides']].plot(kind='bar', stacked=False)

# COMMAND ----------

# Import required libraries
import holidays
from pyspark.sql.functions import to_date, count, when
import plotly.express as px

# Load the data from the Delta table
delta_table = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
daily_rides = spark.read.format("delta").load(delta_table)

# Convert date strings to date type
daily_rides = daily_rides.withColumn("date", to_date("started_at"))

# Get daily ride counts
daily_counts = daily_rides.groupBy("date").agg(count("*").alias("count"))

# Add a column to indicate whether the day is a holiday or not
us_holidays = holidays.US()
daily_counts = daily_counts.withColumn("is_holiday", when(col("date").isin(us_holidays), "Yes").otherwise("No"))

# Plot the daily ride counts using Plotly
fig = px.line(daily_counts.toPandas(), x="date", y="count", color="is_holiday", title="Impact of Holidays on Daily System Use Trend")
fig.show()


# COMMAND ----------



# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")


# COMMAND ----------

import holidays

# Get list of US holidays for the year
us_holidays = holidays.US(years=2022)

# Create new column indicating whether or not each day is a holiday
df_daily_trips_pd['is_holiday'] = df_daily_trips_pd['start_date'].apply(lambda x: x in us_holidays)

# COMMAND ----------

import pandas as pd

pivot = pd.pivot_table(
    df_daily_trips_pd,
    values='ride_id',
    index=['year', 'month'],
    columns=['is_holiday'],
    aggfunc='count',
    fill_value=0
)

# calculate the mean number of rides for each month and year
pivot['mean_rides'] = pivot.mean(axis=1)

# calculate the mean number of rides for each month and year, separated by holiday and non-holiday
pivot['mean_holiday_rides'] = pivot[True] / pivot[True].sum()
pivot['mean_non_holiday_rides'] = pivot[False] / pivot[False].sum()

# create a stacked bar chart to compare the average number of rides per month and year, by holiday and non-holiday
pivot[['mean_non_holiday_rides', 'mean_holiday_rides']].plot(kind='bar', stacked=False)

# COMMAND ----------

delta_table = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
daily_rides = spark.read.format("delta").load(delta_table)
display(daily_rides.show(5)

# COMMAND ----------

delta_table = "dbfs:/FileStore/tables/G06/bronze/historic_weather/"
daily_rides = spark.read.format("delta").load(delta_table)
display(daily_rides.printSchema())
display(daily_rides.limit(10))

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.functions import to_timestamp, from_unixtime
delta_table = "dbfs:/FileStore/tables/G06/bronze/historic_weather/"
weather_df = spark.read.format("delta").load(delta_table)
weather_df_with_datetime = weather_df.withColumn("datetime", to_timestamp(from_unixtime(weather_df["dt"])))

weather_df_with_datetime = weather_df_with_datetime.withColumn("date", weather_df_with_datetime["datetime"].cast("date"))
weather_df_with_datetime = weather_df_with_datetime.withColumn("time", split(weather_df_with_datetime["datetime"].cast("string"), " ")[1])




# COMMAND ----------

from pyspark.sql.functions import to_timestamp, from_unixtime, split, col

delta_table = "dbfs:/FileStore/tables/G06/bronze/history_bike_trips/"
rides_df = spark.read.format("delta").load(delta_table)

# Split the started_at column into date and time columns
rides_df = rides_df.withColumn("date", split(col("started_at"), " ")[0])
rides_df = rides_df.withColumn("time", split(col("started_at"), " ")[1])

# Combine the date and time columns into a single timestamp column
# rides_df = rides_df.withColumn("timestamp", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss"))

# Show the result
display(rides_df.limit(10))


# COMMAND ----------

display(weather_df_with_datetime.printSchema())

# COMMAND ----------


display(rides_df.printSchema())


# COMMAND ----------


