# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

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
weather_df = spark.read \
    .format("csv") \
    .option("header", "true") \
.option("Schema","weather_schema") \
    .load(NYC_WEATHER_FILE_PATH)

output_path = GROUP_DATA_PATH + "/bronze/historic_weather"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

weather_df.write.format("delta").mode("overwrite").saveAsTable("historic_weather_info")

display(weather_df.count())

# COMMAND ----------

# Monthly Trips year-wise

from pyspark.sql.functions import year, month, count

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

df = spark.read.format("delta").load(delta_path)

df = df.withColumn("started_at", df["started_at"].cast("date"))

monthly_trips = df.groupBy(year("started_at").alias("year"), month("started_at").alias("month")) \
                  .agg(count("*").alias("trips")) \
                  .orderBy("year", "month")

display(monthly_trips)


# COMMAND ----------

# Line chart for Monthly Trips Over Time
from pyspark.sql.functions import concat, lit
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

monthly_trips = monthly_trips.withColumn("year_month", 
                    concat(monthly_trips["year"], lit("-"), monthly_trips["month"]))

sns.lineplot(x="year_month", y="trips", data=monthly_trips.toPandas())

plt.title("Monthly Trips Over Time")
plt.xlabel("Year-Month")
plt.ylabel("Trips")
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------

# Bar Chart for Monthly Trips by Year

sns.catplot(x="month", y="trips", hue="year", kind="bar", data=monthly_trips.toPandas(), height=6, aspect=2)

plt.title("Monthly Trips by Year")
plt.xlabel("Month")
plt.ylabel("Trips")
plt.show()


# COMMAND ----------

# Heat Map for Monthly Trips by Year

pivot_table = monthly_trips.toPandas().pivot("month", "year", "trips")

sns.heatmap(pivot_table, cmap="YlGnBu", annot=True, fmt=".0f")

plt.title("Monthly Trips by Year")
plt.xlabel("Year")
plt.ylabel("Month")
plt.show()

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

df = spark.read.format("delta").load(delta_path)

print('Number of rows: ', df.count())
print('Number of columns: ', len(df.columns))

print('Distinct rideable types', df.select('rideable_type').distinct().count())
df.groupBy('rideable_type').count().show()
print('Distinct end station names', df.select('end_station_name').distinct().count())
df.groupBy('end_station_name').count().show()
print('Distinct membership types', df.select('member_casual').distinct().count())
df.groupBy('member_casual').count().show()


# COMMAND ----------

import matplotlib.pyplot as plt

rideable_counts = df.groupBy('rideable_type').count().orderBy('count', ascending=False).toPandas()

plt.figure(figsize=(8,6))
plt.bar(rideable_counts['rideable_type'], rideable_counts['count'])
plt.title('Rideable Type Counts')
plt.xlabel('Rideable Type')
plt.ylabel('Count')
plt.show()


# COMMAND ----------

from pyspark.sql.functions import hour

hourly_counts = df.groupBy(hour('started_at').alias('hour')).count().orderBy('hour').toPandas()

plt.figure(figsize=(10,6))
plt.plot(hourly_counts['hour'], hourly_counts['count'], marker='o')
plt.title('Hourly Ride Counts')
plt.xlabel('Hour of Day')
plt.ylabel('Ride Count')
plt.xticks(range(0,24))
plt.show()


# COMMAND ----------

from pyspark.sql.functions import date_format, dayofweek, count, sum
import matplotlib.pyplot as plt

pivot_table = df.groupBy(date_format('started_at', 'EEEE').alias('day'), 'member_casual')\
                .agg(count('*').alias('ride_count'))\
                .groupBy('day').pivot('member_casual')\
                .agg(sum('ride_count'))\
                .orderBy(dayofweek('day'))

pivot_table_pd = pivot_table.toPandas()

pivot_table_pd['member'] = pivot_table_pd['member'].astype(float)
pivot_table_pd['casual'] = pivot_table_pd['casual'].astype(float)

plt.figure(figsize=(10,6))
plt.bar(pivot_table_pd['day'], pivot_table_pd['casual'], label='Casual')
plt.bar(pivot_table_pd['day'], pivot_table_pd['member'], bottom=pivot_table_pd['casual'], label='Member')
plt.title('Ride Counts by Day of Week and Membership Type')
plt.xlabel('Day of Week')
plt.ylabel('Ride Count')
plt.legend()
plt.show()


# COMMAND ----------

ride_counts = df.groupby(['rideable_type', 'member_casual']).count().select(['rideable_type', 'member_casual', 'count']).toPandas()

pivot_table = ride_counts.pivot(index='member_casual', columns='rideable_type', values='count')
import matplotlib.pyplot as plt

pivot_table.plot(kind='bar', stacked=True)
plt.title('Rideable Type and Customer Type')
plt.xlabel('Customer Type')
plt.ylabel('Number of Rides')
plt.legend(title='Rideable Type', loc='upper left')
plt.show()


# COMMAND ----------

# Daily Trip Trends

from pyspark.sql.functions import date_format

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

df_q2 = spark.read.format("delta").load(delta_path)
df_q2 = df_q2.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

daily_trips = df_q2.groupBy("start_date").count()
daily_trips = daily_trips.orderBy("start_date")

display(daily_trips)


# COMMAND ----------

# line chart for daily bike trip trends

import matplotlib.dates as mdates
import plotly.express as px

fig = px.line(daily_trips.toPandas(), x="start_date", y="count", title="Daily Bike Trip Trends")
fig.show()

# COMMAND ----------

import plotly.graph_objs as go

fig = go.Figure(data=[go.Bar(x=daily_trips.toPandas()['start_date'], y=daily_trips.toPandas()['count'], 
                             marker=dict(color='blue'))])
fig.update_layout(title='Total Trips by Day', xaxis_title='Day', yaxis_title='Trips')
fig.show()


# COMMAND ----------

# MAGIC %pip install holidays

# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
from pyspark.sql.functions import date_format
import holidays
us_holidays_2020 = holidays.US(years=2020)
holidays = [str(date) for date in us_holidays_2020.keys()]
# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)
from pyspark.sql.functions import col, to_date

df = spark.read.format("delta").load(delta_path)
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

import pyspark.sql.functions as F

#holidays = ["2020-01-01", "2020-01-20", "2020-02-17", "2020-05-25", "2020-07-03", "2020-09-07", "2020-11-26", "2020-12-25"]

df = df.withColumn("is_holiday", F.when(F.col("started_at").isin(holidays), 1).otherwise(0))
daily_trips = df.groupBy("started_at", "is_holiday").agg(F.count("*").alias("trips"))
non_holiday_trips = daily_trips.filter(F.col("is_holiday") == 0).groupBy("started_at").agg(F.sum("trips").alias("trips"))
weekly_trips = non_holiday_trips.groupBy(F.weekofyear("started_at").alias("week")).agg(F.sum("trips").alias("trips")).orderBy("week")

import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

sns.lineplot(x="week", y="trips", data=weekly_trips.toPandas())

plt.title("Weekly Trips Over Time (Excluding Holidays)")
plt.xlabel("Week of Year")
plt.ylabel("Trips")
plt.show()


# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
from pyspark.sql.functions import date_format
import holidays
import pyspark.sql.functions as F

# Define US holidays
us_holidays_2020 = holidays.US(years=2020)
holidays = [str(date) for date in us_holidays_2020.keys()]

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

# Filter out weekends and holidays
df = df.withColumn("day_of_week", F.date_format(F.col("start_date"), "E"))
df = df.withColumn("is_weekday", F.when(F.col("day_of_week").isin(["Mon", "Tue", "Wed", "Thu", "Fri"]), 1).otherwise(0))
df = df.withColumn("is_holiday", F.when(F.col("start_date").isin(holidays), 1).otherwise(0))
df = df.filter((F.col("is_weekday") == 1) & (F.col("is_holiday") == 0))

daily_trips = df.groupBy("start_date").agg(F.count("*").alias("trips"))
weekly_trips = daily_trips.groupBy(F.weekofyear("start_date").alias("week")).agg(F.sum("trips").alias("trips")).orderBy("week")

import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

sns.lineplot(x="week", y="trips", data=weekly_trips.toPandas())

plt.title("Weekly Trips Over Time (Excluding Weekends and Holidays)")
plt.xlabel("Week of Year")
plt.ylabel("Trips")
plt.show()

