# Databricks notebook source
display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/"))
GROUP_DATA_PATH = "dbfs:/FileStore/tables/G06"

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/bronze/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/bronze/bike_trip_history/"))

# COMMAND ----------

df_1 = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/bike_trip_history/")

# COMMAND ----------

display(df_1.limit(10))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/bronze/weather_history/"))

# COMMAND ----------

df_weather = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/weather_history/")
display(df_weather.limit(10))

# COMMAND ----------

df_rides = spark.read.format("delta").load("dbfs:/FileStore/tables/G06/historic_bike_trip_g06/")
display(df_rides.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

delta_table = "dbfs:/FileStore/tables/G06/bronze/weather_history/"
daily_weather = spark.read.format("delta").load(delta_table)
display(daily_weather.printSchema())
display(daily_weather.limit(10))

# COMMAND ----------

from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_timestamp, from_unixtime
from pyspark.sql.functions import split

daily_weather_time = daily_weather.withColumn("datetime", to_timestamp(from_unixtime(daily_weather["dt"])))
daily_weather_time = daily_weather_time.withColumn("date", daily_weather_time["datetime"].cast("date"))
daily_weather_time = daily_weather_time.withColumn("time", split(daily_weather_time["datetime"].cast("string"), " ")[1])
display(daily_weather_time.limit(6))

# COMMAND ----------

daily_weather_time.count()

# COMMAND ----------

display(df_1.limit(10))

# COMMAND ----------

display(df_rides.limit(10))
display(df_weather.limit(10))

# COMMAND ----------

print(df_rides.count())
print(df_weather.count())

# COMMAND ----------

from pyspark.sql.functions import col
df_weather = df_weather.drop('dt')

# COMMAND ----------

display(df_weather.limit(10))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import year, month, count,hour, round, date_format,to_timestamp,when,lit,concat,ceil,col
import pandas as pd

#df_rides.select(date_format('INTERRUPTION_TIME', 'M/d/yyyy').alias('INTERRUPTION_DATE'), date_format('INTERRUPTION_TIME', 'h:m:s a').alias('TIME'))
df_rides = df_rides.toPandas()
df_rides['started_at'] = pd.to_datetime(df_rides['started_at'])
df_rides['date'] = df_rides['started_at'].dt.date
df_rides['time'] = df_rides['started_at'].dt.round('H').apply(lambda x: x.strftime('%H:%M:%S'))

# COMMAND ----------

df_rides.head()

# COMMAND ----------

df_rides['ended_at'] = pd.to_datetime(df_rides['ended_at'])
df_rides['date_ended'] = df_rides['ended_at'].dt.date
df_rides['time_ended'] = df_rides['ended_at'].dt.round('H').apply(lambda x: x.strftime('%H:%M:%S'))

# COMMAND ----------

df_rides.head()

# COMMAND ----------

daily_weather_time = daily_weather_time.toPandas()
daily_weather_time.head()

# COMMAND ----------

duplicates = daily_weather_time[daily_weather_time[['date', 'time']].duplicated()]

# COMMAND ----------

daily_weather_time_2 = daily_weather_time.drop_duplicates(subset=['date', 'time'])

# COMMAND ----------

merged_df = pd.merge(df_rides,daily_weather_time_2, how='left', on=['date', 'time'])
spark_df = spark.createDataFrame(merged_df)

# COMMAND ----------

display(spark_df.limit(10))

# COMMAND ----------

merged_df.head()

# COMMAND ----------

df_weather.groupBy('main').count().show()

# COMMAND ----------

hourly_counts = spark_df.groupBy(hour('started_at').alias('hour')).count().orderBy('hour').toPandas()

# COMMAND ----------

merged_df.dtypes

# COMMAND ----------

def plot_daily_trend(data, x_col, y_col, hue_col):
    plt.figure(figsize=(12,6))
    sns.lineplot(x=x_col, y=y_col, hue=hue_col, data=data)
    plt.title('Daily Usage Trend')
    plt.xlabel('Date')
    plt.ylabel('Total Rides')
    plt.show()


def plot_hourly_trend(data, x_col, y_col, hue_col):
    plt.figure(figsize=(12,6))
    sns.lineplot(x=x_col, y=y_col, hue=hue_col, data=data)
    plt.title('Hourly Usage Trend')
    plt.xlabel('Hour of Day')
    plt.ylabel('Total Rides')
    plt.show()

# COMMAND ----------

merged_df.columns

# COMMAND ----------

merged_df['datetime'].value_counts()

# COMMAND ----------

#print('Distinct rideable types', merged_df.select('rideable_type').distinct().count())
spark_df.groupBy('rideable_type').count().show()

# COMMAND ----------

from pyspark.sql.functions import hour

hourly_counts = spark_df.groupBy(hour('time').alias('hour')).count().orderBy('hour').toPandas()
hourly_counts

# COMMAND ----------

import pyspark.sql.functions as F
import plotly.express as px

dec_rides_weather = spark_df.filter((F.col('date') >= '2021-12-01') & (F.col('date') <= '2021-12-31'))

dec_rides_weather_df = dec_rides_weather.toPandas()

# COMMAND ----------

dec_rides_weather_df.head()

# COMMAND ----------

dec_rides_weather_df.shape

# COMMAND ----------

from pyspark.sql import SparkSession
dec_rides_weather_df['ride_count'] = dec_rides_weather_df['ride_id'].value_counts()
spark = SparkSession.builder.appName("dec_spark_df").getOrCreate()
dec_spark_df = spark.createDataFrame(dec_rides_weather_df)
display(dec_spark_df.limit(10))

# COMMAND ----------

dec_rides_weather_df.head()

# COMMAND ----------

from pyspark.sql.functions import avg
rides_by_weather = dec_spark_df.groupBy('date','main','time')\
    .agg(avg('temp').alias('avg_temp'), count('ride_id').alias('ride_count'), avg('clouds').alias('avg_clouds'))
display(rides_by_weather.limit(10))
rides_by_weather_df = rides_by_weather.toPandas()

# COMMAND ----------

# Ride Counts per day in the month of December
import plotly.express as px
fig = px.bar(rides_by_weather_df['ride_count'], rides_by_weather_df['date'])
fig.show()

# COMMAND ----------

import plotly.graph_objects as go

fig = go.Figure()
fig.update_traces(marker_color='blue')
fig.add_trace(go.Bar(x=rides_by_weather_df.date, y=rides_by_weather_df.avg_temp,name="Temp", yaxis="y1"))

fig.update_layout(
   xaxis=dict(domain=[0.15, 0.15]),

yaxis=dict(
   title="Avg. Temp",
   overlaying="y",
   side="right",
   position=1)
)

fig.update_layout(title_text="Daily Trips Vs. Avg. Temperature",
width=1016, height=600)
fig.show()

# COMMAND ----------

fig = go.Figure()
fig.update_traces(marker_color='red')
fig.add_trace(go.Bar(x=rides_by_weather_df.date, y=rides_by_weather_df.avg_clouds,name="Clouds", yaxis="y"))

fig.update_layout(
   xaxis=dict(domain=[0.15, 0.15]),

yaxis=dict(
   title="Avg. Clouds",
   overlaying="y",
   side="right",
   position=1)
)

fig.update_layout(title_text="Daily Trips Vs. Avg. Clouds",
width=1016, height=600)
fig.show()

# COMMAND ----------


