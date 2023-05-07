# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/" 

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql import functions as F
# ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_bike_trip_history_selected")
ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/ml_historic_bike_trip_g06/")
df=ride_df.toPandas()
weather_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected")
weather_df=weather_df.toPandas()
merged_df = pd.merge(weather_df,df, how='left', on=['date', 'time'])
spark_df = spark.createDataFrame(merged_df)
display(spark_df.count())
spark_df=spark_df.drop('dt','started_at','ended_at','start_lat','end_lat','start_lng','end_lng','year','month')
display(spark_df)


# COMMAND ----------

duplicates =ride_df[ride_df[['date', 'time']].duplicated()]
display(duplicates)

# COMMAND ----------

display(spark_df.printSchema())

# COMMAND ----------


from pyspark.sql.functions import hour, sum, when, col
from pyspark.sql.window import Window

# create a window for each hour
hour_window = Window.partitionBy('date', hour('time')).orderBy('time')

# calculate the net bike change for each hour
# calculate the net bike change and count of rides starting/ending at Broadway & E 14 St for each hour
net_bike_change = (sum(when(col('start_station_name') == 'Broadway & E 14 St', -1)
                     .when(col('end_station_name') == 'Broadway & E 14 St', 1)
                     .otherwise(0)).over(hour_window))
rides_started = (sum(when(col('start_station_name') == 'Broadway & E 14 St', 1)
                     .otherwise(0)).over(hour_window))
rides_ended = (sum(when(col('end_station_name') == 'Broadway & E 14 St', 1)
                     .otherwise(0)).over(hour_window))

# add the columns to the dataframe

spark_df = spark_df.withColumn('rides_started', rides_started)
spark_df = spark_df.withColumn('rides_ended', rides_ended)
spark_df = spark_df.withColumn('net_bike_change', net_bike_change)

# select relevant columns, group by hour, and calculate total net bike change
hourly_net_bike_change = spark_df.select('temp', 'feels_like', 'humidity', 'main', 'date', hour('time').alias('hour'), 'rides_started', 'rides_ended','net_bike_change')

# show the result
hourly_net_bike_change = hourly_net_bike_change.dropDuplicates()
for column in hourly_net_bike_change.columns:
    null_count = hourly_net_bike_change.filter(col(column).isNull()).count()
    print(f"Column '{column}' has {null_count} null values.")

display(hourly_net_bike_change)






# COMMAND ----------

from pyspark.sql.functions import hour, sum, when, col, least, lit
from pyspark.sql.window import Window

# create a window for each hour
hour_window = Window.partitionBy('date', hour('time')).orderBy('time')

# calculate the net bike change for each hour and limit it to 113
net_bike_change = least(
    lit(113), 
    sum(
        when(col('start_station_name') == 'Broadway & E 14 St', -1)
        .when(col('end_station_name') == 'Broadway & E 14 St', 1)
        .otherwise(0)
    ).over(hour_window)
)

# add the net_bike_change column to the dataframe
spark_df = spark_df.withColumn('net_bike_change', net_bike_change)

# select relevant columns, group by hour, and calculate total net bike change
hourly_net_bike_change = spark_df.select(
    'temp', 'feels_like', 'humidity', 'main', 'date', hour('time').alias('hour'), 'net_bike_change'
)


# show the result
hourly_net_bike_change = hourly_net_bike_change.dropDuplicates()
display(hourly_net_bike_change)


# COMMAND ----------

display(hourly_net_bike_change.printSchema())

# COMMAND ----------

display(spark_df.sort(spark_df["date"].asc()))

# COMMAND ----------

display(spark_df.filter(spark_df['date']=='2021-11-20'))

# COMMAND ----------

pip install fbprophet

# COMMAND ----------

display(pdf)

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/ml_historic_bike_trip_g06/")
display(df.filter(df["date"]=='2023-04-01'))

# COMMAND ----------

# Dates with zero rides or missing data
import datetime
import pandas as pd
from pyspark.sql.functions import *
start_date = "2021-11-01"
end_date = "2023-04-18"
df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_weather_history")
date_range = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%Y-%m-%d').tolist()
df = df.withColumn("date", date_format("date", "yyyy-MM-dd").cast("date"))
start_date_list = [str(row.date) for row in df.select("date").collect()]
uncommon_elements = list(set(date_range).symmetric_difference(set(start_date_list)))
uncommon_elements = sorted(uncommon_elements, key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
print(f"Total {len(uncommon_elements)} days with zero rides")
for i in uncommon_elements:
    print(i)


# COMMAND ----------

display(df.filter(df['date']=='2023-03-01'))

# COMMAND ----------

w_df=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta")
display(w_df.sort(w_df["time"].asc()))

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import to_timestamp
# Load the necessary libraries
import mlflow
import json
import pandas as pd
import numpy as np
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.diagnostics import cross_validation, performance_metrics
# Visualization
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error

from pyspark.sql.functions import concat, format_string

hourly_net_bike_changes = hourly_net_bike_change.withColumn(
    'start_datetime',
    concat(
        hourly_net_bike_change['date'],
        format_string(' %02d:00:00', hourly_net_bike_change['hour'])
    )
)
display(hourly_net_bike_changes)
# Load the data
net_bike_change_data = hourly_net_bike_changes.toPandas()

# Convert start_datetime to a datetime object
net_bike_change_data['start_datetime'] = pd.to_datetime(net_bike_change_data['start_datetime'])

# Create a Prophet model with all features as regressors
model = Prophet(
    weekly_seasonality=True,
    daily_seasonality=True,
    yearly_seasonality=True,
    changepoint_prior_scale=0.1
)
model.add_regressor('temp')
model.add_regressor('feels_like')
model.add_regressor('humidity')
#model.add_regressor('main')

# Rename the columns to ds (datestamp) and y (target variable)
net_bike_change_data = net_bike_change_data.rename(columns={'start_datetime': 'ds', 'net_bike_change': 'y'})

# Fit the model on the data
model.fit(net_bike_change_data)

# Take the last 10% of the data for testing
test_data = net_bike_change_data.iloc[int(len(net_bike_change_data) * 0.9):]

# Generate predictions for the test data
future = pd.DataFrame({
    'ds': test_data['ds'],
    'temp': test_data['temp'],
    'feels_like': test_data['feels_like'],
    'humidity': test_data['humidity']
})
forecast = model.predict(future)

# Calculate MAPE and MSE
mape = mean_absolute_percentage_error(test_data['y'], forecast['yhat'])
mse = mean_squared_error(test_data['y'], forecast['yhat'])

print("MAPE: {:.2f}%".format(mape * 100))
print("MSE: {:.2f}".format(mse))

# COMMAND ----------

station=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_info.delta")
display(station.filter(station["name"]=="Broadway & E 14 St"))
display(station)

# COMMAND ----------

display(station.filter(station["name"]=="Broadway & E 14 St"))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F
sample=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_status.delta")
sample=sample.filter(sample["station_id"]=='66db6387-0aca-11e7-82f6-3863bb44ef7c')
sample = sample.withColumn("datetime", to_timestamp(from_unixtime(sample["last_reported"])))
sample = sample.withColumn("date", sample["datetime"].cast("date"))
sample = sample.withColumn("time", split(sample["datetime"].cast("string"), " ")[1])
sample=sample.drop('datetime')
sample=sample.select(col("num_ebikes_available"), col("num_docks_available"), col("num_docks_disabled"),col("num_bikes_disabled"),col("num_bikes_available"),col("date"),col("time"))
display(sample)
sample = sample.withColumn("rounded_time", date_trunc("hour", "time")) \
       .withColumn("hour", lpad(hour("rounded_time").cast("string"), 2, "0")).drop("time")\
       .withColumn("time", concat("hour", lit(":00:00"))) \
       .drop("rounded_time", "hour")
num_cols = [c for c in sample.columns if c not in ['date', 'time']]

# Compute the hourly average of each numeric column
data = sample.groupBy('date','time').agg(*[round(avg(col(c))).alias(c) for c in num_cols])

# Show the result
display(data)



# COMMAND ----------


for column in data.columns:
    mode_value = data.select(column).groupBy(column).count().orderBy(F.desc("count")).first()[0]
    data = data.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))

for column in data.columns:
    null_count = data.filter(col(column).isNull()).count()
    print(f"Column '{column}' has {null_count} null values.")

counts = data.groupBy(data.columns).count()

# filter only the rows with count > 1
duplicates = counts.filter("count > 1")

# show the resulting duplicates
duplicates.show()
data = data.dropDuplicates()

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta")
df = df.withColumn("datetime", to_timestamp(from_unixtime(df["dt"])))
df = df.withColumn("date", df["datetime"].cast("date"))
df = df.withColumn("time", split(df["datetime"].cast("string"), " ")[1])
df=df.drop("datetime")
display(df)

# COMMAND ----------

df = df.filter(col('date') > '2023-04-18')
df=df.toPandas()
data=data.toPandas()
merged_df = pd.merge(df,data, how='left', on=['date', 'time'])
spark_df = spark.createDataFrame(merged_df)
display(spark_df)

# COMMAND ----------



# COMMAND ----------

display(spark_df)

# COMMAND ----------

for column in spark_df.columns:
    null_count = spark_df.filter(col(column).isNull()).count()
    print(f"Column '{column}' has {null_count} null values.")


for column in spark_df.columns:
    mode_value = spark_df.select(column).groupBy(column).count().orderBy(F.desc("count")).first()[0]
    spark_df = spark_df.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))



counts = spark_df.groupBy(spark_df.columns).count()

# filter only the rows with count > 1
duplicates = counts.filter("count > 1")

# show the resulting duplicates
duplicates.show()
dat = spark_df.dropDuplicates()

# COMMAND ----------


