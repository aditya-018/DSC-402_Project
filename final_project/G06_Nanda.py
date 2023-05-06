# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_info.delta")
display(df.limit(5))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_his=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/historic_bike_trip_g06//")
display(df_his.limit(5))
df_his.printSchema()

# COMMAND ----------

df_his=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_status.delta")
display(df_his.limit(5))
df_his.printSchema()

# COMMAND ----------

df_his=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta")
display(df_his.limit(5))
df_his.printSchema()

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/bronze"))


display(dbutils.fs.ls("dbfs:/FileStore/tables/G06/"))

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

#streaming the live data to the group data path

# Define the path to write the output

output_path = GROUP_DATA_PATH+"/bronze"

# Define the path to read from and the path to write the output
input_path = "dbfs:/FileStore/tables/bronze_station_info.delta"
output_path = "dbfs:/FileStore/tables/G06/bronze/bike_station_info"

# Define the query to read from the Delta table
query = spark.readStream.format("delta").load(input_path)\
        .writeStream.format("delta")\
        .trigger(processingTime='30 minutes')\
        .option("path", output_path)\
        .option("checkpointLocation", output_path + "/checkpoint")\
        .start()

# Wait for the stream to finish
query.awaitTermination()




# COMMAND ----------

# import shutil
# folder_path = "dbfs:/FileStore/tables/G06/history_bike_trip/"
# shutil.rmtree(folder_path)

dbutils.fs.rm("dbfs:/FileStore/tables/G06/history_bike_trip/", True)



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
df_g06.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

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
distinct_values = df.select(col("rideable_type")).distinct()

display(distinct_values)



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

display(df)

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

# Question 3 Hourly Trip Trends

from pyspark.sql.functions import hour

# Load the Delta table as a DataFrame
delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
df_q3 = spark.read.format("delta").load(delta_path)

# Convert the 'started_at' column to a HourType column and extract the hour
df_q3 = df_q3.withColumn("start_hour", hour("started_at"))

# Group by start hour and count the number of rides per hour
hourly_trips = df_q3.groupBy("start_hour").count()
hourly_trips = hourly_trips.orderBy("start_hour")

# Display the resulting DataFrame
display(hourly_trips)


# COMMAND ----------

# Question 3 Hourly Trip Trends with Date and Day

from pyspark.sql.functions import hour, date_format, dayofweek

# Load the Delta table as a DataFrame
delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
df_q3 = spark.read.format("delta").load(delta_path)

# Extract the start hour, date, and day of week
df_q3 = df_q3.withColumn("start_hour", hour("started_at")) \
             .withColumn("start_date", date_format("started_at", "yyyy-MM-dd")) \
             .withColumn("day_of_week", dayofweek("started_at"))

# Group by start date, day of week, and start hour, and count the number of rides
hourly_trips = df_q3.groupBy("start_date", "day_of_week", "start_hour") \
                    .count() \
                    .orderBy("start_date", "day_of_week", "start_hour")

# Display the resulting DataFrame
display(hourly_trips)


# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql import functions as F
# ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_bike_trip_history_selected")
ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/ml_historic_bike_trip_g06/")
for column in ride_df.columns:
    mode_value = ride_df.select(column).groupBy(column).count().orderBy(F.desc("count")).first()[0]
    ride_df = ride_df.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))

for column in ride_df.columns:
    null_count = ride_df.filter(col(column).isNull()).count()
    print(f"Column '{column}' has {null_count} null values.")

df=ride_df.toPandas()

weather_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected")

for column in weather_df.columns:
    mode_value = weather_df.select(column).groupBy(column).count().orderBy(F.desc("count")).first()[0]
    ride_df = weather_df.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))

for column in weather_df.columns:
    null_count = weather_df.filter(col(column).isNull()).count()
    print(f"Column '{column}' has {null_count} null values.")

weather_df=weather_df.toPandas()

merged_df = pd.merge(weather_df,df, how='left', on=['date', 'time'])

spark_df = spark.createDataFrame(merged_df)
display(spark_df.count())

spark_df=spark_df.drop('dt','started_at','ended_at','start_lat','end_lat','start_lng','end_lng','year','month')

display(spark_df)

# COMMAND ----------

station=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_info.delta")
display(station.filter(station["name"]=="Broadway & E 14 St"))
display(station)

# COMMAND ----------

from pyspark.sql.functions import hour, sum, when, col
from pyspark.sql.window import Window

# create a window for each hour
hour_window = Window.partitionBy('date', hour('time')).orderBy('time')

# calculate the net bike change for each hour
net_bike_change = (sum(when(col('start_station_name') == 'Broadway & E 14 St', -1)
                     .when(col('end_station_name') == 'Broadway & E 14 St', 1)
                     .otherwise(0)).over(hour_window))

# add the net_bike_change column to the dataframe
spark_df = spark_df.withColumn('net_bike_change', net_bike_change)

# select relevant columns, group by hour, and calculate total net bike change
hourly_net_bike_change = spark_df.select('temp', 'feels_like', 'humidity', 'main', 'date', hour('time').alias('hour'), 'net_bike_change')

# show the result
hourly_net_bike_change = hourly_net_bike_change.dropDuplicates()
display(hourly_net_bike_change)

# COMMAND ----------

hourly_net_bike_change = hourly_net_bike_change.withColumn('net_bike_change_plus_113', col('net_bike_change') + 113)
display(hourly_net_bike_change)

# COMMAND ----------

from pyspark.sql.functions import concat, format_string

hourly_net_bike_changes = hourly_net_bike_change.withColumn(
    'start_datetime',
    concat(
        hourly_net_bike_change['date'],
        format_string(' %02d:00:00', hourly_net_bike_change['hour'])
    )
)
display(hourly_net_bike_changes)

# COMMAND ----------

pandas_df = hourly_trips_prophet.toPandas()
pandas_df.shape

pandas_df['y'].min()




# COMMAND ----------

pandas_df = hourly_trips_prophet.toPandas()
pandas_df.shape
import numpy as np
from scipy.stats import boxcox


# Apply the Box-Cox transformation to the "net_bike_change" variable
y = pandas_df['y']
y, lam = boxcox(y + 40)  # Add a small constant to ensure all values are positive

# Update the "net_bike_change" column in the original data with the transformed values
pandas_df['y'] = y

# COMMAND ----------

pandas_df
x = pandas_df['y'].min()
x.abs()

# COMMAND ----------

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
from scipy.stats import boxcox
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
# Hyperparameter tuning
import itertools
# Prepare the data for Prophet
hourly_trips_prophet = hourly_net_bike_changes.selectExpr("start_datetime as ds", "net_bike_change as y")

pandas_df = hourly_trips_prophet.toPandas()


# Assuming pandas_df is your pandas DataFrame
#y_shifted = pandas_df['y'].apply(lambda x: x + 40 + 1)
#y_shifted = pandas_df['y']

#y_transformed, lam = boxcox(y_shifted)

#pandas_df['y'] = y_transformed

#pandas_df['y'] = y_shifted

train_df = pandas_df.iloc[:11000,:]
test_df = pandas_df.iloc[11000:11500,:]

# Create the Prophet model
model = Prophet(changepoint_prior_scale=0.1, daily_seasonality=True, weekly_seasonality=False, yearly_seasonality=False)
#model = Prophet()

# Train the model on the data
model.fit(train_df)

forecast = model.predict(test_df)

#yhat_sh = forecast['yhat'].apply(lambda x: x - 40 - 1)
#y_transformed, lam = boxcox(yhat_sh)
#forecast['yhat'] = yhat_sh

#y_tran = test_df['y'].apply(lambda x: x - 40 - 1)
#y_transformed, lam = boxcox(y_tran)
#test_df['y'] = y_tran

#x = forecast['yhat']
#x, lam = boxcox(y + 40)
#pandas_df['y'] = x

# Calculate MAPE and MSE
mape = mean_absolute_percentage_error(test_df['y'], forecast['yhat'])
mse = mean_squared_error(test_df['y'], forecast['yhat'])

print("MAPE: {:.2f}%".format(mape * 100))
print("MSE: {:.2f}".format(mse))

# Display the forecast
#forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(20)

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,500)]
plt.plot(indices, test_df['y'], 'g-', label='Distance thumb_finger')
plt.plot(indices, forecast['yhat'], 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------


from statsmodels.tsa.arima.model import ARIMA
train_df = pandas_df.iloc[:11000,:]
test_df = pandas_df.iloc[11000:11500,:]

current_data = train_df.copy() 
predictions = []

test_data_num = len(test_df)

#predict one hour ahead using the trained model and re-train the model with the new update as data comes in.
for i in range(test_data_num):

    model = ARIMA(pd.Series(current_data['y'].values, index=current_data['ds']), order=(5,1,0))
    model_fit = model.fit()

    prediction = model_fit.forecast()[0][0]
    predictions.append(prediction)
    latest_obs = test_df.iloc[i]
    current_data = current_data.append(latest_obs)

    print(f'Iteration {i}/{test_data_num} - Predicted: {prediction}, Real Value: {latest_obs["y"]}')


error = mean_squared_error(test['y'].values.tolist(), predictions)

# COMMAND ----------

from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
hourly_trips_prophet = hourly_net_bike_changes.selectExpr("start_datetime as ds", "net_bike_change as y")
pandas_df = hourly_trips_prophet.toPandas()
train_df = pandas_df.iloc[:10800,:]
test_df = pandas_df.iloc[10800:10815,:]

current_data = train_df.copy() 
predictions = []

test_data_num = len(test_df)

#predict one hour ahead using the trained model and re-train the model with the new update as data comes in.
for i in range(test_data_num):

    model = ARIMA(pd.Series(current_data['y'].values, index=current_data['ds']), order=(2,1,2))
    model_fit = model.fit()

    prediction = model_fit.forecast()
    print(list(prediction))
    predictions.append(prediction)
    latest_obs = test_df.iloc[i]
    current_data = current_data.append(latest_obs)
    print(predictions)
    #print(f'Iteration {i}/{test_data_num} - Predicted: {prediction}, Real Value: {latest_obs["y"]}')


error = mean_squared_error(test_df['y'].values.tolist(), predictions)
print(f"Mean Squared Error: {error}")


# COMMAND ----------

import pandas as pd
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
def arima_objective(params):
    order = (params['p'], params['d'], params['q'])
    hourly_trips_prophet = hourly_net_bike_changes.selectExpr("start_datetime as ds", "net_bike_change as y")
    pandas_df = hourly_trips_prophet.toPandas()
    train_df = pandas_df.iloc[:10800,:]
    test_df = pandas_df.iloc[10800:10804,:]
    current_data = train_df.copy() 
    predictions = []
    test_data_num = len(test_df)

    # Predict one hour ahead using the trained model and re-train the model with the new update as data comes in.
    for i in range(test_data_num):
        model = ARIMA(pd.Series(current_data['y'].values, index=current_data['ds']), order=order)
        model_fit = model.fit()
        prediction = model_fit.forecast()
        predictions.append(prediction)
        latest_obs = test_df.iloc[i]
        current_data = current_data.append(latest_obs)
    
    error = mean_squared_error(test_df['y'].values.tolist(), predictions)
    return {'loss': error, 'status': STATUS_OK}

# Define the search space for the hyperparameters
space = {
    'p': hp.quniform('p', 1, 4, 1),
    'd': hp.quniform('d', 1, 3, 1),
    'q': [0]
}

# Use Hyperopt to find the best hyperparameters
trials = Trials()
best = fmin(fn=arima_objective,
            space=space,
            algo=tpe.suggest,
            max_evals=5,
            trials=trials)

print(best)


# COMMAND ----------

print(best)

# COMMAND ----------

predictions

# COMMAND ----------

test_df['y'].values.tolist()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,50)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,50)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,50)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,50)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,50)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,50)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,100)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Frames')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger (2, 1, 0)')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,100)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Predicted')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger (2, 2, 0)')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,100)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Predicted')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger (4, 1, 0)')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
indices = [i for i in range(0,100)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Predicted')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger (4, 2, 0)')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

#Plot the data
import matplotlib.pyplot as plt
indices = [i for i in range(0,15)]
plt.plot(indices, test_df['y'].values.tolist(), 'g-', label='Distance thumb_finger')
plt.plot(indices, predictions, 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Predicted')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger (4, 2, 0)')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].head(20)

# COMMAND ----------

test_df.head(20)

# COMMAND ----------

import matplotlib.pyplot as plt

test_df.set_index('ds')['y'].plot(figsize=(12, 6))
#forecast.set_index('ds')['yhat'].plot(figsize=(12, 6))
plt.show()

# COMMAND ----------

test_df['y'].mean()

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
test_data = net_bike_change_data.iloc[int(len(net_bike_change_data) * 0.99):]

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

forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].head(20)

# COMMAND ----------

test_data.head(20)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import hour, date_format, dayofweek, col, lit
from pyspark.sql.functions import coalesce

# Load the Delta table as a DataFrame
delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
df_q3 = spark.read.format("delta").load(delta_path)

# Extract the start hour, date, and day of week
df_q3 = df_q3.withColumn("start_hour", hour("started_at")) \
             .withColumn("start_date", date_format("started_at", "yyyy-MM-dd")) \
             .withColumn("day_of_week", dayofweek("started_at"))

# Group by start date, day of week, and start hour, and count the number of rides
hourly_trips = df_q3.groupBy("start_date", "day_of_week", "start_hour") \
                    .count() \
                    .orderBy("start_date", "day_of_week", "start_hour")

# Create a dataframe with all possible combinations of start date, day of week, and start hour
dates = df_q3.select("start_date", "day_of_week").distinct()
hours = range(0, 24)
all_hours = spark.createDataFrame([(date.start_date, date.day_of_week, hour) for date in dates.collect() for hour in hours], ["start_date", "day_of_week", "start_hour"])

# Join the hourly_trips dataframe with the all_hours dataframe to include all possible combinations
hourly_trips = all_hours.join(hourly_trips, ["start_date", "day_of_week", "start_hour"], "left") \
                       .withColumn("count", coalesce(col("count"), lit(0))) \
                       .orderBy("start_date", "day_of_week", "start_hour")

# Display the resulting DataFrame
display(hourly_trips)


# COMMAND ----------

# line chart (2)
import matplotlib.dates as mdates
import plotly.express as px

# Create line chart of daily trips

# Create a line chart showing the daily trips over time

fig = px.line(hourly_trips.toPandas(), x="start_date", y="count", title="Daily Bike Trip Trends")
fig.show()

# COMMAND ----------

from pyspark.sql.functions import concat, format_string

hourly_trips = hourly_trips.withColumn(
    'start_datetime',
    concat(
        hourly_trips['start_date'],
        format_string(' %02d:00:00', hourly_trips['start_hour'])
    )
)
display(hourly_trips)

# COMMAND ----------



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

# Hyperparameter tuning
import itertools
# Prepare the data for Prophet
hourly_trips_prophet = hourly_trips.selectExpr("start_datetime as ds", "count as y")
#hourly_trips_prophet = hourly_trips.selectExpr("CAST(start_date AS STRING) AS ds", "CAST(start_hour AS STRING) AS hour", "CAST(count AS STRING) AS y")

pandas_df = hourly_trips_prophet.toPandas()
#hourly_trips_prophet = hourly_trips_prophet.withColumn("ds", to_timestamp("ds", "yyyy-MM-dd"))

#hourly_trips_prophet = hourly_trips.select(to_timestamp("start_date", "yyyy-MM-dd HH:mm:ss").alias("ds"), "start_hour", "count")

train_df = pandas_df.iloc[:11000,:]
test_df = pandas_df.iloc[11000:,:]

# Create the Prophet model
model = Prophet(changepoint_prior_scale=0.01, daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
#model = Prophet()

# Train the model on the data
model.fit(train_df)



# Cross validation
#baseline_model_cv = cross_validation(model=model, initial='365 days', period='50 days', horizon = '100 days', parallel="threads")
#baseline_model_cv.head()

# Model performance metrics
#baseline_model_p = performance_metrics(baseline_model_cv, rolling_window=1)
#baseline_model_p.head()

# Get the performance value
#print(f"MAPE of baseline model: {baseline_model_p['mape'].values[0]}")



# Make predictions for the next 30 days
#future = model.make_future_dataframe(periods=30*24, freq='H')
#print(future)

forecast = model.predict(test_df)

# Display the forecast
forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(20)



# COMMAND ----------

# Model performance metrics
baseline_model_p = performance_metrics(forecast, rolling_window=1)
baseline_model_p.head()

# COMMAND ----------

test_df.tail(20)

# COMMAND ----------

hourly_trips_prophet

# COMMAND ----------

display(hourly_trips_prophet)

# COMMAND ----------

# Question 3 holiday affect the daily (non-holiday) system use trend
%pip install holidays

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


# COMMAND ----------

#COPY THIS FOR THE PPT
#%pip install holidays
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

print("Number of rows in weekly_trips:", non_holiday_trips.count())


# COMMAND ----------

import datetime

year = 2020
weekends = []

# Loop through all the days in the year
for month in range(1, 13):
    for day in range(1, 32):
        try:
            # Try to create a datetime object for the current day
            date = datetime.datetime(year, month, day)
        except ValueError:
            # If the day is not valid for the current month, skip it
            continue
        
        # If the day is a weekend day (Saturday or Sunday), add it to the weekends list
        if date.weekday() in [5, 6]:
            weekends.append(date.strftime('%Y-%m-%d'))

print(weekends)

# COMMAND ----------


for i in range(len(holidays)):
    weekends.append(holidays[i])

# COMMAND ----------

print(weekends)

# COMMAND ----------

holidays = weekends
print(holidays)

# COMMAND ----------

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)
from pyspark.sql.functions import col, to_date

df = spark.read.format("delta").load(delta_path)
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

import pyspark.sql.functions as F

#holidays = ["2020-01-01", "2020-01-20", "2020-02-17", "2020-05-25", "2020-07-03", "2020-09-07", "2020-11-26", "2020-12-25"]

df = df.withColumn("is_holiday", F.when(F.col("started_at").isin(weekends), 1).otherwise(0))
daily_trips = df.groupBy("started_at", "is_holiday").agg(F.count("*").alias("trips"))
non_holiday_trips = daily_trips.filter(F.col("is_holiday") == 0).groupBy("started_at").agg(F.sum("trips").alias("trips"))
weekly_trips = non_holiday_trips.groupBy(F.weekofyear("started_at").alias("week")).agg(F.sum("trips").alias("trips")).orderBy("week")

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

sns.lineplot(x="week", y="trips", data=weekly_trips.toPandas())

plt.title("Weekly Trips Over Time (Excluding Holidays)")
plt.xlabel("Week of Year")
plt.ylabel("Trips")
plt.show()

# COMMAND ----------

print("Number of rows in weekly_trips:", non_holiday_trips.count())

# COMMAND ----------

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)
from pyspark.sql.functions import col, to_date

df = spark.read.format("delta").load(delta_path)
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

import pyspark.sql.functions as F

#holidays = ["2020-01-01", "2020-01-20", "2020-02-17", "2020-05-25", "2020-07-03", "2020-09-07", "2020-11-26", "2020-12-25"]

df = df.withColumn("is_holiday", F.when(~F.col("started_at").isin(weekends), 1).otherwise(0))
daily_trips = df.groupBy("started_at", "is_holiday").agg(F.count("*").alias("trips"))
non_holiday_trips = daily_trips.filter(F.col("is_holiday") == 0).groupBy("started_at").agg(F.sum("trips").alias("trips"))
weekly_trips = non_holiday_trips.groupBy(F.weekofyear("started_at").alias("week")).agg(F.sum("trips").alias("trips")).orderBy("week")

# COMMAND ----------

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
us_holidays_2020 = holidays.US(years=2020)
holidays = [str(date) for date in us_holidays_2020.keys()]

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)
from pyspark.sql.functions import col, to_date

df = spark.read.format("delta").load(delta_path)
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

import pyspark.sql.functions as F

df = df.withColumn("is_weekend", F.when(F.dayofweek(F.col("started_at")).isin([1, 7]), 1).otherwise(0))
df = df.withColumn("is_holiday", F.when(F.col("start_date").isin(holidays), 1).otherwise(0))

daily_trips = df.groupBy("started_at", "is_weekend", "is_holiday").agg(F.count("*").alias("trips"))
non_holiday_non_weekend_trips = daily_trips.filter((F.col("is_holiday") == 0) & (F.col("is_weekend") == 0)).groupBy("started_at").agg(F.sum("trips").alias("trips"))
weekly_trips = non_holiday_non_weekend_trips.groupBy(F.weekofyear("started_at").alias("week")).agg(F.sum("trips").alias("trips")).orderBy("week")

import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

sns.lineplot(x="week", y="trips", data=weekly_trips.toPandas())

plt.title("Weekly Trips Over Time (Including Weekends and Holidays)")
plt.xlabel("Week of Year")
plt.ylabel("Trips")
plt.show()


# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

from pyspark.sql.functions import date_format
import holidays
us_holidays_2020 = holidays.US(years=2020)
holidays = [str(date) for date in us_holidays_2020.keys()]

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)

from pyspark.sql.functions import col, to_date
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

import pyspark.sql.functions as F

df = df.withColumn("day_of_week", F.date_format("start_date", "E"))
df = df.withColumn("is_weekday", F.when(F.col("day_of_week").isin(["Sat", "Sun"]), 0).otherwise(1))
df = df.withColumn("is_holiday", F.when(F.col("start_date").isin(holidays), 1).otherwise(0))

daily_trips = df.groupBy("start_date", "is_weekday", "is_holiday").agg(F.count("*").alias("trips"))
non_weekend_trips = daily_trips.filter(F.col("is_weekday") == 1).filter(F.col("day_of_week").isin(["Mon", "Tue", "Wed", "Thu", "Fri"]))
non_holiday_trips = non_weekend_trips.filter(F.col("is_holiday") == 0).groupBy("start_date").agg(F.sum("trips").alias("trips"))
weekly_trips = non_holiday_trips.groupBy(F.weekofyear("start_date").alias("week")).agg(F.sum("trips").alias("trips")).orderBy("week")

import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

sns.lineplot(x="week", y="trips", data=weekly_trips.toPandas())

plt.title("Weekly Trips Over Time (Excluding Weekends and Holidays)")
plt.xlabel("Week of Year")
plt.ylabel("Trips")
plt.show()


# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
from pyspark.sql.functions import date_format
import holidays
import pyspark.sql.functions as F

us_holidays_2020 = holidays.US(years=2020)
holidays = [str(date) for date in us_holidays_2020.keys()]

# Load the Delta table as a DataFrame
df = spark.read.format("delta").load(delta_path)

# Create a new column for start date
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

# Create columns for is_weekday and is_holiday
df = df.withColumn("is_weekday", F.when(F.dayofweek("start_date").isin(range(2, 7)), 1).otherwise(0))
df = df.withColumn("is_holiday", F.when(F.col("start_date").isin(holidays), 1).otherwise(0))

# Filter out weekends and holidays
non_business_days = ["Sat", "Sun"] + holidays
df = df.filter((~F.dayofweek("start_date").isin([1, 7])) & (~F.col("start_date").isin(non_business_days)))

# Group by week and calculate the number of trips
weekly_trips = df.groupBy(F.weekofyear("start_date").alias("week")).agg(F.count("*").alias("trips")).orderBy("week")

# Plot the data using Seaborn
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

sns.lineplot(x="week", y="trips", data=weekly_trips.toPandas())

plt.title("Weekly Trips Over Time (excluding Weekends and Holidays)")
plt.xlabel("Week of Year")
plt.ylabel("Trips")
plt.show()


# COMMAND ----------

#COPY THIS FOR THE PPT

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


# COMMAND ----------


