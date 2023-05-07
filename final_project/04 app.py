# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/" 

# COMMAND ----------

!pip install folium

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import *
import datetime
import plotly.express as px
from mlflow.tracking.client import MlflowClient

# COMMAND ----------

# Question 1: Current timestamp when the notebook is run (now)
date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-4))).replace(microsecond=0,second=0,minute=0)
utc = '%Y-%m-%d %H:%M:%S'
hour = date.strftime("%Y-%m-%d %H")
date = date.strftime(utc)

# COMMAND ----------

print("Current timestamp when the notebook is run (now)", date)

# COMMAND ----------

# details of production model
client = MlflowClient()
model_production = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])
print("Current Production Model",model_production)
latest_production_version = model_production[0].version
print("The latest production version of the model '%s' is '%s'." % (GROUP_MODEL_NAME, latest_production_version))

# COMMAND ----------

#details of staging model
model_staging = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])
print("Current Staging Model",model_staging)
latest_staging_version = model_staging[0].version
print("The latest staging version of the model '%s' is '%s'." % (GROUP_MODEL_NAME, latest_staging_version))


# COMMAND ----------

# Details of Station
print("Our_station", GROUP_STATION_ASSIGNMENT)

# COMMAND ----------

# Station name and a map location (marker)
import folium
longitude = -73.99074142
latitude = 40.73454567
m = folium.Map(location=[latitude, longitude], zoom_start=15)
folium.Marker(location=[latitude, longitude]).add_to(m)
print("OUR_STATION :",GROUP_STATION_ASSIGNMENT)
# Display the map
m


# COMMAND ----------

# Current weather (temp and precip) 
print("Current timestamp when the notebook is run (now)", date)

data = spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta")
data = data.select("time","temp",'humidity',"pressure","wind_speed","clouds").toPandas()

print(data[data.time == date].reset_index(drop=True))


# COMMAND ----------

station_info=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_info.delta")
station_info=station_info.filter(station_info["station_id"]=="66db6387-0aca-11e7-82f6-3863bb44ef7c")
display(station_info.select("capacity"))

# COMMAND ----------

# Total docks at this station and Total bikes available at this station

from pyspark.sql.functions import col, desc, from_unixtime, date_format

# Assume 'spark' is the SparkSession
temp_df_data = spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_status.delta")

df = (temp_df_data
      .filter(col("station_id") == "66db6387-0aca-11e7-82f6-3863bb44ef7c")
      .withColumn("last_reported", date_format(from_unixtime(col("last_reported").cast("long")), "yyyy-MM-dd HH:mm:ss"))
      .filter(col("last_reported") <= hour)
      .sort(desc("last_reported"))
      .select("num_bikes_available","num_docks_available")
      .limit(1))

display(df)


# COMMAND ----------

# Forecast the available bikes for the next 4 hours.
goe=spark.read.format("delta").load(GROUP_DATA_PATH + "gold"+"/model_information")
goe = goe.withColumn("available_bikes_forecasted", col("num_bikes_available") + col("yhat"))
display(goe)
display(goe.select('ds', "available_bikes_forecasted"))

# COMMAND ----------

# Forecast the available bikes for the next 4 hours.
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F


x = spark.read.format("delta").load(GROUP_DATA_PATH + "gold"+"/model_information")
x = x.withColumn("available_bikes", col("num_bikes_available") + col("yhat"))
display(x.select("available_bikes"))
display(x.printSchema())


# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
pdf = x.select("ds", "available_bikes").toPandas()

# Set x-axis as datetime and y-axis as available_bikes
pdf.set_index("ds", inplace=True)
pdf.sort_index(inplace=True)

# Create plotly figure
fig = go.Figure()

# Add trace for available bikes
fig.add_trace(go.Scatter(x=pdf.index, y=pdf["available_bikes"], name='Available Bikes'))

# Add horizontal line for capacity
fig.add_shape(type='line', x0=pdf.index[0], x1=pdf.index[-1], y0=113, y1=113,
              line=dict(color='red', width=2, dash='dash'))

# Add text annotation for capacity
fig.add_annotation(x=pdf.index[0], y=113,
                   text='Capacity', showarrow=False,
                   xanchor='left', yanchor='bottom', font=dict(color='red'))

# Update layout
fig.update_layout(title='Number of Available Bikes over Time',
                  xaxis_title='Time',
                  yaxis_title='Available Bikes')

# Show plot
fig.show()

# COMMAND ----------

display(goe)

# COMMAND ----------

import plotly.express as px

fig_df = goe.toPandas()

fig = px.scatter(
    fig_df, x='yhat', y='model_res',
    marginal_y='violin',
    trendline='ols',
)
fig.show()

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
