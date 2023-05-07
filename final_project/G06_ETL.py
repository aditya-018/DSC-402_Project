# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------

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



# COMMAND ----------

import requests
import json
import pandas as pd
"""Calling an API for the missing values from Nov 1 to Nov 19"""
api_key = 'd89e65616c8ed4a63153d3c8abe2e1b9'

start_date = '2021-11-01'
end_date = '2021-11-19'

lat = '40.73455'
lon = '-73.99074'
url = 'https://history.openweathermap.org/data/2.5/history/city'


weather_data = []

for date in pd.date_range(start_date, end_date):
    url = f"{url}?lat={lat}&lon={lon}&type=hour&start={start_date}&end={end_date}&appid={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            data = response.json()
            weather_data.append(data)
        except ValueError:
            print(f"Invalid JSON response for date {date}")
    else:
        print(f"API call failed for date {date}: {response.content}")

rows = []

for data in weather_data:
    dt = pd.to_datetime(data['data']['dt']).date()
    temperature = data['data']['temp']
    feels_like = data['data']['feels_like']
    pressure = data['data']['pressure']
    humidity = data['data']['humidity']
    dew_point = data['data']['dew_point']
    uvi = data['data']['uvi']
    clouds = data['data']['clouds']
    visibility = data['data']['visibility']
    wind_speed = data['data']['wind_speed']
    wind_deg = data['data']['wind_deg']
    pop = data['data']['pop']
    snow_1h = data['data']['snow_1h']
    id_ = data['data']['id']
    main = data['data']['main']
    description = data['data']['description']
    icon = data['data']['icon']
    loc = data['data']['loc']
    lat = data['data']['lat']
    lon = data['data']['lon']
    timezone_offset = data['data']['timezone_offset']
    rain_1h = data['data']['rain_1h']    
    rows.append((dt, temperature, feels_like, pressure, humidity, dew_point, uvi, clouds, visibility, wind_speed, wind_deg, pop, snow_1h, id_, main, description, icon, loc, lat, lon, timezone_offset, rain_1h))


df_weather_missing = spark.createDataFrame(rows, weather_schema) 

display(df_weather_missing.limit(10))

# COMMAND ----------

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


