# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/" 

# COMMAND ----------

#import libraries
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
from mlflow.tracking.client import MlflowClient
import datetime
from pyspark.sql.functions import *

# COMMAND ----------

hours_to_forecast = 4 # take value from widget


# COMMAND ----------

# Current timestamp when the notebook is run (now)
current_date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-4))).replace(microsecond=0,second=0,minute=0)
fmt = '%Y-%m-%d %H:%M:%S'
current_hour = current_date.strftime("%Y-%m-%d %H")
current_date = current_date.strftime(fmt)
print("The current timestamp is:",current_date)

# COMMAND ----------

# production and staging models
client = MlflowClient()
prod_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])
stage_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])
print("current production model",prod_model)
print("current staging model",stage_model)
print("our_station",GROUP_STATION_ASSIGNMENT)

# COMMAND ----------

!pip install folium

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
weather_data = spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta").select("time","temp",'humidity',"pressure","wind_speed","clouds").toPandas()
print("Current Weather:")
print(weather_data[weather_data.time==current_date].reset_index(drop=True))
print("Our station capacity - ",113)

# COMMAND ----------

# Total docks at this station and Total bikes available at this station

from pyspark.sql.functions import col, desc, from_unixtime, date_format

# Assume 'spark' is the SparkSession
temp_df_data = spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_status.delta")

df = (temp_df_data
      .filter(col("station_id") == "66db6387-0aca-11e7-82f6-3863bb44ef7c")
      .withColumn("last_reported", date_format(from_unixtime(col("last_reported").cast("long")), "yyyy-MM-dd HH:mm:ss"))
      .filter(col("last_reported") <= current_hour)
      .sort(desc("last_reported"))
      .select("num_bikes_available","num_docks_available")
      .limit(1))

display(df)


# COMMAND ----------

# Forecast the available bikes for the next 4 hours.
go=spark.read.format("delta").load(GROUP_DATA_PATH + "gold"+"/model_information")
go = go.withColumn("available_bikes", col("num_bikes_available") + col("yhat"))
display(go.select("available_bikes"))

# COMMAND ----------

# need to get the bike availability data
#reading bike stream data with hour_window and availability columns
real_time_inventory_data = spark.read.format("delta").load(REAL_TIME_INVENTORY_INFO_DELTA_DIR)
real_time_inventory_data = real_time_inventory_data.orderBy("hour_window", ascending=False)
from pyspark.sql.functions import col
real_time_inventory_data = real_time_inventory_data.withColumnRenamed("diff", "avail")

# COMMAND ----------

# need to get the difference in bike availability
# calculating diff for every hour_window using lag function
# diff is the difference between bike availability between 2 consecutive hours
from pyspark.sql.functions import col, lag, coalesce
from pyspark.sql.window import Window
w = Window.orderBy("hour_window")
real_time_inventory_data = real_time_inventory_data.withColumn("diff", col("avail") - lag(col("avail"), 1).over(w))
real_time_inventory_data = real_time_inventory_data.withColumn("diff", coalesce(col("diff"), col("avail")))
real_time_inventory_data = real_time_inventory_data.orderBy("hour_window", ascending=False)
from pyspark.sql.functions import monotonically_increasing_id
real_time_inventory_data = real_time_inventory_data.withColumn('index', monotonically_increasing_id())
# removing first n values from diff column and making it null, we will impute our forecast
from pyspark.sql.functions import when, col
diff_new_df = real_time_inventory_data.withColumn('diff_new', when(col('index').between(0, 7), None).otherwise(col('diff')))
display(diff_new_df)

# COMMAND ----------

#loading the model information from Gold table
model_output_df = spark.read.format("delta").load(MODEL_INFO)
display(model_output_df)

model_output = model_output_df.toPandas()
model_output["yhat"] = model_output["yhat"].round()
model_output["capacity"] = 113

staging_forecast_df = model_output[model_output.tag == "Staging"]
prod_forecast_df = model_output[model_output.tag == "Production"]
forecast_df = prod_forecast_df.iloc[-hours_to_forecast:,:]
# creating a new dataframe with just yhat values and index column
forecast_temp = forecast_df[["yhat"]]
forecast_temp["index"] = list(range(0, hours_to_forecast))
forecast_temp = spark.createDataFrame(forecast_temp)
