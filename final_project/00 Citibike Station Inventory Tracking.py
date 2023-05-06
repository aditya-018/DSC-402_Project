# Databricks notebook source
# MAGIC %md
# MAGIC ##DSCC 202 - 402 Final Project Spring 2023
# MAGIC <p>
# MAGIC <img src='https://data-science-at-scale.s3.amazonaws.com/images/fp2023.png'>
# MAGIC </p>
# MAGIC see product description and rubric in repo same directory as this notebook.

# COMMAND ----------

from datetime import datetime as dt
from datetime import timedelta
import json

dbutils.widgets.removeAll()

dbutils.widgets.text('01.start_date', "2021-10-01")
dbutils.widgets.text('02.end_date', "2023-03-01")
dbutils.widgets.text('03.hours_to_forecast', '4')
dbutils.widgets.text('04.promote_model', 'No')

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = str(dbutils.widgets.get('04.promote_model'))

print(start_date,end_date,hours_to_forecast, promote_model)

# COMMAND ----------

# DBTITLE 1,Run the ETL Notebook
# Run the Data Prepartion note the one hour 3600 second timeout!
result = dbutils.notebook.run("01 etl", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Run the EDA Notebook
# Run the Data Prepartion
result = dbutils.notebook.run("02 eda", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Run Model Development Notebook
# Run the Data Prepartion
result = dbutils.notebook.run("03 mdl", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Run Station Inventory Forecast Notebook
# Run the Data Prepartion
result = dbutils.notebook.run("04 app", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

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
