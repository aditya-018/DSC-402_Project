# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql import functions as F
# ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_bike_trip_history_selected")
ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/ml_historic_bike_trip_g06/").toPandas()
# for column in ride_df.columns:
#     mode_value = ride_df.select(column).groupBy(column).count().orderBy(F.desc("count")).first()[0]
#     ride_df = ride_df.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))

# for column in ride_df.columns:
#     null_count = ride_df.filter(col(column).isNull()).count()
#     print(f"Column '{column}' has {null_count} null values.")

# delta table for ML model

# delta_path = "dbfs:/FileStore/tables/G06/bronze/nyc_raw_bike_trip_history"

# Read Delta table and filter rows
# ride_df = spark.read.format("delta").load(delta_path)
# ride_df=ride_df.filter(ride_df['start_station_name'] =='Broadway & E 14 St' OR ride_df['end_station_name'] == 'Broadway & E 14 St')

# ride_df = spark.read.format("delta").load(delta_path)\
    # .filter((col("start_station_name") == "Broadway & E 14 St") | (col("end_station_name") == "Broadway & E 14 St")).toPandas()

# weather_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected")



# COMMAND ----------

display(ride_df)

# COMMAND ----------

weather_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected")
weather_df=weather_df.toPandas()

merged_df = pd.merge(weather_df,ride_df, how='left', on=['date', 'time'])

spark_df = spark.createDataFrame(merged_df)
display(spark_df.count())

spark_df=spark_df.drop('dt','started_at','ended_at','start_lat','end_lat','start_lng','end_lng','year','month')

display(spark_df)

# COMMAND ----------

from pyspark.sql.functions import hour, sum, when, col
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, format_string
# create a window for each hour
hour_window = Window.partitionBy('date', hour('time')).orderBy('time')

# calculate the net bike change for each hour
net_bike_change = (sum(when(col('start_station_name') == 'Broadway & E 14 St', -1)
                     .when(col('end_station_name') == 'Broadway & E 14 St', 1)
                     .otherwise(0)).over(hour_window))

# add the net_bike_change column to the dataframe
spark_df = spark_df.withColumn('net_bike_change', net_bike_change)

# select relevant columns, group by hour, and calculate total net bike change
hourly_net_bike_change = spark_df.select('temp', 'humidity', 'wind_speed', 'visibility', 'pressure',  'feels_like', 'humidity', 'main', 'date', hour('time').alias('hour'), 'net_bike_change')

# show the result
hourly_net_bike_change = hourly_net_bike_change.dropDuplicates()
hourly_net_bike_changes = hourly_net_bike_change.withColumn(
    'start_datetime',
    concat(
        hourly_net_bike_change['date'],
        format_string(' %02d:00:00', hourly_net_bike_change['hour'])
    )
)
display(hourly_net_bike_changes)

# COMMAND ----------

pandas_df.shape

# COMMAND ----------

# Set up parameter grid
#!export MLFLOW_SERVER_MAX_PARAM_SIZE=1000

import itertools
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.diagnostics import cross_validation, performance_metrics
import mlflow
import json
import pandas as pd
import numpy as np
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics

# Visualization
import seaborn as sns
import matplotlib.pyplot as plt

# Hyperparameter tuning
import itertools
import mlflow
ARTIFACT_PATH = "Test-model"
np.random.seed(12345)
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}


param_grid = {  
    'changepoint_prior_scale': [0.001, 0.05],
    'seasonality_prior_scale': [1],
    'seasonality_mode': ['additive', 'multiplicative']
}

hourly_trips_prophet = hourly_net_bike_changes.selectExpr("start_datetime as ds", "temp as temp", "humidity as humidity", "feels_like as feels_like", "net_bike_change as y")
pandas_df = hourly_trips_prophet.toPandas()
train_df = pandas_df.iloc[:11000,:]
test_df = pandas_df.iloc[11000:,:]


# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

print("All params", all_params)
print(f"Total training runs {len(all_params)}")

# Create a list to store MAPE values for each combination
mapes = [] 

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run(): 
        # Fit a model using one parameter combination + holidays
        m = Prophet(**params) 
        m.add_regressor('temp')
        m.add_regressor('feels_like')
        m.add_regressor('humidity')
        holidays = pd.DataFrame({"ds": [], "holiday": []})
        #m.add_country_holidays(country_name='US')
        m.fit(train_df) 


        # Cross-validation
        df_cv = cross_validation(model=m, initial='200 days', period='60 days', horizon = '120 days', parallel="threads")
        # Model performance
        df_p = performance_metrics(df_cv, rolling_window=1)
        print(df_p.columns)

        metric_keys = ["mse", "rmse", "mae", "mdape", "smape", "coverage"]
        metrics = {k: df_p[k].mean() for k in metric_keys}
        params = extract_params(m)

        print(f"Logged Metrics: \n{json.dumps(metrics, indent=2)}")
        print(f"Logged Params: \n{json.dumps(params, indent=2)}")

        mlflow.prophet.log_model(m, artifact_path=ARTIFACT_PATH)
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        mapes.append((df_p['rmse'].values[0],model_uri))

# COMMAND ----------

# Tuning results
import pandas as pd
tuning_results = pd.DataFrame(all_params)
tuning_results['mape'] = list(zip(*mapes))[0]
tuning_results['model']= list(zip(*mapes))[1]
print(tuning_results.head())
best_params = dict(tuning_results.iloc[tuning_results[['mape']].idxmin().values[0]])
best_params

# COMMAND ----------

tuning_results

# COMMAND ----------

best_params = {k: int(v) if isinstance(v, np.int64) else v for k, v in best_params.items()}
print(json.dumps(best_params, indent=2))

# COMMAND ----------

loaded_model = mlflow.prophet.load_model(best_params['model'])
# Generate predictions for the test data
future = pd.DataFrame({
    'ds': test_df['ds'],
    'temp': test_df['temp'],
    'feels_like': test_df['feels_like'],
    'humidity': test_df['humidity']
})

forecast = loaded_model.predict(future)
#forecast = loaded_model.predict(loaded_model.make_future_dataframe(36, freq="m"))

print(f"forecast:\n${forecast.tail(40)}")

# COMMAND ----------

prophet_plot = loaded_model.plot(forecast)

# COMMAND ----------

prophet_plot2 = loaded_model.plot_components(forecast)

# COMMAND ----------

results=forecast[['ds','yhat']].join(train_df, lsuffix='_caller', rsuffix='_other')
results['residual'] = results['yhat'] - results['y']

# COMMAND ----------

#plot the residuals
import plotly.express as px
fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols',
)
fig.show()

# COMMAND ----------

model_details = mlflow.register_model(model_uri=best_params['model'], name=ARTIFACT_PATH)

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.transition_model_version_stage(
  name=model_details.name,
  version=model_details.version,
  stage='Staging',
)

# COMMAND ----------

model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=["Staging"])

latest_staging_version = latest_version_info[0].version

print("The latest staging version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_staging_version))

# COMMAND ----------

model_staging_uri = "models:/{model_name}/staging".format(model_name=ARTIFACT_PATH)

print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_staging_uri))

model_staging = mlflow.prophet.load_model(model_staging_uri)

# COMMAND ----------

model_staging.plot(model_staging.predict(future))
#forecast = loaded_model.predict(future)
#prophet_plot = loaded_model.plot(forecast)

# COMMAND ----------



# COMMAND ----------

#Plot the data
import matplotlib.pyplot as plt
indices = [i for i in range(0,250)]
plt.plot(indices, test_df['y'][:250], 'g-', label='Distance thumb_finger')
plt.plot(indices, forecast['yhat'][:250], 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Predicted')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger (4, 2, 0)')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------


