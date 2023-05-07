# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------

hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql import functions as F
ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/ml_historic_bike_trip_g06/").toPandas()
weather_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected")
weather_df=weather_df.toPandas()

merged_df = pd.merge(weather_df,ride_df, how='left', on=['date', 'time'])

spark_df = spark.createDataFrame(merged_df)

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
hourly_net_bike_change = spark_df.select('temp', 'wind_speed', 'visibility', 'pressure',  'feels_like', 'humidity', 'main', 'date', hour('time').alias('hour'), 'net_bike_change')

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

#23-3-2023 is the final entry for net bike change; after than netbike is 0 till april 18th
#hourly_net_bike_changes.tail(500)

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
sample = sample.withColumn("rounded_time", date_trunc("hour", "time")) \
       .withColumn("hour", lpad(hour("rounded_time").cast("string"), 2, "0")).drop("time")\
       .withColumn("time", concat("hour", lit(":00:00"))) \
       .drop("rounded_time", "hour")
num_cols = [c for c in sample.columns if c not in ['date', 'time']]

# Compute the hourly average of each numeric column
data = sample.groupBy('date','time').agg(*[round(avg(col(c))).alias(c) for c in num_cols])

#data = data.withColumn("datetime", date_format(concat_ws(" ", "date", "time"), "yyyy-MM-dd HH:mm:ss"))
data = data.sort(asc("date"))
#data = data.filter(col('date') >= '2023-04-01')
data = data.toPandas()
# Show the result
display(data)

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta")
df = df.withColumn("datetime", to_timestamp(from_unixtime(df["dt"])))
df = df.withColumn("date", df["datetime"].cast("date"))
df = df.withColumn("time", split(df["datetime"].cast("string"), " ")[1])
df=df.drop("datetime")
#display(df)
df = df.filter(col('date') >= '2023-03-23')
df = df.sort(asc("date"))
#df = df.filter(col('date') <= '2023-05-05')
#display(df)

# COMMAND ----------

df=df.toPandas()
#data=data.toPandas()
merged_df = pd.merge(df, data, how='left', on=['date', 'time'])
silver_df = spark.createDataFrame(merged_df)
display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import concat, col

# Assuming you have a DataFrame called 'df' with the columns 'date' and 'time'
#combined_datetime = concat(col('date'), col('time'))

silver_df = silver_df.withColumn("datetime", date_format(concat_ws(" ", "date", "time"), "yyyy-MM-dd HH:mm:ss"))
#display(silver_df)

# COMMAND ----------

hourly_net_bike_changes_new = hourly_net_bike_changes.filter(col('date') <= '2023-03-22')

# COMMAND ----------

hourly_net_bike_changes_new = hourly_net_bike_changes_new.sort(asc("start_datetime"))
display(hourly_net_bike_changes_new)

# COMMAND ----------

silver_df = silver_df.sort(asc("datetime"))

# COMMAND ----------

data['datetime'] = pd.to_datetime(data['date'].astype(str) + ' ' + data['time'].astype(str)).dt.to_pydatetime()

# COMMAND ----------

data = data.sort_values(by='datetime')

# COMMAND ----------

last_datetime = data['datetime'].iloc[-hours_to_forecast-2]

# COMMAND ----------

silverdf_lower = silver_df.filter(col('datetime') <= last_datetime )

# COMMAND ----------

display(silverdf_lower)

# COMMAND ----------

silverdf_upper = silver_df.filter(col('datetime') > last_datetime )
display(silverdf_upper)

# COMMAND ----------

final_silverdf_upper = silverdf_upper.filter(col('datetime') <= data['datetime'].iloc[-1])
display(final_silverdf_upper)

# COMMAND ----------

import pandas as pd
pandas_final_silverdf_upper = final_silverdf_upper.toPandas()

pandas_final_silverdf_upper['net_bike_change'] = pandas_final_silverdf_upper['num_bikes_available'].diff().fillna(0)

# COMMAND ----------

pandas_final_silverdf_upper = pandas_final_silverdf_upper.drop(columns=['dt', 'feels_like', 'pressure', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_deg', 'wind_gust', 'weather', 'pop', 'rain.1h', 'date'])

# COMMAND ----------

pandas_final_silverdf_upper['hour'] = pandas_final_silverdf_upper['time'].str.split(':').str[0]

# COMMAND ----------

import pandas as pd
pandas_silverdf_lower = silverdf_lower.toPandas()

pandas_silverdf_lower['net_bike_change'] = pandas_silverdf_lower['num_bikes_available'].diff().fillna(0)
pandas_silverdf_lower['hour'] = pandas_silverdf_lower['time'].str.split(':').str[0]
pandas_silverdf_lower = pandas_silverdf_lower.drop(columns=['dt', 'num_ebikes_available', 'time', 'num_docks_available', 'feels_like', 'pressure', 'num_docks_disabled', 'num_bikes_available', 'num_bikes_disabled', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_deg', 'wind_gust', 'weather', 'pop', 'rain.1h', 'date'])
pandas_silverdf_lower

# COMMAND ----------



# COMMAND ----------

import pandas as pd
pandas_hourly_net_bike_changes_new = hourly_net_bike_changes_new.toPandas()


pandas_hourly_net_bike_changes_new = pandas_hourly_net_bike_changes_new.drop(columns=['main', 'date', 'pressure', 'feels_like', 'visibility'])
pandas_hourly_net_bike_changes_new.rename(columns={'start_datetime': 'datetime'}, inplace=True)
pandas_hourly_net_bike_changes_new

# COMMAND ----------

main_dataframe = pd.concat([pandas_hourly_net_bike_changes_new, pandas_silverdf_lower], ignore_index=True)
main_dataframe

# COMMAND ----------

main_dataframe.rename(columns={'datetime': 'ds'}, inplace=True)
main_dataframe.rename(columns={'net_bike_change': 'y'}, inplace=True)

# COMMAND ----------

pandas_final_silverdf_upper.rename(columns={'datetime': 'ds'}, inplace=True)
pandas_final_silverdf_upper.rename(columns={'net_bike_change': 'y'}, inplace=True)

# COMMAND ----------

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
ARTIFACT_PATH = GROUP_MODEL_NAME
np.random.seed(12345)
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

param_grid = {  
    'changepoint_prior_scale': [0.001, 0.05],
    'seasonality_prior_scale': [1, 3],
    'seasonality_mode': ['additive', 'multiplicative']
}
train_df = main_dataframe.copy()
test_df = pandas_final_silverdf_upper.copy()
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
        m.add_regressor('wind_speed')
        m.add_regressor('humidity')
        m.add_regressor('hour')
        #holidays = pd.DataFrame({"ds": [], "holiday": []})
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
        mapes.append((df_p['mae'].values[0],model_uri))


# COMMAND ----------

train_df.shape

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

best_params = {k: int(v) if isinstance(v, np.int64) else str(v) if isinstance(v, np.bool_) else v for k, v in best_params.items()}
print(json.dumps(best_params, indent=2))

# COMMAND ----------

loaded_model = mlflow.prophet.load_model(best_params['model'])
# Generate predictions for the test data
future = pd.DataFrame({
    'ds': test_df['ds'],
    'temp': test_df['temp'],
    'wind_speed': test_df['wind_speed'],
    'humidity': test_df['humidity'],
    'hour': test_df['hour']

})

forecast = loaded_model.predict(future)
#forecast = loaded_model.predict(loaded_model.make_future_dataframe(36, freq="m"))

print(f"forecast:\n${forecast.head()}")

# COMMAND ----------

prophet_plot2 = loaded_model.plot_components(forecast)

# COMMAND ----------

results=forecast[['ds','yhat']].join(test_df, lsuffix='_caller')
results['residual'] = results['yhat'] - results['y']

# COMMAND ----------

results

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

# COMMAND ----------

try:
    model_information = spark.read.format("delta").load(GROUP_DATA_PATH + "gold" + "/model_information")
except:
    model_information = None
try:
    #retrain_error = model_information.filter(col("model_type") == "staging").select("error_mae").head(1)[0][0]
    retrain_error = model_information.filter(col("model_type") == "Staging").select("error_mae").collect()[0][0]

    #retrain_error = retrain_error[0][0]
except:
    retrain_error = 1234

current_ver = None  

# prod_model_str = dbutils.widgets.get('Promote Model')


#prod_model_str='yes'
#Production = True if prod_model_str.lower() == 'yes' else False
Production = promote_model

if Production:
    current_ver = client.get_latest_versions(ARTIFACT_PATH, stages = ["Production"])
    stage = "Production"

elif best_params['mape'] < retrain_error:
    current_ver = client.get_latest_versions(ARTIFACT_PATH, stages = ["Staging"])
    stage = "Staging"

else:
    stage = "Archived"

if current_ver:
    client.transition_model_version_stage(
        name = GROUP_MODEL_NAME,
        version = current_ver[0].version,
        stage = "Archived",
        )

client.transition_model_version_stage(
  name=model_details.name,
  version=model_details.version,
  stage=stage,
)

model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))



# COMMAND ----------

results

# COMMAND ----------

main_df = None
pred_df = pd.DataFrame(columns=['ds', 'y', 'yhat', 'model_res'])
pred_df['model_type'] = []
pred_df['error_mae'] = []
pred_df['num_bikes_available'] = []

try:
    extract_stage_data = model_info.filter(col("mode_type") == 'Staging')
    extract_production_data = model_info.filter(col("model_type") == 'Production')
except:
    extract_stage_data = None
    extract_production_data = None

if Production:
    df = results.copy()
    df['model_type'] = "Production"
    df["error_mae"] = best_params['mape']
    pred_df[['ds', 'y', 'yhat', 'model_res', 'model_type', 'error_mae', 'num_bikes_available']] =  df[['ds_caller', 'y', 'yhat', 'residual', 'model_type', 'error_mae', 'num_bikes_available']]
    if extract_stage_data:
        main_df = extract_stage_data.union(spark.createDataFrame(pred_df))
    else:
        main_df = spark.createDataFrame(pred_df)

elif best_params['mape'] < retrain_error:
    df = results.copy()
    df['model_type'] = "Staging"
    df["error_mae"] = best_params['mape']
    pred_df[['ds', 'y', 'yhat', 'model_res', 'model_type', 'error_mae', 'num_bikes_available']] =  df[['ds_caller', 'y', 'yhat', 'residual', 'model_type', 'error_mae', 'num_bikes_available']]
    if extract_production_data:
        main_df = extract_production_data.union(spark.createDataFrame(pred_df))
    else:
        main_df = spark.createDataFrame(pred_df)
else:
    pass


# COMMAND ----------

main_df = main_df.withColumn("yhat", round(main_df["yhat"]))
display(main_df)

# COMMAND ----------

if main_df:
    main_df.write.format("delta").option("path", GROUP_DATA_PATH + "gold" + "/model_information").mode("overwrite").save()

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
