# Databricks notebook source
delta_path = "dbfs:/FileStore/tables/G06/bronze/bike_trip_history"

# Register Delta table as temporary view
spark.read.format("delta").load(delta_path).createOrReplaceTempView("bike_trip_history_delta")

# COMMAND ----------

display(spark.sql("SELECT * FROM bike_trip_history_delta"))

# COMMAND ----------

df = spark.read.format("delta").load(delta_path)

display(df)

# COMMAND ----------

# Define the columns to be used for Z-Ordering
zorder_cols = ["started_at", "ended_at"]

# Apply Z-Ordering to the DataFrame
df.write.format("delta").option("zorderCols", ",".join(zorder_cols)).mode("overwrite").save(delta_path)

display(df)

# COMMAND ----------

#ML modeling
from pyspark.sql.functions import *
import pandas as pd
ride_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_bike_trip_history_selected")
df=ride_df.groupBy('date', 'time').agg(count('ride_id').alias('ride_count')).toPandas()
weather_df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/silver/nyc_weather_history_selected").toPandas()
merged_df = pd.merge(weather_df,df, how='left', on=['date', 'time'])
spark_df = spark.createDataFrame(merged_df)
display(spark_df)

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

from pyspark.sql.functions import hour, sum, when, col
from pyspark.sql.window import Window

# create a window for each hour
hour_window = Window.partitionBy('date', hour('time')).orderBy('time')

# calculate the net bike change for each hour
net_bike_change = (sum(when(col('start_station_name') == 'Broadway & E 14 St', 1)
                     .when(col('end_station_name') == 'Broadway & E 14 St', -1)
                     .otherwise(0)).over(hour_window))

# add the net_bike_change column to the dataframe
spark_df = spark_df.withColumn('net_bike_change', net_bike_change)

# select relevant columns, group by hour, and calculate total net bike change
hourly_net_bike_change = spark_df.select('temp', 'visibility', 'humidity', 'main', 'date', hour('time').alias('hour'), 'net_bike_change')

# show the result
hourly_net_bike_change = hourly_net_bike_change.dropDuplicates()
display(hourly_net_bike_change)

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
hourly_net_bike_change = hourly_net_bike_change.withColumn("temp",hourly_net_bike_change.temp.cast('float')).withColumn("visibility",hourly_net_bike_change.visibility.cast('float')).withColumn("humidity",hourly_net_bike_change.humidity.cast('float'))

hourly_net_bike_change. schema

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline


# Define the features and target column to be used in the model
features = ['temp', 'visibility', 'humidity', 'hour']
target_col = 'net_bike_change'

# Create a VectorAssembler to combine the features into a single vector column
assembler = VectorAssembler(inputCols=features, outputCol='features')

# Transform the input data using the VectorAssembler
#transformed_data = assembler.transform(hourly_net_bike_change)

# Split the data into training and test sets
(train_data, test_data) = hourly_net_bike_change.randomSplit([0.7, 0.3], seed=42)

# Create a Random Forest regressor
rf = RandomForestRegressor(featuresCol='features', labelCol=target_col)

pipeline = Pipeline(stages=[assembler, rf])

# define the parameter grid for tuning
param_grid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [2, 5, 10]) \
    .addGrid(rf.numTrees, [10, 20, 30]) \
    .build()

# define the cross-validator
cv = CrossValidator(estimator=pipeline, evaluator=RegressionEvaluator(labelCol=target_col), estimatorParamMaps=param_grid, numFolds=3)

# fit the model on the training data
cv_model = cv.fit(train_data)

# Make predictions on the test data
predictions = cv_model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol='prediction', metricName='rmse')
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the R-squared value of the model on the test data
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
print('R-squared on test data = {:.2f}'.format(r2))


# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

# use the pipeline model to make predictions on the test data
predictions = cv_model.transform(test_data)

# extract the actual values and predicted values
actual = predictions.select('net_bike_change').collect()
predicted = predictions.select('prediction').collect()

# convert the actual and predicted values to numpy arrays
actual = np.array([float(row['net_bike_change']) for row in actual])
predicted = np.array([float(row['prediction']) for row in predicted])

# plot the actual and predicted values
plt.plot(actual[2500:2600], label='Actual')
plt.plot(predicted[2500:2600], label='Predicted')
plt.legend()
plt.show()



# COMMAND ----------


