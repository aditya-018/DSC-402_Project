# Databricks notebook source
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
from mlflow.tracking.client import MlflowClient
import datetime
from pyspark.sql.functions import *

# COMMAND ----------

currentdate = pd.Timestamp.now(tz='US/Eastern').round(freq="H")
fmt = '%Y-%m-%d %H:%M:%S'
currenthour = currentdate.strftime("%Y-%m-%d %H") 
currentdate = currentdate.strftime(fmt) 
print("The current timestamp is:",currentdate)

# COMMAND ----------


