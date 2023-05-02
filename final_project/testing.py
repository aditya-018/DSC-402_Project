# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/" 

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/G02/bronze_nyc_weather/")


# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.functions import to_timestamp, from_unixtime,desc,asc
df=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/weather_history/")

df = df.withColumn("datetime", to_timestamp(from_unixtime(df["dt"])))

df = df.withColumn("date", df["datetime"].cast("date"))
df = df.withColumn("time", split(df["datetime"].cast("string"), " ")[1])
display(df.sort(desc("date")))


# COMMAND ----------

# Q2 What are the daily trip trends for your given station?
from pyspark.sql.functions import date_format

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

df_q2 = spark.read.format("delta").load(delta_path)
df_q2 = df_q2.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

daily_trips = df_q2.groupBy("start_date").count()
daily_trips = daily_trips.orderBy("start_date")

display(daily_trips)

# COMMAND ----------

# Dates with zero rides or missing data

import pandas as pd
from pyspark.sql.functions import to_date

start_date = "2021-11-01"
end_date = "2023-04-18"

date_range = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%Y-%m-%d').tolist()
# df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))
# print(date_range)
start_date_list = [str(row.date) for row in df.select("date").collect()]
# print(start_date_list)
uncommon_elements = list(set(date_range).symmetric_difference(set(start_date_list)))
print(uncommon_elements)
uncommon_elements = sorted(uncommon_elements, key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
print(f"Total {len(uncommon_elements)} days with zero rides")
for i in uncommon_elements:
    print(i)


# COMMAND ----------

# Q2 What are the daily trip trends for your given station?
from pyspark.sql.functions import date_format

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

df_q2 = spark.read.format("delta").load(delta_path)
df_q2 = df_q2.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

daily_trips = df_q2[df_q2["start_date"] == "2021-11-17"]


# daily_trips = daily_trips.orderBy("start_date")

display(daily_trips.count())

# COMMAND ----------

dates=['2021-11-02', '2021-11-16', '2022-11-06', '2021-11-13', '2021-11-11', '2021-11-14', '2021-11-18', '2021-11-10', '2021-11-12', '2021-11-17', '2021-11-01', '2021-11-06', '2021-11-07', '2021-11-05', '2021-11-09', '2022-11-04', '2022-11-03', '2021-11-15', '2021-11-03', '2022-11-02', '2021-11-04', '2021-11-08', '2022-11-05']
counts=0
for i in dates:

    df_q2 = df_q2.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))
    daily_trips = df_q2[df_q2["start_date"] == i]
    counts+=daily_trips.count()



# daily_trips = daily_trips.orderBy("start_date")

display(counts)

# COMMAND ----------

from pyspark.sql.functions import col
for i in df.columns:
    s=df.filter(col(i).isNull())
    if s.count()==0:
        print("column",i,"has no null values")
    else:
        print("column",i,"has NULL values")
        display(df.filter(col(i).isNull()))

# COMMAND ----------


