# Databricks notebook source
# MAGIC %run "../final_project/includes/includes/"

# COMMAND ----------

from pyspark.sql.functions import year, month, count, to_timestamp,from_unixtime,split

sample=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_status.delta")
sample = sample.withColumn("datetime", to_timestamp(from_unixtime(sample["last_reported"])))

sample = sample.withColumn("date", sample["datetime"].cast("date"))
sample = sample.withColumn("time", split(sample["datetime"].cast("string"), " ")[1])


# COMMAND ----------

sample=sample.sort(sample["date"].asc())


# COMMAND ----------

display(sample.limit(5))

# COMMAND ----------

station_info=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_station_info.delta")
display(station_info.limit(5))

# COMMAND ----------

weather_live=spark.read.format("delta").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta")
display(weather_live.sort(weather_live["time"].asc()))

# COMMAND ----------

weather=spark.read.format("delta").load("dbfs:/FileStore/tables/G06/bronze/nyc_weather_history/")
display(weather.limit(5))

# COMMAND ----------

# Q1 What are the monthly trip trends for your assigned station?

from pyspark.sql.functions import year, month, count

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

df = spark.read.format("delta").load(delta_path)
# display(df.orderBy("started_at").tail(5))
df = df.withColumn("started_at", df["started_at"].cast("date"))

monthly_trips = df.groupBy(year("started_at").alias("year"), month("started_at").alias("month")) \
                  .agg(count("*").alias("trips")) \
                  .orderBy("year", "month")

display(monthly_trips)


# COMMAND ----------

# Line chart for Monthly Trips Over Time
from pyspark.sql.functions import concat, lit
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="darkgrid")

monthly_trips = monthly_trips.withColumn("year_month", 
                    concat(monthly_trips["year"], lit("-"), monthly_trips["month"]))

sns.lineplot(x="year_month", y="trips", data=monthly_trips.toPandas())

plt.title("Monthly Trips Over Time")
plt.xlabel("Year-Month")
plt.ylabel("Trips")
plt.xticks(rotation=45)
plt.show()


import plotly.express as px
import plotly.graph_objects as go

monthly_trips = monthly_trips.withColumn("year_month", 
                    concat(monthly_trips["year"], lit("-"), monthly_trips["month"]))

fig = px.line(monthly_trips.toPandas(), x="year_month", y="trips")

fig.update_layout(title="Monthly Trips Over Time",
                  xaxis_title="Year-Month",
                  yaxis_title="Trips",
                  xaxis_tickangle=-45)

fig.show()


# COMMAND ----------

# Bar Chart for Monthly Trips by Year

sns.catplot(x="month", y="trips", hue="year", kind="bar", data=monthly_trips.toPandas(), height=6, aspect=2)

plt.title("Monthly Trips by Year")
plt.xlabel("Month")
plt.ylabel("Trips")
plt.show()


# COMMAND ----------

# Heat Map for Monthly Trips by Year

pivot_table = monthly_trips.toPandas().pivot("month", "year", "trips")

sns.heatmap(pivot_table, cmap="YlGnBu", annot=True, fmt=".0f")

plt.title("Monthly Trips by Year")
plt.xlabel("Year")
plt.ylabel("Month")
plt.show()

# COMMAND ----------

from pyspark.sql.functions import col,desc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

df = spark.read.format("delta").load(delta_path)

print('Number of rows: ', df.count())
print('Number of columns: ', len(df.columns))

print('Distinct rideable types', df.select('rideable_type').distinct().count())
df.groupBy('rideable_type').count().show()
print('Distinct end station names', df.select('end_station_name').distinct().count())
df.groupBy('end_station_name').count().orderBy(desc('count')).show()
print('Distinct membership types', df.select('member_casual').distinct().count())
df.groupBy('member_casual').count().show()


# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd

# Convert PySpark DataFrame to Pandas DataFrame
df_pd = df.toPandas()

# Plot count of rideable types
rideable_count = df_pd['rideable_type'].value_counts()
fig = go.Figure([go.Bar(x=rideable_count.index, y=rideable_count.values, marker_color='blue')])
fig.update_layout(title='Count of rideable types', xaxis_title='Rideable type', yaxis_title='Count')
fig.show()

# Plot count of membership types
membership_count = df_pd['member_casual'].value_counts()
fig = go.Figure([go.Bar(x=membership_count.index, y=membership_count.values, marker_color='green')])
fig.update_layout(title='Count of membership types', xaxis_title='Membership type', yaxis_title='Count')
fig.show()


# COMMAND ----------

# get top 20 end stations

import plotly.graph_objs as go

top_end_stations = df.groupBy('end_station_name').count().orderBy(desc('count')).limit(20)

# create bar plot
data = go.Bar(x=top_end_stations.select('end_station_name').rdd.flatMap(lambda x: x).collect(),
              y=top_end_stations.select('count').rdd.flatMap(lambda x: x).collect())

layout = go.Layout(title='Top 20 End Stations',
                   xaxis=dict(title='End Station Name'),
                   yaxis=dict(title='Count'))

fig = go.Figure(data=data, layout=layout)
fig.show()


# COMMAND ----------

# rideable count types

import matplotlib.pyplot as plt

rideable_counts = df.groupBy('rideable_type').count().orderBy('count', ascending=False).toPandas()

plt.figure(figsize=(8,6))
plt.bar(rideable_counts['rideable_type'], rideable_counts['count'])
plt.title('Rideable Type Counts')
plt.xlabel('Rideable Type')
plt.ylabel('Count')
plt.show()


# COMMAND ----------

# Hourly count of rides for the entire data

from pyspark.sql.functions import hour

hourly_counts = df.groupBy(hour('started_at').alias('hour')).count().orderBy('hour').toPandas()

plt.figure(figsize=(10,6))
plt.plot(hourly_counts['hour'], hourly_counts['count'], marker='o')
plt.title('Hourly Ride Counts')
plt.xlabel('Hour of Day')
plt.ylabel('Ride Count')
plt.xticks(range(0,24))
plt.show()


# COMMAND ----------

# Ride Counts by Day of Week and Membership Type

from pyspark.sql.functions import date_format, dayofweek, count, sum
import matplotlib.pyplot as plt

pivot_table = df.groupBy(date_format('started_at', 'EEEE').alias('day'), 'member_casual')\
                .agg(count('*').alias('ride_count'))\
                .groupBy('day').pivot('member_casual')\
                .agg(sum('ride_count'))\
                .orderBy(dayofweek('day'))

pivot_table_pd = pivot_table.toPandas()

pivot_table_pd['member'] = pivot_table_pd['member'].astype(float)
pivot_table_pd['casual'] = pivot_table_pd['casual'].astype(float)

plt.figure(figsize=(10,6))
plt.bar(pivot_table_pd['day'], pivot_table_pd['casual'], label='Casual')
plt.bar(pivot_table_pd['day'], pivot_table_pd['member'], bottom=pivot_table_pd['casual'], label='Member')
plt.title('Ride Counts by Day of Week and Membership Type')
plt.xlabel('Day of Week')
plt.ylabel('Ride Count')
plt.legend()
plt.show()


# COMMAND ----------

# Rideable Type and Customer Type

ride_counts = df.groupby(['rideable_type', 'member_casual']).count().select(['rideable_type', 'member_casual', 'count']).toPandas()

pivot_table = ride_counts.pivot(index='member_casual', columns='rideable_type', values='count')
import matplotlib.pyplot as plt

pivot_table.plot(kind='bar', stacked=True)
plt.title('Rideable Type and Customer Type')
plt.xlabel('Customer Type')
plt.ylabel('Number of Rides')
plt.legend(title='Rideable Type', loc='upper left')
plt.show()


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

# line chart for daily bike trip trends

import matplotlib.dates as mdates
import plotly.express as px

fig = px.line(daily_trips.toPandas(), x="start_date", y="count", title="Daily Bike Trip Trends")
fig.show()

# COMMAND ----------

# bar graph

import plotly.graph_objs as go

fig = go.Figure(data=[go.Bar(x=daily_trips.toPandas()['start_date'], y=daily_trips.toPandas()['count'], 
                             marker=dict(color='blue'))])
fig.update_layout(title='Total Trips by Day', xaxis_title='Day', yaxis_title='Trips')
fig.show()


# COMMAND ----------

# Dates with zero rides or missing data

import pandas as pd
from pyspark.sql.functions import to_date
start_date = "2021-11-01"
end_date = "2023-02-28"
date_range = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%Y-%m-%d').tolist()
df = df.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))
start_date_list = [str(row.start_date) for row in df.select("start_date").collect()]
uncommon_elements = list(set(date_range).symmetric_difference(set(start_date_list)))
uncommon_elements = sorted(uncommon_elements, key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
print(f"Total {len(uncommon_elements)} days with zero rides")
for i in uncommon_elements:
    print(i)


# COMMAND ----------

from pyspark.sql.functions import date_format, col, udf
from pyspark.sql.types import BooleanType
import datetime
# Create a list to store the holiday dates
holiday_dates = []

# Loop through the years 2021 to 2023
for year in range(2021, 2024):

    # Loop through all the days in the current year
    for month in range(1, 13):
        for day in range(1, 32):
            try:
                date = datetime.date(year, month, day)
                # Check if the current date is a Saturday or Sunday
                if date.weekday() in [5, 6]:
                    holiday_dates.append(date.strftime('%Y-%m-%d'))
            except ValueError:
                # Skip invalid dates
                pass

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"
# holidays = ["2021-11-25", "2021-12-25", "2022-01-01", "2022-01-17", "2022-02-21"]
# holidays = [datetime.datetime.strptime(h, '%Y-%m-%d').date() for h in holidays]
# print(holidays)

df_q2 = spark.read.format("delta").load(delta_path)
# df_q2 = df_q2.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))

# Define a UDF to check if a date is a holiday
def is_holiday(date):
  return str(date) in holiday_dates
is_holiday_udf = udf(is_holiday, BooleanType())

# Add a column to indicate whether each date is a holiday or not
df_q2 = df_q2.withColumn("is_holiday", is_holiday_udf(col("date")))

# Group the data by date and holiday/non-holiday
daily_trips = df_q2.groupBy("date", "is_holiday").count()
daily_trips = daily_trips.orderBy("date")

# Split the data into holiday and non-holiday data frames
daily_trips_holiday = daily_trips.filter(col("is_holiday") == True)
daily_trips_nonholiday = daily_trips.filter(col("is_holiday") == False)

# Display the daily trip trends for holidays and non-holidays
print("Daily Trip Trends for Holidays:")
display(daily_trips_holiday)

print("Daily Trip Trends for Non-Holidays:")
display(daily_trips_nonholiday)



# COMMAND ----------

avg_trips_nonholiday = daily_trips_nonholiday.filter(col("is_holiday") == False).agg({"count": "avg"}).collect()[0][0]

avg_trips_holiday = daily_trips_holiday.filter(col("is_holiday") == True).agg({"count": "avg"}).collect()[0][0]

print("Average trips per day for non-holidays (excluding holidays):", int(avg_trips_nonholiday))
print("Average trips per day for holidays:", int(avg_trips_holiday))

percentage_change = (avg_trips_holiday - avg_trips_nonholiday) / avg_trips_nonholiday * 100

print("Percentage change in the average number of trips per day due to holidays:", percentage_change, "%")


# COMMAND ----------

# Q4: How does weather affect the daily/hourly trend of system use?

from pyspark.sql.functions import split
from pyspark.sql.functions import to_timestamp, from_unixtime,desc
delta_table = "dbfs:/FileStore/tables/G06/bronze/nyc_weather_history/"
weather_df_with_datetime = spark.read.format("delta").load(delta_table)

# weather_df_with_datetime = weather_df.withColumn("datetime", to_timestamp(from_unixtime(weather_df["dt"])))

# weather_df_with_datetime = weather_df_with_datetime.withColumn("date", weather_df_with_datetime["datetime"].cast("date"))
# weather_df_with_datetime = weather_df_with_datetime.withColumn("time", split(weather_df_with_datetime["datetime"].cast("string"), " ")[1])
# display(weather_df_with_datetime.limit(5))
# from pyspark.sql.functions import col

# sd = weather_df_with_datetime.filter(col('date') == '2021-11-15')
# display(sd)
weather_df_with_datetime=weather_df_with_datetime.toPandas()
# check for duplicates in two columns
duplicates = weather_df_with_datetime[weather_df_with_datetime[['date', 'time']].duplicated()]
# Drop the duplicates in the weather dataframe
weather_df_with_datetime = weather_df_with_datetime.drop_duplicates(subset=['date', 'time'])

# display(weather_df_with_datetime.orderBy(desc("date")))



# COMMAND ----------

from pyspark.sql.functions import year, month, count,hour,round,date_format,to_timestamp,when,lit,concat,ceil,col
import pandas as pd

delta_path = "dbfs:/FileStore/tables/G06/historic_bike_trip_g06/"

rides_df = spark.read.format("delta").load(delta_path)
display(rides_df.count())
rides_df=rides_df.toPandas()
# rides_df['started_at'] = pd.to_datetime(rides_df['started_at'])

# # extract the date and hour from the 'started_at' column using .dt accessor
# rides_df['date'] = rides_df['started_at'].dt.date
# # rides_df['time'] = rides_df['started_at'].dt.hour.astype(str).str.zfill(2) + ':00:00'
# rides_df['time'] = rides_df['started_at'].dt.round('H').apply(lambda x: x.strftime('%H:%M:%S'))

# COMMAND ----------

merged_df = pd.merge(weather_df_with_datetime,rides_df, how='left', on=['date', 'time'])
spark_df = spark.createDataFrame(merged_df)

# COMMAND ----------

display(spark_df.count())

# COMMAND ----------

display(spark_df.limit(5))

# COMMAND ----------

display(spark_df.printSchema())

# COMMAND ----------


display(spark_df.select('main').distinct())
sf=spark_df.filter(col('main').isNull())
display(sf)

# COMMAND ----------

from pyspark.sql.functions import count

rides_by_weather = spark_df.groupBy('main').agg(count('ride_id').alias('ride_count'))

display(rides_by_weather)


# COMMAND ----------

import plotly.express as px

# Create a bar chart of ride counts by weather main category
rides_by_weather = spark_df.groupBy('main').agg(count('ride_id').alias('ride_count')).toPandas()

fig = px.bar(rides_by_weather, x='main', y='ride_count', title='Ride counts by weather category')
fig.show()


# COMMAND ----------

from pyspark.sql.functions import col, round

# Round off the feels_like column to 2 decimal places
spark_df = spark_df.withColumn('feels_like', round(col('feels_like'), 0))
rides_by_weather = spark_df.groupBy('feels_like').agg(count('ride_id').alias('ride_count'))

display(rides_by_weather)


# COMMAND ----------

spark_df = spark_df.withColumn('temp_category', 
                               when(col('feels_like') > 298, 'High')
                               .when(col('feels_like').between(278, 298), 'Medium')
                               .otherwise('Low'))
display(spark_df.limit(5))
rides_by_weather = spark_df.groupBy('temp_category').agg(count('ride_id').alias('ride_count'))

display(rides_by_weather)

# COMMAND ----------


