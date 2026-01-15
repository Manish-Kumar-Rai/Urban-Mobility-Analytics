# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "70f02442-d3b2-4bef-abcf-d3f9b0d1c126",
# META       "default_lakehouse_name": "UrbanLakehouse",
# META       "default_lakehouse_workspace_id": "b063ec64-26f2-4a88-871e-79decd7384da",
# META       "known_lakehouses": [
# META         {
# META           "id": "70f02442-d3b2-4bef-abcf-d3f9b0d1c126"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp, explode, when, lit, explode, from_unixtime
import datetime, requests, json, os
from notebookutils import fs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path = "Files/weather_data.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

api_key = "3cdbe6b58e717d97430441f12ef9f021"
city = "Warsaw"
url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Fetching weathermap data for {city}...")
response = requests.get(url)

if response.status_code == 200:
    # Convert dictionary to a JSON string
    json_string = json.dumps(response.json())
    
    # Use Fabric utilities to put the file in 'Files'
    fs.put("Files/weather_data", json_string) # True allows overwriting
    print("Done! Data saved using notebookutils.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_raw = spark.read.option("multiline", "true").json("Files/weather_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = df_raw.select(explode("list").alias("forecast_step"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_exploded.select(
    from_unixtime(col("forecast_step.dt")).alias("forecast_time"),
    col("forecast_step.main.temp").alias("temperature"),
    col("forecast_step.main.feels_like").alias("feels_like"),
    col("forecast_step.main.humidity").alias("humidity"),
    col("forecast_step.main.pressure").alias("pressure"),
    col("forecast_step.weather")[0]["description"].alias("condition"),
    col("forecast_step.wind.speed").alias("wind_speed")
).withColumn("ingested_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "gold_weather_forecast"
df_final.write.format("delta").mode("overwrite").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.table(table_name).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
