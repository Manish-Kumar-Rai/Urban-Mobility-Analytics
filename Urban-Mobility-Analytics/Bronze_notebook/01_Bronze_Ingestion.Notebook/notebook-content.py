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

# MARKDOWN ********************

# # Raw Data

# CELL ********************

# Installing Library
!pip install openaq

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # NYC Taxi ingestion

# CELL ********************

import requests
from pyspark.sql.functions import lit, current_timestamp, col
from notebookutils import mssparkutils
from datetime import datetime, timedelta
import pandas as pd
import json
from math import ceil
from openaq import OpenAQ


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_path = "Files/bronze_nyc_taxi"
temp_path = "Files/_temp_download"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# --- CONFIGURATION ---
taxi_types = ["yellow", "green"]
years = [str(i) for i in range(2020,2026)]
months = [f"{i:02d}" for i in range(1, 13)]
current_year = datetime.now().year
current_month = datetime.now().month

# --- COUNTERS FOR SUMMARY ---
stats = {"loaded": 0, "skipped": 0, "missing": 0}

def valid_months(year):
    if int(year) == current_year:
        # TLC data usually has a 2-month lag, but we'll check up to current
        return [f"{i:02d}" for i in range(1, current_month + 1)]
    return months

for taxi in taxi_types:
    for year in years:
        for month in valid_months(year):
            
            raw_storage_path = f"Files/bronze_nyc_taxi/{taxi}/{year}/{month}"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month}.parquet"
            
            # 1. IDEMPOTENCY CHECK (Don't re-download if we have the file)
            if mssparkutils.fs.exists(raw_storage_path):
                print(f"🟡 Skipped (Exists): {taxi} {year}-{month}")
                stats["skipped"] += 1
                continue

            # 2. AVAILABILITY CHECK (Peeking before pulling)
            response = requests.head(url)
            if response.status_code != 200:
                print(f"🚫 Not Available (404): {taxi} {year}-{month}")
                stats["missing"] += 1
                continue

            try:
                # 3. LAND RAW DATA (Files Section)
                mssparkutils.fs.mkdirs(raw_storage_path)
                mssparkutils.fs.cp(url, f"{raw_storage_path}/data.parquet")

                # 4. PROCESS TO BRONZE TABLE (Tables Section)
                # We read from our local Lakehouse copy, not the URL, for stability
                df = spark.read.parquet(f"{raw_storage_path}/data.parquet")
                
                df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                       .withColumn("source_file", lit(url)) \
                       .withColumn("taxi_type", lit(taxi))

                # Note: Partitioning by taxi_type and vendorid as requested
                df.write.format("delta") \
                  .mode("append") \
                  .option("mergeSchema","true")\
                  .saveAsTable("bronze_nyc_taxi")
                
                print(f"✅ Success: {taxi} {year}-{month}")
                stats["loaded"] += 1
                
            except Exception as e:
                print(f"❌ Error processing {taxi} {year}-{month}: {str(e)}")
                stats["missing"] += 1

# ================= SUMMARY =================
print("\n" + "="*30)
print("       INGESTION SUMMARY")
print("="*30)
print(f"Successfully Loaded : {stats['loaded']}")
print(f"Already Existed     : {stats['skipped']}")
print(f"Server 404/Missing  : {stats['missing']}")
print("="*30)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("bronze_green_taxi")
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Taxi Zone:

# CELL ********************

zone_lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
mssparkutils.fs.cp(zone_lookup_url, "Files/bronze/static/taxi_zone_lookup.csv")

path = "Files/bronze/static/taxi_zone_lookup.csv"
df_lookup = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# 2. Clean and Standardize
# We ensure columns are renamed to snake_case for consistency across Silver
bronze_taxi_zone = df_lookup.select(
    col("LocationID").alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone_name"),
    col("service_zone")
)

# 3. Save as Delta Table
bronze_taxi_zone.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_taxi_zone")

print("✅ bronze_taxi_zone created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## OpenAQ Ingestions:

# PARAMETERS CELL ********************


API_KEY = "c359c43896d505d4fba78a0edf200d32e76ea9c812cc30b70f6c3013089fedae"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

NYC_BBOX = "-74.259,40.477,-73.700,40.917"
headers = {"X-API-Key": API_KEY}

# 1. FIND SENSORS IN NYC
print("🔍 Searching for all active sensors in NYC...")
loc_url = "https://api.openaq.org/v3/locations"
loc_res = requests.get(loc_url, params={"bbox": NYC_BBOX, "limit": 100}, headers=headers).json()

sensor_list = []
for loc in loc_res.get('results', []):
    for s in loc.get('sensors', []):
        sensor_list.append({
            "id": s['id'],
            "parameter": s['parameter']['name'],
            "unit": s['parameter']['units'],
            "lat": loc['coordinates']['latitude'],
            "lon": loc['coordinates']['longitude'],
            "location_name": loc['name']
        })



# 2. FETCH HISTORICAL DAILY MEASUREMENTS
print(f"📈 Found {len(sensor_list)} sensors. Pulling historical daily data...")
all_measurements = []

# We'll pull the last year of data for each sensor
for sensor in sensor_list:
    meas_url = f"https://api.openaq.org/v3/sensors/{sensor['id']}/days"
    # Filtering for the last year to ensure we match your Taxi data range
    params = {
        "date_from": (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d"),
        "limit": 1000 
    }
    
    try:
        m_res = requests.get(meas_url, params=params, headers=headers).json()
        for m in m_res.get('results', []):
            all_measurements.append({
                "sensor_id": sensor['id'],
                "location_name": sensor['location_name'],
                "parameter": sensor['parameter'],
                "lat": sensor['lat'],
                "lon": sensor['lon'],
                "value": m['value'],
                "unit": sensor['unit'],
                "date_utc": m['period']['datetimeTo']['utc']
            })
        print(f"✅ Ingested: {sensor['location_name']} - {sensor['parameter']}")
    except:
        print(f"⚠️ Skipped sensor {sensor['id']} due to error.")

# 3. SAVE TO BRONZE TABLE
if all_measurements:
    df_aq = spark.createDataFrame(all_measurements)
    df_aq = df_aq.withColumn("ingestion_timestamp", current_timestamp())
    
    # We use overwriteSchema because we changed column names from the previous failed run
    df_aq.write.format("delta") \
         .mode("overwrite") \
         .option("overwriteSchema", "true") \
         .saveAsTable("bronze_openaq")
         
    print(f"\n✨ SUCCESS: {len(all_measurements)} historical records saved to bronze_openaq")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GDP Ingestions:

# CELL ********************

print("🚀 Ingesting GDP Data...")
gdp_url = "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json"
response_gdp = requests.get(gdp_url).json()

try:
    response_gdp = requests.get(gdp_url).json()
    
    # World Bank JSON structure: [metadata, actual_data]
    gdp_list = response_gdp[1]
    
    # Convert to pandas then Spark
    pdf_gdp = pd.DataFrame(gdp_list)
    df_gdp = spark.createDataFrame(pdf_gdp)
    
    # Bronze Logic: Keep columns but fix 'value' type and add audit columns
    df_gdp_bronze = df_gdp.withColumn("value", col("value").cast("double")) \
                          .withColumn("ingestion_timestamp", current_timestamp()) \
                          .withColumn("source_api", lit("World Bank"))

    # Overwrite the table with the full schema
    df_gdp_bronze.write.format("delta") \
                 .mode("overwrite") \
                 .option("overwriteSchema", "true") \
                 .saveAsTable("bronze_gdp")
    
    print("✅ bronze_gdp created successfully with full schema.")

except Exception as e:
    print(f"❌ Failed to ingest GDP: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FX (ECB - USD/EUR) Ingestions:

# CELL ********************

print("🚀 Ingesting FX Data...")
fx_url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"
# Using pandas to read the CSV stream directly
df_fx_pd = pd.read_csv(fx_url)
df_fx = spark.createDataFrame(df_fx_pd)

df_fx.write.format("delta").mode("overwrite").saveAsTable("bronze_fx")
print("✅ bronze_fx created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
