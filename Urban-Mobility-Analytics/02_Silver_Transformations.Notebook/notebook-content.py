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

from pyspark.sql.functions import col, coalesce, to_date, current_timestamp, avg, last
from pyspark.sql import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver Taxi (Handling the Hybrid Schema):

# CELL ********************

df_bronze_taxi = spark.read.table("bronze_nyc_taxi")

silver_taxi = df_bronze_taxi.select(
    col("VendorID"),
    col("taxi_type"),
    # Unify the pickup/dropoff columns
    coalesce(col("tpep_pickup_datetime"), col("lpep_pickup_datetime")).alias("pickup_datetime"),
    coalesce(col("tpep_dropoff_datetime"), col("lpep_dropoff_datetime")).alias("dropoff_datetime"),
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    col("PULocationID").alias("pickup_zone_id"),
    col("DOLocationID").alias("dropoff_zone_id"),
    col("fare_amount"),
    col("total_amount"),
    col("ingestion_timestamp").alias("bronze_ingestion_time"),
    current_timestamp().alias("silver_processing_time")
).filter("trip_distance > 0 AND total_amount > 0") # Basic quality check

silver_taxi.write.format("delta").mode("overwrite").saveAsTable("silver_nyc_taxi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deduplicate(location id) in bronze_dim_zone:

# CELL ********************

df_bronze_taxi_zone = spark.table("bronze_taxi_zone")
silver_zone = df_bronze_taxi_zone.dropDuplicates(["location_id"])

# 4. Save as Delta Table
silver_zone.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_dim_zone")

print("✅ silver_dim_zone created. The bridge is ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver GDP (Flattening the Structs):

# CELL ********************

df_bronze_gdp = spark.read.table("bronze_gdp")

silver_gdp = df_bronze_gdp.select(
    col("country.value").alias("country_name"),
    col("countryiso3code"),
    col("indicator.value").alias("indicator_name"),
    col("date").cast("int").alias("gdp_year"),
    col("value").alias("gdp_value"),
    col("obs_status")
)

silver_gdp.write.format("delta").mode("overwrite").saveAsTable("silver_gdp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver FX (Cleaning the ECB Data):

# CELL ********************

# 1. Read from Bronze
df_bronze_fx = spark.read.table("bronze_fx")

# 2. Basic Cleaning (Date conversion)
silver_fx_base = df_bronze_fx.select(
    to_date(col("TIME_PERIOD")).alias("fx_date"),
    col("CURRENCY"),
    col("OBS_VALUE").alias("exchange_rate_to_eur")
).filter("fx_date IS NOT NULL")

# 3. Handle NULLs using Forward Fill (The Senior DE Move)
# We define a window that looks at all previous rows up to the current one
window_spec = Window.partitionBy("CURRENCY").orderBy("fx_date").rowsBetween(Window.unboundedPreceding, 0)

# F.last with ignorenulls=True fills the gap with the most recent valid value
silver_fx_final = silver_fx_base.withColumn(
    "exchange_rate_to_eur", 
    last("exchange_rate_to_eur", ignorenulls=True).over(window_spec)
)

# 4. Save to Silver
silver_fx_final.write.format("delta").mode("overwrite").saveAsTable("silver_fx")

print("✅ Silver FX updated with Forward-Fill logic for missing rates.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver OpenAQ (Pivoting Long to Wide):

# CELL ********************

df_bronze_aq = spark.read.table("bronze_openaq")

silver_aq = df_bronze_aq.withColumn("reading_date", to_date(col("date_utc"))) \
    .groupBy("location_name", "reading_date", "lat", "lon") \
    .pivot("parameter") \
    .agg(avg("value"))

silver_aq.write.format("delta").mode("overwrite").saveAsTable("silver_openaq")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
