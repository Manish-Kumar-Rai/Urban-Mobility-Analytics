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

from pyspark.sql.functions import col, coalesce, to_date, current_timestamp, avg, last, lit
from pyspark.sql import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver Taxi (Handling the Hybrid Schema):

# CELL ********************

# 1. Load the two separate tables
df_yellow = spark.read.table("bronze_yellow_taxi").withColumn("taxi_type", lit("yellow"))
df_green = spark.read.table("bronze_green_taxi").withColumn("taxi_type", lit("green"))

# 2. Union them safely using unionByName
# The allowMissingColumns=True is crucial because Green has 'ehail_fee' 
# and Yellow has 'airport_fee'
df_bronze_combined = df_yellow.unionByName(df_green, allowMissingColumns=True)

# 3. Apply your Silver transformations
# Note: I've updated the pickup/dropoff logic to match your bronze column names
silver_taxi = df_bronze_combined.select(
    col("VendorID"),
    col("taxi_type"),
    
    # Coalesce the specific pickup/dropoff columns
    coalesce(
        col("tpep_pickup_datetime"), 
        col("lpep_pickup_datetime")
    ).alias("pickup_datetime"),
    
    coalesce(
        col("tpep_dropoff_datetime"), 
        col("lpep_dropoff_datetime")
    ).alias("dropoff_datetime"),
    
    # Cast passenger_count to int (it came in as double/long in Bronze)
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    
    # Standardizing Location IDs
    col("PULocationID").alias("pickup_zone_id"),
    col("DOLocationID").alias("dropoff_zone_id"),
    
    # Financials
    col("fare_amount"),
    col("total_amount"),
    
    # Timing (if you added ingestion_timestamp in bronze, keep it, 
    # otherwise lit(None) or just remove it)
    current_timestamp().alias("silver_processing_time")
).filter("trip_distance > 0 AND total_amount > 0")

# 4. Write the final Silver Table
silver_taxi.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_nyc_taxi")

print("Silver layer successfully created from unified Bronze tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Load the green taxi bronze table
df_green = spark.read.table("bronze_green_taxi")

# 2. Apply Silver transformations
silver_green_taxi = (
    df_green
    .select(
        F.col("VendorID"),
        # Standardizing names for easier downstream use
        F.col("lpep_pickup_datetime").alias("pickup_datetime"),
        F.col("lpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("PULocationID").alias("pickup_location_id"),
        F.col("DOLocationID").alias("dropoff_location_id"),
        
        # Casting types
        F.col("passenger_count").cast("int"),
        F.col("trip_distance").cast("double"),
        F.col("fare_amount").cast("double"),
        F.col("total_amount").cast("double"),
        F.col("payment_type").cast("int"),
        F.col("trip_type").cast("int"),
        F.col("RatecodeID").cast("int"),
        
        # Metadata
        F.lit("green").alias("taxi_type"),
        F.current_timestamp().alias("silver_processing_time")
    )
    # 3. Add Calculated Field: Trip Duration in Minutes
    .withColumn("trip_duration_minutes", 
                (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60)
    
    # 4. Data Quality Filters
    .filter(
        (F.col("trip_distance") > 0) & 
        (F.col("total_amount") > 0) &
        (F.col("pickup_datetime") < F.col("dropoff_datetime")) # Ensure time flows forward
    )
)

# 5. Write to Silver Delta Table
silver_green_taxi.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_nyc_taxi")

print("Silver Green Taxi table successfully created.")

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
