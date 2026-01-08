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

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Load your Silver Tables
df_taxi = spark.table("silver_nyc_taxi")
df_aq = spark.table("silver_openaq")
df_fx = spark.table("silver_fx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Define our "Anomalies" (Data that shouldn't exist)
# Rule: No trips with 0 passengers, no negative distances, no impossible pollution
bad_taxi = df_taxi.filter((F.col("passenger_count") <= 0) | (F.col("trip_distance") <= 0))
bad_aq = df_aq.filter((F.col("pm25") < 0) | (F.col("pm25") > 500))
null_fx = df_fx.filter(F.col("exchange_rate_to_eur").isNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Calculate Counts
total_taxi = df_taxi.count()
failed_taxi = bad_taxi.count()
failed_aq = bad_aq.count()
failed_fx = null_fx.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Senior Logic: The Threshold
# We allow a tiny margin of error (e.g., 0.1%), but if more data is bad, we STOP.
taxi_fail_rate = failed_taxi / total_taxi

print("--- DATA QUALITY REPORT ---")
print(f"Taxi Failure Rate: {taxi_fail_rate:.4%}")
print(f"Air Quality Anomalies: {failed_aq}")
print(f"FX Nulls: {failed_fx}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. THE CIRCUIT BREAKER
if taxi_fail_rate > 0.01: # 1% threshold
    raise Exception(f"🚨 PIPELINE HALTED: Taxi failure rate ({taxi_fail_rate:.2%}) exceeds 1% threshold!")

if failed_fx > 0:
    raise Exception("🚨 PIPELINE HALTED: FX rates contains NULL values. Financial downstream will break.")

print("✅ Data Quality Passed. Safe to proceed to Gold Layer.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
