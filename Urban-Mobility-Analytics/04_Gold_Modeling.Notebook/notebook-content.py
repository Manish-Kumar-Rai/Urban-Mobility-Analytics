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

# --- 1. DimZone & DimGDP & DimFX (Promoting from Silver) ---
spark.table("silver_dim_zone").write.format("delta").mode("overwrite").saveAsTable("gold_dim_zone")
spark.table("silver_gdp").write.format("delta").mode("overwrite").saveAsTable("gold_dim_gdp")
spark.table("silver_fx").write.format("delta").mode("overwrite").saveAsTable("gold_dim_fx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 2. FactTaxiDaily (Summarizing 86M rows for Performance) ---
# We aggregate by Date and Zone so Power BI doesn't have to calculate 86M rows on the fly.
spark.table("silver_nyc_taxi") \
    .withColumn("pickup_date", F.to_date("pickup_datetime")) \
    .groupBy("pickup_date", "pickup_zone_id") \
    .agg(
        F.count("*").alias("total_trips"),
        F.sum("total_amount").alias("daily_revenue_usd"),
        F.avg("trip_distance").alias("avg_trip_distance")
    ).write.format("delta").mode("overwrite").saveAsTable("gold_fact_taxi_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 3. FactAirQualityDaily (Comprehensive Pollutant Suite) ---
df_silver_aq = spark.table("silver_openaq")

gold_fact_aq = df_silver_aq.groupBy("reading_date", "location_name", "lat", "lon") \
    .agg(
        F.avg("pm25").alias("avg_pm25"),
        F.avg("no2").alias("avg_no2"),
        F.avg("o3").alias("avg_o3"),
        F.avg("co").alias("avg_co"),
        F.avg("so2").alias("avg_so2"),
        F.avg("pm10").alias("avg_pm10"),
        F.avg("temperature").alias("avg_temp") # Useful for weather-pollution correlation
    )

# Filter out rows where all major pollutants are null to keep Gold clean
gold_fact_aq = gold_fact_aq.filter(
    "avg_pm25 IS NOT NULL OR avg_no2 IS NOT NULL OR avg_o3 IS NOT NULL"
)

gold_fact_aq.write.format("delta").mode("overwrite").saveAsTable("gold_fact_aq_daily")

print("✅ Gold Air Quality Fact Table updated with all available pollutants!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 4. DimDate (The Universal Joiner) ---
df_dates = spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2025-11-30'), interval 1 day)) as date_key")
dim_date = df_dates.select(
    "date_key",
    F.year("date_key").alias("year"),
    F.month("date_key").alias("month"),
    F.date_format("date_key", "EEEE").alias("day_name"),
    F.when(F.date_format("date_key", "E").isin("Sat", "Sun"), 1).otherwise(0).alias("is_weekend")
)
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold_dim_date")

print("✨ Gold Layer Built Successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
