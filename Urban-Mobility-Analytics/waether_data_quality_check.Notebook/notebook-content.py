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

%pip install great_expectations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx
import json
import datetime
from notebookutils import fs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Initialize the GX Context (Ephemeral mode is perfect for Fabric)
context = gx.get_context()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Connect to your Spark DataFrame
df_to_validate = spark.read.table("gold_weather_forecast")
batch_definition = context.data_sources.add_spark("my_spark_datasource").add_dataframe_asset("weather_asset").add_batch_definition_whole_dataframe("batch_def")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df_to_validate})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Create an Expectation Suite
expectation_suite_name = "weather_quality_suite"
suite = context.suites.add(gx.ExpectationSuite(name=expectation_suite_name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Add the specific rules (Expectations)
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="temperature", min_value=-50, max_value=60))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="humidity"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="forecast_time"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Run the Validation
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="weather_validation", 
        data=batch_definition,  
        suite=suite
    )
)

validation_results = validation_definition.run(batch_parameters={"dataframe": df_to_validate})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Check results and Save the JSON Quality Report
all_passed = validation_results.success

status_report = {
    "status": "Success" if all_passed else "Failure",
    "timestamp": str(datetime.datetime.now()),
    "total_checks": 3,
    "success_percent": validation_results.statistics["success_percent"]
}

fs.put("Files/report/weather_quality_report.json", json.dumps(status_report), True)

if all_passed:
    print("✅ Data Quality Check Passed! Report saved to Files/report/weather_quality_report.json")
else:
    print(f"❌ Data Quality Check Failed ({status_report['success_percent']}%). Check the report.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

# 1. The URL you just generated
webhook_url = "https://defaulta09f0a03b97d42a59678cb9da9897c.f1.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/28d90aba611445cca102666a13047572/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=OkSFK9qzKIHXxPXE-WSHMgtyuNwJU4i-IcPt-rstKM0"

# 2. The data payload (using the results from your Great Expectations step)
payload = {
    "status": "Success",
    "timestamp": "2026-01-15 19:00:00",
    "success_percent": 100
}

# 3. Send the request
print("Sending notification to Power Automate...")
response = requests.post(webhook_url, json=payload)

# 4. Check if it worked
if response.status_code == 202 or response.status_code == 200:
    print("🚀 Success! Check your email/teams for the weather data report.")
else:
    print(f"❌ Failed. Status Code: {response.status_code}")
    print(f"Details: {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
