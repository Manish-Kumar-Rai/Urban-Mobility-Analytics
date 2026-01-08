# 🏙️ Urban Mobility & Environmental Impact Analytics  
## End-to-End Data Engineering Pipeline on Microsoft Fabric

---

## 📌 Project Overview
This project implements a scalable **Medallion Architecture (Bronze ➡️ Silver ➡️ Gold)** to analyze the relationship between **urban mobility (NYC Taxi data)**, **environmental health (OpenAQ Air Quality)**, and **macroeconomic trends (GDP & FX Rates)**.  
The objective is to generate **data-driven insights** into how city traffic correlates with air pollution levels and economic shifts.

---

## 🏗️ Technical Architecture
The entire solution is built within the **Microsoft Fabric ecosystem**, leveraging **Direct Lake mode** for high-performance analytics without data duplication.

- **Ingestion**: Python / Spark Notebooks via REST APIs  
- **Orchestration**: Fabric Data Pipelines with parameterized triggers  
- **Storage**: OneLake (Delta Lake format) across Bronze, Silver, and Gold layers  
- **Transformation**: PySpark (Spark SQL) for cleansing, deduplication, and star-schema modeling  
- **Governance**: Circuit Breaker pattern for Data Quality validation  
- **Visualization**: Power BI (Direct Lake) for near real-time insights  
- **Monitoring**: Automated Telegram Bot notifications for pipeline status and DQ reports  

---

## 📂 Data Layers (Medallion Architecture)

### 🥉 Bronze — Raw Ingestion
- **Sources**:
  - NYC Taxi API  
  - OpenAQ Air Quality API  
  - World Bank GDP API  
  - FX Exchange Rates API  
- **State**: Raw JSON / Parquet files stored in the **Files/** section of the Lakehouse  
- **Key Feature**: Parameterized ingestion with secure handling of API keys  

---

### 🥈 Silver — Cleansed & Conformed
- **Processing**:
  - Schema enforcement  
  - Null handling and type casting  
  - Data standardization  
- **Logic**:
  - Deduplication of air quality sensor readings  
  - Standardized date and time dimensions  
- **Tables**:
  - `silver_nyc_taxi`  
  - `silver_openaq`  
  - `silver_gdp`  

---

### 🥇 Gold — Analytical Star Schema
- **Process**: Dimensional modeling optimized for BI workloads  
- **Fact Tables**:
  - `fact_taxi_daily`: Daily aggregated trips and revenue  
  - `fact_aq_daily`: Daily average PM2.5 and NO₂ concentrations  
- **Dimension Tables**:
  - `dim_date`: Enterprise date dimension (2025 range)  
  - `dim_zone`: NYC geographic zone lookup  

---

## 📊 Dashboards & Insights
- **Mobility Dashboard**:
  - Trip volumes  
  - Busiest zones  
  - Average fares  
- **Air Quality Dashboard**:
  - Pollution hotspots (PM2.5)  
  - Temporal and geographic trends  
- **Correlation Insights**:
  - Overlay of taxi traffic with pollutant spikes  
- **Economic Impact Analysis**:
  - Revenue comparison (USD vs EUR)  
  - Mobility trends correlated with GDP growth  

---

## 🚀 Key Engineering Features
- **Security**: API credentials managed via **Pipeline Parameters** (no hardcoded secrets)  
- **Data Quality**: Circuit Breaker logic halts pipelines when critical DQ thresholds fail  
- **Performance**: Delta Lake optimizations using `OPTIMIZE` and `V-ORDER`  
- **Automation**: Fully orchestrated end-to-end pipelines in Microsoft Fabric  

---

## 🛠️ How to Run
1. **Clone**: Import all notebooks into your Microsoft Fabric workspace  
2. **Setup**: Create a Lakehouse named `Urban_Analytics`  
3. **Parameters**: Configure required `API_KEY` values in Fabric Data Pipelines  
4. **Execute**: Trigger the `PL_Urban_Analytics_Daily` pipeline  

---

## 📩 Monitoring & Alerts
Upon successful execution, the system sends a **Telegram notification** containing:
- Pipeline execution status  
- Daily taxi trip count  
- Average PM2.5 pollution level  
- Data Quality validation summary  

---
