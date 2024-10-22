# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1: Create a notebook and add SQL pipeline code
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Bronze layer
# MAGIC #### Bronze Table: Raw data ingestion
# MAGIC
# MAGIC Here, raw taxi trip data is ingested, with a basic data quality check applied to ensure trip distances are positive.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_ws_01.nyctaxi_bronze.taxi_raw_records AS
# MAGIC SELECT *
# MAGIC FROM samples.nyctaxi.trips
# MAGIC WHERE trip_distance > 0.0;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver layer
# MAGIC
# MAGIC The Silver layer creates two tables: 
# MAGIC
# MAGIC Silver Table 1:  Flagged rides
# MAGIC
# MAGIC This table identifies potentially suspicious rides based on fare and distance criteria.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver Table 1: Flagged rides
# MAGIC
# MAGIC CREATE OR REPLACE TABLE catalog_ws_01.nyctaxi_silver.flagged_rides AS
# MAGIC SELECT
# MAGIC   date_trunc("week", tpep_pickup_datetime) AS week,
# MAGIC   pickup_zip AS zip,
# MAGIC   fare_amount,
# MAGIC   trip_distance
# MAGIC FROM
# MAGIC   catalog_ws_01.nyctaxi_bronze.taxi_raw_records
# MAGIC WHERE ((pickup_zip = dropoff_zip AND fare_amount > 50) OR
# MAGIC        (trip_distance < 5 AND fare_amount > 50));

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Table 2: Weekly statistics
# MAGIC
# MAGIC This silver table calculates weekly average fares and trip distances. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver layer 2: Weekly statistics
# MAGIC
# MAGIC CREATE OR REPLACE TABLE catalog_ws_01.nyctaxi_gold.weekly_stats AS
# MAGIC SELECT
# MAGIC   date_trunc("week", tpep_pickup_datetime) AS week,
# MAGIC   AVG(fare_amount) AS avg_amount,
# MAGIC   AVG(trip_distance) AS avg_distance
# MAGIC FROM
# MAGIC   catalog_ws_01.nyctaxi_bronze.taxi_raw_records
# MAGIC GROUP BY week
# MAGIC ORDER BY week ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold layer
# MAGIC Gold Table 1: Top N rides
# MAGIC
# MAGIC Here, these silver tables are integrated to provide a comprehensive view of the top three highest-fare rides.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold layer: Top N rides to investigate
# MAGIC
# MAGIC CREATE OR REPLACE TABLE catalog_ws_01.nyctaxi_gold.top_n AS
# MAGIC SELECT
# MAGIC   ws.week,
# MAGIC   ROUND(ws.avg_amount, 2) AS avg_amount,
# MAGIC   ROUND(ws.avg_distance, 3) AS avg_distance,
# MAGIC   fr.fare_amount,
# MAGIC   fr.trip_distance,
# MAGIC   fr.zip
# MAGIC FROM
# MAGIC   catalog_ws_01.nyctaxi_silver.flagged_rides fr
# MAGIC LEFT JOIN catalog_ws_01.nyctaxi_gold.weekly_stats ws ON ws.week = fr.week
# MAGIC ORDER BY fr.fare_amount DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc("week", tpep_pickup_datetime) AS week,* 
# MAGIC FROM catalog_ws_01.nyctaxi_bronze.taxi_raw_records
# MAGIC LIMIT 100
