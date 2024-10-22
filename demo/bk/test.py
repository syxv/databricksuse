# Databricks notebook source
update_query = """
MERGE INTO catalog_ws_01.control.control_table AS target
USING (SELECT 'taxi_raw_records' AS table_name) AS source
ON target.table_name = source.table_name
WHEN MATCHED THEN
  UPDATE SET update_time = '2000-02-14T16:52:13.000+00:00' 
"""

spark.sql(update_query)
