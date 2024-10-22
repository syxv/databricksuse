# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Create a notebook and add SQL pipeline code
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.セットアップ

# COMMAND ----------

## ライブラリのインポート
import sys
import datetime
import logging
import pyspark.sql.functions as F
import pyspark.sql.types as T
from typing import List

# COMMAND ----------

## 定数定義
PHYSICAL_TASK_NAME = 'poc-bronze-job'
LOGICAL_TASK_NAME = 'bronzeテーブル更新'

# COMMAND ----------

logger = logging.Logger(name='poc-logger')
logger.level = logging.INFO

# COMMAND ----------

## エラーハンドリング用関数定義
def error_handling(table_names:List[str], batch_time:datetime.datetime, logger:logging.Logger, err:Exception) -> None:
    """
    Input:
        table_name: 今回更新したテーブルの名前のリスト。カタログ名・スキーマ名も必要
        batch_time: 入力時の引数として入手したbatch_time
        logger: このノートブックで使用しているLogger
        err: 発生したエラー
    """
    restore_time = batch_time.strftime('%Y-%m-%d %H:%M:%S')
    for name in table_names:
        formatted_name = f"`{name}`"
        spark.sql(f"RESTORE {formatted_name} TO TIMESTAMP AS OF '{restore_time}'")
    logger.log(logging.ERROR, f'[ERROR] {datetime.datetime.now()} [{PHYSICAL_TASK_NAME}] {LOGICAL_TASK_NAME}処理は次のエラーにより異常終了しました。{str(err)}')
    sys.exit(1)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `catalog_ws_01`.nyctaxi_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS `catalog_ws_01`.nyctaxi_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS `catalog_ws_01`.nyctaxi_gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS `catalog_ws_01`.control;

# COMMAND ----------

table_list = ["catalog_ws_01.nyctaxi_bronze.taxi_raw_records", "catalog_ws_01.control.control_table"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 実行開始

# COMMAND ----------

logger.log(logging.INFO, f'[INFO] {datetime.datetime.now()} [{PHYSICAL_TASK_NAME}] {LOGICAL_TASK_NAME}処理を開始します。')

# COMMAND ----------

dbutils.widgets.text('BATCH_DATE', '2024-10-01T00:00:00+0900')
batch_time = datetime.datetime.strptime(dbutils.widgets.get('BATCH_DATE'), "%Y-%m-%dT%H:%M:%S%z")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Bronze layer
# MAGIC #### Bronze Table: Raw data ingestion
# MAGIC
# MAGIC Here, raw taxi trip data is ingested, with a basic data quality check applied to ensure trip distances are positive.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 連携対象データ抽出

# COMMAND ----------

try:
    bronze_df = spark.sql(
        """
        -- CREATE OR REPLACE TABLE `catalog-wc-01`.nyctaxi_bronze.taxi_raw_records AS
        SELECT *
        FROM samples.nyctaxi.trips
        INNER JOIN catalog_ws_01.control.control_table AS control
            ON control.table_name = 'taxi_raw_records'
        WHERE trip_distance > 0.0  
        AND control.update_time < trips.tpep_pickup_datetime;
        """
    )
except Exception as e:
    error_handling(table_list, batch_time, logger, e)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. データ加工

# COMMAND ----------

try:
    now_time = datetime.datetime.now()
    bronze_df = (
        bronze_df
        .withColumn('pickup_zip', F.col('pickup_zip').cast(T.IntegerType()))
        .withColumn('trip_distance', F.col('trip_distance').cast(T.DoubleType()))
    )
except Exception as e:
    error_handling(table_list, batch_time, logger, e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. データ更新

# COMMAND ----------

try:
    bronze_df.createOrReplaceTempView('bronze_table')
except Exception as e:
    error_handling(table_list, batch_time, logger, e)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO catalog_ws_01.nyctaxi_bronze.taxi_raw_records AS tbl1 
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     bronze_table
# MAGIC ) AS tbl2 
# MAGIC ON (
# MAGIC   tbl1.tpep_pickup_datetime = tbl2.tpep_pickup_datetime
# MAGIC   AND tbl1.tpep_dropoff_datetime = tbl2.tpep_dropoff_datetime
# MAGIC )
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tbl1.trip_distance = tbl2.trip_distance,
# MAGIC     tbl1.fare_amount = tbl2.fare_amount,
# MAGIC     tbl1.pickup_zip = tbl2.pickup_zip,
# MAGIC     tbl1.dropoff_zip = tbl2.dropoff_zip
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     tpep_pickup_datetime,
# MAGIC     tpep_dropoff_datetime,
# MAGIC     trip_distance,
# MAGIC     fare_amount,
# MAGIC     pickup_zip,
# MAGIC     dropoff_zip
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     tbl2.tpep_pickup_datetime,
# MAGIC     tbl2.tpep_dropoff_datetime,
# MAGIC     tbl2.trip_distance,
# MAGIC     tbl2.fare_amount,
# MAGIC     tbl2.pickup_zip,
# MAGIC     tbl2.dropoff_zip
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. 最終更新日更新

# COMMAND ----------

try:
    res = spark.sql('''
        MERGE INTO catalog_ws_01.control.control_table tbl1
        USING(
            SELECT
                NOW() as update_time,
                "taxi_raw_records" as table_name
        ) tbl2
        ON tbl1.table_name = tbl2.table_name
        WHEN MATCHED THEN
        UPDATE SET
            tbl1.update_time = tbl2.update_time
        WHEN NOT MATCHED THEN
        INSERT (
            table_name,
            update_time
        )
        VALUES (
            tbl2.table_name,
            tbl2.update_time
        )
    ''')
except Exception as e:
   error_handling(table_list, batch_time, logger, e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6-1. 正常終了処理

# COMMAND ----------

logger.log(logging.INFO, f'[INFO] {datetime.datetime.now()} [{PHYSICAL_TASK_NAME}] {LOGICAL_TASK_NAME}処理は正常終了しました。')

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
# MAGIC CREATE OR REPLACE TABLE `catalog-ws-01`.nyctaxi_silver.flagged_rides AS
# MAGIC SELECT
# MAGIC   date_trunc("week", tpep_pickup_datetime) AS week,
# MAGIC   pickup_zip AS zip,
# MAGIC   fare_amount,
# MAGIC   trip_distance
# MAGIC FROM
# MAGIC   `catalog-ws-01`.nyctaxi_bronze.taxi_raw_records
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
# MAGIC CREATE OR REPLACE TABLE `catalog-wc-01`.nyctaxi_silver.weekly_stats AS
# MAGIC SELECT
# MAGIC   date_trunc("week", tpep_pickup_datetime) AS week,
# MAGIC   AVG(fare_amount) AS avg_amount,
# MAGIC   AVG(trip_distance) AS avg_distance
# MAGIC FROM
# MAGIC   `catalog-wc-01`.nyctaxi_bronze.taxi_raw_records
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
# MAGIC CREATE OR REPLACE TABLE `catalog-wc-01`.nyctaxi_gold.top_n AS
# MAGIC SELECT
# MAGIC   ws.week,
# MAGIC   ROUND(ws.avg_amount, 2) AS avg_amount,
# MAGIC   ROUND(ws.avg_distance, 3) AS avg_distance,
# MAGIC   fr.fare_amount,
# MAGIC   fr.trip_distance,
# MAGIC   fr.zip
# MAGIC FROM
# MAGIC   `catalog-wc-01`.nyctaxi_silver.flagged_rides fr
# MAGIC LEFT JOIN `catalog-wc-01`.nyctaxi_gold.weekly_stats ws ON ws.week = fr.week
# MAGIC ORDER BY fr.fare_amount DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc("week", tpep_pickup_datetime) AS week,* 
# MAGIC FROM `catalog-wc-01`.nyctaxi_bronze.taxi_raw_records
# MAGIC LIMIT 100
