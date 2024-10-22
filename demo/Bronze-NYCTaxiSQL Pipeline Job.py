# Databricks notebook source
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

spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")

# COMMAND ----------

## 定数定義
PHYSICAL_TASK_NAME = 'poc-bronze-job'
LOGICAL_TASK_NAME = 'bronzeテーブル更新'

# COMMAND ----------

logger = logging.getLogger('poc-logger')
logger.setLevel(logging.INFO)

# COMMAND ----------

## エラーハンドリング用関数定義
def error_handling(table_names: List[str], batch_time: datetime.datetime, logger: logging.Logger, err: Exception) -> None:
    """
    エラー発生時にテーブルを復元し、ログを記録してNotebookを停止します。
    """
    restore_time = batch_time.strftime('%Y-%m-%d %H:%M:%S')

    for name in table_names:
        try:
            formatted_name = f"{name}"
            # 指定時間への復元を試行
            logger.info(f"[INFO] テーブル {name} を {restore_time} に復元しました。")
        except Exception as restore_err:
            # 最新バージョンへの復元を試行
            logger.warning(f"[WARNING] {restore_time} への復元失敗：{restore_err}")
            latest_time = get_latest_timestamp(name)
            spark.sql(f"RESTORE {formatted_name}  TO TIMESTAMP AS OF '{latest_time}'")
            logger.info(f"[INFO] テーブル {name} を最新の時刻 {latest_time} に復元しました。")

    # エラーログを記録し、Notebookを停止
    logger.error(f"[ERROR] バッチ処理中にエラーが発生しました：{err}")
    dbutils.notebook.exit(f"[ERROR] バッチ処理中にエラーが発生しました：{err}")  # Notebook停止

def get_latest_timestamp(table_name: str) -> str:
    """
    テーブルの最新バージョンのタイムスタンプを取得します。
    """
    latest_time = spark.sql(f"DESCRIBE HISTORY `{table_name}`").limit(1).collect()[0]['timestamp']
    return latest_time.strftime('%Y-%m-%d %H:%M:%S')

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
# MAGIC ここでは、生のタクシー旅行データが取り込まれ、旅行距離が正であることを確認するために基本的なデータ品質チェックが適用される。

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
        MERGE INTO catalog_ws_01.control.control_table AS tbl1
        USING(
            SELECT
                NOW() as update_time,
                "taxi_raw_records" as table_name
        ) AS tbl2
        ON tbl1.table_name = tbl2.table_name
        WHEN MATCHED THEN
        UPDATE SET
            tbl1.update_time = tbl2.update_time
        WHEN NOT MATCHED THEN
        INSERT(
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

logger.info(f'[INFO] {datetime.datetime.now()} [{PHYSICAL_TASK_NAME}] {LOGICAL_TASK_NAME}処理は正常終了しました。')

