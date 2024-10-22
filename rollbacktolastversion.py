# Databricks notebook source
# Deltaテーブルの更新処理とロールバック処理の実装

# 処理開始前の最新の更新時刻を取得
latest_version_time = spark.sql("""
    DESCRIBE HISTORY orders_w1 LIMIT 1;
""").collect()[0]["timestamp"]

try:
    # orders_w1テーブルのO_TOTALPRICEを2倍に更新
    spark.sql("""
        UPDATE orders_w1
        SET O_TOTALPRICE = O_TOTALPRICE * 2
        WHERE O_ORDERKEY = 1
    """)
    # 更新が成功した場合のメッセージ
    print('更新が成功しました。')

except Exception as e:
    # エラーが発生した場合の処理
    print('エラーが発生しました。ロールバックを実行します。')
    print(e)
    # テーブルを処理開始前の最新の状態に復元
    spark.sql(f"""
        RESTORE TABLE orders_w1 TO TIMESTAMP AS OF '{latest_version_time}'
    """)

# 注意: このコードはDatabricks環境でのみ動作します。
# RESTORE TABLEコマンドはDelta Lakeのバージョン管理機能を利用しています。


# COMMAND ----------

# MAGIC %md
# MAGIC Databricksでは、以下の手順でトランザクションの実装を実施しています。
# MAGIC - 
# MAGIC - 処理開始直前に該当テーブルの最終更新時刻をで取得します。
# MAGIC - spark.sql()を使用して該当テーブルの更新処理を実行します。
# MAGIC - 更新処理中にエラーが発生した場合、exceptブロックが実行され、RESTORE TABLE文を使用してテーブルを処理開始直前の状態に復元します。

# COMMAND ----------

formatted_name="catalog_ws_01.nyctaxi_bronze.taxi_raw_records"
#res=spark.sql(f"DESCRIBE HISTORY {formatted_name}").limit(1)
res=spark.sql(f"DESCRIBE HISTORY {formatted_name}")
display(res)

# COMMAND ----------

from datetime import datetime
import logging
from typing import List

def restore_to_latest(table_names: List[str], logger: logging.Logger) -> None:
    """
    指定されたテーブルを最新のバージョンに復元します。
    """
    for name in table_names:
        # テーブルの最新バージョンのタイムスタンプを取得
        history = spark.sql(f"DESCRIBE HISTORY {name}").limit(1).collect()
        if not history:
            logger.error(f"[ERROR] テーブル {name} には利用可能な履歴がありません。復元できません。")
            dbutils.notebook.exit(f"ノートブック停止：テーブル {name} には利用可能な履歴がありません。")

        latest_time = history[0]['timestamp']
        restore_time = latest_time.strftime('%Y-%m-%d %H:%M:%S')

        # 復元操作を実行
        spark.sql(f"RESTORE {name} TO TIMESTAMP AS OF '{restore_time}'")
        logger.info(f"[INFO] テーブル {name} を最新の時刻 {restore_time} に復元しました。")

def error_handling(table_names: List[str], logger: logging.Logger, err: Exception) -> None:
    """
    エラー発生時にテーブルを復元し、エラーメッセージを記録します。
    復元が成功した場合は、例外を再度スローしません。
    """
    try:
        restore_to_latest(table_names, logger)
    except Exception as restore_err:
        # 復元に失敗した場合、エラーログを記録し、ノートブックを停止
        logger.error(f"[ERROR] バッチ処理がエラー終了し、復元も失敗しました：{str(restore_err)}")
        dbutils.notebook.exit(f"ノートブック停止：エラー {str(restore_err)}")

    # 復元が成功した場合、元のエラーを記録してノートブックを停止
    logger.error(f"[ERROR] バッチ処理がエラー終了しました：{str(err)}")
    dbutils.notebook.exit(f"ノートブック停止：エラー {str(err)}")

# 使用例
logger = logging.getLogger('poc-logger')
logger.setLevel(logging.INFO)

table_list = ["catalog_ws_01.nyctaxi_bronze.taxi_raw_records", "catalog_ws_01.control.control_table"]

try:
    # エラーをシミュレーション
    raise ValueError("テストエラー")
except Exception as e:
    error_handling(table_list, logger, e)


# COMMAND ----------

import datetime
import logging
import sys
from typing import List

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

# 使用例
logger = logging.getLogger('poc-logger')
logger.setLevel(logging.INFO)

table_list = ["catalog_ws_01.nyctaxi_bronze.taxi_raw_records", "catalog_ws_01.control.control_table"]

try:
    # エラーをシミュレーション
    raise ValueError("テストエラー")
except Exception as e:
    error_handling(table_list, datetime.datetime.now(), logger, e)

