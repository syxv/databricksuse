# Databricks notebook source
import sys
import logging
import datetime
import pytz

tokyo = pytz.timezone('Asia/Tokyo')

# ロガーのセットアップ
logger = logging.getLogger('poc-logger')
logger.setLevel(logging.DEBUG)  # DEBUG 以上のログを処理

# ログの伝播を無効化（重複出力を防止）
logger.propagate = False

# stdout 用ハンドラー（DEBUG と INFO レベル）
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)  # DEBUG 以上を受け付け
stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)  # INFO 以下のみ表示

# stderr 用ハンドラー（WARNING 以上）
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.WARNING)  # WARNING 以上を表示

# 共通のフォーマット設定
formatter = logging.Formatter('%(levelname)s: %(message)s')
stdout_handler.setFormatter(formatter)
stderr_handler.setFormatter(formatter)

# 既存のハンドラーをクリアして重複防止
if logger.hasHandlers():
    logger.handlers.clear()

# ハンドラーを追加
logger.addHandler(stdout_handler)
logger.addHandler(stderr_handler)

# テストログの出力
logger.debug("これは DEBUG メッセージです")  # stdout
logger.info("これは INFO メッセージです")  # stdout
logger.warning("これは WARNING メッセージです")  # stderr
logger.error("これは ERROR メッセージです")  # stderr
logger.critical("これは CRITICAL メッセージです")  # stderr

# ビジネスロジック用のログ
PHYSICAL_TASK_NAME = 'poc-bronze-job'
LOGICAL_TASK_NAME = 'bronzeテーブル更新'
logger.info(f'[INFO] {datetime.datetime.now(tokyo)} [{PHYSICAL_TASK_NAME}] {LOGICAL_TASK_NAME}処理は正常終了しました。')


# COMMAND ----------

import sys
import logging
import datetime

# ロガーのセットアップ
logger = logging.getLogger('poc-logger')
logger.setLevel(logging.DEBUG)  # DEBUG 以上のログを処理

# ログの伝播を無効化（重複出力を防止）
logger.propagate = False

# ---- ハンドラーの設定 ----

# 重複するハンドラーを防ぐために、既存のハンドラーを削除
if logger.hasHandlers():
    logger.handlers.clear()  # すべての既存ハンドラーをクリア

# 1. stdout 用ハンドラー（DEBUG と INFO レベル）
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)

# 2. stderr 用ハンドラー（WARNING 以上）
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.WARNING)

# 3. ファイルハンドラー（WARNING 以上をファイルに保存）
file_handler = logging.FileHandler('./log/warning_and_above.log', mode='a')
file_handler.setLevel(logging.WARNING)


# ---- フォーマットの設定 ----
formatter = logging.Formatter('%(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)
stderr_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)


# ---- ハンドラーの追加 ----
logger.addHandler(stdout_handler)
logger.addHandler(stderr_handler)
logger.addHandler(file_handler)

# ---- テストログの出力 ----
logger.debug("これは DEBUG メッセージです")  # stdout
logger.info("これは INFO メッセージです")  # stdout
logger.warning("これは WARNING メッセージです")  # stderr + ファイル
logger.error("これは ERROR メッセージです")  # stderr + ファイル
logger.critical("これは CRITICAL メッセージです")  # stderr + ファイル

# ---- ビジネスロジック用のログ ----
PHYSICAL_TASK_NAME = 'poc-bronze-job'
LOGICAL_TASK_NAME = 'bronzeテーブル更新'
logger.info(f'[INFO] {datetime.datetime.now()} [{PHYSICAL_TASK_NAME}] {LOGICAL_TASK_NAME}処理は正常終了しました。')

