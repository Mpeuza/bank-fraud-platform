# =============================================================
# 02_bronze_card.py — Card Domain Bronze Validation
# Reads from bronze_card_txn and bronze_card_acc
# Validates schema, checks for duplicates and nulls
# Logs results to logtable
# NOTE: Bronze = raw as-is. No transformations here.
#       We only VALIDATE what was loaded in step 01.
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shared.config import (
    BRONZE_CARD_TXN,
    BRONZE_CARD_ACC,
    TARGET_CATALOG,
    CARD_BRONZE_SCHEMA
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import assert_not_empty

PIPELINE = "card_bronze"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Card Bronze Validation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Bronze validation started")

# =============================================================
# STEP 1 — Read bronze tables
# =============================================================
log_info(spark, PIPELINE, f"Step 1: Reading {BRONZE_CARD_TXN}")

try:
    df_txn = spark.read.format("delta").table(BRONZE_CARD_TXN)
    df_acc = spark.read.format("delta").table(BRONZE_CARD_ACC)
    assert_not_empty(df_txn, BRONZE_CARD_TXN)
    assert_not_empty(df_acc, BRONZE_CARD_ACC)
    log_info(spark, PIPELINE, f"Step 1: bronze_card_txn = {df_txn.count()} rows")
    log_info(spark, PIPELINE, f"Step 1: bronze_card_acc = {df_acc.count()} rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED - could not read bronze tables: {str(e)}")
    raise

# =============================================================
# STEP 2 — Schema validation
# Check all expected columns exist
# =============================================================
log_info(spark, PIPELINE, "Step 2: Validating schemas")

EXPECTED_TXN_COLS = [
    "transaction_id", "account_id", "customer_id",
    "transaction_date", "transaction_time", "amount",
    "currency", "merchant_name", "merchant_category_code",
    "merchant_country", "channel", "status", "is_fraud_label"
]

EXPECTED_ACC_COLS = [
    "account_id", "customer_id", "card_type",
    "card_tier", "account_status", "credit_limit",
    "billing_country", "billing_city"
]

txn_cols = df_txn.columns
acc_cols  = df_acc.columns

missing_txn = [c for c in EXPECTED_TXN_COLS if c not in txn_cols]
missing_acc = [c for c in EXPECTED_ACC_COLS if c not in acc_cols]

if missing_txn:
    log_error(spark, PIPELINE, f"Step 2: Missing columns in bronze_card_txn: {missing_txn}")
    raise ValueError(f"Schema validation failed - missing: {missing_txn}")
else:
    log_info(spark, PIPELINE, "Step 2: bronze_card_txn schema OK")

if missing_acc:
    log_error(spark, PIPELINE, f"Step 2: Missing columns in bronze_card_acc: {missing_acc}")
    raise ValueError(f"Schema validation failed - missing: {missing_acc}")
else:
    log_info(spark, PIPELINE, "Step 2: bronze_card_acc schema OK")

# =============================================================
# STEP 3 — Duplicate check
# Flag if duplicate transaction_ids or account_ids exist
# =============================================================
log_info(spark, PIPELINE, "Step 3: Checking for duplicates")

txn_total    = df_txn.count()
txn_distinct = df_txn.select("transaction_id").distinct().count()
acc_total    = df_acc.count()
acc_distinct = df_acc.select("account_id").distinct().count()

txn_dupes = txn_total - txn_distinct
acc_dupes = acc_total - acc_distinct

if txn_dupes > 0:
    log_warning(spark, PIPELINE, f"Step 3: {txn_dupes} duplicate transaction_ids found in bronze_card_txn")
else:
    log_info(spark, PIPELINE, "Step 3: No duplicate transaction_ids found")

if acc_dupes > 0:
    log_warning(spark, PIPELINE, f"Step 3: {acc_dupes} duplicate account_ids found in bronze_card_acc")
else:
    log_info(spark, PIPELINE, "Step 3: No duplicate account_ids found")

# =============================================================
# STEP 4 — Null checks on critical columns
# =============================================================
log_info(spark, PIPELINE, "Step 4: Checking nulls in critical columns")

critical_txn_cols = ["transaction_id", "account_id", "customer_id", "amount", "transaction_date"]
critical_acc_cols = ["account_id", "customer_id", "credit_limit"]

for col in critical_txn_cols:
    null_count = df_txn.filter(F.col(col).isNull() | (F.col(col) == "")).count()
    if null_count > 0:
        log_warning(spark, PIPELINE, f"Step 4: {null_count} nulls in bronze_card_txn.{col}")
    else:
        log_info(spark, PIPELINE, f"Step 4: No nulls in bronze_card_txn.{col}")

for col in critical_acc_cols:
    null_count = df_acc.filter(F.col(col).isNull() | (F.col(col) == "")).count()
    if null_count > 0:
        log_warning(spark, PIPELINE, f"Step 4: {null_count} nulls in bronze_card_acc.{col}")
    else:
        log_info(spark, PIPELINE, f"Step 4: No nulls in bronze_card_acc.{col}")

# =============================================================
# STEP 5 — Basic stats preview
# Print quick summary of what's in the bronze table
# =============================================================
log_info(spark, PIPELINE, "Step 5: Bronze stats preview")

print("\n── Card Transactions: Status breakdown ──")
df_txn.groupBy("status").count().orderBy("count", ascending=False).show()

print("\n── Card Transactions: Channel breakdown ──")
df_txn.groupBy("channel").count().orderBy("count", ascending=False).show()

print("\n── Card Transactions: Fraud label breakdown ──")
df_txn.groupBy("is_fraud_label").count().show()

print("\n── Card Accounts: Card type breakdown ──")
df_acc.groupBy("card_type").count().show()

log_info(spark, PIPELINE, "Step 5: Stats preview complete")

# =============================================================
# SUMMARY
# =============================================================
print(f"\nbronze_card_txn : {txn_total} rows | {txn_dupes} duplicates")
print(f"bronze_card_acc : {acc_total} rows | {acc_dupes} duplicates")

print_run_summary(spark, PIPELINE)