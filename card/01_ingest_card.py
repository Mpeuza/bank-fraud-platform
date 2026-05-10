# =============================================================
# 01_ingest_card.py — Card Domain Ingestion
# Reads card_transactions.csv and card_accounts.csv
# Writes raw data to Bronze Delta tables
# Logs every step to the central logtable
# =============================================================

# ── What is the same across environments ─────────────────────
# Code lives in GitHub repo
# Shared config, logger, utils imported from shared/

# ── What is different per environment ────────────────────────
# TARGET_CATALOG : test or prod
# TARGET_SCHEMA  : bronze_card
# File paths     : data/sample/ for local dev

# =============================================================
# IMPORTS
# =============================================================
import sys
import os

# Add project root to path so shared/ imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from shared.config import (
    BRONZE_CARD_TXN,
    BRONZE_CARD_ACC,
    CARD_TXN_FILE,
    CARD_ACC_FILE,
    TARGET_CATALOG,
    CARD_BRONZE_SCHEMA,
    LOGTABLE
)
from shared.logger import (
    create_logtable_if_not_exists,
    log_info,
    log_warning,
    log_error,
    print_run_summary
)
from shared.utils import (
    read_csv,
    add_ingestion_metadata,
    write_delta,
    assert_not_empty
)

# =============================================================
# PIPELINE NAME — used in all log entries
# =============================================================
PIPELINE = "card_ingest"

# =============================================================
# START SPARK SESSION
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Card Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================
# SETUP — Create catalog, schema, logtable if not exists
# =============================================================
log_info(spark, PIPELINE, "Pipeline started")

try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{CARD_BRONZE_SCHEMA}")
    log_info(spark, PIPELINE, f"Schema ready: {TARGET_CATALOG}.{CARD_BRONZE_SCHEMA}")
except Exception as e:
    log_error(spark, PIPELINE, f"Schema creation failed: {str(e)}")
    raise

# =============================================================
# STEP 1 — Ingest card_transactions.csv → bronze_card_txn
# =============================================================
log_info(spark, PIPELINE, f"Step 1: Reading {CARD_TXN_FILE}")

try:
    df_txn = read_csv(spark, CARD_TXN_FILE)
    assert_not_empty(df_txn, "card_transactions.csv")
    log_info(spark, PIPELINE, f"Step 1: Loaded {df_txn.count()} rows from card_transactions.csv")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED - could not read card_transactions.csv: {str(e)}")
    raise

# Add ingestion metadata
df_txn = add_ingestion_metadata(df_txn, "card_transactions.csv")

# Write to Bronze
try:
    write_delta(df_txn, BRONZE_CARD_TXN, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 1: Written to {BRONZE_CARD_TXN} successfully")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED - could not write to {BRONZE_CARD_TXN}: {str(e)}")
    raise

# =============================================================
# STEP 2 — Ingest card_accounts.csv → bronze_card_acc
# =============================================================
log_info(spark, PIPELINE, f"Step 2: Reading {CARD_ACC_FILE}")

try:
    df_acc = read_csv(spark, CARD_ACC_FILE)
    assert_not_empty(df_acc, "card_accounts.csv")
    log_info(spark, PIPELINE, f"Step 2: Loaded {df_acc.count()} rows from card_accounts.csv")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED - could not read card_accounts.csv: {str(e)}")
    raise

# Add ingestion metadata
df_acc = add_ingestion_metadata(df_acc, "card_accounts.csv")

# Write to Bronze
try:
    write_delta(df_acc, BRONZE_CARD_ACC, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 2: Written to {BRONZE_CARD_ACC} successfully")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED - could not write to {BRONZE_CARD_ACC}: {str(e)}")
    raise

# =============================================================
# STEP 3 — Basic data quality checks
# =============================================================
log_info(spark, PIPELINE, "Step 3: Running data quality checks")

# Check for nulls in critical columns
txn_nulls = df_txn.filter(df_txn["transaction_id"].isNull()).count()
acc_nulls  = df_acc.filter(df_acc["account_id"].isNull()).count()

if txn_nulls > 0:
    log_warning(spark, PIPELINE, f"Step 3: {txn_nulls} null transaction_ids found in bronze_card_txn")
else:
    log_info(spark, PIPELINE, "Step 3: No null transaction_ids - quality check passed")

if acc_nulls > 0:
    log_warning(spark, PIPELINE, f"Step 3: {acc_nulls} null account_ids found in bronze_card_acc")
else:
    log_info(spark, PIPELINE, "Step 3: No null account_ids - quality check passed")

# =============================================================
# STEP 4 — Print row counts as confirmation
# =============================================================
log_info(spark, PIPELINE, "Step 4: Final row count verification")

txn_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_CARD_TXN}").collect()[0]["cnt"]
acc_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_CARD_ACC}").collect()[0]["cnt"]

log_info(spark, PIPELINE, f"bronze_card_txn row count: {txn_count}")
log_info(spark, PIPELINE, f"bronze_card_acc row count: {acc_count}")

print(f"\nbronze_card_txn : {txn_count} rows")
print(f"bronze_card_acc : {acc_count} rows")

# =============================================================
# SUMMARY — Print run summary and fail if errors found
# =============================================================
print_run_summary(spark, PIPELINE)