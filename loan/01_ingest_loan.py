# =============================================================
# 01_ingest_loan.py — Loan Domain Ingestion
# Reads loan_applications.csv and loan_repayments.csv
# Writes raw data to Bronze Delta tables
# Logs every step to the central logtable
# =============================================================

# ── What is the same across environments ─────────────────────
# Code lives in GitHub repo
# Shared config, logger, utils imported from shared/

# ── What is different per environment ────────────────────────
# TARGET_CATALOG : test or prod
# TARGET_SCHEMA  : bronze_loan
# File paths     : data/sample/ for local dev

# =============================================================
# IMPORTS
# =============================================================
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from shared.config import (
    BRONZE_LOAN_APP,
    BRONZE_LOAN_REP,
    LOAN_APP_FILE,
    LOAN_REP_FILE,
    TARGET_CATALOG,
    LOAN_BRONZE_SCHEMA,
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

PIPELINE = "loan_ingest"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Loan Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Pipeline started")

# =============================================================
# SETUP — Create catalog, schema, logtable if not exists
# =============================================================
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{LOAN_BRONZE_SCHEMA}")
    log_info(spark, PIPELINE, f"Schema ready: {TARGET_CATALOG}.{LOAN_BRONZE_SCHEMA}")
except Exception as e:
    log_error(spark, PIPELINE, f"Schema creation failed: {str(e)}")
    raise

# =============================================================
# STEP 1 — Ingest loan_applications.csv → bronze_loan_app
# =============================================================
log_info(spark, PIPELINE, f"Step 1: Reading {LOAN_APP_FILE}")

try:
    df_app = read_csv(spark, LOAN_APP_FILE)
    assert_not_empty(df_app, "loan_applications.csv")
    log_info(spark, PIPELINE, f"Step 1: Loaded {df_app.count()} rows from loan_applications.csv")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED - could not read loan_applications.csv: {str(e)}")
    raise

# Add ingestion metadata
df_app = add_ingestion_metadata(df_app, "loan_applications.csv")

# Write to Bronze
try:
    write_delta(df_app, BRONZE_LOAN_APP, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 1: Written to {BRONZE_LOAN_APP} successfully")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED - could not write to {BRONZE_LOAN_APP}: {str(e)}")
    raise

# =============================================================
# STEP 2 — Ingest loan_repayments.csv → bronze_loan_rep
# =============================================================
log_info(spark, PIPELINE, f"Step 2: Reading {LOAN_REP_FILE}")

try:
    df_rep = read_csv(spark, LOAN_REP_FILE)
    assert_not_empty(df_rep, "loan_repayments.csv")
    log_info(spark, PIPELINE, f"Step 2: Loaded {df_rep.count()} rows from loan_repayments.csv")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED - could not read loan_repayments.csv: {str(e)}")
    raise

# Add ingestion metadata
df_rep = add_ingestion_metadata(df_rep, "loan_repayments.csv")

# Write to Bronze
try:
    write_delta(df_rep, BRONZE_LOAN_REP, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 2: Written to {BRONZE_LOAN_REP} successfully")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED - could not write to {BRONZE_LOAN_REP}: {str(e)}")
    raise

# =============================================================
# STEP 3 — Basic data quality checks
# =============================================================
log_info(spark, PIPELINE, "Step 3: Running data quality checks")

app_nulls = df_app.filter(df_app["application_id"].isNull()).count()
rep_nulls = df_rep.filter(df_rep["repayment_id"].isNull()).count()

if app_nulls > 0:
    log_warning(spark, PIPELINE, f"Step 3: {app_nulls} null application_ids in bronze_loan_app")
else:
    log_info(spark, PIPELINE, "Step 3: No null application_ids - quality check passed")

if rep_nulls > 0:
    log_warning(spark, PIPELINE, f"Step 3: {rep_nulls} null repayment_ids in bronze_loan_rep")
else:
    log_info(spark, PIPELINE, "Step 3: No null repayment_ids - quality check passed")

# =============================================================
# STEP 4 — Row count verification
# =============================================================
log_info(spark, PIPELINE, "Step 4: Final row count verification")

app_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_LOAN_APP}").collect()[0]["cnt"]
rep_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_LOAN_REP}").collect()[0]["cnt"]

log_info(spark, PIPELINE, f"bronze_loan_app row count: {app_count}")
log_info(spark, PIPELINE, f"bronze_loan_rep row count: {rep_count}")

print(f"\nbronze_loan_app : {app_count} rows")
print(f"bronze_loan_rep : {rep_count} rows")

# =============================================================
# SUMMARY
# =============================================================
print_run_summary(spark, PIPELINE)