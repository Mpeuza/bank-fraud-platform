# =============================================================
# 02_bronze_loan.py — Loan Domain Bronze Validation
# Reads from bronze_loan_app and bronze_loan_rep
# Validates schema, checks for duplicates and nulls
# Logs results to logtable
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shared.config import (
    BRONZE_LOAN_APP,
    BRONZE_LOAN_REP
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import assert_not_empty

PIPELINE = "loan_bronze"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Loan Bronze Validation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Bronze validation started")

# =============================================================
# STEP 1 — Read bronze tables
# =============================================================
log_info(spark, PIPELINE, "Step 1: Reading bronze_loan tables")

try:
    df_app = spark.read.format("delta").table(BRONZE_LOAN_APP)
    df_rep = spark.read.format("delta").table(BRONZE_LOAN_REP)
    assert_not_empty(df_app, BRONZE_LOAN_APP)
    assert_not_empty(df_rep, BRONZE_LOAN_REP)
    log_info(spark, PIPELINE, f"Step 1: bronze_loan_app = {df_app.count()} rows")
    log_info(spark, PIPELINE, f"Step 1: bronze_loan_rep = {df_rep.count()} rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Schema validation
# =============================================================
log_info(spark, PIPELINE, "Step 2: Validating schemas")

EXPECTED_APP_COLS = [
    "application_id", "customer_id", "application_date",
    "loan_type", "loan_amount_requested", "loan_term_months",
    "stated_income", "stated_employment_status", "id_number",
    "agent_id", "application_channel", "application_status",
    "is_fraud_label"
]

EXPECTED_REP_COLS = [
    "repayment_id", "application_id", "customer_id",
    "due_date", "payment_date", "due_amount",
    "paid_amount", "payment_status", "days_past_due",
    "outstanding_balance"
]

missing_app = [c for c in EXPECTED_APP_COLS if c not in df_app.columns]
missing_rep = [c for c in EXPECTED_REP_COLS if c not in df_rep.columns]

if missing_app:
    log_error(spark, PIPELINE, f"Step 2: Missing columns in bronze_loan_app: {missing_app}")
    raise ValueError(f"Schema validation failed: {missing_app}")
else:
    log_info(spark, PIPELINE, "Step 2: bronze_loan_app schema OK")

if missing_rep:
    log_error(spark, PIPELINE, f"Step 2: Missing columns in bronze_loan_rep: {missing_rep}")
    raise ValueError(f"Schema validation failed: {missing_rep}")
else:
    log_info(spark, PIPELINE, "Step 2: bronze_loan_rep schema OK")

# =============================================================
# STEP 3 — Duplicate check
# =============================================================
log_info(spark, PIPELINE, "Step 3: Checking for duplicates")

app_total    = df_app.count()
app_distinct = df_app.select("application_id").distinct().count()
rep_total    = df_rep.count()
rep_distinct = df_rep.select("repayment_id").distinct().count()

app_dupes = app_total - app_distinct
rep_dupes = rep_total - rep_distinct

if app_dupes > 0:
    log_warning(spark, PIPELINE, f"Step 3: {app_dupes} duplicate application_ids in bronze_loan_app")
else:
    log_info(spark, PIPELINE, "Step 3: No duplicate application_ids found")

if rep_dupes > 0:
    log_warning(spark, PIPELINE, f"Step 3: {rep_dupes} duplicate repayment_ids in bronze_loan_rep")
else:
    log_info(spark, PIPELINE, "Step 3: No duplicate repayment_ids found")

# =============================================================
# STEP 4 — Null checks on critical columns
# =============================================================
log_info(spark, PIPELINE, "Step 4: Checking nulls in critical columns")

critical_app_cols = ["application_id", "customer_id", "loan_amount_requested", "stated_income"]
critical_rep_cols = ["repayment_id", "application_id", "customer_id", "due_amount"]

for col in critical_app_cols:
    null_count = df_app.filter(F.col(col).isNull() | (F.col(col) == "")).count()
    if null_count > 0:
        log_warning(spark, PIPELINE, f"Step 4: {null_count} nulls in bronze_loan_app.{col}")
    else:
        log_info(spark, PIPELINE, f"Step 4: No nulls in bronze_loan_app.{col}")

for col in critical_rep_cols:
    null_count = df_rep.filter(F.col(col).isNull() | (F.col(col) == "")).count()
    if null_count > 0:
        log_warning(spark, PIPELINE, f"Step 4: {null_count} nulls in bronze_loan_rep.{col}")
    else:
        log_info(spark, PIPELINE, f"Step 4: No nulls in bronze_loan_rep.{col}")

# =============================================================
# STEP 5 — Stats preview
# =============================================================
log_info(spark, PIPELINE, "Step 5: Bronze stats preview")

print("\n── Loan Applications: Status breakdown ──")
df_app.groupBy("application_status").count().orderBy("count", ascending=False).show()

print("\n── Loan Applications: Type breakdown ──")
df_app.groupBy("loan_type").count().orderBy("count", ascending=False).show()

print("\n── Loan Applications: Channel breakdown ──")
df_app.groupBy("application_channel").count().orderBy("count", ascending=False).show()

print("\n── Loan Applications: Fraud label breakdown ──")
df_app.groupBy("is_fraud_label").count().show()

print("\n── Loan Repayments: Payment status breakdown ──")
df_rep.groupBy("payment_status").count().orderBy("count", ascending=False).show()

log_info(spark, PIPELINE, "Step 5: Stats preview complete")

# =============================================================
# SUMMARY
# =============================================================
print(f"\nbronze_loan_app : {app_total} rows | {app_dupes} duplicates")
print(f"bronze_loan_rep : {rep_total} rows | {rep_dupes} duplicates")

print_run_summary(spark, PIPELINE)