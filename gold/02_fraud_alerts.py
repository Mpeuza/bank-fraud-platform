# =============================================================
# 02_fraud_alerts.py — Gold Layer: Fraud Alerts Table
# Reads from gold_card_scores, gold_loan_scores
# and gold_customer_risk
# Creates one unified alerts table for Power BI
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid
from shared.config import (
    GOLD_CARD_SCORES,
    GOLD_LOAN_SCORES,
    GOLD_CUSTOMER_RISK,
    GOLD_FRAUD_ALERTS,
    TARGET_CATALOG,
    GOLD_SCHEMA
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import write_delta, assert_not_empty

PIPELINE = "gold_fraud_alerts"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Gold Fraud Alerts") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Fraud alerts generation started")

# =============================================================
# STEP 1 — Read gold scores tables
# =============================================================
log_info(spark, PIPELINE, "Step 1: Reading gold score tables")

try:
    df_card  = spark.read.format("delta").table(GOLD_CARD_SCORES)
    df_loan  = spark.read.format("delta").table(GOLD_LOAN_SCORES)
    df_risk  = spark.read.format("delta").table(GOLD_CUSTOMER_RISK)
    assert_not_empty(df_card, GOLD_CARD_SCORES)
    assert_not_empty(df_loan, GOLD_LOAN_SCORES)
    assert_not_empty(df_risk, GOLD_CUSTOMER_RISK)
    log_info(spark, PIPELINE, f"Step 1: card={df_card.count()}, loan={df_loan.count()}, risk={df_risk.count()}")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Generate card fraud alerts
# Only flag customers with medium or high card risk
# =============================================================
log_info(spark, PIPELINE, "Step 2: Generating card alerts")

try:
    rand_udf = F.udf(lambda: str(uuid.uuid4())[:8].upper())

    df_card_alerts = df_card \
        .filter(F.col("card_risk_tier").isin("medium", "high")) \
        .select(
            rand_udf().alias("alert_id"),
            F.col("customer_id"),
            F.lit("card_transaction").alias("alert_type"),
            F.lit("card").alias("alert_source"),
            F.col("account_id").alias("reference_id"),
            F.col("card_fraud_score").alias("fraud_score"),
            F.col("card_risk_tier").alias("risk_tier"),
            F.col("top_fraud_signal").alias("top_signal"),
            F.lit("open").alias("alert_status"),
            F.lit(None).cast("string").alias("assigned_to"),
            F.current_timestamp().alias("created_at"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
            F.lit(None).cast("string").alias("resolution_note")
        )

    # Also generate account takeover alerts for device anomalies
    df_takeover_alerts = df_card \
        .filter(
            (F.col("device_anomaly_flag") == True) &
            (F.col("card_risk_tier").isin("medium", "high"))
        ) \
        .select(
            rand_udf().alias("alert_id"),
            F.col("customer_id"),
            F.lit("card_takeover").alias("alert_type"),
            F.lit("card").alias("alert_source"),
            F.col("account_id").alias("reference_id"),
            F.col("card_fraud_score").alias("fraud_score"),
            F.col("card_risk_tier").alias("risk_tier"),
            F.lit("unknown_device").alias("top_signal"),
            F.lit("open").alias("alert_status"),
            F.lit(None).cast("string").alias("assigned_to"),
            F.current_timestamp().alias("created_at"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
            F.lit(None).cast("string").alias("resolution_note")
        )

    log_info(spark, PIPELINE, f"Step 2: {df_card_alerts.count()} card alerts, {df_takeover_alerts.count()} takeover alerts")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 3 — Generate loan fraud alerts
# =============================================================
log_info(spark, PIPELINE, "Step 3: Generating loan alerts")

try:
    df_loan_app_alerts = df_loan \
        .filter(F.col("loan_risk_tier").isin("medium", "high")) \
        .select(
            rand_udf().alias("alert_id"),
            F.col("customer_id"),
            F.lit("loan_application").alias("alert_type"),
            F.lit("loan").alias("alert_source"),
            F.col("application_id").alias("reference_id"),
            F.col("loan_fraud_score").alias("fraud_score"),
            F.col("loan_risk_tier").alias("risk_tier"),
            F.col("top_fraud_signal").alias("top_signal"),
            F.lit("open").alias("alert_status"),
            F.lit(None).cast("string").alias("assigned_to"),
            F.current_timestamp().alias("created_at"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
            F.lit(None).cast("string").alias("resolution_note")
        )

    df_loan_rep_alerts = df_loan \
        .filter(
            (F.col("repayment_risk_flag") == True) &
            (F.col("loan_risk_tier").isin("medium", "high"))
        ) \
        .select(
            rand_udf().alias("alert_id"),
            F.col("customer_id"),
            F.lit("loan_repayment").alias("alert_type"),
            F.lit("loan").alias("alert_source"),
            F.col("application_id").alias("reference_id"),
            F.col("loan_fraud_score").alias("fraud_score"),
            F.col("loan_risk_tier").alias("risk_tier"),
            F.lit("repayment_risk").alias("top_signal"),
            F.lit("open").alias("alert_status"),
            F.lit(None).cast("string").alias("assigned_to"),
            F.current_timestamp().alias("created_at"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
            F.lit(None).cast("string").alias("resolution_note")
        )

    log_info(spark, PIPELINE, f"Step 3: {df_loan_app_alerts.count()} loan app alerts, {df_loan_rep_alerts.count()} repayment alerts")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 3 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 4 — Union all alerts into one table
# =============================================================
log_info(spark, PIPELINE, "Step 4: Combining all alerts")

try:
    df_all_alerts = df_card_alerts \
        .union(df_takeover_alerts) \
        .union(df_loan_app_alerts) \
        .union(df_loan_rep_alerts)

    total_alerts = df_all_alerts.count()
    log_info(spark, PIPELINE, f"Step 4: Total alerts generated: {total_alerts}")

    write_delta(df_all_alerts, GOLD_FRAUD_ALERTS, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 4: Written to {GOLD_FRAUD_ALERTS}")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 5 — Preview alerts
# =============================================================
log_info(spark, PIPELINE, "Step 5: Alerts preview")

print("\n── Alerts by type ──")
df_all_alerts.groupBy("alert_type").count().orderBy("count", ascending=False).show()

print("\n── Alerts by risk tier ──")
df_all_alerts.groupBy("risk_tier").count().orderBy("count", ascending=False).show()

print("\n── Alerts by top signal ──")
df_all_alerts.groupBy("top_signal").count().orderBy("count", ascending=False).show()

print("\n── Top 10 highest scoring alerts ──")
df_all_alerts.select(
    "customer_id", "alert_type", "fraud_score",
    "risk_tier", "top_signal"
).orderBy("fraud_score", ascending=False).show(10)

log_info(spark, PIPELINE, "Step 5: Preview complete")

# =============================================================
# SUMMARY
# =============================================================
high_alerts = df_all_alerts.filter(F.col("risk_tier") == "high").count()
print(f"\ngold_fraud_alerts : {total_alerts} total alerts")
print(f"High risk alerts  : {high_alerts}")

print_run_summary(spark, PIPELINE)