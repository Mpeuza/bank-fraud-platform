# =============================================================
# 03_export_powerbi.py — Gold Layer: Power BI Export
# Reads from gold_customer_risk and gold_fraud_alerts
# Exports clean CSV files ready for Power BI Desktop
# Output goes to data/powerbi/ folder
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shared.config import (
    GOLD_CUSTOMER_RISK,
    GOLD_FRAUD_ALERTS,
    GOLD_CARD_SCORES,
    GOLD_LOAN_SCORES
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import assert_not_empty

PIPELINE    = "export_powerbi"
OUTPUT_PATH = "data/powerbi/"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Power BI Export") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Power BI export started")

# =============================================================
# SETUP — Create output folder
# =============================================================
os.makedirs(OUTPUT_PATH, exist_ok=True)
log_info(spark, PIPELINE, f"Output folder ready: {OUTPUT_PATH}")

# =============================================================
# HELPER — Write single CSV file (not Spark partitions)
# =============================================================
def export_csv(df, filename):
    path = os.path.join(OUTPUT_PATH, filename)
    df.toPandas().to_csv(path, index=False)
    print(f"Exported: {path} ({df.count()} rows)")
    return path

# =============================================================
# STEP 1 — Export customer risk table
# Main table for Power BI — one row per customer
# =============================================================
log_info(spark, PIPELINE, "Step 1: Exporting gold_customer_risk")

try:
    df_risk = spark.read.format("delta").table(GOLD_CUSTOMER_RISK)
    assert_not_empty(df_risk, GOLD_CUSTOMER_RISK)

    df_risk_export = df_risk.select(
        "customer_id",
        "full_name",
        "country",
        "city",
        "age",
        "account_status",
        "employment_status",
        "stated_income",
        F.round("card_fraud_score", 4).alias("card_fraud_score"),
        F.round("loan_fraud_score", 4).alias("loan_fraud_score"),
        F.round("overall_risk_score", 4).alias("overall_risk_score"),
        "overall_risk_tier",
        "card_risk_tier",
        "loan_risk_tier",
        "flagged_in_card",
        "flagged_in_loan",
        "flagged_in_both",
        "risk_updated_at"
    )

    export_csv(df_risk_export, "customer_risk.csv")
    log_info(spark, PIPELINE, "Step 1: customer_risk.csv exported")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Export fraud alerts table
# =============================================================
log_info(spark, PIPELINE, "Step 2: Exporting gold_fraud_alerts")

try:
    df_alerts = spark.read.format("delta").table(GOLD_FRAUD_ALERTS)
    assert_not_empty(df_alerts, GOLD_FRAUD_ALERTS)

    df_alerts_export = df_alerts.select(
        "alert_id",
        "customer_id",
        "alert_type",
        "alert_source",
        "reference_id",
        F.round("fraud_score", 4).alias("fraud_score"),
        "risk_tier",
        "top_signal",
        "alert_status",
        "created_at"
    )

    export_csv(df_alerts_export, "fraud_alerts.csv")
    log_info(spark, PIPELINE, "Step 2: fraud_alerts.csv exported")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 3 — Export card scores summary
# =============================================================
log_info(spark, PIPELINE, "Step 3: Exporting card scores summary")

try:
    df_card = spark.read.format("delta").table(GOLD_CARD_SCORES)

    df_card_export = df_card.select(
        "customer_id",
        F.round("card_fraud_score", 4).alias("card_fraud_score"),
        "card_risk_tier",
        "top_fraud_signal",
        "velocity_flag",
        "geo_anomaly_flag",
        "device_anomaly_flag",
        "declined_txn_flag",
        "score_updated_at"
    )

    export_csv(df_card_export, "card_scores.csv")
    log_info(spark, PIPELINE, "Step 3: card_scores.csv exported")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 3 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 4 — Export loan scores summary
# =============================================================
log_info(spark, PIPELINE, "Step 4: Exporting loan scores summary")

try:
    df_loan = spark.read.format("delta").table(GOLD_LOAN_SCORES)

    df_loan_export = df_loan.select(
        "customer_id",
        F.round("loan_fraud_score", 4).alias("loan_fraud_score"),
        "loan_risk_tier",
        "top_fraud_signal",
        "income_fraud_flag",
        "identity_fraud_flag",
        "loan_stacking_flag",
        "agent_risk_flag",
        "repayment_risk_flag",
        "score_updated_at"
    )

    export_csv(df_loan_export, "loan_scores.csv")
    log_info(spark, PIPELINE, "Step 4: loan_scores.csv exported")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 5 — Export KPI summary for Power BI dashboard cards
# =============================================================
log_info(spark, PIPELINE, "Step 5: Exporting KPI summary")

try:
    df_risk_reload   = spark.read.format("delta").table(GOLD_CUSTOMER_RISK)
    df_alerts_reload = spark.read.format("delta").table(GOLD_FRAUD_ALERTS)

    total_customers  = df_risk_reload.count()
    high_risk        = df_risk_reload.filter(F.col("overall_risk_tier") == "high").count()
    flagged_both     = df_risk_reload.filter(F.col("flagged_in_both") == True).count()
    total_alerts     = df_alerts_reload.count()
    open_alerts      = df_alerts_reload.filter(F.col("alert_status") == "open").count()
    card_alerts      = df_alerts_reload.filter(F.col("alert_source") == "card").count()
    loan_alerts      = df_alerts_reload.filter(F.col("alert_source") == "loan").count()

    kpi_data = [{
        "total_customers":   total_customers,
        "high_risk_customers": high_risk,
        "flagged_in_both":   flagged_both,
        "total_alerts":      total_alerts,
        "open_alerts":       open_alerts,
        "card_alerts":       card_alerts,
        "loan_alerts":       loan_alerts,
        "high_risk_pct":     round(high_risk / total_customers * 100, 2)
    }]

    import pandas as pd
    pd.DataFrame(kpi_data).to_csv(os.path.join(OUTPUT_PATH, "kpi_summary.csv"), index=False)
    print(f"Exported: {OUTPUT_PATH}kpi_summary.csv")
    log_info(spark, PIPELINE, "Step 5: kpi_summary.csv exported")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 5 FAILED: {str(e)}")
    raise

# =============================================================
# SUMMARY
# =============================================================
print("\n── Power BI Export Complete ──")
print(f"Output folder : {OUTPUT_PATH}")
print(f"Files exported:")
print(f"  customer_risk.csv   — main customer risk table")
print(f"  fraud_alerts.csv    — all fraud alerts")
print(f"  card_scores.csv     — card fraud scores")
print(f"  loan_scores.csv     — loan fraud scores")
print(f"  kpi_summary.csv     — dashboard KPI cards")
print(f"\nOpen Power BI Desktop and connect to: {os.path.abspath(OUTPUT_PATH)}")

print_run_summary(spark, PIPELINE)