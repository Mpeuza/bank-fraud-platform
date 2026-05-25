# =============================================================
# 04_features_loan.py — Loan Domain Feature Engineering
# Reads from silver_loan_app and silver_loan_rep
# Builds one row per customer with all fraud signals
# Writes to silver_loan_features
# This table feeds the ML model and Power BI dashboard
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shared.config import (
    SILVER_LOAN_APP,
    SILVER_LOAN_REP,
    SILVER_LOAN_FEATURES,
    TARGET_CATALOG,
    LOAN_SILVER_SCHEMA
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import write_delta, assert_not_empty, assign_risk_tier

PIPELINE = "loan_features"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Loan Features") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Loan feature engineering started")

# =============================================================
# STEP 1 — Read silver tables
# =============================================================
log_info(spark, PIPELINE, "Step 1: Reading silver tables")

try:
    df_app = spark.read.format("delta").table(SILVER_LOAN_APP)
    df_rep = spark.read.format("delta").table(SILVER_LOAN_REP)
    assert_not_empty(df_app, SILVER_LOAN_APP)
    assert_not_empty(df_rep, SILVER_LOAN_REP)
    log_info(spark, PIPELINE, f"Step 1: {df_app.count()} app rows, {df_rep.count()} rep rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Application-level features per customer
# =============================================================
log_info(spark, PIPELINE, "Step 2: Computing application features")

try:
    app_features = df_app.groupBy("customer_id").agg(
        F.count("application_id").alias("applications_last_30d"),
        F.countDistinct("branch_id").alias("distinct_branches_30d"),
        F.max("loan_to_income_ratio").alias("loan_to_income_ratio"),
        F.max("id_shared_flag").alias("id_shared_flag"),
        F.max("address_shared_flag").alias("address_shared_flag"),
        F.max("agent_approval_rate").alias("agent_approval_rate"),
        F.max("is_income_realistic").alias("income_realistic_flag"),
        F.first("application_id").alias("application_id")
    ).withColumn(
        "agent_risk_flag",
        F.when(F.col("agent_approval_rate") >= 0.9, True).otherwise(False)
    )

    log_info(spark, PIPELINE, "Step 2: Application features computed")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 3 — Repayment-level features per customer
# =============================================================
log_info(spark, PIPELINE, "Step 3: Computing repayment features")

try:
    rep_features = df_rep.groupBy("customer_id").agg(
        F.sum(F.when(F.col("is_missed"), 1).otherwise(0)).alias("missed_payment_count"),
        F.max("consecutive_missed_count").alias("consecutive_missed_count"),
        F.avg(F.when(F.col("due_amount") > 0,
            F.col("paid_amount") / F.col("due_amount")
        )).alias("avg_payment_ratio"),
        F.avg("days_past_due").alias("avg_days_past_due"),
        F.sum("outstanding_balance").alias("total_outstanding_balance"),
        (F.sum(F.when(F.col("is_partial"), 1).otherwise(0)) /
         F.count("repayment_id")).alias("partial_payment_ratio")
    )

    log_info(spark, PIPELINE, "Step 3: Repayment features computed")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 3 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 4 — Join all features
# =============================================================
log_info(spark, PIPELINE, "Step 4: Joining all features")

try:
    df_features = app_features.join(rep_features, on="customer_id", how="left")

    # Fill nulls
    numeric_cols = [
        "applications_last_30d", "distinct_branches_30d",
        "loan_to_income_ratio", "agent_approval_rate",
        "missed_payment_count", "consecutive_missed_count",
        "avg_payment_ratio", "avg_days_past_due",
        "total_outstanding_balance", "partial_payment_ratio"
    ]
    df_features = df_features.fillna(0, subset=numeric_cols)
    df_features = df_features.fillna(False, subset=[
        "id_shared_flag", "address_shared_flag",
        "agent_risk_flag", "income_realistic_flag"
    ])

    log_info(spark, PIPELINE, f"Step 4: Features joined — {df_features.count()} customer rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 5 — Compute loan fraud score
# =============================================================
log_info(spark, PIPELINE,