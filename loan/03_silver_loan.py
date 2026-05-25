# =============================================================
# 03_silver_loan.py — Loan Domain Silver Transformation
# Reads from bronze_loan_app and bronze_loan_rep
# Cleans data, casts types, engineers fraud signals
# Writes to silver_loan_app and silver_loan_rep
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from shared.config import (
    BRONZE_LOAN_APP,
    BRONZE_LOAN_REP,
    SILVER_LOAN_APP,
    SILVER_LOAN_REP,
    TARGET_CATALOG,
    LOAN_SILVER_SCHEMA
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import write_delta, assert_not_empty, deduplicate

PIPELINE = "loan_silver"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Loan Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Silver transformation started")

# =============================================================
# SETUP
# =============================================================
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{LOAN_SILVER_SCHEMA}")
    log_info(spark, PIPELINE, f"Schema ready: {TARGET_CATALOG}.{LOAN_SILVER_SCHEMA}")
except Exception as e:
    log_error(spark, PIPELINE, f"Schema creation failed: {str(e)}")
    raise

# =============================================================
# STEP 1 — Read bronze tables
# =============================================================
log_info(spark, PIPELINE, "Step 1: Reading bronze tables")

try:
    df_app = spark.read.format("delta").table(BRONZE_LOAN_APP)
    df_rep = spark.read.format("delta").table(BRONZE_LOAN_REP)
    assert_not_empty(df_app, BRONZE_LOAN_APP)
    assert_not_empty(df_rep, BRONZE_LOAN_REP)
    log_info(spark, PIPELINE, f"Step 1: {df_app.count()} app rows, {df_rep.count()} rep rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Clean and cast silver_loan_app
# =============================================================
log_info(spark, PIPELINE, "Step 2: Cleaning loan applications")

try:
    df_app_silver = df_app \
        .withColumn("application_date",        F.to_date(F.col("application_date"), "yyyy-MM-dd")) \
        .withColumn("loan_amount_requested",    F.col("loan_amount_requested").cast("double")) \
        .withColumn("loan_term_months",         F.col("loan_term_months").cast("int")) \
        .withColumn("stated_income",            F.col("stated_income").cast("double")) \
        .withColumn("stated_years_employed",    F.col("stated_years_employed").cast("int")) \
        .withColumn("num_dependants",           F.col("num_dependants").cast("int")) \
        .withColumn("is_fraud_label",           F.col("is_fraud_label").cast("int")) \
        .withColumn("loan_type",                F.lower(F.trim(F.col("loan_type")))) \
        .withColumn("application_channel",      F.lower(F.trim(F.col("application_channel")))) \
        .withColumn("application_status",       F.lower(F.trim(F.col("application_status")))) \
        .withColumn("stated_employment_status", F.lower(F.trim(F.col("stated_employment_status"))))

    # ── Fraud signal columns ──────────────────────────────────

    # 1. loan_to_income_ratio — high ratio = possible overstatement
    df_app_silver = df_app_silver.withColumn(
        "loan_to_income_ratio",
        F.when(
            F.col("stated_income") > 0,
            F.round(F.col("loan_amount_requested") / F.col("stated_income"), 4)
        ).otherwise(None)
    )

    # 2. is_income_realistic — flag if income seems inflated vs employment
    df_app_silver = df_app_silver.withColumn(
        "is_income_realistic",
        F.when(
            (F.col("stated_employment_status") == "unemployed") &
            (F.col("stated_income") > 50000), False
        ).when(
            (F.col("stated_years_employed") <= 1) &
            (F.col("stated_income") > 100000), False
        ).otherwise(True)
    )

    # 3. applications_last_30d — count apps per customer
    w_cust = Window.partitionBy("customer_id")
    df_app_silver = df_app_silver.withColumn(
        "applications_last_30d",
        F.count("application_id").over(w_cust)
    )

    # 4. id_shared_flag — same ID used by multiple customers
    id_counts = df_app_silver.groupBy("id_number") \
        .agg(F.countDistinct("customer_id").alias("id_cust_count"))
    df_app_silver = df_app_silver.join(id_counts, on="id_number", how="left") \
        .withColumn("id_shared_flag", F.when(F.col("id_cust_count") > 1, True).otherwise(False)) \
        .drop("id_cust_count")

    # 5. address_shared_flag — same address used by multiple customers
    addr_counts = df_app_silver.groupBy("address") \
        .agg(F.countDistinct("customer_id").alias("addr_cust_count"))
    df_app_silver = df_app_silver.join(addr_counts, on="address", how="left") \
        .withColumn("address_shared_flag", F.when(F.col("addr_cust_count") > 1, True).otherwise(False)) \
        .drop("addr_cust_count")

    # 6. agent_approval_rate — agents with very high approval rates
    agent_stats = df_app_silver.groupBy("agent_id").agg(
        (F.sum(F.when(F.col("application_status") == "approved", 1).otherwise(0)) /
         F.count("application_id")).alias("agent_approval_rate")
    )
    df_app_silver = df_app_silver.join(agent_stats, on="agent_id", how="left")

    df_app_silver = deduplicate(df_app_silver, "application_id")
    log_info(spark, PIPELINE, f"Step 2: silver_loan_app ready — {df_app_silver.count()} rows")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 3 — Clean and cast silver_loan_rep
# =============================================================
log_info(spark, PIPELINE, "Step 3: Cleaning loan repayments")

try:
    df_rep_silver = df_rep \
        .withColumn("due_date",           F.to_date(F.col("due_date"), "yyyy-MM-dd")) \
        .withColumn("payment_date",       F.to_date(F.col("payment_date"), "yyyy-MM-dd")) \
        .withColumn("due_amount",         F.col("due_amount").cast("double")) \
        .withColumn("paid_amount",        F.col("paid_amount").cast("double")) \
        .withColumn("days_past_due",      F.col("days_past_due").cast("int")) \
        .withColumn("outstanding_balance",F.col("outstanding_balance").cast("double")) \
        .withColumn("payment_status",     F.lower(F.trim(F.col("payment_status"))))

    # ── Fraud signal columns ──────────────────────────────────

    # 1. payment_shortfall
    df_rep_silver = df_rep_silver.withColumn(
        "payment_shortfall",
        F.round(F.col("due_amount") - F.col("paid_amount"), 2)
    )

    # 2. is_missed, is_partial, is_late flags
    df_rep_silver = df_rep_silver \
        .withColumn("is_missed",  F.when(F.col("payment_status") == "missed",  True).otherwise(False)) \
        .withColumn("is_partial", F.when(F.col("payment_status") == "partial", True).otherwise(False)) \
        .withColumn("is_late",    F.when(F.col("payment_status") == "late",    True).otherwise(False))

    # 3. consecutive_missed_count per customer
    w_rep = Window.partitionBy("customer_id").orderBy("due_date")
    df_rep_silver = df_rep_silver.withColumn(
        "missed_int", F.when(F.col("is_missed"), 1).otherwise(0)
    ).withColumn(
        "consecutive_missed_count",
        F.sum("missed_int").over(w_rep)
    ).drop("missed_int")

    df_rep_silver = deduplicate(df_rep_silver, "repayment_id")
    log_info(spark, PIPELINE, f"Step 3: silver_loan_rep ready — {df_rep_silver.count()} rows")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 3 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 4 — Write to Silver Delta tables
# =============================================================
log_info(spark, PIPELINE, "Step 4: Writing to silver tables")

try:
    write_delta(df_app_silver, SILVER_LOAN_APP, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 4: Written to {SILVER_LOAN_APP}")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED writing silver_loan_app: {str(e)}")
    raise

try:
    write_delta(df_rep_silver, SILVER_LOAN_REP, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 4: Written to {SILVER_LOAN_REP}")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED writing silver_loan_rep: {str(e)}")
    raise

# =============================================================
# STEP 5 — Preview fraud signals
# =============================================================
log_info(spark, PIPELINE, "Step 5: Fraud signal preview")

print("\n── Loan to income ratio distribution ──")
df_app_silver.select(
    F.min("loan_to_income_ratio").alias("min"),
    F.max("loan_to_income_ratio").alias("max"),
    F.avg("loan_to_income_ratio").alias("avg")
).show()

print("\n── Income realistic flag ──")
df_app_silver.groupBy("is_income_realistic").count().show()

print("\n── ID shared flag ──")
df_app_silver.groupBy("id_shared_flag").count().show()

print("\n── Address shared flag ──")
df_app_silver.groupBy("address_shared_flag").count().show()

print("\n── Repayment status breakdown ──")
df_rep_silver.groupBy("payment_status").count().orderBy("count", ascending=False).show()

print("\n── Missed payment distribution ──")
df_rep_silver.groupBy("is_missed").count().show()

log_info(spark, PIPELINE, "Step 5: Preview complete")

# =============================================================
# SUMMARY
# =============================================================
print(f"\nsilver_loan_app : {df_app_silver.count()} rows")
print(f"silver_loan_rep : {df_rep_silver.count()} rows")

print_run_summary(spark, PIPELINE)