$content = @"
# =============================================================
# logger.py — Shared logging for bank fraud platform
# Every notebook (card, loan, gold) calls this to log events
# Writes to the central logtable — same concept as Task 176
# =============================================================

from datetime import datetime
import traceback

# ── Log levels ────────────────────────────────────────────────
INFO    = "INFO"
WARNING = "WARNING"
ERROR   = "ERROR"

# =============================================================
# MAIN LOG FUNCTION
# Call this from any notebook like:
#   from shared.logger import log
#   log(spark, "card", INFO, "Bronze load started")
# =============================================================

def log(spark, pipeline: str, level: str, message: str, logtable: str = None):
    """
    Write a log entry to the central logtable.

    Args:
        spark    : Active SparkSession
        pipeline : Which pipeline is logging e.g. card, loan, gold
        level    : INFO / WARNING / ERROR
        message  : What happened
        logtable : Full table path. Falls back to config if not provided.
    """
    from shared.config import LOGTABLE

    table = logtable if logtable else LOGTABLE
    ts    = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{ts}] [{pipeline.upper()}] [{level}] {message}")

    try:
        spark.sql(f"""
            INSERT INTO {table}
            VALUES (
                '{pipeline}',
                '{level}',
                '{message.replace("'", "''")}',
                '{ts}'
            )
        """)
    except Exception as e:
        print(f"[LOGGER ERROR] Could not write to logtable: {e}")


# =============================================================
# HELPER FUNCTIONS
# Shortcuts so notebooks don't have to pass level every time
# =============================================================

def log_info(spark, pipeline: str, message: str):
    log(spark, pipeline, INFO, message)

def log_warning(spark, pipeline: str, message: str):
    log(spark, pipeline, WARNING, message)

def log_error(spark, pipeline: str, message: str):
    log(spark, pipeline, ERROR, message)


# =============================================================
# JOB RUN SUMMARY
# Call this at the END of every notebook — like your Task 176
# Prints a structured summary of what happened in the run
# =============================================================

def print_run_summary(spark, pipeline: str, logtable: str = None):
    """
    Reads the logtable and prints a summary of the current pipeline run.
    Fails the job if any ERROR entries are found.

    Args:
        spark    : Active SparkSession
        pipeline : Which pipeline to summarise e.g. card, loan, gold
        logtable : Full table path. Falls back to config if not provided.
    """
    from shared.config import LOGTABLE

    table = logtable if logtable else LOGTABLE

    print("=" * 60)
    print(f"  JOB RUN SUMMARY — {pipeline.upper()} PIPELINE")
    print("=" * 60)

    try:
        df = spark.sql(f"""
            SELECT level, COUNT(*) as count
            FROM {table}
            WHERE pipeline = '{pipeline}'
            AND log_timestamp >= current_timestamp() - INTERVAL 1 HOUR
            GROUP BY level
            ORDER BY level
        """)
        df.show()

        error_count = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {table}
            WHERE pipeline = '{pipeline}'
            AND level = 'ERROR'
            AND log_timestamp >= current_timestamp() - INTERVAL 1 HOUR
        """).collect()[0]["cnt"]

        if error_count > 0:
            print(f"PIPELINE FAILED — {error_count} error(s) detected.")
            raise Exception(f"{pipeline.upper()} pipeline failed with {error_count} error(s). Check logtable.")
        else:
            print(f"PIPELINE COMPLETED SUCCESSFULLY.")

    except Exception as e:
        print(f"[SUMMARY ERROR] Could not read logtable: {e}")
        raise


# =============================================================
# LOGTABLE SETUP
# Run this ONCE to create the logtable if it does not exist yet
# =============================================================

def create_logtable_if_not_exists(spark, logtable: str = None):
    """
    Creates the central log table if it does not already exist.
    Run this once at the start of your first notebook.
    """
    from shared.config import LOGTABLE, TARGET_CATALOG, LOG_SCHEMA

    table = logtable if logtable else LOGTABLE

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{LOG_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            pipeline      STRING,
            level         STRING,
            message       STRING,
            log_timestamp TIMESTAMP
        )
        USING DELTA
    """)
    print(f"Logtable ready: {table}")
"@

Set-Content -Path "shared\logger.py" -Value $content
Write-Host "logger.py created successfully!" -ForegroundColor Green