# 04_databricks_pyspark/01_ingestion_bronze/01_ingest_csv_to_bronze.py
# AutoScout Marketplace Conversion & Monetization Analytics (EUR)
# STEP 3 — PySpark ingestion (CSV Volume -> Bronze Delta)
#
# Constraints honored:
# - Dates in Bronze stored as STRING
# - Loop-based ingestion for multiple CSVs
# - Missing files -> friendly warnings
# - Malformed files -> quarantine and continue
# - No COPY INTO

from __future__ import annotations

from typing import Dict, List, Tuple
import re
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------
# 1) Catalog/schema setup
# -----------------------------
CATALOG = "autoscout_mkt_eur"
BRONZE_SCHEMA = "00_bronze"

# Unity Catalog volume paths
INBOUND_VOLUME = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/inbound_csv"
QUARANTINE_PATH = f"{INBOUND_VOLUME}/_quarantine"


# -----------------------------
# 2) CSV -> Bronze table mapping
# -----------------------------
CSV_TO_TABLE: Dict[str, str] = {
    "users.csv": "users",
    "dealers.csv": "dealers",
    "listings.csv": "listings",
    "searches.csv": "searches",
    "contacts.csv": "contacts",
    "sales.csv": "sales",
    "revenue.csv": "revenue",
}


# -----------------------------
# 3) Columns that must be STRING in Bronze
#    (pattern-based, because schema inference may guess date/timestamp)
# -----------------------------
DATE_LIKE_PATTERNS = [
    r".*_date$",          # *_date
    r".*_dt$",            # *_dt
    r".*_timestamp$",     # *_timestamp
    r".*_ts$",            # *_ts
    r"^date$",            # date
    r"^timestamp$",       # timestamp
]


def is_date_like(col_name: str) -> bool:
    name = col_name.lower()
    return any(re.match(p, name) for p in DATE_LIKE_PATTERNS)


def coerce_date_like_to_string(df: DataFrame) -> DataFrame:
    """
    Force date-like columns to STRING regardless of inferred type.
    """
    out = df
    for c in df.columns:
        if is_date_like(c):
            out = out.withColumn(c, F.col(c).cast("string"))
    return out


def safe_read_csv(path: str) -> Tuple[bool, DataFrame, str]:
    """
    Returns: (ok, df, error_message)
    """
    try:
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "FAILFAST")  # raise on malformed rows
            .load(path)
        )
        return True, df, ""
    except Exception as e:
        return False, None, str(e)


def file_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def quarantine_file(src_path: str) -> None:
    """
    Move malformed file into quarantine folder with timestamp suffix.
    """
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = src_path.split("/")[-1]
    dst = f"{QUARANTINE_PATH}/{base}.bad_{ts}"
    try:
        dbutils.fs.mkdirs(QUARANTINE_PATH)
        dbutils.fs.mv(src_path, dst)
        print(f"[QUARANTINED] Moved malformed file to: {dst}")
    except Exception as e:
        print(f"[WARN] Failed to quarantine file {src_path}: {e}")


def write_bronze(df: DataFrame, table_name: str) -> None:
    full_table = f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{table_name}`"
    (
        df.write
        .format("delta")
        .mode("overwrite")  # initial load pattern; safe for synthetic
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )


def main() -> None:
    print("====================================================")
    print("STEP 3 — CSV Ingestion to Bronze Delta")
    print(f"Catalog: {CATALOG}")
    print(f"Bronze schema: {BRONZE_SCHEMA}")
    print(f"Inbound volume: {INBOUND_VOLUME}")
    print("====================================================\n")

    spark.sql(f"USE CATALOG `{CATALOG}`")
    spark.sql(f"USE SCHEMA `{BRONZE_SCHEMA}`")

    total_ok = 0
    total_warn = 0
    total_quarantine = 0

    for csv_name, table_name in CSV_TO_TABLE.items():
        csv_path = f"{INBOUND_VOLUME}/{csv_name}"

        if not file_exists(csv_path):
            print(f"[WARN] Missing optional input file: {csv_path} -> skipping {table_name}")
            total_warn += 1
            continue

        ok, df, err = safe_read_csv(csv_path)
        if not ok:
            print(f"[WARN] Malformed file detected: {csv_path}")
            print(f"       Error: {err[:400]}{'...' if len(err) > 400 else ''}")
            quarantine_file(csv_path)
            total_quarantine += 1
            continue

        # Force date-like columns to STRING for Bronze
        df = coerce_date_like_to_string(df)

        # Basic row count validation
        row_count = df.count()
        print(f"[OK] Read {csv_name}: {row_count:,} rows")

        # Write to Bronze Delta table
        write_bronze(df, table_name)

        # Post-write validation
        written = spark.table(f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{table_name}`").count()
        if written != row_count:
            print(f"[WARN] Row count mismatch for {table_name}: read={row_count:,}, written={written:,}")
            total_warn += 1
        else:
            print(f"[OK] Written Bronze table: `{CATALOG}`.`{BRONZE_SCHEMA}`.`{table_name}` ({written:,} rows)")
            total_ok += 1

        print("")

    print("====================================================")
    print("Ingestion summary")
    print(f"Tables written OK: {total_ok}")
    print(f"Warnings (missing/mismatch): {total_warn}")
    print(f"Quarantined files: {total_quarantine}")
    print("====================================================")


# Databricks notebook entrypoint
main()
