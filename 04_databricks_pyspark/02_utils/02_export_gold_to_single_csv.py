# 04_databricks_pyspark/02_utils/02_export_gold_to_single_csv.py
# AutoScout Marketplace Conversion & Monetization Analytics (EUR)
# STEP 6 — Export Gold tables to single CSV files in a Volume
#
# Guarantees: one CSV file per Gold table (not folders)
# Approach:
# - Write to temp folder (coalesce(1))
# - Locate part-*.csv
# - Move it to final path as <table>.csv
# - Cleanup temp folder

from __future__ import annotations

from typing import List
from datetime import datetime

from pyspark.sql import functions as F

CATALOG = "autoscout_mkt_eur"
GOLD_SCHEMA = "02_gold"

GOLD_EXPORT_VOLUME = f"/Volumes/{CATALOG}/{GOLD_SCHEMA}/gold_exports"
TMP_BASE = f"{GOLD_EXPORT_VOLUME}/_tmp_single_csv_exports"

GOLD_TABLES: List[str] = [
    "dim_date_month",
    "dim_geo",
    "dim_make",
    "dim_dealer",
    "dim_listing",
    "agg_funnel_month",
    "agg_listing_month",
    "agg_dealer_month",
    "agg_revenue_stream_month",
    "kpi_growth_drivers_month",
]


def _ls(path: str):
    return dbutils.fs.ls(path)


def _mkdirs(path: str):
    dbutils.fs.mkdirs(path)


def _rm(path: str):
    dbutils.fs.rm(path, True)


def _mv(src: str, dst: str):
    dbutils.fs.mv(src, dst, True)


def export_table_to_single_csv(table_name: str) -> None:
    full_table = f"`{CATALOG}`.`{GOLD_SCHEMA}`.`{table_name}`"

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    tmp_dir = f"{TMP_BASE}/{table_name}_{ts}"
    final_path = f"{GOLD_EXPORT_VOLUME}/{table_name}.csv"

    print("----------------------------------------------------")
    print(f"Exporting: {full_table}")
    print(f"Temp dir:  {tmp_dir}")
    print(f"Final:     {final_path}")

    # Read table
    df = spark.table(full_table)

    # If table is missing or empty, still produce a header-only CSV
    # (We keep it simple: write df as-is. Spark will write header even if empty if schema exists.)
    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", "true")
          .csv(tmp_dir)
    )

    # Find the single part file
    files = _ls(tmp_dir)
    part_files = [f.path for f in files if f.path.endswith(".csv") and "part-" in f.path]

    if len(part_files) != 1:
        print(f"[WARN] Expected 1 part file, found {len(part_files)}. Files: {[f.path for f in files]}")
        # Try to pick the first csv part if present
        if len(part_files) >= 1:
            part_file = part_files[0]
        else:
            print("[ERROR] No CSV part file found; leaving temp folder for inspection.")
            return
    else:
        part_file = part_files[0]

    # Move to final (overwrite)
    _mv(part_file, final_path)

    # Cleanup temp
    _rm(tmp_dir)

    print(f"[OK] Wrote single CSV: {final_path}")


def main() -> None:
    print("====================================================")
    print("STEP 6 — Export Gold tables to single CSV files")
    print(f"Gold export volume: {GOLD_EXPORT_VOLUME}")
    print("====================================================")

    _mkdirs(GOLD_EXPORT_VOLUME)
    _mkdirs(TMP_BASE)

    # Use catalog/schema
    spark.sql(f"USE CATALOG `{CATALOG}`")
    spark.sql(f"USE SCHEMA `{GOLD_SCHEMA}`")

    for t in GOLD_TABLES:
        try:
            export_table_to_single_csv(t)
        except Exception as e:
            print(f"[WARN] Failed to export {t}: {str(e)[:400]}{'...' if len(str(e)) > 400 else ''}")
            # Continue for the rest
            continue

    print("\nDone.")


main()
