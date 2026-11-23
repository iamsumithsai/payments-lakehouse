"""
Step 4b: run the SQL reports in sql/reports.sql against the Gold Delta tables.

Each Gold table is registered as a temporary view (a name Spark SQL can query),
then every block in the .sql file is executed and printed.

Run inside Docker:  docker compose run --rm spark python src/reports.py
"""
import os
import re
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

ROOT = Path(os.environ.get("LAKE_BASE", Path(__file__).resolve().parents[1]))
GOLD = ROOT / "data" / "lakehouse" / "gold"
SQL_FILE = ROOT / "sql" / "reports.sql"
TABLES = ["dim_date", "dim_customer", "dim_merchant", "dim_payment_method", "fact_payments"]


def parse_reports(text: str):
    """Split the file into (name, sql) pairs at every '-- name:' marker."""
    parts = re.split(r"^-- name:\s*(\S+)\s*$", text, flags=re.MULTILINE)
    # parts = [preamble, name1, body1, name2, body2, ...]
    reports = []
    for name, body in zip(parts[1::2], parts[2::2]):
        sql = "\n".join(line for line in body.splitlines() if not line.strip().startswith("--")).strip()
        reports.append((name, sql.rstrip(";").strip()))
    return reports


def main() -> int:
    if not (GOLD / "fact_payments").exists():
        print("Gold tables not found. Run src/gold.py first.")
        return 1

    builder = (
        SparkSession.builder.appName("reports")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for t in TABLES:
        spark.read.format("delta").load(str(GOLD / t)).createOrReplaceTempView(t)

    reports = parse_reports(SQL_FILE.read_text(encoding="utf-8"))
    for name, sql in reports:
        print(f"\n=== {name} ===")
        spark.sql(sql).show(40, truncate=False)

    print(f"\nRESULT: PASS - {len(reports)} reports ran")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
