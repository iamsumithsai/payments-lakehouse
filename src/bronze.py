"""
Step 2: raw files -> Bronze Delta tables.

Bronze rule: keep the data exactly as it arrived.
No cleaning, no type fixing, no dropping rows. We only ADD metadata columns
so every row can be traced back to the file and run that produced it.

Every source column is read as STRING on purpose. The raw data mixes numbers,
text and dates in the same column (e.g. amount can be 101.73 or "₹1,234.50",
event_ts can be text or an epoch number). Typing happens later, in Silver.

Run inside Docker:  docker compose run --rm spark python src/bronze.py
"""
import json
import os
import sys
import uuid
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

BASE = Path(os.environ.get("LAKE_BASE", Path(__file__).resolve().parents[1])) / "data"
RAW = BASE / "raw"
BRONZE = BASE / "lakehouse" / "bronze"

TXN_COLUMNS = [
    "txn_id", "customer_id", "merchant_id", "amount", "currency",
    "payment_method", "status", "event_ts", "ingest_source",
]


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("bronze-load")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def add_metadata(df, batch_id: str):
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def load_transactions(spark, batch_id: str) -> None:
    schema = StructType(
        [StructField(c, StringType(), True) for c in TXN_COLUMNS]
        # Lines that are not valid JSON land here instead of being lost
        + [StructField("_corrupt_record", StringType(), True)]
    )
    df = (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(RAW / "transactions_*.jsonl"))
        .withColumn("_source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
    )
    df = add_metadata(df, batch_id)
    (
        df.write.format("delta")
        .mode("overwrite")  # Step 5 replaces this with incremental loads
        .option("overwriteSchema", "true")
        .save(str(BRONZE / "transactions"))
    )


def load_dimension(spark, name: str, batch_id: str) -> None:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)  # keep everything as string
        .csv(str(RAW / f"{name}.csv"))
        .withColumn("_source_file", F.lit(f"{name}.csv"))
    )
    df = add_metadata(df, batch_id)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(BRONZE / name))
    )


def verify(spark) -> bool:
    manifest = json.loads((RAW / "_manifest.json").read_text(encoding="utf-8"))
    path = str(BRONZE / "transactions")
    txn = spark.read.format("delta").load(path)

    n = txn.count()
    expected = manifest["total_rows"]
    corrupt = txn.filter(F.col("_corrupt_record").isNotNull()).count()

    print("\n=== Bronze transactions ===")
    print(f"rows in Bronze : {n}  | manifest total_rows: {expected}")
    print(f"corrupt lines  : {corrupt}")
    print("\nRows per source file:")
    txn.groupBy("_source_file").count().orderBy("_source_file").show(truncate=False)

    print("Sample rows (raw values, untouched):")
    txn.select("txn_id", "amount", "event_ts", "status", "_source_file").show(5, truncate=False)

    print("Delta table history:")
    (
        DeltaTable.forPath(spark, path)
        .history()
        .select("version", "timestamp", "operation", F.col("operationMetrics")["numOutputRows"].alias("rows"))
        .show(truncate=False)
    )

    for name in ("merchants", "customers"):
        c = spark.read.format("delta").load(str(BRONZE / name)).count()
        print(f"{name}: {c} rows")

    return n == expected


def main() -> int:
    if not RAW.exists():
        print(f"Raw folder not found: {RAW}. Run generate_payments.py first.")
        return 1

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    batch_id = str(uuid.uuid4())

    load_transactions(spark, batch_id)
    load_dimension(spark, "merchants", batch_id)
    load_dimension(spark, "customers", batch_id)

    ok = verify(spark)
    print("\nRESULT:", "PASS - Bronze row count matches the manifest" if ok else "FAIL - row count mismatch")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
