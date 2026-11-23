"""
Step 3: Bronze -> Silver.

What Silver means: data you can trust. Every row is typed, standardized and unique.
Rows that cannot be trusted are NOT deleted. They go to a quarantine table with the
reason(s), so nothing disappears silently and bad rows can be reviewed or reprocessed.

Order of work (the order matters):
  1. Standardize  : trim text, parse amount/timestamp, map status, fix casing
  2. Deduplicate  : one row per txn_id (keep the most advanced status)
  3. Validate     : apply rules, collect reject reasons per row
  4. Split        : no reasons -> silver.transactions, reasons -> silver.quarantine_transactions
  5. Verify       : compare counts with the manifest (your answer key)

Run inside Docker:  docker compose run --rm spark python src/silver.py
"""
import json
import os
import sys
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BASE = Path(os.environ.get("LAKE_BASE", Path(__file__).resolve().parents[1])) / "data"
RAW = BASE / "raw"
BRONZE = BASE / "lakehouse" / "bronze"
SILVER = BASE / "lakehouse" / "silver"

VALID_CURRENCIES = ["INR", "USD", "EUR", "GBP"]
VALID_METHODS = ["UPI", "CARD", "NETBANKING", "WALLET"]

# Typos and short forms seen in the source -> the official status.
# Anything not listed here (e.g. "UNKNOWN") becomes NULL and is rejected as invalid_status.
STATUS_MAP = {
    "SUCCESS": "SUCCESS", "SUCCES": "SUCCESS",
    "FAILED": "FAILED", "FAIL": "FAILED",
    "PENDING": "PENDING", "REFUNDED": "REFUNDED",
}
# Lifecycle order. When the same txn_id arrives with different statuses and there is no
# updated_at column, the most advanced status wins. (With a version/updated_at column
# you would order by that instead.)
STATUS_RANK = {"REFUNDED": 4, "SUCCESS": 3, "FAILED": 2, "PENDING": 1}


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("silver-load")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        # Source timestamps have no timezone; we treat them as UTC consistently.
        .config("spark.sql.session.timeZone", "UTC")
        # Bad patterns give NULL instead of raising an upgrade error.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def clean(col: str):
    """Trim text; turn empty strings into NULL."""
    t = F.trim(F.col(col))
    return F.when(t == "", F.lit(None)).otherwise(t)


def mapping(d: dict):
    return F.create_map(*[F.lit(x) for x in chain.from_iterable(d.items())])


def standardize(df):
    amount_digits = F.regexp_replace(F.col("amount"), r"[^0-9.\-]", "")  # "₹1,234.50" -> "1234.50"
    ts = F.trim(F.col("event_ts"))
    event_ts = (
        F.when(ts.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"), F.to_timestamp(ts, "yyyy-MM-dd HH:mm:ss"))
        .when(ts.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"), F.to_timestamp(ts, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .when(ts.rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"), F.to_timestamp(ts, "dd/MM/yyyy HH:mm:ss"))
        .when(ts.rlike(r"^\d{9,11}$"), F.timestamp_seconds(ts.cast("long")))
    )  # anything else (e.g. "not-a-date") stays NULL -> rejected as bad_timestamp
    return (
        df.withColumn("txn_id_c", clean("txn_id"))
        .withColumn("customer_id_c", clean("customer_id"))
        .withColumn("merchant_id_c", clean("merchant_id"))
        .withColumn(
            "amount_c",
            F.when(amount_digits.rlike(r"^-?\d+(\.\d+)?$"), amount_digits.cast(DecimalType(18, 2))),
        )
        .withColumn("currency_c", F.upper(clean("currency")))
        .withColumn("method_c", F.upper(clean("payment_method")))
        .withColumn("status_c", mapping(STATUS_MAP)[F.upper(clean("status"))])
        .withColumn("event_ts_c", event_ts)
        .withColumn("_status_rank", F.coalesce(mapping(STATUS_RANK)[F.col("status_c")], F.lit(0)))
    )


def deduplicate(df):
    has_id = df.filter(F.col("txn_id_c").isNotNull())
    no_id = df.filter(F.col("txn_id_c").isNull())  # cannot be deduplicated; rejected later

    pick = Window.partitionBy("txn_id_c").orderBy(
        F.col("_status_rank").desc(), F.col("_source_file").desc(), F.col("_ingested_at").desc()
    )
    all_copies = Window.partitionBy("txn_id_c")
    kept = (
        has_id.withColumn("_rn", F.row_number().over(pick))
        .withColumn("_dup_count", F.count(F.lit(1)).over(all_copies))  # how many copies we saw
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    no_id = no_id.withColumn("_dup_count", F.lit(1).cast("long"))
    return kept.unionByName(no_id)


def flag_rejects(df, merchants):
    m = merchants.select(F.trim(F.col("merchant_id")).alias("_m_id")).distinct()
    # merchants is tiny, so broadcast it: every worker gets a copy and no shuffle is needed
    joined = df.join(F.broadcast(m), df["merchant_id_c"] == m["_m_id"], "left")
    reasons = F.concat_ws(
        ";",  # concat_ws skips NULLs, so only the rules that fired remain
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("corrupt_record")),
        F.when(F.col("txn_id_c").isNull(), F.lit("missing_txn_id")),
        F.when(F.col("customer_id_c").isNull(), F.lit("missing_customer")),
        F.when(F.col("_m_id").isNull(), F.lit("orphan_merchant")),
        F.when(F.col("amount_c").isNull(), F.lit("bad_amount")),
        F.when(F.col("amount_c") <= 0, F.lit("non_positive_amount")),
        F.when(F.col("event_ts_c").isNull(), F.lit("bad_timestamp")),
        F.when(F.col("status_c").isNull(), F.lit("invalid_status")),
        F.when(F.col("currency_c").isNull() | ~F.col("currency_c").isin(*VALID_CURRENCIES), F.lit("invalid_currency")),
        F.when(F.col("method_c").isNull() | ~F.col("method_c").isin(*VALID_METHODS), F.lit("invalid_payment_method")),
    )
    return joined.withColumn("_reject_reasons", reasons)


def write_delta(df, name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")  # Step 5 makes this incremental
        .option("overwriteSchema", "true")
        .save(str(SILVER / name))
    )


def build_dimensions(spark) -> None:
    merchants = (
        spark.read.format("delta").load(str(BRONZE / "merchants"))
        .select(
            F.trim("merchant_id").alias("merchant_id"),
            F.trim("merchant_name").alias("merchant_name"),
            F.trim("category").alias("category"),
            F.trim("city").alias("city"),
        )
        .dropDuplicates(["merchant_id"])
    )
    customers = (
        spark.read.format("delta").load(str(BRONZE / "customers"))
        .select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("customer_name").alias("customer_name"),
            F.trim("city").alias("city"),
            F.to_date("signup_date").alias("signup_date"),
        )
        .dropDuplicates(["customer_id"])
    )
    write_delta(merchants, "merchants")
    write_delta(customers, "customers")


def verify(spark) -> bool:
    manifest = json.loads((RAW / "_manifest.json").read_text(encoding="utf-8"))
    d = manifest["defects_injected"]

    b = spark.read.format("delta").load(str(BRONZE / "transactions"))
    s = spark.read.format("delta").load(str(SILVER / "transactions"))
    q = spark.read.format("delta").load(str(SILVER / "quarantine_transactions"))

    bronze_rows = b.count()
    distinct_ids = b.select("txn_id").distinct().count()
    silver_rows, quarantine_rows = s.count(), q.count()
    silver_distinct = s.select("txn_id").distinct().count()

    checks = [
        ("silver + quarantine = distinct txn_ids", silver_rows + quarantine_rows, distinct_ids),
        ("duplicates removed = manifest duplicates", bronze_rows - distinct_ids,
         d["exact_duplicate"] + d["conflicting_duplicate"]),
        ("no duplicate txn_id left in silver", silver_rows, silver_distinct),
    ]

    reasons = {
        r["reason"]: r["count"]
        for r in q.select(F.explode(F.split("_reject_reasons", ";")).alias("reason")).groupBy("reason").count().collect()
    }
    pairs = {
        "bad_amount": "null_amount",
        "non_positive_amount": "negative_amount",
        "bad_timestamp": "bad_timestamp",
        "missing_customer": "null_customer",
        "orphan_merchant": "orphan_merchant",
    }
    for reason, key in pairs.items():
        checks.append((f"quarantine {reason} = manifest {key}", reasons.get(reason, 0), d[key]))

    # Sanity: an event happened on the file's day, or the day before (late arrivals)
    out_of_range = (
        s.withColumn("_file_date", F.to_date(F.regexp_extract("_source_file", r"transactions_(\d{4}-\d{2}-\d{2})\.jsonl", 1)))
        .filter(~F.datediff("_file_date", "event_date").isin(0, 1))
        .count()
    )
    checks.append(("silver rows with event_date far from file date", out_of_range, 0))

    print("\n=== Silver summary ===")
    print(f"bronze rows {bronze_rows} -> silver {silver_rows} + quarantine {quarantine_rows}"
          f" (distinct txn_ids {distinct_ids})")
    print("\nAll reject reasons found (one row can have several):")
    for k, v in sorted(reasons.items(), key=lambda kv: -kv[1]):
        print(f"  {k:24s} {v}")

    print("\nChecks:")
    ok_all = True
    for name, actual, expected in checks:
        ok = actual == expected
        ok_all &= ok
        print(f"  [{'OK ' if ok else 'BAD'}] {name}: {actual} (expected {expected})")

    print("\nStatus values in silver:")
    s.groupBy("status").count().orderBy("status").show()
    print("Silver schema:")
    s.printSchema()
    print("Sample silver rows:")
    s.select("txn_id", "amount", "currency", "status", "event_ts", "_dup_count").show(5, truncate=False)
    print("Sample quarantine rows:")
    q.select("txn_id", "amount", "event_ts", "status", "_reject_reasons").show(5, truncate=False)
    return ok_all


def main() -> int:
    if not (BRONZE / "transactions").exists():
        print("Bronze table not found. Run src/bronze.py first.")
        return 1

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_ts = F.lit(datetime.now(timezone.utc))

    bronze = spark.read.format("delta").load(str(BRONZE / "transactions"))
    merchants = spark.read.format("delta").load(str(BRONZE / "merchants"))

    flagged = flag_rejects(deduplicate(standardize(bronze)), merchants).cache()

    silver = flagged.filter(F.col("_reject_reasons") == "").select(
        F.col("txn_id_c").alias("txn_id"),
        F.col("customer_id_c").alias("customer_id"),
        F.col("merchant_id_c").alias("merchant_id"),
        F.col("amount_c").alias("amount"),
        F.col("currency_c").alias("currency"),
        F.col("method_c").alias("payment_method"),
        F.col("status_c").alias("status"),
        F.col("event_ts_c").alias("event_ts"),
        F.to_date("event_ts_c").alias("event_date"),
        "ingest_source",
        "_dup_count",
        "_source_file",
        "_batch_id",
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        run_ts.alias("_silver_processed_at"),
    )
    # Quarantine keeps the ORIGINAL raw values so a bad row can be inspected or reprocessed.
    quarantine = flagged.filter(F.col("_reject_reasons") != "").select(
        "txn_id", "customer_id", "merchant_id", "amount", "currency", "payment_method",
        "status", "event_ts", "ingest_source",
        "_reject_reasons", "_dup_count", "_source_file", "_batch_id",
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        run_ts.alias("_silver_processed_at"),
    )

    write_delta(silver, "transactions")
    write_delta(quarantine, "quarantine_transactions")
    build_dimensions(spark)

    ok = verify(spark)
    print("\nRESULT:", "PASS - Silver matches the manifest" if ok else "FAIL - see [BAD] checks above")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
