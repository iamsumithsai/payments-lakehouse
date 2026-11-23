"""
Step 4a: Silver -> Gold (star schema).

Gold is shaped for questions, not for storage. A star schema has:
  - one FACT table  : one row per payment, with numbers (amount) and keys pointing to dimensions
  - DIMENSION tables: descriptive lookups (who, where, when, how) that you filter and group by

    dim_date ----\
    dim_customer --+--> fact_payments <---- dim_merchant
    dim_payment_method -/

Surrogate keys (merchant_key = 1, 2, 3 ...) are small integers we own. They make joins fast and
keep Gold stable even if a source system changes its IDs. Each dimension also has a row with key -1
("UNKNOWN"), so a fact row that cannot find its dimension row is never lost.

Run inside Docker:  docker compose run --rm spark python src/gold.py
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE = Path(os.environ.get("LAKE_BASE", Path(__file__).resolve().parents[1])) / "data"
SILVER = BASE / "lakehouse" / "silver"
GOLD = BASE / "lakehouse" / "gold"


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("gold-load")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_silver(spark, name):
    return spark.read.format("delta").load(str(SILVER / name))


def read_gold(spark, name):
    return spark.read.format("delta").load(str(GOLD / name))


def write_delta(df, name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")  # Step 5 makes this incremental
        .option("overwriteSchema", "true")
        .save(str(GOLD / name))
    )


def with_unknown(df, unknown_values: tuple):
    """Add the -1 'UNKNOWN' member row to a dimension."""
    unknown = df.sparkSession.createDataFrame([unknown_values], df.schema)
    return unknown.unionByName(df)


def build_dim_merchant(merchants):
    # Window.orderBy without partitionBy pulls rows to one partition. Fine for 200 merchants;
    # for a huge dimension you would use a different key strategy.
    w = Window.orderBy("merchant_id")
    d = merchants.select(
        F.row_number().over(w).alias("merchant_key"), "merchant_id", "merchant_name", "category", "city"
    )
    return with_unknown(d, (-1, "UNKNOWN", "Unknown", "Unknown", "Unknown"))


def build_dim_customer(customers):
    w = Window.orderBy("customer_id")
    d = customers.select(
        F.row_number().over(w).alias("customer_key"), "customer_id", "customer_name", "city", "signup_date"
    )
    return with_unknown(d, (-1, "UNKNOWN", "Unknown", "Unknown", None))


def build_dim_payment_method(txn):
    w = Window.orderBy("payment_method")
    d = txn.select("payment_method").distinct().select(
        F.row_number().over(w).alias("payment_method_key"), "payment_method"
    )
    return with_unknown(d, (-1, "UNKNOWN"))


def build_dim_date(spark, txn):
    r = txn.agg(F.min("event_date").alias("lo"), F.max("event_date").alias("hi")).first()
    days = spark.range(1).select(
        F.explode(F.sequence(F.lit(r["lo"]), F.lit(r["hi"]), F.expr("interval 1 day"))).alias("full_date")
    )
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.month("full_date").alias("month"),
        F.dayofmonth("full_date").alias("day"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),  # Spark: 1 = Sunday, 7 = Saturday
    )


def build_fact(txn, dim_merchant, dim_customer, dim_method):
    # Dimensions are small, so broadcast them: each worker gets a copy, the big fact is not shuffled.
    f = (
        txn.join(F.broadcast(dim_merchant.select("merchant_key", "merchant_id")), "merchant_id", "left")
        .join(F.broadcast(dim_customer.select("customer_key", "customer_id")), "customer_id", "left")
        .join(F.broadcast(dim_method.select("payment_method_key", "payment_method")), "payment_method", "left")
    )
    return f.select(
        "txn_id",
        F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.coalesce("customer_key", F.lit(-1)).alias("customer_key"),
        F.coalesce("merchant_key", F.lit(-1)).alias("merchant_key"),
        F.coalesce("payment_method_key", F.lit(-1)).alias("payment_method_key"),
        "amount",
        "currency",
        "status",
        "event_ts",
        (F.col("status") == "SUCCESS").cast("int").alias("is_success"),
        F.lit(datetime.now(timezone.utc)).alias("_gold_built_at"),
    )


def verify(spark) -> bool:
    silver = read_silver(spark, "transactions")
    fact = read_gold(spark, "fact_payments")
    dims = {
        "merchant_key": read_gold(spark, "dim_merchant"),
        "customer_key": read_gold(spark, "dim_customer"),
        "payment_method_key": read_gold(spark, "dim_payment_method"),
        "date_key": read_gold(spark, "dim_date"),
    }

    n_silver, n_fact = silver.count(), fact.count()
    checks = [
        ("fact rows = silver rows", n_fact, n_silver),
        ("fact txn_id unique", fact.select("txn_id").distinct().count(), n_fact),
    ]
    for key, dim in dims.items():
        orphans = fact.join(dim.select(key), key, "left_anti").count()
        checks.append((f"fact rows with no match in dimension on {key}", orphans, 0))
    unknown_cust = fact.filter(F.col("customer_key") == -1).count()
    unknown_merch = fact.filter(F.col("merchant_key") == -1).count()
    checks.append(("fact rows pointing to UNKNOWN customer", unknown_cust, 0))
    checks.append(("fact rows pointing to UNKNOWN merchant", unknown_merch, 0))

    # Money must reconcile per currency, down to the paisa
    s_sum = {r["currency"]: r["t"] for r in silver.groupBy("currency").agg(F.sum("amount").alias("t")).collect()}
    f_sum = {r["currency"]: r["t"] for r in fact.groupBy("currency").agg(F.sum("amount").alias("t")).collect()}
    for cur in sorted(s_sum):
        checks.append((f"sum(amount) {cur}: gold vs silver", f_sum.get(cur), s_sum[cur]))

    print("\n=== Gold summary ===")
    for name in ["dim_date", "dim_customer", "dim_merchant", "dim_payment_method", "fact_payments"]:
        print(f"  {name:20s} {read_gold(spark, name).count()} rows")
    print("\nChecks:")
    ok_all = True
    for name, actual, expected in checks:
        ok = actual == expected
        ok_all &= ok
        print(f"  [{'OK ' if ok else 'BAD'}] {name}: {actual} (expected {expected})")

    print("\nfact_payments schema:")
    fact.printSchema()
    print("Sample fact rows:")
    fact.drop("_gold_built_at").show(5, truncate=False)
    return ok_all


def main() -> int:
    if not (SILVER / "transactions").exists():
        print("Silver tables not found. Run src/silver.py first.")
        return 1

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    txn = read_silver(spark, "transactions")
    dim_merchant = build_dim_merchant(read_silver(spark, "merchants"))
    dim_customer = build_dim_customer(read_silver(spark, "customers"))
    dim_method = build_dim_payment_method(txn)
    dim_date = build_dim_date(spark, txn)
    fact = build_fact(txn, dim_merchant, dim_customer, dim_method)

    write_delta(dim_merchant, "dim_merchant")
    write_delta(dim_customer, "dim_customer")
    write_delta(dim_method, "dim_payment_method")
    write_delta(dim_date, "dim_date")
    write_delta(fact, "fact_payments")

    ok = verify(spark)
    print("\nRESULT:", "PASS - Gold star schema is consistent with Silver" if ok else "FAIL - see [BAD] checks above")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
