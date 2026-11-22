"""
Step 1: Generate fake, MESSY payment data for the Payments Transaction Lakehouse project.

Stdlib only (no pip needed). Deterministic via --seed so results are reproducible.

Outputs (in --out-dir, default data/raw/):
  transactions_<YYYY-MM-DD>.jsonl   one file per day (good for daily incremental loads in Step 5)
  merchants.csv                     merchant dimension source
  customers.csv                     customer dimension source
  _manifest.json                    counts of every defect injected (your "answer key" for Silver tests)

Usage:
  python generate_payments.py --days 7 --rows-per-day 5000 --start-date 2026-11-01 --seed 42
"""
import argparse
import csv
import json
import random
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path

CURRENCIES = ["INR", "USD", "EUR", "GBP"]
METHODS = ["UPI", "CARD", "NETBANKING", "WALLET"]
STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]
STATUS_WEIGHTS = [0.86, 0.08, 0.04, 0.02]
CITIES = ["Hyderabad", "Warangal", "Bengaluru", "Mumbai", "Delhi", "Chennai", "Pune"]
CATEGORIES = ["grocery", "fuel", "food", "travel", "electronics", "utilities", "fashion"]


def make_merchants(n, rng):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "merchant_id": f"M{i:04d}",
            "merchant_name": f"Merchant {i}",
            "category": rng.choice(CATEGORIES),
            "city": rng.choice(CITIES),
        })
    return rows


def make_customers(n, rng):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "customer_id": f"C{i:06d}",
            "customer_name": f"Customer {i}",
            "city": rng.choice(CITIES),
            "signup_date": (date(2022, 1, 1) + timedelta(days=rng.randint(0, 1400))).isoformat(),
        })
    return rows


def pick_merchant(merchants, rng):
    """Skewed: ~30% of traffic goes to the first merchant (hot key) -> used in Step 6 (skew fix)."""
    if rng.random() < 0.30:
        return merchants[0]["merchant_id"]
    return rng.choice(merchants)["merchant_id"]


def fmt_ts(ts, rng, defects):
    """Mixed timestamp formats = messy source systems."""
    r = rng.random()
    if r < 0.70:
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if r < 0.85:
        defects["ts_format_iso_T"] += 1
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    if r < 0.95:
        defects["ts_format_dmy"] += 1
        return ts.strftime("%d/%m/%Y %H:%M:%S")
    defects["ts_format_epoch"] += 1
    return int(ts.timestamp())


def make_day(day, n_rows, merchants, customers, rng, defects):
    rows = []
    merchant_ids = [m["merchant_id"] for m in merchants]
    for _ in range(n_rows):
        event_ts = datetime(day.year, day.month, day.day) + timedelta(seconds=rng.randint(0, 86399))
        amount = round(rng.lognormvariate(6.0, 1.0), 2)  # long-tail amounts
        status = rng.choices(STATUSES, STATUS_WEIGHTS)[0]
        row = {
            "txn_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "customer_id": rng.choice(customers)["customer_id"],
            "merchant_id": pick_merchant(merchants, rng),
            "amount": amount,
            "currency": rng.choice(CURRENCIES),
            "payment_method": rng.choice(METHODS),
            "status": status,
            "event_ts": None,
            "ingest_source": rng.choice(["app", "web", "pos"]),
        }

        # ---- inject defects ----
        if rng.random() < 0.02:                      # null / missing amount
            row["amount"] = None; defects["null_amount"] += 1
        elif rng.random() < 0.02:                    # negative amount (not a refund)
            row["amount"] = -abs(amount); defects["negative_amount"] += 1
        elif rng.random() < 0.03:                    # amount as string with symbol/comma
            row["amount"] = f"₹{amount:,.2f}"; defects["amount_as_string"] += 1

        if rng.random() < 0.03:                      # currency casing / whitespace
            row["currency"] = rng.choice([" inr", "usd ", "Eur", "gbp"]); defects["currency_dirty"] += 1
        if rng.random() < 0.03:                      # status casing / typos
            row["status"] = rng.choice(["success", "Succes", "fail", "Pending ", "UNKNOWN"]); defects["status_dirty"] += 1
        if rng.random() < 0.015:                     # missing customer
            row["customer_id"] = None; defects["null_customer"] += 1
        if rng.random() < 0.02:                      # orphan merchant (not in dimension)
            row["merchant_id"] = f"M{rng.randint(9000, 9999)}"; defects["orphan_merchant"] += 1
        if rng.random() < 0.01:                      # unparseable timestamp
            row["event_ts"] = "not-a-date"; defects["bad_timestamp"] += 1
        else:
            row["event_ts"] = fmt_ts(event_ts, rng, defects)

        rows.append(row)

    # exact duplicates (same txn re-sent by upstream retry)
    dups = rng.sample(rows, int(n_rows * 0.03))
    for d in dups:
        rows.append(dict(d))
    defects["exact_duplicate"] += len(dups)

    # conflicting duplicates (same txn_id, later version with a different status)
    conflicts = rng.sample(rows[:n_rows], int(n_rows * 0.01))
    for c in conflicts:
        v2 = dict(c)
        v2["status"] = "SUCCESS" if c["status"] != "SUCCESS" else "REFUNDED"
        rows.append(v2)
    defects["conflicting_duplicate"] += len(conflicts)

    rng.shuffle(rows)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--rows-per-day", type=int, default=5000)
    ap.add_argument("--start-date", default="2026-11-01")
    ap.add_argument("--merchants", type=int, default=200)
    ap.add_argument("--customers", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out-dir", default="data/raw")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    merchants = make_merchants(args.merchants, rng)
    customers = make_customers(args.customers, rng)

    for name, rows in (("merchants", merchants), ("customers", customers)):
        with open(out / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)

    defects = {k: 0 for k in [
        "null_amount", "negative_amount", "amount_as_string", "currency_dirty",
        "status_dirty", "null_customer", "orphan_merchant", "bad_timestamp",
        "ts_format_iso_T", "ts_format_dmy", "ts_format_epoch",
        "exact_duplicate", "conflicting_duplicate", "late_arriving"]}

    start = date.fromisoformat(args.start_date)
    total = 0
    for i in range(args.days):
        day = start + timedelta(days=i)
        rows = make_day(day, args.rows_per_day, merchants, customers, rng, defects)

        # late-arriving data: ~2% of rows from today land in the NEXT day's file instead
        if i < args.days - 1:
            late_n = int(len(rows) * 0.02)
            late = [rows.pop() for _ in range(late_n)]
            defects["late_arriving"] += late_n
            carry = out / f"_late_carry_{(day + timedelta(days=1)).isoformat()}.tmp"
            carry.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in late), encoding="utf-8")

        carry_in = out / f"_late_carry_{day.isoformat()}.tmp"
        if carry_in.exists():
            rows += [json.loads(l) for l in carry_in.read_text(encoding="utf-8").splitlines() if l]
            carry_in.unlink()

        with open(out / f"transactions_{day.isoformat()}.jsonl", "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        total += len(rows)
        print(f"{day}: {len(rows)} rows")

    manifest = {"seed": args.seed, "days": args.days, "total_rows": total, "defects_injected": defects}
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\nTotal rows: {total}")
    print("Defects injected:", json.dumps(defects, indent=2))


if __name__ == "__main__":
    main()
