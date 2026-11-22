import json, glob
from pathlib import Path

raw = Path("data/raw")
manifest = json.loads((raw / "_manifest.json").read_text(encoding="utf-8"))

rows = []
for f in sorted(glob.glob(str(raw / "transactions_*.jsonl"))):
    with open(f, encoding="utf-8") as fh:
        rows += [json.loads(line) for line in fh if line.strip()]

d = manifest["defects_injected"]
total = len(rows)
unique = len({r["txn_id"] for r in rows})
null_amt = sum(1 for r in rows if r["amount"] is None)
bad_ts = sum(1 for r in rows if r["event_ts"] == "not-a-date")

print("rows in files      :", total, "| manifest:", manifest["total_rows"])
print("extra copies       :", total - unique, "| manifest dups:", d["exact_duplicate"] + d["conflicting_duplicate"])
print("null amounts       :", null_amt, "| manifest:", d["null_amount"], "(files may be a bit higher)")
print("bad timestamps     :", bad_ts, "| manifest:", d["bad_timestamp"], "(files may be a bit higher)")
