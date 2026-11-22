# Payments Transaction Lakehouse
A data engineering portfolio project: a Bronze/Silver/Gold lakehouse for payment transactions, built with PySpark and Delta.
The source data is synthetic and deliberately messy (duplicates, bad timestamps, orphan merchants, late arrivals), because real payment feeds are, and cleaning them is the point.
`generate_payments.py` is seeded, so anyone can recreate the same data and check results against `_manifest.json`.
