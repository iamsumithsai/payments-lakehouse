-- Gold reports. Each block starts with "-- name:" and ends with a semicolon.
-- Written in plain SQL that runs on both Spark SQL and SQLite.
--
-- IMPORTANT: the data has four currencies (INR, USD, EUR, GBP) and there is no
-- exchange-rate table, so amounts in different currencies must never be added together.
-- Every money total below is either grouped by currency or filtered to one currency.

-- name: 1_daily_revenue_day_over_day_and_moving_average
-- Window functions: LAG (yesterday's value) and AVG over a 3-row frame (moving average).
WITH daily AS (
    SELECT d.full_date, f.currency, COUNT(*) AS txns, SUM(f.amount) AS revenue
    FROM fact_payments f
    JOIN dim_date d ON d.date_key = f.date_key
    WHERE f.status = 'SUCCESS'
    GROUP BY d.full_date, f.currency
)
SELECT full_date, currency, txns,
       ROUND(revenue, 2) AS revenue,
       ROUND(revenue - LAG(revenue) OVER (PARTITION BY currency ORDER BY full_date), 2) AS change_vs_prev_day,
       ROUND(AVG(revenue) OVER (PARTITION BY currency ORDER BY full_date
                                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3_days
FROM daily
ORDER BY currency, full_date;

-- name: 2_top3_merchants_per_category_inr
-- Window function: RANK() restarts for every category (PARTITION BY).
WITH m AS (
    SELECT dm.category, dm.merchant_id, COUNT(*) AS txns, SUM(f.amount) AS revenue
    FROM fact_payments f
    JOIN dim_merchant dm ON dm.merchant_key = f.merchant_key
    WHERE f.status = 'SUCCESS' AND f.currency = 'INR'
    GROUP BY dm.category, dm.merchant_id
),
ranked AS (
    SELECT category, merchant_id, txns, revenue,
           RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rnk
    FROM m
)
SELECT category, rnk, merchant_id, txns, ROUND(revenue, 2) AS revenue
FROM ranked
WHERE rnk <= 3
ORDER BY category, rnk;

-- name: 3_cumulative_revenue_and_share_of_period
-- Window functions: running SUM (ORDER BY) and a total SUM (no ORDER BY) in the same query.
WITH daily AS (
    SELECT d.full_date, f.currency, SUM(f.amount) AS revenue
    FROM fact_payments f
    JOIN dim_date d ON d.date_key = f.date_key
    WHERE f.status = 'SUCCESS'
    GROUP BY d.full_date, f.currency
)
SELECT full_date, currency,
       ROUND(revenue, 2) AS revenue,
       ROUND(SUM(revenue) OVER (PARTITION BY currency ORDER BY full_date), 2) AS running_total,
       ROUND(100.0 * revenue / SUM(revenue) OVER (PARTITION BY currency), 2) AS pct_of_period
FROM daily
ORDER BY currency, full_date;

-- name: 4_customer_spend_quartiles_inr
-- Window function: NTILE(4) cuts customers into four equal-sized groups by spend.
WITH spend AS (
    SELECT f.customer_key, SUM(f.amount) AS total_spend
    FROM fact_payments f
    WHERE f.status = 'SUCCESS' AND f.currency = 'INR' AND f.customer_key <> -1
    GROUP BY f.customer_key
),
bucketed AS (
    SELECT customer_key, total_spend,
           NTILE(4) OVER (ORDER BY total_spend DESC) AS quartile
    FROM spend
)
SELECT quartile,
       COUNT(*) AS customers,
       ROUND(MIN(total_spend), 2) AS min_spend,
       ROUND(MAX(total_spend), 2) AS max_spend,
       ROUND(SUM(total_spend), 2) AS group_revenue
FROM bucketed
GROUP BY quartile
ORDER BY quartile;

-- name: 5_merchant_failure_rate_ranking
-- Window functions: RANK by failure rate, and SUM() OVER () for share of all traffic.
-- Merchants with fewer than 100 transactions are skipped (too few to compare fairly).
WITH s AS (
    SELECT dm.merchant_id,
           COUNT(*) AS total_txns,
           SUM(CASE WHEN f.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_txns
    FROM fact_payments f
    JOIN dim_merchant dm ON dm.merchant_key = f.merchant_key
    GROUP BY dm.merchant_id
)
SELECT merchant_id, total_txns, failed_txns,
       ROUND(100.0 * failed_txns / total_txns, 2) AS failure_pct,
       RANK() OVER (ORDER BY 1.0 * failed_txns / total_txns DESC) AS failure_rank,
       ROUND(100.0 * total_txns / SUM(total_txns) OVER (), 2) AS share_of_all_traffic_pct
FROM s
WHERE total_txns >= 100
ORDER BY failure_rank
LIMIT 10;

-- name: 6_traffic_concentration_top_merchants
-- Window functions: ROW_NUMBER and a running share. This exposes the "hot key" (data skew)
-- that we fix in Step 6.
WITH s AS (
    SELECT dm.merchant_id, COUNT(*) AS txns
    FROM fact_payments f
    JOIN dim_merchant dm ON dm.merchant_key = f.merchant_key
    GROUP BY dm.merchant_id
)
SELECT ROW_NUMBER() OVER (ORDER BY txns DESC, merchant_id) AS position,
       merchant_id, txns,
       ROUND(100.0 * txns / SUM(txns) OVER (), 2) AS share_pct,
       ROUND(100.0 * SUM(txns) OVER (ORDER BY txns DESC, merchant_id) / SUM(txns) OVER (), 2) AS cumulative_share_pct
FROM s
ORDER BY position
LIMIT 5;
