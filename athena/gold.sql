-- Replace BUCKET and BATCH_ID with one committed batch from manifest.json.
-- Use one table/location per batch to avoid summing full replacement snapshots twice.
CREATE EXTERNAL TABLE IF NOT EXISTS order_daily_batch (
  currency string,
  event_count bigint,
  revenue_cents bigint
)
PARTITIONED BY (event_date date)
STORED AS PARQUET
LOCATION 's3://BUCKET/data-pipeline/batches/BATCH_ID/gold/';

ALTER TABLE order_daily_batch ADD IF NOT EXISTS
PARTITION (event_date='2026-01-01')
LOCATION 's3://BUCKET/data-pipeline/batches/BATCH_ID/gold/event_date=2026-01-01/';

SELECT event_date, currency, sum(event_count), sum(revenue_cents)
FROM order_daily_batch GROUP BY event_date, currency;
