CREATE TABLE IF NOT EXISTS `project_id.trade_event.trade_rejected`
(
  trade_id        STRING,
  version         INT64,
  party_id        STRING,
  book_id         INT64,
  product_id      STRING,
  price           NUMERIC,
  currency        STRING,
  trade_date      TIMESTAMP,
  source          STRING,
  maturity_date   TIMESTAMP,
  created_at      TIMESTAMP,
  received_at     TIMESTAMP,
  reason          STRING,
  error_message   STRING,
  -- System-managed ingestion timestamp
  ingestion_ts    TIMESTAMP  DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(received_at)
CLUSTER BY trade_id
OPTIONS (
  description = "Rejected trade details",
  -- Cost & performance
  require_partition_filter = true,
  -- Retention policy for rejected data (example: 90 days)
  partition_expiration_days = 90
);