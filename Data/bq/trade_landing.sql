CREATE TABLE IF NOT EXISTS `project_id.trade_event.trade_landing`
(
  trade_id        STRING     NOT NULL,
  version         INT64      NOT NULL,
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
  -- System-managed ingestion timestamp
  ingestion_ts    TIMESTAMP  DEFAULT CURRENT_TIMESTAMP() 
)
PARTITION BY DATE(received_at)
CLUSTER BY trade_id
OPTIONS (
  description = "Validated trade events from the real-time streaming pipeline",
  -- Cost & performance
  require_partition_filter = true,
  -- Short retention for landing layer (example: 30 days)
  partition_expiration_days = 30
);