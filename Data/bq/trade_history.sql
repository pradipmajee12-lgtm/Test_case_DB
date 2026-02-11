CREATE TABLE IF NOT EXISTS `project_id.trade_event.trade_history`
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
  expired         BOOL,
  -- System-managed ingestion timestamp
  ingestion_ts    TIMESTAMP  DEFAULT CURRENT_TIMESTAMP() 
)
PARTITION BY DATE(ingested_at)
CLUSTER BY trade_id
OPTIONS (
  description = "History data of accepted trades (append only)",
  -- Cost & performance
  require_partition_filter = true,
  -- Data retention (example: 7 years for regulatory compliance)
  partition_expiration_days = 2555
);