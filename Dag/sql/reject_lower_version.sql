--Parameters PROJECT_ID & BQ_DATASET are dynamically provided by the Airflow DAG at runtime.
INSERT INTO `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_rejected` 
(
    trade_id, 
    version, 
    party_id, 
    book_id, 
    product_id, 
    price, 
    currency, 
    trade_date, 
    source, 
    maturity_date, 
    created_at, 
    received_at, 
    reason, 
    error_message
)
SELECT 
    s.trade_id, 
    s.version, 
    s.party_id, 
    s.book_id, 
    s.product_id, 
    s.price, 
    s.currency, 
    s.trade_date, 
    s.source, 
    s.maturity_date, 
    s.created_at, 
    s.received_at, 
    'LOWER_VERSION' AS reason, 
    'Incoming version lower than existing' AS error_message
FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_landing` s
JOIN `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_current` c 
    ON s.trade_id = c.trade_id
WHERE s.version < c.version 
  AND DATE(s.received_at) = CURRENT_DATE();