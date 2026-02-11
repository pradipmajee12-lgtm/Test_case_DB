--Parameters PROJECT_ID & BQ_DATASET are dynamically provided by the Airflow DAG at runtime.
INSERT INTO `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_history` 
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
    expired
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
    FALSE AS expired
FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_landing` s
WHERE DATE(s.received_at) = CURRENT_DATE()
/* Only insert into history if this version doesn't already exist in 'current' 
   or if it's a brand new trade */
AND NOT EXISTS (
    SELECT 1 
    FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_current` c 
    WHERE c.trade_id = s.trade_id 
      AND c.version >= s.version
);