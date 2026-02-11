--Parameters PROJECT_ID & BQ_DATASET are dynamically provided by the Airflow DAG at runtime.

MERGE `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_current` AS T
USING (
    SELECT s.* FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_landing` AS s
    WHERE DATE(s.received_at) = CURRENT_DATE()
    AND NOT EXISTS (
        SELECT 1 
        FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_current` AS c 
        WHERE c.trade_id = s.trade_id 
          AND c.version > s.version
    )
) AS S 
ON T.trade_id = S.trade_id
WHEN MATCHED THEN 
  UPDATE SET 
    version       = S.version, 
    party_id      = S.party_id, 
    book_id       = S.book_id, 
    product_id    = S.product_id, 
    price         = S.price, 
    currency      = S.currency, 
    trade_date    = S.trade_date, 
    source        = S.source, 
    maturity_date = S.maturity_date, 
    created_at    = S.created_at, 
    updated_at    = CURRENT_TIMESTAMP(), 
    expired       = FALSE

WHEN NOT MATCHED THEN 
  INSERT (
    trade_id, version, party_id, book_id, product_id, price, 
    currency, trade_date, source, maturity_date, created_at, 
    updated_at, expired
  )
  VALUES (
    S.trade_id, S.version, S.party_id, S.book_id, S.product_id, S.price, 
    S.currency, S.trade_date, S.source, S.maturity_date, S.created_at, 
    CURRENT_TIMESTAMP(), FALSE
  );