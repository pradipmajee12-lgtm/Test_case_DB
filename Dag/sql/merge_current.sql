--Parameter project & dataset should come from python progarm and these varibale need to maintain in airflow variable
MERGE `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_current` T
USING (
    SELECT s.* FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_landing` s
    WHERE DATE(s.received_at) = CURRENT_DATE()
    AND NOT EXISTS (
        SELECT 1 FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trades_current` c 
        WHERE c.trade_id = s.trade_id AND c.version > s.version
    )
) S ON T.trade_id = S.trade_id
WHEN MATCHED THEN UPDATE SET 
    version = S.version, 
    counter_party = S.counter_party, 
    book_id = S.book_id, 
    product = S.product, 
    notional = S.notional, 
    currency = S.currency, 
    maturity_date = S.maturity_date, 
    created_at = S.created_at, 
    updated_at = CURRENT_TIMESTAMP(), 
    expired = FALSE, 
    source = S.source
WHEN NOT MATCHED THEN INSERT 
    (trade_id, version, counter_party, book_id, product, notional, currency, maturity_date, created_at, updated_at, expired, source)
VALUES 
    (S.trade_id, S.version, S.counter_party, S.book_id, S.product, S.notional, S.currency, S.maturity_date, S.created_at, CURRENT_TIMESTAMP(), FALSE, S.source);