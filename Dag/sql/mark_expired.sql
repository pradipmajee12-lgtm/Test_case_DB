--Parameters PROJECT_ID & BQ_DATASET are dynamically provided by the Airflow DAG at runtime.
UPDATE `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trade_current`
SET expired = TRUE, 
    updated_at = CURRENT_TIMESTAMP()
WHERE maturity_date < CURRENT_DATE() 
  AND expired = FALSE;