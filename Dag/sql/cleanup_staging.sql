--Parameters PROJECT_ID & BQ_DATASET are dynamically provided by the Airflow DAG at runtime.
DELETE FROM `{{ params.PROJECT_ID }}.{{ params.BQ_DATASET }}.trades_landing` 
WHERE DATE(received_at) < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY);