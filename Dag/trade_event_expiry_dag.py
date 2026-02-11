"""
DAG NAME: trade_expiry_dag
DESCRIPTION: 
    Daily maintenance task that marks trades as 'expired' once they 
    pass their maturity date.

DEPENDENCIES:
    - SQL: mark_expired.sql (located in /home/airflow/gcs/dags/sql)
    
AMENDMENT HISTORY:
        - 2025-02-11: Initial Version - New
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "trades_dw")

with DAG(
    dag_id="trade_expiry_job",
    start_date=datetime(2025, 1, 1),
    schedule_interval="10 0 * * *",
    catchup=False,
    template_searchpath=["/home/airflow/gcs/dags/sql"],
    tags=["trades", "expiry"],
) as dag:

    mark_expired = BigQueryInsertJobOperator(
        task_id="mark_expired",
        configuration={
            "query": {
                "query": "mark_expired.sql",
                "useLegacySql": False,
            }
        },
    )