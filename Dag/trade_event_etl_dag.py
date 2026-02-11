"""
DAG NAME: trade_event_etl
DESCRIPTION: 
    Orchestrates the Trade Event processing pipeline. 
    1. Triggers a Dataflow streaming job for ingestion.
    2. Identifies and routes rejected trades (version mismatch).
    3. Maintains an audit history of all valid updates.
    4. Merges latest records into the  trade_current table.

MAINTENANCE:
    - Infrastructure: Project ID & Dataflow path are maintained in GCP Console Env Vars.
    - Business Logic: Dataset name & GCS buckets are maintained in Airflow Variables UI.
    - SQL: Found in the /sql/ subdirectory within the DAGs folder.

AMENDMENT HISTORY:
        - 2025-02-11: Initial Version - New
"""

import os
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT")
REGION = os.environ.get("GCP_REGION", "us-central1")
# Maintained under Airflow Variables (UI)
# Setting default_var prevents "Variable not found" errors during DAG parsing.
BQ_DATASET = Variable.get("trade_bq_dataset", default_var="trade_event")
GCS_TEMP_BUCKET = Variable.get("trade_gcs_bucket", default_var="gs://my-default-bucket")
PUBSUB_SUSCRIPTION = Variable.get("pubsub_suscription")

default_args = {
    "owner": "trade-event-etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="trade_event_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    sql_path=["/home/airflow/gcs/dags/sql"], # Path to your SQL folder in Composer
    tags=["trades", "dataflow"],
) as dag:

    start_dataflow = DataflowCreatePythonJobOperator(
        task_id="start_dataflow",
        py_file=os.environ.get("DATAFLOW_PY"),  # maintain in Console Environment Variables
        job_name="trade-event-stream-pipeline",
        options={
            "project": PROJECT_ID,
            "region": REGION,
            "input_subscription": PUBSUB_SUSCRIPTION,
            "bq_dataset": BQ_DATASET,
            "temp_location": GCS_TEMP_BUCKET,
            "runner": "DataflowRunner",
        },
        wait_until_finished=False,
    )

    # 1) Rejects
    rejects = BigQueryInsertJobOperator(
        task_id="insert_lower_version_rejects",
        configuration={"query": {"query": "reject_lower_version.sql", "useLegacySql": False}}
    )

    # 2 & 3) History Tracking
    hist_inserts = BigQueryInsertJobOperator(
        task_id="insert_history_inserts",
        configuration={"query": {"query": "history_inserts.sql", "useLegacySql": False}}
    )

    hist_updates = BigQueryInsertJobOperator(
        task_id="insert_history_updates",
        configuration={"query": {"query": "history_updates.sql", "useLegacySql": False}}
    )

    # 4) Merge
    merge = BigQueryInsertJobOperator(
        task_id="merge_current",
        configuration={"query": {"query": "merge_current.sql", "useLegacySql": False}}
    )

    # 5) Cleanup
    cleanup = BigQueryInsertJobOperator(
        task_id="cleanup_staging",
        configuration={"query": {"query": "cleanup_staging.sql", "useLegacySql": False}}
    )

    start_dataflow >> rejects >> [hist_inserts, hist_updates] >> merge >> cleanup