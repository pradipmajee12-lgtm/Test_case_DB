import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
REGION = os.environ.get("GCP_REGION", "us-central1")
SUBSCRIPTION = os.environ.get("PUBSUB_SUBSCRIPTION")
BQ_DATASET = os.environ.get("BQ_DATASET", "trades_dw")
GCS_TEMP = os.environ.get("GCS_TEMP")
PY_FILE = os.environ.get("DATAFLOW_PY", "gs://your-bucket/code/pipeline.py")

default_args = {
    "owner": "trade-etl",
    "depends_on_past": False,
    "email": [os.environ.get("ALERT_EMAIL", "alerts@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG 1: Main Trade ETL ---
with DAG(
    dag_id="trade_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=["trades", "dataflow", "bigquery"],
) as dag:

    start_dataflow = DataflowCreatePythonJobOperator(
        task_id="start_dataflow",
        py_file=PY_FILE,
        job_name="trade-stream-pipeline",
        options={
            "project": PROJECT_ID,
            "region": REGION,
            "input_subscription": SUBSCRIPTION,
            "bq_dataset": BQ_DATASET,
            "temp_location": GCS_TEMP,
            "runner": "DataflowRunner",
        },
        location=REGION,
        wait_until_finished=False,
    )

    # 1) Reject lower-version rows
    insert_lower_version_rejects = BigQueryInsertJobOperator(
        task_id="insert_lower_version_rejects",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{BQ_DATASET}.rejected_trades` 
                    (trade_id, version, counter_party, book_id, product, notional, currency, maturity_date, created_at, received_at, reason, details, source)
                    SELECT s.trade_id, s.version, s.counter_party, s.book_id, s.product, s.notional, s.currency, s.maturity_date, s.created_at, s.received_at, 
                           'LOWER_VERSION' AS reason, 'Incoming version lower than existing' AS details, s.source
                    FROM `{PROJECT_ID}.{BQ_DATASET}.trades_staging` s
                    JOIN `{PROJECT_ID}.{BQ_DATASET}.trades_current` c ON s.trade_id = c.trade_id
                    WHERE s.version < c.version AND DATE(s.received_at) = CURRENT_DATE();
                """,
                "useLegacySql": False,
            }
        },
    )

    # 2) Insert history "INSERT" rows (New trades)
    insert_history_inserts = BigQueryInsertJobOperator(
        task_id="insert_history_inserts",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{BQ_DATASET}.trades_history` 
                    (trade_id, version, counter_party, book_id, product, notional, currency, maturity_date, created_at, ingested_at, action, expired, source)
                    SELECT s.trade_id, s.version, s.counter_party, s.book_id, s.product, s.notional, s.currency, s.maturity_date, s.created_at, 
                           CURRENT_TIMESTAMP(), 'INSERT', FALSE, s.source
                    FROM `{PROJECT_ID}.{BQ_DATASET}.trades_staging` s
                    WHERE DATE(s.received_at) = CURRENT_DATE()
                    AND NOT EXISTS (
                        SELECT 1 FROM `{PROJECT_ID}.{BQ_DATASET}.trades_current` c WHERE c.trade_id = s.trade_id
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # 3) Insert history "UPDATE" rows
    insert_history_updates = BigQueryInsertJobOperator(
        task_id="insert_history_updates",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{BQ_DATASET}.trades_history`
                    (trade_id, version, counter_party, book_id, product, notional, currency, maturity_date, created_at, ingested_at, action, expired, source)
                    SELECT s.trade_id, s.version, s.counter_party, s.book_id, s.product, s.notional, s.currency, s.maturity_date, s.created_at, 
                           CURRENT_TIMESTAMP(), 'UPDATE', FALSE, s.source
                    FROM `{PROJECT_ID}.{BQ_DATASET}.trades_staging` s
                    WHERE DATE(s.received_at) = CURRENT_DATE()
                    AND EXISTS (
                        SELECT 1 FROM `{PROJECT_ID}.{BQ_DATASET}.trades_current` c 
                        WHERE c.trade_id = s.trade_id AND s.version >= c.version
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # 4) MERGE current
    merge_current = BigQueryInsertJobOperator(
        task_id="merge_current",
        configuration={
            "query": {
                "query": f"""
                    MERGE `{PROJECT_ID}.{BQ_DATASET}.trades_current` T
                    USING (
                        SELECT s.* FROM `{PROJECT_ID}.{BQ_DATASET}.trades_staging` s
                        WHERE DATE(s.received_at) = CURRENT_DATE()
                        AND NOT EXISTS (
                            SELECT 1 FROM `{PROJECT_ID}.{BQ_DATASET}.trades_current` c 
                            WHERE c.trade_id = s.trade_id AND c.version > s.version
                        )
                    ) S ON T.trade_id = S.trade_id
                    WHEN MATCHED THEN UPDATE SET 
                        version = S.version, counter_party = S.counter_party, book_id = S.book_id, 
                        product = S.product, created_at = S.created_at, updated_at = CURRENT_TIMESTAMP(), 
                        notional = S.notional, currency = S.currency, maturity_date = S.maturity_date, 
                        expired = FALSE, source = S.source
                    WHEN NOT MATCHED THEN INSERT 
                        (trade_id, version, counter_party, book_id, product, notional, currency, maturity_date, created_at, updated_at, expired, source)
                    VALUES 
                        (S.trade_id, S.version, S.counter_party, S.book_id, S.product, S.notional, S.currency, S.maturity_date, S.created_at, CURRENT_TIMESTAMP(), FALSE, S.source);
                """,
                "useLegacySql": False,
            }
        },
    )

    # 5) Cleanup staging
    cleanup_staging = BigQueryInsertJobOperator(
        task_id="cleanup_staging",
        configuration={
            "query": {
                "query": f"DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.trades_staging` WHERE DATE(received_at) < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY);",
                "useLegacySql": False,
            }
        },
    )

    # Dependencies
    start_dataflow >> insert_lower_version_rejects >> [insert_history_inserts, insert_history_updates] >> merge_current >> cleanup_staging


# --- DAG 2: Daily Expiry Job ---
with DAG(
    dag_id="trade_expiry_job",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="10 0 * * *",  # 00:10 UTC daily
    catchup=False,
    tags=["trades", "expiry"],
) as expiry_dag:

    mark_expired = BigQueryInsertJobOperator(
        task_id="mark_expired",
        configuration={
            "query": {
                "query": f"""
                    UPDATE `{PROJECT_ID}.{BQ_DATASET}.trades_current`
                    SET expired = TRUE, updated_at = CURRENT_TIMESTAMP()
                    WHERE maturity_date < CURRENT_DATE() AND expired = FALSE;
                """,
                "useLegacySql": False,
            }
        },
    )