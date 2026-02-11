"""
====================================================================================
PIPELINE NAME: Trade Processing Ingestion (Pub/Sub to BigQuery)
Amendment date- 2026-02-11
===================================================================================
DESCRIPTION:
    A streaming pipeline that consumes trade event data from Pub/Sub, validates 
    JSON schema, enforces business rules regarding maturity dates, and routes 
    data into BigQuery.
LOGIC FLOW:
    1. Reads raw bytes from Google Pub/Sub.
    2. Converts bytes to JSON. Catches malformed JSON as PARSE_ERROR.
    3. Ensures all 10 required fields exist and casts data types 
       (int, float, date). Catches missing fields or type mismatches as SCHEMA_ERROR.
    4. Checks 'maturity_date' against 'CURRENT_DATE'. 
       - If maturity < Today: Sent to Rejected Table as MATURITY_IN_PAST.
       - If maturity >= Today: Sent to Staging Table.
    5. STORE: Writes to BigQuery using STREAMING_INSERTS. 
       - Tables handle 'ingested_date' automatically via DB-level DEFAULT values.
CHANGE HISTORY:
    Date        Version  Author      Description
    ----------  -------  ----------  -----------------------------------------------
    2026-02-10  1.0      Initial     Trade event data- Source--> Pub/Sub--> BQ flow.
====================================================================================
"""
import argparse
import json
import logging
from datetime import datetime, timezone, date

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
)

# ----------------------------------------------------------------------
# Constants & Schema
# ----------------------------------------------------------------------
REQUIRED_FIELDS = {
    "trade_id": str,
    "version": int,
    "party_id": str,
    "book_id": str,
    "product_id": str,
    "price":(int, float),
    "currency":str,
    "trade_date":str,   
    "source":str,
    "maturity_date": str,
    "created_at":str,
    "recevied_at":str  
}

# ----------------------------------------------------------------------
# Parsing & Validation
# ----------------------------------------------------------------------

def parse_json(msg: str):
    try:
        return json.loads(msg), None
    except Exception as e:
        return None, f"JSON_PARSE_ERROR: {e}"


def validate_schema(data: dict):
    missing = [k for k in REQUIRED_FIELDS if k not in data]
    if missing:
        return None, f"SCHEMA_ERROR: missing fields {missing}"

    try:
        # Create a copy to avoid mutating input if needed
        clean_data = dict(data)
        clean_data["version"] = int(data["version"])
        clean_data["notional"] = float(data["notional"])
        clean_data["maturity_date"] = date.fromisoformat(data["maturity_date"])
        clean_data["created_at"] = datetime.fromisoformat(data["created_at"])
        return clean_data, None
    except Exception as e:
        return None, f"SCHEMA_ERROR: cast/parse failed: {e}"

# ----------------------------------------------------------------------
# Business Rule Routing
# ----------------------------------------------------------------------

class RouteByMaturity(beam.DoFn):
    def process(self, data: dict):
        today = datetime.now(timezone.utc).date()

        if data["maturity_date"] < today:
            yield beam.pvalue.TaggedOutput(
                "rejected",
                {
                    **data,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "reason": "MATURITY_IN_PAST",
                    "details": f"Maturity {data['maturity_date']} is before {today}",
                },
            )
        else:
            yield {
                **data,
                "received_at": datetime.now(timezone.utc).isoformat(),
            }

# ----------------------------------------------------------------------
# BigQuery Row Mappers
# ----------------------------------------------------------------------

def to_bq_row_staging(data: dict) -> dict:
    return {
        "trade_id": data["trade_id"],
        "version": data["version"],
        "counter_party": data["counter_party"],
        "book_id": data["book_id"],
        "product": data["product"],
        "notional": data["notional"],
        "currency": data["currency"],
        "maturity_date": data["maturity_date"].isoformat(),
        "created_at": data["created_at"].isoformat(),
        "received_at": data["received_at"],
        "source": data["source"],
    }


def to_bq_row_rejected(data: dict, reason: str, details: str) -> dict:
    def safe_iso(val):
        return val.isoformat() if hasattr(val, "isoformat") else str(val) if val else None

    # Handle cases where data might be None or not a dict (e.g., parse errors)
    d = data if isinstance(data, dict) else {}

    return {
        "trade_id": d.get("trade_id"),
        "version": d.get("version"),
        "counter_party": d.get("counter_party"),
        "book_id": d.get("book_id"),
        "product": d.get("product"),
        "notional": d.get("notional"),
        "currency": d.get("currency"),
        "maturity_date": safe_iso(d.get("maturity_date")),
        "created_at": safe_iso(d.get("created_at")),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "details": details,
        "source": d.get("source"),
    }

# ----------------------------------------------------------------------
# Pipeline Runner
# ----------------------------------------------------------------------

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--staging_table", default="trades_staging")
    parser.add_argument("--rejected_table", default="rejected_trades")
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--runner", default="DataflowRunner")

    args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        beam_args,
        runner=args.runner,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
    )
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    bq_staging = f"{args.project}:{args.bq_dataset}.{args.staging_table}"
    bq_rejected = f"{args.project}:{args.bq_dataset}.{args.rejected_table}"

    with beam.Pipeline(options=options) as p:
        # 1. Read and Parse
        raw_msgs = (
            p 
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "BytesToStr" >> beam.Map(lambda b: b.decode("utf-8"))
        )

        parsed_results = raw_msgs | "ParseJSON" >> beam.Map(parse_json)

        # 2. Extract Parse Errors
        parse_errors = (
            parsed_results 
            | "FilterParseErrors" >> beam.Filter(lambda t: t[1] is not None)
            | "MapParseErrors" >> beam.Map(lambda t: to_bq_row_rejected(None, "PARSE_ERROR", t[1]))
        )

        # 3. Schema Validation
        validation_results = (
            parsed_results
            | "FilterValidJSON" >> beam.FlatMap(lambda t: [t[0]] if t[0] else [])
            | "ValidateSchema" >> beam.Map(validate_schema)
        )

        # 4. Extract Schema Errors
        schema_errors = (
            validation_results
            | "FilterSchemaErrors" >> beam.Filter(lambda t: t[1] is not None)
            | "MapSchemaErrors" >> beam.Map(lambda t: to_bq_row_rejected(t[0], "SCHEMA_ERROR", t[1]))
        )

        # 5. Routing Business Logic
        routed = (
            validation_results
            | "FilterValidSchema" >> beam.FlatMap(lambda t: [t[0]] if t[0] else [])
            | "RouteByMaturity" >> beam.ParDo(RouteByMaturity()).with_outputs("rejected", main="valid")
        )

        # 6. Success Path
        _ = (
            routed.valid
            | "ToBQStagingRow" >> beam.Map(to_bq_row_staging)
            | "WriteStaging" >> beam.io.WriteToBigQuery(
                table=bq_staging,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

        # 7. Combined Error Path (Dead Letter Queue)
        maturity_errors = routed.rejected | "MapMaturityErrors" >> beam.Map(
            lambda d: to_bq_row_rejected(d, d["reason"], d["details"])
        )

        _ = (
            (parse_errors, schema_errors, maturity_errors)
            | "FlattenErrors" >> beam.Flatten()
            | "WriteRejects" >> beam.io.WriteToBigQuery(
                table=bq_rejected,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()