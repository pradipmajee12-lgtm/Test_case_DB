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
    3. Ensures all required fields exist and casts data types.
    4. Checks 'maturity_date' against 'CURRENT_DATE'. 
       - If maturity < Today: Sent to trade_rejected as MATURITY_IN_PAST.
       - If maturity >= Today: Sent to trade_landing.
    5. Writes to BigQuery using STREAMING_INSERTS. 
       - Tables handle 'ingestion_ts' automatically via BQ DEFAULT values.
CHANGE HISTORY:
    Date        Version  Author      Description
    ----------  -------  ----------  -----------------------------------------------
    2026-02-11  1.0      Initial     Source--> Pub/Sub--> BQ flow.
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
# Constants & Schema Validation Setup
# ----------------------------------------------------------------------
REQUIRED_FIELDS = {
    "trade_id": str,
    "version": int,
    "party_id": str,
    "book_id": (int, str),
    "product_id": str,
    "price": (int, float),
    "currency": str,
    "trade_date": str,   
    "source": str,
    "maturity_date": str,
    "created_at": str
}

def format_bq_timestamp(val):
    """Ensures dates/times are compatible with BQ TIMESTAMP."""
    if not val: return None
    val_str = str(val)
    # If it's a date 'YYYY-MM-DD', append time for BQ TIMESTAMP
    return f"{val_str}T00:00:00Z" if len(val_str) == 10 else val_str

# ----------------------------------------------------------------------
# Parsing & Validation Functions
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
        clean_data = dict(data)
        clean_data["version"] = int(data["version"])
        clean_data["book_id"] = int(data["book_id"])
        clean_data["price"] = float(data["price"])
        clean_data["maturity_date_obj"] = date.fromisoformat(data["maturity_date"])
        return clean_data, None
    except Exception as e:
        return None, f"SCHEMA_ERROR: cast/parse failed: {e}"

# ----------------------------------------------------------------------
# Business Rule Routing
# ----------------------------------------------------------------------

class RouteByMaturity(beam.DoFn):
    def process(self, data: dict):
        today = datetime.now(timezone.utc).date()
        # Add metadata shared by both landing and rejected paths
        data["received_at"] = datetime.now(timezone.utc).isoformat()

        if data["maturity_date_obj"] < today:
            yield beam.pvalue.TaggedOutput(
                "rejected",
                {
                    **data,
                    "reason": "MATURITY_IN_PAST",
                    "error_message": f"Maturity {data['maturity_date']} is before {today}",
                },
            )
        else:
            yield data

# ----------------------------------------------------------------------
# BigQuery Row Mappers (Aligned with DDLs)
# ----------------------------------------------------------------------

def to_bq_row_landing(data: dict) -> dict:
    """Mapping for project_id.trade_event.trade_landing"""
    return {
        "trade_id": data["trade_id"],
        "version": data["version"],
        "party_id": data["party_id"],
        "book_id": data["book_id"],
        "product_id": data["product_id"],
        "price": data["price"],
        "currency": data["currency"],
        "trade_date": format_bq_timestamp(data["trade_date"]),
        "source": data["source"],
        "maturity_date": format_bq_timestamp(data["maturity_date"]),
        "created_at": format_bq_timestamp(data["created_at"]),
        "received_at": data["received_at"]
    }

def to_bq_row_rejected(data: dict, reason: str, error_msg: str) -> dict:
    """Mapping for project_id.trade_event.trade_rejected"""
    d = data if isinstance(data, dict) else {}
    return {
        "trade_id": d.get("trade_id"),
        "version": d.get("version"),
        "party_id": d.get("party_id"),
        "book_id": d.get("book_id") if isinstance(d.get("book_id"), int) else None,
        "product_id": d.get("product_id"),
        "price": d.get("price"),
        "currency": d.get("currency"),
        "trade_date": format_bq_timestamp(d.get("trade_date")),
        "source": d.get("source"),
        "maturity_date": format_bq_timestamp(d.get("maturity_date")),
        "created_at": format_bq_timestamp(d.get("created_at")),
        "received_at": d.get("received_at") or datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "error_message": error_msg
    }

# ----------------------------------------------------------------------
# Pipeline Execution
# ----------------------------------------------------------------------

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--landing_table", default="trade_landing")
    parser.add_argument("--rejected_table", default="trade_rejected")
    parser.add_argument("--temp_location", required=True)

    args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args, streaming=True, save_main_session=True)
    bq_landing = f"{args.project}:{args.bq_dataset}.{args.landing_table}"
    bq_rejected = f"{args.project}:{args.bq_dataset}.{args.rejected_table}"

    with beam.Pipeline(options=options) as p:
        # 1. Read and Parse
        raw_msgs = (
            p 
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "DecodeBytes" >> beam.Map(lambda b: b.decode("utf-8"))
        )
        parsed_results = raw_msgs | "JSONParse" >> beam.Map(parse_json)

        # 2. Extract Parse Errors (Dead Letter)
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

        # 4. Extract Schema Errors (Dead Letter)
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
            | "ToLandingRow" >> beam.Map(to_bq_row_landing)
            | "WriteToLanding" >> beam.io.WriteToBigQuery(
                table=bq_landing,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

        # 7. Combined Rejected Path (Maturity Errors + Flattened DLQ)
        maturity_errors = (
            routed.rejected 
            | "MapMaturityErrors" >> beam.Map(lambda d: to_bq_row_rejected(d, d["reason"], d["error_message"]))
        )

        _ = (
            (parse_errors, schema_errors, maturity_errors)
            | "FlattenAllErrors" >> beam.Flatten()
            | "WriteToRejected" >> beam.io.WriteToBigQuery(
                table=bq_rejected,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()