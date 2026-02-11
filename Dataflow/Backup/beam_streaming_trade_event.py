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
    "counter_party": str,
    "book_id": str,
    "product": str,
    "notional": (int, float),
    "currency": str,
    "maturity_date": str,
    "created_at": str,
    "source": str,
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
        data["version"] = int(data["version"])
        data["notional"] = float(data["notional"])
        data["maturity_date"] = date.fromisoformat(data["maturity_date"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
    except Exception as e:
        return None, f"SCHEMA_ERROR: cast/parse failed: {e}"

    return data, None

# ----------------------------------------------------------------------
# Business Rule Routing
# ----------------------------------------------------------------------

class RouteByMaturity(beam.DoFn):
    def __init__(self):
        self.rejected_counter = beam.metrics.Metrics.counter(
            "trades", "rejected_maturity"
        )
        self.valid_counter = beam.metrics.Metrics.counter(
            "trades", "valid_candidates"
        )

    def process(self, data: dict):
        today = datetime.now(timezone.utc).date()

        if data["maturity_date"] < today:
            self.rejected_counter.inc()
            yield beam.pvalue.TaggedOutput(
                "rejected",
                {
                    **data,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "reason": "MATURITY_IN_PAST",
                    "details": "Maturity date is earlier than today",
                },
            )
        else:
            self.valid_counter.inc()
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
    def safe(val):
        try:
            return val.isoformat()
        except Exception:
            return None

    is_dict = isinstance(data, dict)

    return {
        "trade_id": data.get("trade_id") if is_dict else None,
        "version": data.get("version") if is_dict else None,
        "counter_party": data.get("counter_party") if is_dict else None,
        "book_id": data.get("book_id") if is_dict else None,
        "product": data.get("product") if is_dict else None,
        "notional": data.get("notional") if is_dict else None,
        "currency": data.get("currency") if is_dict else None,
        "maturity_date": safe(data.get("maturity_date")) if is_dict else None,
        "created_at": safe(data.get("created_at")) if is_dict else None,
        "received_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "details": details,
        "source": data.get("source") if is_dict else None,
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

    pipeline_options = PipelineOptions(
        beam_args,
        runner=args.runner,
        project=args.project,
        region=args.region,
        streaming=True,
        temp_location=args.temp_location,
        save_main_session=True,
    )

    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    bq_staging = f"{args.project}:{args.bq_dataset}.{args.staging_table}"
    bq_rejected = f"{args.project}:{args.bq_dataset}.{args.rejected_table}"

    with beam.Pipeline(options=pipeline_options) as p:
        raw = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=args.input_subscription,
                with_output_types=bytes,
            )
            | "BytesToStr" >> beam.Map(lambda b: b.decode("utf-8"))
        )

        parsed = raw | "ParseJSON" >> beam.Map(parse_json)

        parse_errors = parsed | "OnlyParseErrors" >> beam.FlatMap(
            lambda t: [] if t[1] is None else [t[1]]
        )

        validated = (
            parsed
            | "KeepParsed" >> beam.FlatMap(lambda t: [t[0]] if t[0] else [])
            | "ValidateSchema" >> beam.Map(validate_schema)
        )

        schema_errors = validated | "OnlySchemaErrors" >> beam.FlatMap(
            lambda t: [] if t[1] is None else [t]
        )

        clean = validated | "KeepValidSchema" >> beam.FlatMap(
            lambda t: [t[0]] if t[0] else []
        )

        routed = clean | "RouteByMaturity" >> beam.ParDo(RouteByMaturity()).with_outputs(
            "rejected", main="valid"
        )

        routed.valid \
            | "ToBQStaging" >> beam.Map(to_bq_row_staging) \
            | "WriteStaging" >> beam.io.WriteToBigQuery(
                table=bq_staging,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=args.temp_location,
            )

        routed.rejected \
            | "WriteRejectsMaturity" >> beam.io.WriteToBigQuery(
                table=bq_rejected,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=args.temp_location,
            )

        parse_errors \
            | "MapParseErrors" >> beam.Map(
                lambda e: to_bq_row_rejected({}, "PARSE_ERROR", e)
            ) \
            | "WriteRejectsParse" >> beam.io.WriteToBigQuery(
                table=bq_rejected,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=args.temp_location,
            )

        schema_errors \
            | "MapSchemaErrors" >> beam.Map(
                lambda t: to_bq_row_rejected(t[0] or {}, "SCHEMA_ERROR", t[1])
            ) \
            | "WriteRejectsSchema" >> beam.io.WriteToBigQuery(
                table=bq_rejected,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=args.temp_location,
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()