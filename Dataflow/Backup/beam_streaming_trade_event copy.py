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

# Required fields and expected types
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


def parse_json(msg: str):
    try:
        data = json.loads(msg)
        return data, None
    except Exception as e:
        return None, f"JSON PARSE ERROR: {e}"
def validate_schema(data: dict):
    missing = [k for k in REQUIRED_FIELDS if k not in data]
    if missing:
        return None, f"SCHEMA_ERROR: missing {missing}"
    try:
        data["version"] = int(data["version"])
        data["notional"] = float(data["notional"])
        data["maturity date"] = date.fromisoformat(data["maturity date"])
        data["created at"] = datetime.fromisoformat(data["created at"])
    except Exception as e:
        return None, f"SCHEMA_ERROR: cast/parse failed: {e}"
    return data, None


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

        if data["maturity date"] < today:
            self.rejected_counter.inc()
            yield beam.pvalue.TaggedOutput(
                "rejected",
                {
                    **data,
                    "received at": datetime.now(timezone.utc).isoformat(),
                    "reason": "MATURITY_IN_PAST",
                    "details": "Maturity date is earlier than today",
                },
            )
        else:
            self.valid_counter.inc()
            yield {
                **data,
                "received at": datetime.now(timezone.utc).isoformat(),
            }

def to_bq_row_staging(data: dict) -> dict:
    return {
        "trade_id": data["trade id"],
        "version": data["version"],
        "counter_party": d["counter party"],
        "book_id": data["book_id"],
        "product": data["product"],
        "notional": data["notional"],
        "currency": data["currency"],
        "maturity_date": data["maturity date"].isoformat(),
        "created_at": data["created_at"].isoformat(),
        "received_at": data["received_at"],
        "source": data["source"],
    }

def to_ba_row_rejected(data,reason: str, details: str):
     # data may be None or partial
    def safe_iso(val):
        try:
            return val.isoformat()
        except Exception:
            return None

    is_dict = isinstance(data, dict)

    base = {
        "trade_id": data.get("trade_id") if is_dict else None,
        "version": data.get("version") if is_dict else None,
        "counter party": data.get("counter party") if is_dict else None,
        "book id": data.get("book_id") if is_dict else None,
        "product": data.get("product") if is_dict else None,
        "notional": float(data["notional"]) if is_dict and "notional" in data else None,
        "currency": data.get("currency") if is_dict else None,
        "maturity date": safe_iso(data.get("maturity date")) if is_dict else None,
        "created at": safe_iso(data.get("created_at")) if is_dict else None,
        "received at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "details": details,
        "source": data.get("source") if is_dict else None,
    }
    return base

def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument(
        "--input_subscription",
        required=True,
        help="projects/.../subscriptions/..."
    )
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
    pipeline_options.view_as(StandardOptions).Streaming =True
    pipeline_options.view_as(SetupOptions).save_main_session =True
    bq_staging = f"{args.project}:{args.bq_dataset}.{args.staging_table}"
    bq_rejected = f"{args.project}:{args.bq_dataset}.{args.rejected_table}"

    with beam.Pipeline(options=pipeline_options) as p:
        # Read messages from Pub/Sub
        raw = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription, with_output_types=bytes)
            | "BytesToStr" >> beam.Map(lambda b: b.decode("utf-8"))
        )

        # Parse JSON
        parsed = raw | "ParseJSON" >> beam.Map(parse_json)  # parse_json returns (dict_or_none, error_or_none)

        # Extract parse errors (t[1] holds error string)
        parse_errors = parsed | "OnlyParseErrs" >> beam.FlatMap(
            lambda t: [] if t[1] is None else [t[1]])
        )

        # Keep successfully parsed dicts
        valid_dicts = (
            parsed
            | "KeepParsed" >> beam.FlatMap(lambda t: [t[0]] if t[0] is not None else [])
            | "ValidateSchema" >> beam.Map(validate_schema)  # returns (valid_dict_or_none, schema_error_or_none)
        )

        # Extract schema errors
        schema_errors = valid_dicts | "OnlySchemaErrs" >> beam.FlatMap(
            lambda t: [] if t[1] is not None else [t]
        )

        # Keep clean valid dicts
        clean= valid_dicts | "KeepValidSchema" >> beam.FlatMap(
            lambda t: [t[0]] if t[0] is not None else []
        )

        # Route by maturity
        routed = clean | "RouteMaturity" >> beam.ParDo(RouteByMaturity()).with_outputs(
            "rejected", main="valid"
        )

        # Split outputs
        valid_candidates = routed.valid
        maturity_rejects = routed.rejected
        ##Write to BigQuery
        valid_candidates \
    | "ToBQStaging" >> beam.Map(to_bq_row_staging) \
    | "WriteStaging" >> beam.io.WriteToBigQuery(
        table=bq_staging,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        custom_gcs_temp_location=args.temp_location
    )

    
     valid_candidates | "ToBQStaging" >> beam.Map(to_bq_row_staging) \
                 | "WriteStaging" >> beam.io.WriteToBigQuery(
                     table=bq_staging,
                     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                     create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                     custom_gcs_temp_location=args.temp_location
                 )

# Persist maturity rejects
maturity_rejects \
    | "WriteRejectsMaturity" >> beam.io.WriteToBigQuery(
        table=bq_rejected,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        custom_gcs_temp_location=args.temp_location,
    )

# Persist parse errors
parse_errors \
    | "MapParseErrRows" >> beam.Map(
        lambda err: to_bq_row_rejected({}, "PARSE_ERROR", err)
    ) \
    | "WriteRejectsParse" >> beam.io.WriteToBigQuery(
        table=bq_rejected,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        custom_gcs_temp_location=args.temp_location,
    )

# Persist schema errors
schema_errors \
    | "MapSchemaErrRows" >> beam.Map(
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
    import logging
    logging.getLogger().setLevel(logging.INFO)
    run()



