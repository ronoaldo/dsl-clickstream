#!/usr/bin/env python
import os
import sys
import logging
import hashlib
import json
import typing
import datetime
import google
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
from apache_beam.pvalue import TaggedOutput
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

# Global settings
PROJECT_ID = os.getenv("PROJECT_ID")
RAWDATA_BUCKET = os.getenv("RAWDATA_BUCKET", default=f"clickstream-raw-{PROJECT_ID}")
DEADLETTER_BUCKET = os.getenv("RAWDATA_BUCKET", default=f"clickstream-deadletter-{PROJECT_ID}")
TEMPORARY_BUCKET = os.getenv("TEMPORARY_BUCKET", default=f"clickstream-tmp-{PROJECT_ID}")

# Bigquery Schemas
RAW = {
    "fields": [
        {"name": "session_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "geolocation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_agent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "events", "type": "RECORD", "mode": "REPEATED",
            "fields": [
                {"name": "event", "type": "RECORD", "mode": "NULLABLE",
                     "fields": [
                        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
                        {"name": "details", "type": "RECORD", "mode": "NULLABLE",
                             "fields": [
                                 {"name": "page_url", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "referrer_url", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
                                 {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
                                 {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
                                 {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "items", "type":"RECORD", "mode": "REPEATED",
                                      "fields": [
                                          {"name":"product_id", "type":"STRING", "mode": "NULLABLE"},
                                          {"name":"product_name", "type":"STRING", "mode": "NULLABLE"},
                                          {"name":"category", "type":"STRING", "mode": "NULLABLE"},
                                          {"name":"price", "type":"FLOAT", "mode": "NULLABLE"},
                                          {"name":"quantity", "type":"INTEGER", "mode": "NULLABLE"},
                                      ],
                                 },
                             ],
                        },
                     ],
                },
            ],
        },
    ],
}

SESSION = {
    "fields": [
        {"name": "session_key","type": "STRING","mode": "NULLABLE"},
        {"name": "session_id","type": "STRING","mode": "NULLABLE"},
        {"name": "user_id","type": "STRING","mode": "NULLABLE" },
        {"name": "user_agent","type": "STRING","mode": "NULLABLE"},
        {"name": "geolocation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_type","type": "STRING","mode": "NULLABLE"},
    ]
}

PAGEVIEW = {
    "fields": [
        {"name": "session_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "page_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referrer_url", "type": "STRING", "mode": "NULLABLE"},
    ]
}

ADDTOCART = {
    "fields": [
        {"name": "session_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}

PURCHASE = {
    "fields": [
        {"name": "session_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "items", "type": "RECORD", "mode": "REPEATED",
            "fields": [
                {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "product_id", "type": "STRING", "mode": "NULLABLE"}
            ]
        },
    ]
}

class Options(PipelineOptions):
    """ Options provide custom options for the batch pipeline execution. """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--src-bucket", default=f"gs://{RAWDATA_BUCKET}",
                            help="The data bucket to read from")
        parser.add_argument("--temp-bucket", default=f"gs://{TEMPORARY_BUCKET}",
                            help="The temporary bucket to write artifacts into")
        parser.add_argument("--lake-dataset", default=f"{PROJECT_ID}:clickstream_lake",
                            help="The dataset to be used for the Data Lake")
        parser.add_argument("--dw-dataset", default=f"{PROJECT_ID}:clickstream_dw",
                            help="The dataset to be used for the Data Wharehouse")
        parser.add_argument("--deadletter-bucket", default=f"gs://{DEADLETTER_BUCKET}",
                            help="The bucket where to store unparseable data and the errors")
        parser.add_argument("--force-recreate", action="store_true",
                            help="Delete the tables before running the Pipeline in order to recreate them.")

def parse_json(line):
    return json.loads(line)

def new_session_key(session):
    """
    session_key calculates a unique session key from the session info.

    Due to possible data duplication, we are using the tuple (session_id, user_id, device)
    as input to SHA256 hash function, and the resulting hex representation is the session_key.
    """
    data = [
        session["session_id"],
        session["user_id"],
        session["device_type"]
    ]
    return hashlib.sha256(":".join(data).encode("utf-8")).hexdigest()

def parse_event(session_key, session_data):
    """
    parse_event will unnest the data from the raw schema into the proper event type schema.

    It returns 'invalid', raw_data in case the event type is not a known one.
    """
    event = session_data["event"]
    details = event["details"]

    if event["event_type"] == "page_view":
        return "page_view", {
            "session_key": session_key,
            "timestamp": event["timestamp"],
            "page_url": details["page_url"],
            "referrer_url": details["referrer_url"]
        }
    elif event["event_type"] == "add_item_to_cart":
        return "add_item_to_cart", {
            "session_key": session_key,
            "timestamp": event["timestamp"],
            "product_id": details["product_id"],
            "product_name": details["product_name"],
            "category": details["category"],
            "price": details["price"],
            "quantity": details["quantity"]
        }
    elif event["event_type"] == "purchase":
        #TODO(ronoaldo): fix the amount rounding error from the input
        return "purchase", {
            "session_key": session_key,
            "timestamp": event["timestamp"],
            "order_id": details["order_id"],
            "amount": details["amount"],
            "currency": details["currency"],
            "items": details["items"],
        }
    else:
        "invalid", event

def parse_sessions(line):
    """
    parse_sessions will parse all the session data received as a JSONL string.

    The sessions will be output into a tagged output called 'sessions'.
    Additionally, each event will be output into separeted pcollections, according to the event type.

    Invalid events or unknown event_types will be rejected into the _invalid output pcollection.
    """
    try:
        # Convert the raw session to JSON and yield the session table row
        session = json.loads(line)
        session_key = new_session_key(session)
        yield {
            "session_key": session_key,
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "device_type": session["device_type"],
            "geolocation": session["geolocation"],
            "user_agent": session["user_agent"],
        }
    except Exception as e:
        yield TaggedOutput("invalid", f"error={e}; line={line}")
        # Potentially invalid JSON found, so let's ignore the record entirely and return
        return

    # For each event, emit the appropriate event by type
    for session_data in session["events"]:
        try:
            event_type, event = parse_event(session_key, session_data)
            yield TaggedOutput(event_type, event)
        except Exception as e:
            yield TaggedOutput("invalid", f"error={e}, session_data={session_data}")

class RecreateTable(beam.PTransform):
    """
    Custom PTransform to overwrite a Bigquery table.

    Defaults to partition by montly timestamp and no clustering.
    """
    def __init__(self, table=None, schema=None, partition_by="timestamp", partition_type="MONTH", cluster_by=None):
        self.table = table
        self.schema = schema

        self.bq_params = {}
        if partition_by is not None:
            self.bq_params["timePartitioning"] = {"type": partition_type, "field": partition_by}
        if cluster_by is not None:
            cluster_by = cluster_by if isinstance(cluster_by, (list, tuple)) else [cluster_by]
            self.bq_params["clustering"] = {"fields": cluster_by}

    def default_label(self):
        table_name = self.table.split(".")[-1]
        return str(f"{self.__class__.__name__}_{table_name}")

    def expand(self, pcoll):
        return pcoll | WriteToBigQuery(
            table=self.table,
            schema=self.schema or "SCHEMA_AUTODETECT",
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            method="FILE_LOADS",
            additional_bq_parameters=self.bq_params
        )

def delete_tables(table_ids=[]):
    LOG = logging.getLogger("batch_pipeline")

    client = bigquery.Client()
    for table_id in table_ids:
        LOG.info("Removing %s ...", table_id)
        try:
            client.delete_table(table_id)
        except google.api_core.exceptions.NotFound:
            pass

def run_pipeline(args):
    """
    run_pipeline initializes the pipeline and executes according to the arguments provided.
    """
    LOG = logging.getLogger("batch_pipeline")
    LOG.setLevel(logging.DEBUG)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    LOG.info("Parsing arguments ...")
    opts = Options(flags=args)
    # Let's simplify the launching args and set a default temp_bucket when running the job
    opts.view_as(GoogleCloudOptions).temp_location = f"{opts.temp_bucket}/{timestamp}/"

    # This is required to avoid NameError when running on Dataflow.
    # Ref: https://cloud.google.com/dataflow/docs/guides/common-errors#name-error
    opts.view_as(SetupOptions).save_main_session = True

    # Also, let's prepare the deadletter timestamped folder
    dead_letter_folder = f"{opts.deadletter_bucket}/{timestamp}/"
    LOG.info("Parsed arguments %s", str(opts))

    # Initializes the pipeline
    LOG.info("Preparing the pipeline graph ...")
    pipeline = beam.Pipeline(options=opts)

    # Read from Cloud Storage
    json_lines = pipeline | "LoadJSONFiles" >> ReadFromText(opts.src_bucket + "/*.jsonl")

    # Load the raw data after parsing into a raw bucket for our data lake
    json_lines | "ParseRawAsJSON" >> beam.Map(parse_json) | RecreateTable(
        table=f"{opts.lake_dataset}.visits",
        schema=RAW,
        cluster_by=None,
        partition_by=None)

    # Parse the session data into multiple tagged outputs
    parsed = json_lines | "ParseSessionData" >> beam.ParDo(parse_sessions).with_outputs(
        "page_view", "add_item_to_cart", "purchase", "invalid", main="session"
    )

    # Send errors to dead letter bucket
    parsed.invalid | "InvalidToDeadLetter" >> WriteToText(dead_letter_folder)

    # Store the sessions
    parsed.session | RecreateTable(
        table=f"{opts.dw_dataset}.sessions",
        schema=SESSION,
        partition_by=None,
        cluster_by=["session_id", "user_id"])

    # Store the events
    parsed.page_view | RecreateTable(
        table=f"{opts.dw_dataset}.pageview_events",
        schema=PAGEVIEW,
        cluster_by="session_key")
    parsed.add_item_to_cart | RecreateTable(
        table=f"{opts.dw_dataset}.addtocart_events",
        schema=ADDTOCART,
        cluster_by="session_key")
    parsed.purchase | RecreateTable(
        table=f"{opts.dw_dataset}.purchase_events",
        schema=PURCHASE,
        cluster_by="session_key")

    # If drastic schema changes are needed, the disposition of CREATE_IF_NEEDED does not
    # forcibly recreate the table. So if we change the clustering keys, the easier way is
    # to delete then recreate the table.
    if opts.force_recreate == True:
        LOG.info("force_recreate: removing removing old versions of tables")
        delete_tables(table_ids=[
            f"{opts.lake_dataset.replace(':','.')}.visits",
            f"{opts.dw_dataset.replace(':','.')}.sessions",
            f"{opts.dw_dataset.replace(':','.')}.pageview_events",
            f"{opts.dw_dataset.replace(':','.')}.addtocart_events",
            f"{opts.dw_dataset.replace(':','.')}.purchase_events"])

    # Run the pipeline
    LOG.info("Launching the pipeline ...")
    result = pipeline.run()
    result.wait_until_finish()
    LOG.info("Execution finished.")
    return result

if __name__ == "__main__":
    result = run_pipeline(sys.argv[1:])
    print(result)
