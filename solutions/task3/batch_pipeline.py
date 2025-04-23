#!/usr/bin/env python3

import os
import sys
import logging
import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from clickstream.gcp import delete_tables, create_bucket_if_needed
from clickstream.schemas import RAW, SESSION, PAGEVIEW, ADDTOCART, PURCHASE
from clickstream.transforms import RecreateTable, ParseSessionsFn, ParseJSONfn

# Global settings
PROJECT_ID = os.getenv("PROJECT_ID")

# Bucket names derive from PROJECT_ID environment variable
RAWDATA_BUCKET = os.getenv("RAWDATA_BUCKET", default=f"clickstream-raw-{PROJECT_ID}")
DEADLETTER_BUCKET = os.getenv("DEADLETTER_BUCKET", default=f"clickstream-deadletter-{PROJECT_ID}")
TEMPORARY_BUCKET = os.getenv("TEMPORARY_BUCKET", default=f"clickstream-temp-{PROJECT_ID}")

class Options(PipelineOptions):
    """ Options provide custom options for the batch pipeline execution. """
    @classmethod
    def _add_argparse_args(cls, parser):
        # Buckets used by the pipeline
        parser.add_argument("--src-bucket", default=f"gs://{RAWDATA_BUCKET}",
                            help="The data bucket to read from")
        parser.add_argument("--temp-bucket", default=f"gs://{TEMPORARY_BUCKET}",
                            help="The temporary bucket to write artifacts into")
        parser.add_argument("--deadletter-bucket", default=f"gs://{DEADLETTER_BUCKET}",
                            help="The bucket where to store unparseable data and the errors")
        # Datasets used by the pipeline
        parser.add_argument("--lake-dataset", default=f"{PROJECT_ID}:clickstream_lake",
                            help="The dataset to be used for the Data Lake")
        parser.add_argument("--dw-dataset", default=f"{PROJECT_ID}:clickstream_dw",
                            help="The dataset to be used for the Data Wharehouse")
        # Helper flags
        parser.add_argument("--force-recreate", action="store_true",
                            help="Delete the tables before running the Pipeline in order to recreate them.")

def run_pipeline(args):
    """
    run_pipeline initializes the pipeline and executes according to the arguments provided.
    """
    LOG = logging.getLogger("clickstream")
    LOG.setLevel(logging.DEBUG)
    #logging.getLogger().setLevel(logging.INFO)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    LOG.info("Parsing arguments ...")
    opts = Options(flags=args)
    
    # Let's simplify the launching args and set a default temp_bucket when running the job
    opts.view_as(GoogleCloudOptions).temp_location = f"{opts.temp_bucket}/{timestamp}/"

    # Also, let's prepare the deadletter timestamped folder
    dead_letter_folder = f"{opts.deadletter_bucket}/{timestamp}/"
    LOG.info("Parsed arguments %s", str(opts))

    # Check if all buckets exist
    create_bucket_if_needed(opts.src_bucket)
    create_bucket_if_needed(opts.temp_bucket)
    create_bucket_if_needed(opts.deadletter_bucket)

    # Initializes the pipeline
    LOG.info("Preparing the pipeline graph ...")
    pipeline = beam.Pipeline(options=opts)

    # Read from Cloud Storage
    json_lines = pipeline | "LoadJSONFiles" >> ReadFromText(opts.src_bucket + "/*.jsonl")

    # Load the raw data after parsing into a raw bucket for our data lake
    json_lines | "ParseRawAsJSON" >> beam.Map(ParseJSONfn) | RecreateTable(
        table=f"{opts.lake_dataset}.visits",
        schema=RAW,
        cluster_by=None,
        partition_by=None)

    # Parse the session data into multiple tagged outputs
    parsed = json_lines | "ParseSessionData" >> beam.ParDo(ParseSessionsFn).with_outputs(
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
    print("*** Pipeline result = ", result.state, "***")
    try:
        from pprint import pprint
        pprint(result.metrics())
    except:
        # Worker may not support metrics() method call
        pass
