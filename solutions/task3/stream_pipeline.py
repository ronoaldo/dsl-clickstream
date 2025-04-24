#!/usr/bin/env python3

import os
import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import WriteToFiles, default_file_naming
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from  clickstream import schemas
from clickstream.transforms import ParseJSONFn, ParseSessionsFn, ExtractPageViewsFn, FormatEventCountFn, LogInfoFn

LOG = logging.getLogger("clickstream.stream_pipeline")
PROJECT_ID = os.getenv("PROJECT_ID")

class Options(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--src-topic", default=f"projects/{PROJECT_ID}/topics/clickstream-dev")
        parser.add_argument("--lake-bucket", default=f"gs://clickstream-raw-{PROJECT_ID}")
        parser.add_argument("--lake-write-interval", default=5*60, type=int,
                            help="The number of seconds to write data to Cloud Storage. Defaults to 5 minutes.")
        parser.add_argument("--dw-dataset", default=f"{PROJECT_ID}:clickstream_dw",
                            help="The dataset to be used for the Data Wharehouse")        

def run_pipeline(args):
    opts = Options(flags=args)
    pipeline = beam.Pipeline(options=opts)

    LOG.info("Building the pipeline graph (opts=%s)...", opts)
    
    # Read from PubSub
    messages = (
        pipeline
        | "PullFromPubSub" >> ReadFromPubSub(topic=opts.src_topic).with_output_types(bytes)
        | "DecodeBytesAsStrings" >> beam.Map(lambda b: b.decode("utf-8"))
    )

    # Write files to Cloud Storage
    # Here we use shards=1 and max_writers_per_bundle=0 to force fewer files per shard, otherwise
    # beam will go grazy and create one file per visit which can be a problem down the road.
    # Ref: https://stackoverflow.com/a/64325494
    interval_in_minutes = opts.lake_write_interval // 60
    (
        messages
        | f"Every{interval_in_minutes}Minute" >> beam.WindowInto(beam.window.FixedWindows(opts.lake_write_interval))
        | "WriteRawToGCS" >> WriteToFiles(
            path=opts.lake_bucket,
            file_naming=default_file_naming("visits", ".jsonl"),
            shards=1,
            max_writers_per_bundle=0)
    )

    # Parse lines and write to BigQuery
    parsed = messages | "ParseSessionData" >> beam.ParDo(ParseSessionsFn).with_outputs(
        "page_view", "add_item_to_cart", "purchase", "invalid", main="session"
    )
    
    # Store the session data in our Analytics start schema
    bqkwargs = {
        "write_disposition": beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
        "create_disposition": beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
    }
    (
        parsed.session
        | "SessionsToBQ" >> WriteToBigQuery(table=f"{opts.dw_dataset}.sessions", schema=schemas.SESSION, **bqkwargs)
    )
    (
        parsed.page_view
        | "PageViewsToBQ" >> WriteToBigQuery(table=f"{opts.dw_dataset}.pageview_events", schema=schemas.PAGEVIEW, **bqkwargs)
    )
    (
        parsed.add_item_to_cart
        | "AddToCartToBQ" >> WriteToBigQuery(table=f"{opts.dw_dataset}.addtocart_events", schema=schemas.ADDTOCART, **bqkwargs)
    )
    (
        parsed.purchase
        | "PurchaseToBQ" >> WriteToBigQuery(table=f"{opts.dw_dataset}.purchase_events", schema=schemas.PURCHASE, **bqkwargs)
    )

    # Calculate views per minute and write both to Bigquery (reporting) and Logs (analytics)
    pageviews_per_minute = (
        messages
        | "ParseRawASJson" >> beam.Map(ParseJSONFn)
        | "ExtractPageViews" >> beam.FlatMap(ExtractPageViewsFn)
        | "EveryMinute" >> beam.WindowInto(beam.window.FixedWindows(60))
        | "CountEventsPerMinute" >> beam.combiners.Count.PerElement()
        | "FormatPageViews" >> beam.ParDo(FormatEventCountFn())
    )
    pageviews_per_minute | "PPMToCloudLogging" >> beam.ParDo(LogInfoFn(msgformat="stats:pageviews:%s"))
    
    pageviews_per_minute | "PPMToBigQuery" >> WriteToBigQuery(
        table=f"{opts.dw_dataset}.realtime_events", schema=schemas.REALTIME_EVENTS, **bqkwargs
    )

    # Launch the pipeline
    LOG.info("Running the pipline...")
    result = pipeline.run()
    result.wait_until_finish()
    LOG.info("Pipeline finished.")
    return result

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    result = run_pipeline(sys.argv[1:])
    LOG.info("Result: %s", result.state)
