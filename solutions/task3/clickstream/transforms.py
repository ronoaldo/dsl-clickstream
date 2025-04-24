"""
Module transforms contains the Apache Beam pipeline utilities to read, parse and load data.
"""

import json
import apache_beam as beam
import logging

from clickstream.parsers import parse_session, parse_event
from apache_beam.pvalue import TaggedOutput
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

def ParseJSONFn(line):
    """
    ParseJSONfn is a small DoFn that wraps json.loads().
    """
    return json.loads(line)

def ExtractPageViewsFn(session):
    """
    Extract page views from the raw data and yield each of the "page_view"
    """
    events = session["events"]
    for l in events:
        event = l["event"]
        if event["event_type"] == "page_view":
            yield "page_view"

class FormatEventCountFn(beam.DoFn):
    """
    FormatEventCount expects elements in the format ('page_view', 1) and will format it as
    a BigQuey row in the expected schema for the realtime monitoring.
    """
    def process(self, element, window=beam.DoFn.WindowParam):
        event_type, count = element
        window_end = window.end.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S")
        yield {
            "event_type": event_type,
            "count": count,
            "timestamp": window_end
        }

def ParseSessionsFn(line):
    """
    ParseSessionsFn will parse all the session data received as a JSONL string.

    The sessions will be output into a tagged output called 'sessions'.
    Additionally, each event will be output into separeted pcollections, according to the event type.

    Invalid events or unknown event_types will be rejected into the _invalid output pcollection.
    """
    # Convert the raw session to JSON and yield the session table row
    tag, session, events = parse_session(line)
    if tag is not None:
        yield TaggedOutput(tag, session)
        return
    yield session

    # For each event, emit the appropriate event by type
    for session_data in events:
        try:
            event_type, event = parse_event(session["session_key"], session_data)
            yield TaggedOutput(event_type, event)
        except Exception as e:
            yield TaggedOutput("invalid", f"error={e}, session_data={session_data}")

class LogInfoFn(beam.DoFn):
    msgformat: str

    def __init__(self, msgformat="element:%s"):
        self.msgformat = msgformat

    def process(self, element):
        log = logging.getLogger("clickstream")
        log.info(self.msgformat, element)

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
