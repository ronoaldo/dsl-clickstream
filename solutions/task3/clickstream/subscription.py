"""
subscription module is a helper module to reuse the same implementation on both
pull and push subscribers.

"""

import logging

from clickstream.parsers import parse_session, parse_event
from clickstream.gcp import stream_to_bigquery

LOG = logging.getLogger("clickstream.subscription")

def stream_events_to_bq(line):
    tag, session, events = parse_session(line)
    if tag is not None:
        LOG.warning("Invalid session data received: %s", session)
        return
    
    session_id = session["session_id"]
    print(f"Streaming data for session {session_id}")
    stream_to_bigquery("clickstream_dw","sessions", [session])
    
    for session_data in events:
        try:
            event_type, event = parse_event(session["session_key"], session_data)
            table = "pageview_events"
            if event_type == "add_item_to_cart":
                table = "addtocart_events"
            elif event_type == "purchase":
                table = "purchase_events"
            stream_to_bigquery("clickstream_dw", table, [event])
        except Exception as e:
            LOG.warning("Failed to parse and ingest event: %s, %s", str(e), session_data)
