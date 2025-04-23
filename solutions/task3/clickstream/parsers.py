"""
parsers module holds helper functions used to parse event data.
"""

import json
import hashlib

def new_session_key(session):
    """
    new_session_key calculates a unique session key from the session info.

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

def parse_session(line):
    """
    parse_session parses the provided JSON line and returns three values:
    * The tag key if an error happens, or None if no errors while parsing
    * The dictionary with the session basic data
    * The dictionary with the events this session holds
    """
    try:
        # Convert the raw session to JSON and yield the session table row
        session = json.loads(line)
        session_key = new_session_key(session)
        session_dict = {
            "session_key": session_key,
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "device_type": session["device_type"],
            "geolocation": session["geolocation"],
            "user_agent": session["user_agent"],
        }
        events = session["events"]
        return None, session_dict, events
    except Exception as e:
        return "invalid", f"error={e}; line={line}", None