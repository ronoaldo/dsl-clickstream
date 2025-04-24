import base64
import functions_framework

from cloudevents.http import CloudEvent
from clickstream.subscription import stream_events_to_bq

@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent):
    visit_json = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    stream_events_to_bq(visit_json)