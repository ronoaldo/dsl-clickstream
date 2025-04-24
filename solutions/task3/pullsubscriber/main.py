import os
import subprocess
import logging
import google
import argparse

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from clickstream.subscription import stream_events_to_bq

from flask import Flask, jsonify
app = Flask(__name__)

logging.basicConfig()
LOG = logging.getLogger("clickstream.pullsubscriber")
LOG.setLevel(logging.INFO)

def detect_project_id():
    project_id = os.getenv("PROJECT_ID")
    if project_id == "" or project_id is None:
        gcloud = subprocess.run(["gcloud", "config", "get-value", "project"], capture_output=True)
        project_id = gcloud.stdout.decode("utf-8").strip()
    return project_id

PROJECT_ID = detect_project_id()
TOPIC_ID = "clickstream-dev"
SUBSCRIPTION_ID = "pullsubscriber"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def create_subscription_if_needed():
    try:
        s = subscriber.get_subscription(request={"subscription": subscription_path})
    except google.api_core.exceptions.NotFound as e:
        LOG.info("Error fetching subscription: %s [%s]", str(e), type(e))
        s = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path
            }
        )
    LOG.info("Subscription ready to receive messages: %s", s.name)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    LOG.info(f"Received {message}.")
    stream_events_to_bq(message.data.decode("utf-8"))
    message.ack()

def run_consumer():
    LOG.info("Checking for the subscription on %s ...", PROJECT_ID)
    create_subscription_if_needed()

    LOG.info("Starting pull subscriber ...")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        LOG.info("Pulling for messages on topic %s using %s...", topic_path, subscription_path)
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

@app.route("/healthz")
def health():
    """
    health will monitor if the other process is running and return 200 OK if it is.
    It returns 500 if not and will cause the health check to fail ant the VM to be restarted.
    """
    ps = subprocess.run(["bash", "-c", "ps faux | grep 'consumer' | grep -v grep"], capture_output=True)
    output = f"stdout={ ps.stdout.decode('utf-8') }; stderr={ ps.stderr.decode('utf-8') }"
    
    if b"python3" in ps.stdout and b"main.py" in ps.stdout and b"--consumer" in ps.stdout:
        return jsonify({"status": "ok"})
    return jsonify({"status": f"exit code: {ps.returncode}; output: {output}"}), 500

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumer", action="store_true")
    opts = parser.parse_args()

    if opts.consumer:
        # Run as --consumer
        run_consumer()
    else:
        # Spawn the consumer
        consumer = subprocess.Popen(["python3", "main.py", "--consumer"])
        # Run the health endpoint
        try:
            app.run(host="0.0.0.0", port=8080)
        except:
            consumer.kill()
