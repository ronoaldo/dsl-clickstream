#!/usr/bin/env python3

import os
import json
import time
import datetime
import uuid
import random
import logging
import argparse
from pprint import pprint
from faker import Faker
from google.cloud import pubsub_v1

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC = "clickstream-dev"
MIN_DELAY = 0.200

PRODUCTS = [
    { "product_id": "HDW-001", "product_name": "Laptop X200", "category": "hardware", "price": 999.99 },
    { "product_id": "HDW-002", "product_name": "Desktop Z500", "category": "hardware", "price": 1299.99 },
    { "product_id": "HDW-003", "product_name": "Gaming PC Y900", "category": "hardware", "price": 1899.99 },
    { "product_id": "HDW-004", "product_name": "Ultrabook A400", "category": "hardware", "price": 1199.99 },
    { "product_id": "HDW-005", "product_name": "Workstation Pro 9000", "category": "hardware", "price": 2599.99 },
    { "product_id": "HDW-006", "product_name": "Mini PC Cube", "category": "hardware", "price": 699.99 },
    { "product_id": "PER-001", "product_name": "Wireless Mouse", "category": "peripherals", "price": 29.99 },
    { "product_id": "PER-002", "product_name": "Mechanical Keyboard", "category": "peripherals", "price": 89.99 },
    { "product_id": "PER-003", "product_name": "27\" 4K Monitor", "category": "peripherals", "price": 399.99 },
    { "product_id": "PER-004", "product_name": "USB-C Docking Station", "category": "peripherals", "price": 129.99 },
    { "product_id": "PER-005", "product_name": "Noise Cancelling Headphones", "category": "peripherals", "price": 199.99 },
    { "product_id": "PER-006", "product_name": "Webcam HD 1080p", "category": "peripherals", "price": 49.99 },
    { "product_id": "SFT-001", "product_name": "Office Suite Pro", "category": "software", "price": 199.99 },
    { "product_id": "SFT-002", "product_name": "Antivirus Shield", "category": "software", "price": 49.99 },
    { "product_id": "SFT-003", "product_name": "Photo Editor Pro", "category": "software", "price": 79.99 },
    { "product_id": "SFT-004", "product_name": "Project Manager Plus", "category": "software", "price": 299.99 },
    { "product_id": "SFT-005", "product_name": "Video Editor Pro", "category": "software", "price": 149.99 },
    { "product_id": "SFT-006", "product_name": "Music Studio 2024", "category": "software", "price": 89.99 },
]

logging.basicConfig()
LOG = logging.getLogger("clickstream.simulator")

def url(path):
    return f"https://www.example.com{path}"

class Clock(object):
    dt: datetime.datetime

    def __init__(self):
        self.dt = datetime.datetime.now()

    def tick(self):
        passed = datetime.timedelta(seconds=random.randint(5, 45))
        self.dt = self.dt + passed
        return self.dt.isoformat()

class VisitSimulator(object):
    """
    VisitSimulator creates a set of random visits.
    """

    visits_per_minute: int

    def __init__(self, visits_per_minute=10):
        self.visits_per_minute = visits_per_minute
        self.clock = Clock()
        self.message_count = 0

    def new_pageview(self, url=url("/"), referrer=""):
        return {
            "event": {
                "timestamp": self.clock.tick(),
                "event_type": "page_view",
                "details": {
                    "page_url": url,
                    "referrer_url": referrer,
                }
            }
        }

    def new_addtocart(self, item):
        return {
            "event": {
                "timestamp": self.clock.tick(),
                "event_type": "add_item_to_cart",
                "details": {
                    "product_id": item["product_id"],
                    "product_name": item["product_name"],
                    "category": item["category"],
                    "price": item["price"],
                    "quantity": item["quantity"],
                }
            }
        }

    def new_purchase(self, added_to_cart):
        return {
            "event": {
                "timestamp": self.clock.tick(),
                "event_type": "purchase",
                "details": {
                    "order_id": f"ORD-{uuid.uuid4()}",
                    "amount": sum([item["price"] * item["quantity"] for item in added_to_cart]),
                    "currency": "USD",
                    "items": added_to_cart,
                }
            }
        }

    def new_visit(self):
        """
        new_visit creates a random visit and all nested fields.
        It simulates a user accessing the website
        """
        fake = Faker()
        self.clock = Clock()
        lat, lng = fake.latlng()

        visit = {
            "session_id": f"SID-{str(uuid.uuid4())}",
            "user_id": f"UID-{str(uuid.uuid4())}",
            "device_type": random.choices(["mobile", "desktop", "tablet"], [0.3, 0.4, 0.1])[0],
            "user_agent": fake.user_agent(),
            "geolocation": f"{lat},{lng}",
        }

        events = []
        events.append(self.new_pageview(url("/")))

        # Simulate some visits around the website
        num_page_views = random.randint(1, 5)
        for _ in range(num_page_views):
            page_url = random.choice([url("/"), url("/products"), url("/faq")])
            referrer_url = random.choice(["", url("/"), url("/products"), url("/faq")])
            events.append(self.new_pageview(page_url, referrer_url))

        # Simulate adding items to cart in products page
        added_to_cart = []
        if random.random() < 0.5:
            events.append(self.new_pageview(url("/products"), url("/")))
            num_items_to_add = random.randint(1, 3)
            for _ in range(num_items_to_add):
                item = random.choice(PRODUCTS)
                item["quantity"] = random.randint(1, 5)
                added_to_cart.append(item)
                events.append(self.new_addtocart(item))

        # Simulate purchase
        if random.random() < 0.5 and len(added_to_cart) > 0:
            events.append(self.new_purchase(added_to_cart))

        visit["events"] = events
        return visit

    def run(self):
        LOG.info("Initializing PublisherClient to PubSub...")
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC)
        self.simulation_start = datetime.datetime.now()

        delay = datetime.timedelta(seconds=60 / self.visits_per_minute)
        LOG.info("Simulation setup for %d visits per minute (delay=%ds)", self.visits_per_minute, delay.total_seconds())
        LOG.info("Sending messages ...")
        while True:
            if self.message_count % 10 == 0:
                avg_messages = float(self.message_count) / (datetime.datetime.now() - self.simulation_start).total_seconds()
                LOG.info("Total messages sent: %d / Troughput: %.02f messages/s", self.message_count, avg_messages)

            visit = self.new_visit()
            event_types = set(e["event"]["event_type"] for e in visit["events"])

            LOG.debug("Sending session %s with %d events [%s]", visit["session_id"], len(visit["events"]), event_types)

            start = datetime.datetime.now()
            future = publisher.publish(topic_path, json.dumps(visit).encode("utf-8"))
            LOG.debug("Result: %s", future.result())
            end = datetime.datetime.now()
            latency = end - start
            LOG.debug("Latency: %ss", latency.total_seconds())

            self.message_count += 1
            if delay > latency:
                remaining = delay-latency
                time.sleep(remaining.total_seconds())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--visits-per-minute", type=int, default=10)
    parser.add_argument("--verbose", action="store_true")
    opts = parser.parse_args()

    if opts.verbose:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)

    LOG.info("Initializing simulator ... ")
    s = VisitSimulator(visits_per_minute=opts.visits_per_minute)

    LOG.info("Simulating ...")
    s.run()
