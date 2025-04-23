"""
Module gcp has some basic utilities for handling Google Cloud resources.
"""

import logging
import google
from google.cloud import bigquery, storage

LOG = logging.getLogger("clickstream.gcp")

def delete_tables(table_ids=[]):
    client = bigquery.Client()
    for table_id in table_ids:
        LOG.info("Removing %s ...", table_id)
        try:
            client.delete_table(table_id)
        except google.api_core.exceptions.NotFound:
            pass

def create_bucket_if_needed(bucket):
    # Sanitize bucket name
    if "gs://" in bucket:
        bucket = bucket.replace("gs://", "")
    
    client = storage.Client()
    try:
        LOG.info("Checking if bucket %s exists", bucket)
        client.get_bucket(bucket)
    except google.cloud.exceptions.NotFound:
        LOG.info("Bucket not found, creating bucket %s ...", bucket)
        client.create_bucket(bucket, enable_object_retention=False)
        LOG.info("Bucket %s created.", bucket)
