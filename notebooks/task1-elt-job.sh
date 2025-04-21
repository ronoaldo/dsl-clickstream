#!/usr/bin/env bash
set -e

# The project ID to load the data is assumed to be the same where the job runs
# Also, the host running the job must provide credentials
PROJECT_ID="$(gcloud config get-value project)"

# The raw data bucket to load data from.
# This is assumed to be the source bucket where events are currently saved
# For the exercise, this is the copy of the exercise provided data
RAW_DATA_BUCKET="clickstream-raw-${PROJECT_ID}"

# The Bigquery data lake with the raw data, with the auto-detected
# schema from Bigquery
LAKE_DATASET="${PROJECT_ID}:clickstream_lake"
# Helper variable to run the queries easier
LAKE_DATASET_FROM="${LAKE_DATASET/:/.}"

# The Bigquery Data Wharehouse where our start schema is loaded
DW_DATASET="${PROJECT_ID}:clickstream_dw"

# Helper function to log with timestamp, useful to inspect the load job itself.
function log() {
  echo "[INFO] elt-job: $(date '+%Y-%m-%d %H:%M:%S'): $@"
}

# Data Lake
log "Building data lake into ${LAKE_DATASET}"
bq ls ${LAKE_DATASET} 2>/dev/null || bq mk ${LAKE_DATASET}
bq load \
  --source_format=NEWLINE_DELIMITED_JSON \
  --replace=true \
  --autodetect=true \
  ${LAKE_DATASET}.visits gs://${RAW_DATA_BUCKET}/*.jsonl

# Data Wharehouse
log "Building data wharehouse into ${DW_DATASET}"
bq ls ${DW_DATASET} 2>/dev/null || bq mk ${DW_DATASET}

log "Creating fact table ${DW_DATASET}.sessions ..."
bq query \
  --use_legacy_sql=false \
  --destination_table=${DW_DATASET}.sessions \
  --replace=true \
  --clustering_fields=session_id,user_id \
  --max_rows=0 \
  "SELECT session_id, user_id, device_type, geolocation, user_agent FROM \`${LAKE_DATASET_FROM}.visits\`"

log "Creating fact table ${DW_DATASET}.pageview_events ..."
bq query \
  --use_legacy_sql=false \
  --destination_table=${DW_DATASET}.pageview_events \
  --replace=true \
  --time_partitioning_field=timestamp \
  --time_partitioning_type=MONTH \
  --clustering_fields=session_id \
  --max_rows=0 <<SQL
SELECT
  v.session_id as session_id,
  v.user_id as user_id,
  e.event.timestamp as timestamp,
  e.event.details.page_url as page_url,
  e.event.details.referrer_url as referrer_url
FROM \`${LAKE_DATASET_FROM}.visits\` v,
UNNEST(v.events) e
WHERE e.event.event_type = 'page_view'
SQL

log "Creating fact table ${DW_DATASET}.addtochart_events ..."
bq query \
  --use_legacy_sql=false \
  --destination_table=${DW_DATASET}.addtochart_events \
  --replace=true \
  --time_partitioning_field=timestamp \
  --time_partitioning_type=MONTH \
  --clustering_fields=session_id \
  --max_rows=0 <<SQL
SELECT
  v.session_id as session_id,
  v.user_id as user_id,
  e.event.timestamp as timestamp,
  e.event.details.product_id as product_id,
  e.event.details.product_name as product_name,
  e.event.details.category as category,
  e.event.details.price as price,
  e.event.details.quantity as quantity
FROM \`${LAKE_DATASET_FROM}.visits\` v,
UNNEST(v.events) e
WHERE e.event.event_type = 'add_item_to_cart'
SQL

log "Creating fact table ${DW_DATASET}.purchase_events ..."
bq query \
  --use_legacy_sql=false \
  --destination_table=${DW_DATASET}.purchase_events \
  --replace=true \
  --time_partitioning_field=timestamp \
  --time_partitioning_type=MONTH \
  --clustering_fields=session_id \
  --max_rows=0 <<SQL
SELECT
  v.session_id as session_id,
  v.user_id as user_id,
  e.event.timestamp as timestamp,
  e.event.details.order_id as order_id,
  ROUND(e.event.details.amount, 2) as amount,
  e.event.details.currency as currency,
  e.event.details.items as items
FROM \`${LAKE_DATASET_FROM}.visits\` v,
UNNEST(v.events) e
WHERE e.event.event_type = 'purchase'
SQL
