#!/usr/bin/env bash
set -e
set -x

PROJECT_ID="$(gcloud config get-value project)"
DAGS_FOLDER="$(gcloud composer environments describe clickstream-composer --format='value(config.dagGcsPrefix)')"
gcloud storage cp -v batch_pipeline_dag.py $DAGS_FOLDER
