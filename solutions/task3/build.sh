#!/usr/bin/env bash

export PROJECT="$(gcloud config get-value project)"
export IMAGE="gcr.io/${PROJECT_ID}/dataflow/batch_pipeline:latest"
export TEMPLATE_FILE="gs://dataflow-templates-${PROJECT_ID}/batch_pipeline.json"

docker build -t ${IMAGE} .
docker push ${IMAGE}
gcloud dataflow flex-template build ${TEMPLATE_FILE} \
    --image ${IMAGE} \
    --sdk-language PYTHON \
    --metadata-file=metadata.json \
    --project $PROJECT
