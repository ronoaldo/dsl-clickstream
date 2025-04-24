#!/usr/bin/env bash

BASEDIR=$(dirname $0)
BASEDIR=$(readlink -f $BASEDIR)
cd $BASEDIR

PROJECT_ID="$(gcloud config get-value project)"

CREDENTIAL=$(readlink -f ${BASEDIR}/../../.service-account.json)
ls $CREDENTIAL
docker run \
    -it \
    --rm \
	--entrypoint /bin/bash \
    --name batch_pipeline \
    --volume ${CREDENTIAL}:/service-account.json \
    -e GOOGLE_APPLICATION_CREDENTIALS=/service-account.json \
    -e PROJECT_ID=${PROJECT_ID} \
    gcr.io/${PROJECT_ID}/dataflow/batch_pipeline:latest
