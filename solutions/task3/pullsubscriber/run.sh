#!/usr/bin/env bash

BASEDIR=$(dirname $0)
BASEDIR=$(readlink -f $BASEDIR)
cd $BASEDIR

PROJECT_ID="$(gcloud config get-value project)"

CREDENTIAL=$(readlink -f ${BASEDIR}/../../../.service-account.json)
ls $CREDENTIAL
docker run \
    -it \
    --rm \
    --name pullsubscriber \
    --volume ${CREDENTIAL}:/service-account.json \
    -e GOOGLE_APPLICATION_CREDENTIALS=/service-account.json \
    -e PROJECT_ID=${PROJECT_ID} \
    gcr.io/${PROJECT_ID}/clickstream/pullsubscriber:latest
