#!/usr/bin/env bash

case $1 in
    "--local")
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
    ;;
    "--dataflow")
        gcloud dataflow flex-template run \
            dataflow-template-batch-pipeline \
            --template-file-gcs-location gs://dataflow-templates-bill-arki1-25-4/batch_pipeline.json \
            --region us-central1 \
            --additional-user-labels "" \
            --parameters src-bucket=gs://clickstream-raw-bill-arki1-25-4,temp-bucket=gs://clickstream-temp-bill-arki1-25-4,deadletter-bucket=gs://clickstream-deadletter-bill-arki1-25-4,lake-dataset=bill-arki1-25-4:clickstream_lake,dw-dataset=bill-arki1-25-4:clickstream_dw,sdk_container_image=gcr.io/bill-arki1-25-4/dataflow/batch_pipeline
    ;;
esac
