#!/usr/bin/env bash
set -e

BASEDIR=$(dirname $0)
BASEDIR=$(readlink -f $BASEDIR)
cd $BASEDIR

function update_vendor() {
    # In order to make it easier to use reuse the code for parsing events
    # from our shared library and avoid having to build/deploy to artifact registry
    # let's use the vendoring aproach to deploy to cloud functions as described here:
    # https://cloud.google.com/run/docs/runtimes/python-dependencies#create_copied_dependencies
    rm -rf vendor/*

    pushd ../
        python3 setup.py clean && rm -rf dist
        python3 setup.py sdist
        python3 -m pip install -e ./
        cp dist/clickstream*.tar.gz -d pushsubscriber/vendor/
    popd

    python3 -m pip download -r requirements.txt --only-binary=:all: \
    -d vendor \
    --python-version 312 \
    --platform manylinux2014_x86_64 \
    --implementation cp
}

case $1 in 
    --create-trigger)
        gcloud eventarc triggers create pushsubscriber-trigger \
            --location=${REGION} \
            --destination-run-service=pushsubscriber \
            --destination-run-region=${REGION} \
            --event-filters="type=google.cloud.pubsub.topic.v1.messagePublished" \
            --service-account=dataflow-dev-svc@bill-arki1-25-4.iam.gserviceaccount.com \
            --transport-topic=projects/bill-arki1-25-4/topics/clickstream-dev
    ;;
    *)
        update_vendor
        gcloud run deploy pushsubscriber \
            --source . \
            --function subscribe \
            --base-image python312 \
            --set-build-env-vars GOOGLE_VENDOR_PIP_DEPENDENCIES=vendor
        ;;
esac
