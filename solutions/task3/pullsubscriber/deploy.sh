#!/usr/bin/env bash
set -e

BASEDIR=$(dirname $0)
BASEDIR=$(readlink -f $BASEDIR)
cd $BASEDIR

PROJECT_ID="$(gcloud config get-value project)"
IMAGE=gcr.io/bill-arki1-25-4/clickstream/pullsubscriber:latest

function update_vendor() {
    # In order to make it easier to use reuse the code for parsing events
    # from our shared library and avoid having to build/deploy to artifact registry
    # let's use the vendoring aproach to deploy to cloud functions as described here:
    # https://cloud.google.com/run/docs/runtimes/python-dependencies#create_copied_dependencies
    mkdir -p vendor
    rm -rf vendor/*

    pushd ../
        python3 setup.py clean && rm -rf dist
        python3 setup.py sdist
        cp dist/clickstream*.tar.gz -d pullsubscriber/vendor/
    popd
}

update_vendor
docker build -t ${IMAGE} .
docker push ${IMAGE}

# TODO(ronoaldo): implement the cloud deployment on Compute Engine either using Docker or another aproach.
# Create an instance template using our container image
# Create a health check for the VM instance that shows our system is running
# Create the instance group for the VM using our container
