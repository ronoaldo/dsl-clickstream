#!/usr/bin/env bash

python3 -m venv .venv
source .venv/bin/activate
pip install pip wheel setuptools build -u
pip install apache-beam[gcp]
