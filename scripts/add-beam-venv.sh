#!/usr/bin/env bash

python3 -m venv .venv
source .venv/bin/activate
pip install pip -u
pip install wheel apache-beam[gcp]
