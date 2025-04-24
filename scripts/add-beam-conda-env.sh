#!/usr/bin/env bash

# Remove if pre-existing
conda remove -n apache-beam --all

# Creates a conda environment.
conda create -n apache-beam -y
conda activate apache-beam

# Install packages using a pip local to the conda environment.
conda install pip wheel setuptools build pyyaml jsonschema -y
conda install apache-beam[gcp] -y
pip install apache-beam[gcp] -y

# Adds the conda kernel.
DL_ANACONDA_ENV_HOME="${DL_ANACONDA_HOME}/envs/apache-beam"
python -m ipykernel install --prefix "${DL_ANACONDA_ENV_HOME}" --name apache-beam --display-name "Apache Beam"