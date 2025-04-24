FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:latest as template_launcher
FROM apache/beam_python3.12_sdk:2.64.0

COPY . /src/pipeline
WORKDIR /src/pipeline
RUN pip install . --root-user-action=ignore

# Configure the Flex Template here.
COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY batch_pipeline.py /template/
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/batch_pipeline.py"
