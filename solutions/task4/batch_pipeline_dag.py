"""
Batch Pipeline Launcher using Cloud Dataflow Flex Templates.

Based on the sample code from the documentation.
"""

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)

project_id = "{{var.value.project_id}}"
gce_region = "{{var.value.gce_region}}"

src_bucket = "{{var.value.src_bucket}}"
temp_bucket = "{{var.value.temp_bucket}}"
deadletter_bucket = "{{var.value.deadletter_bucket}}"

lake_dataset = "{{var.value.lake_dataset}}"
dw_dataset = "{{var.value.dw_dataset}}"

with models.DAG(
    "batch_pipeline_dag",
    schedule=None,
) as dag:
    start_template_job = DataflowStartFlexTemplateOperator(
        # The task id of your job
        body={
            "launchParameter": {
                "jobName": "airflow-job-batch-pipeline",
                "parameters":{
                    "src-bucket": src_bucket,
                    "temp-bucket": temp_bucket,
                    "deadletter-bucket": deadletter_bucket,
                    "lake-dataset": lake_dataset,
                    "dw-dataset": dw_dataset,
                    "sdk_container_image": "gcr.io/" + project_id + "/dataflow/batch_pipeline",
                },
                "containerSpecGcsPath": "gs://dataflow-templates-" + project_id + "/batch_pipeline.json"
            }
        },
        location=gce_region,
        project_id=project_id,
        task_id="dataflow_operator_batch_pipeline",
        wait_until_finished=True,
    )
