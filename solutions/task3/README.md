# DSL Clickstream - Task 3

This is another aproach for the code building and execution.
While Jupyter is great for experimentation, as we are now writing a
good chunck of code, and some parts of it can be reused, I opted to
properly organize the code from Task 2 into a more structured module
and allow for a more robust evolution of the project.

## Initial refactor of batch_pipeline.py

The fist refactor was to implement the code in a such a way there we
can more easily reuse the parsing and processing of raw view data.

The entrypoint is the same, however the code is now hosted under the
'clickstream' package.

To run a test on localhost (it will actually load data into BigQuery, tho):

```bash
export PROJECT_ID="$(gcloud config get-value project)"
python3 batch_pipeline.py
```

To run the pipeline on Google Cloud Dataflow:

```bash
export PROJECT_ID="$(gcloud config get-value project)"
python3 batch_pipeline.py \
    --runner=DataflowRunner \
    --setup=./setup.py \
    --project=$PROJECT_ID
```

With this aproach all the reusable dependencies will be packaged with the
pipeline artifacts and be made available to all workers on Cloud Dataflow,
as documented [here](https://cloud.google.com/dataflow/docs/guides/manage-dependencies#python-define-dependencies)