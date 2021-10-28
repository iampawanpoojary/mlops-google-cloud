import kfp
from kfp.v2 import compiler
from kfp.v2.google.client import AIPlatformClient
from google.cloud import aiplatform
from google_cloud_pipeline_components import aiplatform as gcc_aip

project_id = 'mlop-cg-data-and-insights'
region = 'us-central1'
pipeline_root_path = 'gs://pawan-vertex-demo/test_pipeline'

@kfp.dsl.pipeline(
    name="automl-image-training",
    pipeline_root=pipeline_root_path)
def pipeline(project_id: str):
    ds_op = gcc_aip.ImageDatasetCreateOp(
        project=project_id,
        display_name="flowers",
        gcs_source="gs://cloud-samples-data/vision/automl_classification/flowers/all_data_v2.csv",
        import_schema_uri=aiplatform.schema.dataset.ioformat.image.single_label_classification,
    )

    training_job_run_op = gcc_aip.AutoMLImageTrainingJobRunOp(
        project=project_id,
        display_name="train-iris-automl-mbsdk-1",
        prediction_type="classification",
        model_type="CLOUD",
        base_model=None,
        dataset=ds_op.outputs["dataset"],
        model_display_name="iris-classification-model-mbsdk",
        training_fraction_split=0.6,
        validation_fraction_split=0.2,
        test_fraction_split=0.2,
        budget_milli_node_hours=8000,
    )

    endpoint_op = gcc_aip.ModelDeployOp(
        project=project_id, model=training_job_run_op.outputs["model"]
    )


compiler.Compiler().compile(pipeline_func=pipeline,
        package_path='image_classification_pipeline.json')

