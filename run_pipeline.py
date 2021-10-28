from kfp.v2.google.client import AIPlatformClient


project_id = 'mlop-cg-data-and-insights'
region = 'us-central1'
pipeline_root_path = 'gs://pawan-vertex-demo/test_pipeline'

api_client = AIPlatformClient(project_id=project_id, region=region)

response = api_client.create_run_from_job_spec(
    'image_classification_pipeline.json',
    pipeline_root=pipeline_root_path,
    parameter_values={
        'project_id': project_id
    })