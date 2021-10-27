from kfp.v2.google.client import AIPlatformClient

api_client = AIPlatformClient(project_id='389886591986',
                              region='us-central1')

pipeline_run_name = api_client.create_run_from_job_spec(
    job_spec_path='gs://pawan-vertex-demo/test_pipeline.json',
    pipeline_root='gs://pawan-vertex-demo/test_pipeline')

