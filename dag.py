from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.hooks.datafusion import PipelineStates

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('employee_data',
          default_args=default_args,
          description='Automates employee ETL pipeline',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='generate_extract_fake_data',
        bash_command='python /home/airflow/gcs/data/script/new_extract.py',
    )

# Pipeline needs to be created in CDF (GCS -> Wrangler -> BQ)

    start_pipeline = CloudDataFusionStartPipelineOperator(
    location="us-central1",
    pipeline_name="etl-emp-job",
    instance_name="datafusion-dev",
    task_id="start_datafusion_pipeline",
    success_states=[PipelineStates.COMPLETED], # https://stackoverflow.com/a/72473362
    pipeline_timeout=3600,  # in seconds, default is currently 300 seconds
    )

    run_script_task >> start_pipeline