from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)


args = {
    'owner': 'Airflow',
    'start_date': days_ago(0),
    'depends_on_past': False
}

folders = ['dags', 'planning', 'quality']


def read_s3_data():
    s3_hook = S3Hook(aws_conn_id='aws_lab')
    bucket_name = '638059466675-source'
    file_name = 'Empregados/glassdoor_consolidado_join_match_less_v2.csv'
    data = s3_hook.read_key(key=file_name, bucket_name=bucket_name)
    print(data)


with DAG('s3_read_example_with_hook',
         default_args=args,
         schedule_interval=None
         ) as dag:
    # Only display - start_dag
    start_dag = DummyOperator(task_id="start_dag")

    read_s3_task = PythonOperator(
        task_id='read_s3_data',
        python_callable=read_s3_data,
    )
    job_flow_emr = []
    # Create the EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=job_flow_emr,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
    )