from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG('copy_s3_to_local',
          default_args=default_args,
          description='Copy file from S3 to local directory',
          schedule_interval='@once',
          catchup=False)

s3_bucket = 'nyc-tlc'
s3_key = 'trip data/'
local_path = '/home/ec2-user/767398038964-raw/raw'  # Update with your local path

copy_file_task = BashOperator(
    task_id='copy_s3_file_to_local',
    bash_command=f'aws s3 cp s3://{s3_bucket}/{s3_key} {local_path} --recursive',
    dag=dag
)

copy_file_task
