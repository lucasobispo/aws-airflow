from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


def copy_all_from_s3_to_s3(source_bucket, source_key, dest_bucket, dest_key_prefix, aws_conn_id='aws_lab'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # Lista todos os objetos no bucket/prefixo de origem
    source_objects = s3_hook.list_keys(bucket_name=source_bucket, prefix=source_key)

    if source_objects is None:
        print("No objects found in source bucket/prefix")
        return

    for source_object_key in source_objects:
        # Cria o novo key para o objeto no bucket de destino
        # Isso assume que você quer manter a estrutura de diretórios
        dest_object_key = f"{dest_key_prefix}/{source_object_key.split('/')[-1]}"

        # Usa o método copy do S3Hook para copiar o objeto diretamente entre buckets
        s3_hook.copy_object(source_bucket_key=source_object_key,
                            dest_bucket_key=dest_object_key,
                            source_bucket_name=source_bucket,
                            dest_bucket_name=dest_bucket)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    's3_to_s3_copy_all',
    default_args=default_args,
    description='Copy all files from one S3 bucket/prefix to another',
    schedule_interval='@once',
    catchup=False,
)

copy_all_task = PythonOperator(
    task_id='copy_all_s3_to_s3',
    python_callable=copy_all_from_s3_to_s3,
    op_kwargs={
        'source_bucket': 'nyc-tlc',
        'source_key': 'trip data/',  # Use source-prefix/ to copy everything under this prefix
        'dest_bucket': '767398038964-raw',
        'dest_key_prefix': 'raw',  # Prefix under which to store copied objects in the destination bucket
    },
    dag=dag,
)