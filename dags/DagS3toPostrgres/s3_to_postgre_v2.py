from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime
import boto3
import pandas as pd
import pyarrow.parquet as pq
import os
from io import BytesIO
from airflow.hooks.postgres_hook import PostgresHook

default_args={
    'owner': 'sayuri.matsumoto',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 7, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
 
def read_from_s3_and_save_in_csv():
    aws_access_key_id=''
    aws_secret_access_key=''
    aws_session_token=''

    bucket_name='041334882807-trab4-target'
    file_name='Resultados/tabela_final/'

    s3_client=boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    file_objects=s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_name)['Contents']
    dfs=[]

    for file_object in file_objects:
        file_key=file_object['Key']
        file_obj=s3_client.get_object(Bucket=bucket_name, Key=file_key)
        parquet_file=pq.ParquetFile(BytesIO(file_obj['Body'].read()))
        df=parquet_file.read().to_pandas()
        dfs.append(df)
    df_tabela_final = pd.concat(dfs)
    df_tabela_final.to_csv('tabela_final.csv',index=False, header=False, sep=';')

def save_to_postgre():
    db_hook = PostgresHook(postgres_conn_id='postgres_con')
    db_conn = db_hook.get_conn()
    db_cursor = db_conn.cursor()

    table = 'tabela_final'
    csv_file='tabela_final.csv'

    with open(csv_file, 'r+') as f_output:
        db_cursor.copy_from(f_output,table,sep=';')
        db_conn.commit()

def deleteCSV (file_csv) :
    os.remove(file_csv)

with DAG(
    dag_id="s3_to_postgre_v2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    read_s3_save_csv=PythonOperator(
        task_id='read_s3_and_save_csv',
        python_callable=read_from_s3_and_save_in_csv,
    )

    created_table=PostgresOperator(
        task_id='created_tabela_final',
        postgres_conn_id='postgres_con',
        sql="tabela_final.sql",
    )

    csv_to_postgre=PythonOperator(
        task_id='csv_to_postgre',
        python_callable=save_to_postgre,
    )

    delete_csv = PythonOperator(
        task_id= "delete_csv_temp",
        python_callable=deleteCSV,
            op_kwargs={'file_csv': 'tabela_final.csv' },
            dag=dag
    )

    created_table >> csv_to_postgre
    read_s3_save_csv >> csv_to_postgre >> delete_csv
