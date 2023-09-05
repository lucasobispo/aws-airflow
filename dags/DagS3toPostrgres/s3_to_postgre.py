from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime
import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from sqlalchemy import create_engine

default_args={
    'owner': 'sayuri.matsumoto',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 7, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
 
def read_from_s3():
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
    return pd.concat(dfs)

def save_to_rds(df):
    user='postgres'
    password='postgres'
    hostname='database-1.co3ixxuf4csr.us-east-1.rds.amazonaws.com:5432'
    database='postgres'
    table_name="tabela_final"

    conn_str=f'postgresql://{user}:{password}@{hostname}/{database}?sslmode=require'
    engine=create_engine(conn_str)

    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    engine.dispose()

def s3_to_postgre():
    df=read_from_s3()
    save_to_rds(df)

with DAG(
    dag_id="s3_to_postgre",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    s3_to_postgre=PythonOperator(
        task_id='s3_to_postgre',
        python_callable=s3_to_postgre,
    )

    created_table=PostgresOperator(
        task_id='created_tabela_fina',
        postgres_conn_id='postgres_con',
        sql="tabela_final.sql",
    )

    created_table >> s3_to_postgre
