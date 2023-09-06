from pyspark.sql import SparkSession
import boto3
import json 
from botocore.exceptions import ClientError


spark = (
    SparkSession.builder.appName(f"processing table")
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    .config("spark.hadoop.fs.s3n.multiobjectdelete.enable", "false")
    .config("spark.hadoop.fs.s3.multiobjectdelete.enable", "false")
    .getOrCreate()
)

aws_access_key_id="ASIAZJD2OZ6ZXONZSU4D"
aws_secret_access_key="1thzSDkH/yukoegez1XROJeaAK9wsSDOW8YWzekv"
aws_session_token="FwoGZXIvYXdzELf//////////wEaDINRfn9OcRw3yE8QXyK/AZGYZQvC5LAlQIwsztE5jNUrM/oLgPkqyOVsf3rf/UNwqN7nV+YoPI41u7yofaCOC0kEqbq43/m+3NgQhnwHYLHV3i4R6fXhbq+CzurfprJ+R1d3K2TJzAA5WAllWArjN/jt2u13RCed65/+aLktzxMoI3bTwS0ZVDh79Ui+GNjGOhLCVrdlOUoPFrvriPfhxYvx2E3Yx4KH3BlmxNks1gYTKd2k5+nUikEIKigSs+8vy2/g26ZUEx1SAXJ//VWfKJ3g46cGMi0alLQUrYnq9L78YcqF0xffvnXnnLQekwg16EOQlWUei4K9pyUHX+b2FYZlYyw="

def get_secret():

    secret_name = "samba_sm"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name, 
        aws_access_key_id=aws_access_key_id, 
        aws_secret_access_key=aws_secret_access_key, 
        aws_session_token=aws_session_token
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return secret
    # Your code goes here.
def read_parquet(path):
    return spark.read.parquet(path)


def save_postgres(df, table_name, secret_string):
    print(secret_string)

    jsonload = json.loads(secret_string)

    properties = {
    "user": jsonload['username'],
    "password": jsonload['password'],
    "driver": "org.postgresql.Driver"
    }

    postgres_url = f"jdbc:postgresql://{jsonload['host'].strip()}/"
    print(postgres_url)
    df.write.mode("overwrite").jdbc(postgres_url,table_name,properties=properties)

if __name__ == "__main__":
    table_name = "public.tabela_final"
    print(table_name)
    secret_manager_value = get_secret()
    df_table = read_parquet("s3://638059466675-delivery/tabela_final/")
    print(f"df_table {df_table}")
    save_postgres(df_table,table_name,secret_manager_value)