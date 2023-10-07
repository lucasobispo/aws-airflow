import pyspark
from pyspark.sql.types import IntegerType,StringType,StructType,StructField,DoubleType, TimestampType
from datetime import datetime as dt
from datetime import timedelta
from datetime import date
from datetime import datetime
import pyspark.sql.functions as f  
from pyspark.sql.window import Window
import sys
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import logging
import boto3
import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
import json
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
yaml = YAMLHandler()
s3_client = boto3.client('s3')

datasource_yaml = f"""
    name: my_spark_datasource
    class_name: Datasource
    module_name: great_expectations.datasource
    execution_engine:
        module_name: great_expectations.execution_engine
        class_name: SparkDFExecutionEngine
        force_reuse_spark_context: true
    data_connectors:
        my_runtime_data_connector:
            class_name: RuntimeDataConnector
            batch_identifiers: [default_identifier_name]
    """
def create_context_ge():
    context = ge.get_context()

    context.add_datasource(**yaml.load(datasource_yaml))

    return context

def create_validator(context, suite, df):
    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_datasource",
        data_connector_name="my_runtime_data_connector",
        data_asset_name="dq_data",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "YOUR_MEANINGFUL_IDENTIFIER"}
    )

    df_validator: Validator = context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite
    )

    return df_validator

def process_suite_ge(spark,  df, table):    
    df_ge = SparkDFDataset(df)

    context = create_context_ge()
    suite = context.add_expectation_suite(expectation_suite_name=f"dq_{table}")
    df_validator = create_validator(context, suite, df)


    if table =="Empregados" :
        expectation_type="expect_column_values_to_match_regex",
        kwargs={
            "column": "Nome",
            regex: "^\D*$",
        }
        suite.add_expectation(expectation_configuration="Empregados_1")
    elif table =="Banco":
        expectation_type="expect_column_value_lengths_to_equal",
        kwargs={
            "column": "CNPJ",
            value: 12,
        }
        suite.add_expectation(expectation_configuration="Bancos_1")
    else:
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={
            "column": "quantidade_total_de_clientes",
            type_ : int,
        }
        suite.add_expectation(expectation_configuration="Reclamacoes_1")
    results = df_validator.validate(expectation_suite=suite)
    context.save_expectation_suite(suite)
    return results

def send_json_to_s3(results_json, bucket, table):
    results_dict = results_json.to_json_dict()
    data = json.dumps(results_dict)
    now = datetime.now()
    year = now.year
    month = now.strftime('%m')
    day = now.strftime('%d')
    date = now.strftime('%Y-%m-%d')
    load_path = f"s3://638059466675-target/{table}/{year}/{month}/{day}/{table}_{date}.json"
    s3_client.put_object(
    Bucket=bucket,
    Key=load_path,
    Body=data,
    ContentType='application/json'
    )