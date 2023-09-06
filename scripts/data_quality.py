import pyspark
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType,StringType,StructType,StructField, TimestampType, DoubleType
from  pyspark.sql.functions import split, col,to_date,lit,translate
from datetime import datetime as dt, timedelta
from  pyspark.sql.functions import input_file_name
import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.checkpoint import SimpleCheckpoint




conf = SparkConf().setAll((
 ("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"),
 ("spark.sql.parquet.mergeSchema", "false")
))


sc = pyspark.SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext

file_s3 = "s3://638059466675-source/Empregados/glassdoor_consolidado_join_match_v2.csv"
output_path = "s3://638059466675-delivery/"


suite_name = 'suite_tests_glassdoor_data'
suite_profile_name = 'profile_glassdoor_data'
yaml = YAMLHandler()

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


def create_context_ge(output_path):
    context = ge.get_context()

    context.add_datasource(**yaml.load(datasource_yaml))

    return context

def config_data_docs_site(context, output_path):
    data_context_config = DataContextConfig()

    data_context_config["data_docs_sites"] = {
        "s3_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": output_path.replace("s3://", "")
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder"
            }
        }
    }

    context._project_config["data_docs_sites"] = data_context_config["data_docs_sites"]
    

def create_validator(context, suite, df):
    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_datasource",
        data_connector_name="my_runtime_data_connector",
        data_asset_name="glassdoor_data",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "YOUR_MEANINGFUL_IDENTIFIER"}
    )

    df_validator: Validator = context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite
    )

    return df_validator

def process_suite_ge(spark, file_s3, output_path):
    
    df = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("encoding", "UTF-8").option("ignoreLeadingWhiteSpace", "true").option("multiline", "true").option("escape", '"').option("header", "true").load(file_s3)
    
    df_ge = SparkDFDataset(df)

    context = create_context_ge(output_path)

    #create suite expectativas
    suite = context.add_expectation_suite(expectation_suite_name="my_suite")

    expectation_configuration_1 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "reviews_count",
            "min_value": 100, 
            "max_value": 300, 
            "mostly": 1.0,
        },
        meta={
            "notes": {
                "format": "markdown",
                "content": "Some clever comment about this expectation. **Markdown** `Supported`",
            }
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration_1)
    expectation_configuration_2 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "employer-headquarters",
            "mostly": 1.0,
        },
        meta={
            "notes": {
                "format": "markdown",
                "content": "Some clever comment about this expectation. **Markdown** `Supported`",
            }
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration_2)
    context.save_expectation_suite(suite)

    df_validator = create_validator(context, suite, df)
    results = df_validator.validate(expectation_suite=suite)
    print(results)
    config_data_docs_site(context, output_path)
    context.build_data_docs(site_names=["s3_site"])


process_suite_ge(spark, file_s3, output_path)
    


