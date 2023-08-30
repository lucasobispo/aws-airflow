from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

args = {
    'owner': 'Airflow',
    'start_date': days_ago(0),
    'depends_on_past': False
}

folders = ['dags', 'planning', 'quality']
job_flow_emr = {
    "Name": "spark-process",
    "ReleaseLabel": "emr-5.30.0",
    "Applications": [{"Name": "Hive"}, {"Name": "Spark"}, {"Name": "Hadoop"}],
    "LogUri": "s3://aws-logs-638059466675-us-east-1/elasticmapreduce/",
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}

spark_steps = [
    {
        "Name": "Reclamacoes",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {"Jar": "command-runner.jar",
                            "Args": [
                                    "spark-submit",
                                    "--master", "yarn",
                                    "--deploy-mode", "cluster",
                                    "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                                    "--conf", "spark.sql.hive.convertMetastoreParquet=false",
                                    "--conf", "spark.dynamicAllocation.enabled=true",
                                    "s3://638059466675-application/spark_read.py",
                                    "--bucket_name", "s3a://638059466675-source/Reclamacoes/",
                                    "--separator", ";",
                                    "--encoding", "ISO-8859-1",
                                    "--file_format", "csv",
                                    "--column_name", "instituicao_financeira",
                                    "--s3_path", "s3://638059466675-target/Reclamacoes"
                                    ],
                        },
    },
    {
        "Name": "Banco",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {"Jar": "command-runner.jar",
                          "Args": [
                              "spark-submit",
                              "--master", "yarn",
                              "--deploy-mode", "cluster",
                              "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                              "--conf", "spark.sql.hive.convertMetastoreParquet=false",
                              "--conf", "spark.dynamicAllocation.enabled=true",
                              "s3://638059466675-application/spark_read.py",
                              "--bucket_name", "s3a://638059466675-source/Banco/",
                              "--separator", "\t",
                              "--encoding", "utf-8",
                              "--file_format", "tsv",
                              "--column_name", "nome",
                              "--s3_path", "s3://638059466675-target/Banco"
                          ],
                          },
    },
    {
        "Name": "Empregados",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {"Jar": "command-runner.jar",
                          "Args": [
                              "spark-submit",
                              "--master", "yarn",
                              "--deploy-mode", "cluster",
                              "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                              "--conf", "spark.sql.hive.convertMetastoreParquet=false",
                              "--conf", "spark.dynamicAllocation.enabled=true",
                              "s3://638059466675-application/spark_read.py",
                              "--bucket_name", "s3a://638059466675-source/Empregados/",
                              "--separator", "|",
                              "--encoding", "utf-8",
                              "--file_format", "csv",
                              "--column_name", "nome",
                              "--s3_path", "s3://638059466675-target/Empregados"
                          ],
                          },
    },
    {
        "Name": "agg-final",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {"Jar": "command-runner.jar",
                          "Args": [
                              "spark-submit",
                              "--master", "yarn",
                              "--deploy-mode", "cluster",
                              "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                              "--conf", "spark.sql.hive.convertMetastoreParquet=false",
                              "--conf", "spark.dynamicAllocation.enabled=true",
                              "s3://638059466675-application/agg.py",
                              "--s3_path", "s3://638059466675-delivery/tabela_final"
                          ],
                          },
    }
]


def read_s3_data():
    s3_hook = S3Hook(aws_conn_id='aws_lab')
    bucket_name = '638059466675-source'
    file_name = 'Empregados/glassdoor_consolidado_join_match_less_v2.csv'
    data = s3_hook.read_key(key=file_name, bucket_name=bucket_name)
    print(data)


with DAG('s3_read_example_with_hook',
         default_args=args,
         schedule_interval=None
         ):
    # Only display - start_dag
    start_dag = DummyOperator(task_id="start_dag")

    read_s3_task = PythonOperator(
        task_id='read_s3_data',
        python_callable=read_s3_data,
    )

    # Create the EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=job_flow_emr,
        aws_conn_id="aws_lab",
        region_name="us-east-1"
    )

    # Add the steps to the EMR cluster
    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_lab",
        steps=spark_steps,
    )
    last_step = len(spark_steps) - 1
    # Wait executions of all steps
    check_execution_steps = EmrStepSensor(
        task_id="check_execution_steps",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
                + str(last_step)
                + "] }}",
        aws_conn_id="aws_lab",
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_lab",
    )

    # Only display - end_dag
    end_dag = DummyOperator(task_id="end_dag")
    # Data pipeline flow
    start_dag >> [read_s3_task, create_emr_cluster] >> add_steps
    add_steps >> check_execution_steps >> terminate_emr_cluster >> end_dag