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
    'owner': 'lucas.bispo',
    'start_date': days_ago(0),
    'depends_on_past': False
}

job_flow_emr = {
    "Name": "trusted-process",
    "ReleaseLabel": "emr-5.30.0",
    "Applications": [{"Name": "Hive"}, {"Name": "Spark"}, {"Name": "Hadoop"}],
    "LogUri": "s3://aws-logs-767398038964-us-east-1/elasticmapreduce/",
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
    "BootstrapActions": [
                {
                    "Name": "Bootstrap action",
                    "ScriptBootstrapAction": {
                        "Path": "s3://767398038964s3-application/bootstrapfile.sh",
                        "Args": [],
                    },
                },
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
        "Name": "split2",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {"Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                            "Args": [
                                    "s3://767398038964s3-application/split.sh"
                                    ],
                        },
    }
]


def read_s3_data():
    s3_hook = S3Hook(aws_conn_id='aws_lab')
    bucket_name = '767398038964s3-application'
    file_name = 'csv_teste.csv'
    data = s3_hook.read_key(key=file_name, bucket_name=bucket_name)
    print(data)


with DAG('trusted_layer_bash',
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
    start_dag >> [read_s3_task, create_emr_cluster] >> add_steps >> check_execution_steps  >> terminate_emr_cluster >> end_dag