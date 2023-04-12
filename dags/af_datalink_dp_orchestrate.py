from airflow import DAG
from airflow import settings
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models import Connection, XCom
from airflow.models.param import Param
from airflow.models.xcom_arg import XComArg
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    get_current_context,
)
from datetime import datetime

default_args = {
    "owner": "Airflow User",
    "start_date": datetime(2022, 2, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id='af_datalink_dp_orchestrate',
    schedule=None,
    default_args=default_args,
    params={
        "email": Param(default="abhilash.p@anblicks.com", type="string"),
        "customer_id": Param(default=120, type=["integer", "string"], min=1, max=255),
        "config_db": Param(default="DEV_OPS_DB", type=["string"], min=1, max=255),
        "config_schema": Param(default="CONFIG", type=["string"], min=1, max=255),
        "dest_container_name": Param(default="cont-datalink-dp-shared", type=["string"], min=3, max=255),
        "dest_folder_name": Param(default="LANDING", type=["string"], min=3, max=255),
        "database":Param(default='DEV_OPS_DB',type=["string"]),
        "schema":Param(default='CONFIG',type=["string"]),
        "run_failed":Param(default=True),
        "dbt_cloud_job_id": Param(default="263406", type=["string"], min=1, max=255),
        "airbyte_job_id": Param(default="", type=["string"], min=1, max=255)
    },
) as dag:
    
    
    af_sftp_to_adls_parallel_test = TriggerDagRunOperator(
        task_id="af_sftp_to_adls_parallel_test",
        trigger_dag_id="af_sftp_to_adls_parallel_test",
        wait_for_completion=True,
        conf={
            "email":"""{{params.email}}""",
            "customer_id":"""{{params.customer_id}}""",
            "container_name":"""{{params.dest_container_name}}""",
            "dest_folder_name":"""{{params.dest_folder_name}}"""
        },
    )
    
    
    af_adls_to_snowflake = TriggerDagRunOperator(
        task_id="af_adls_to_snowflake",
        trigger_dag_id="af_adls_to_snowflake",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={
            "email":"""{{params.email}}""",
            "customer_id":"""{{params.customer_id}}""",
            "container_name":"""{{params.dest_container_name}}""",
            "root_folder":"""{{params.dest_folder_name}}"""
        },
    )
    
    af_bronze_to_silver = TriggerDagRunOperator(
        task_id="af_bronze_to_silver",
        trigger_dag_id="af_bronze_to_silver",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={
            "database":"""{{params.database}}""",
            "schema":"""{{params.schema}}""",
            "run_failed":"""{{params.run_failed}}"""
        }
    )
    
    af_trigger_dbt_jobs = TriggerDagRunOperator(
        task_id="af_trigger_dbt_jobs",
        trigger_dag_id="af_trigger_dbt_jobs",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={
            "config_db":"""{{params.config_db}}""",
            "config_schema":"""{{params.config_schema}}""",
            "dbt_cloud_job_id":"""{{params.dbt_cloud_job_id}}""",
            "customer_id":"""{{params.customer_id}}"""
        }
    )
    
    af_trigger_airbyte_jobs = TriggerDagRunOperator(
        task_id="af_trigger_airbyte_jobs",
        trigger_dag_id="af_trigger_airbyte_jobs",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={
            "config_db":"""{{params.config_db}}""",
            "config_schema":"""{{params.config_schema}}""",
            "airbyte_job_id":"""{{params.airbyte_job_id}}""",
            "customer_id":"""{{params.customer_id}}"""
        }
    )
    
af_sftp_to_adls_parallel_test >> af_adls_to_snowflake >> af_bronze_to_silver >> af_trigger_dbt_jobs >> af_trigger_airbyte_jobs