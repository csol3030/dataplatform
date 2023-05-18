from airflow import DAG
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "owner": "Airflow User",
    "start_date": datetime(2022, 2, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id='af_datalink_dp_ccd_orchestrate',
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
        "sp_exec_cmd": Param(
            default="CALL SAMPLE_DB.PUBLIC.SAMPLE_STORED_PROC();",
            description="List of stored procedure names separated by semi-colon(;)",
            type=["string"],
            min=1,
            max=255,
        ),
    },
) as dag:

    
    af_adls_to_snowflake = TriggerDagRunOperator(
        task_id="af_adls_to_snowflake",
        trigger_dag_id="af_adls_to_snowflake",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={
            "email":"""{{params.email}}""",
            "customer_id":"""{{params.customer_id}}""",
            "container_name":"""{{params.dest_container_name}}""",
            "root_folder":"""{{params.dest_folder_name}}""",
            "database": """{{params.config_db}}""",
            "schema": """{{params.config_schema}}""",
            "run_failed": """{{params.run_failed}}""",
        },
    )
    
    af_process_ccd_info = TriggerDagRunOperator(
        task_id="af_process_ccd_info",
        trigger_dag_id="af_process_ccd_info",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={
            "sp_exec_cmd":"""{{params.sp_exec_cmd}}"""
        }
    )
    
   
    
af_adls_to_snowflake >> af_process_ccd_info