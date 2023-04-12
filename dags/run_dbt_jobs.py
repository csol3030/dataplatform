import os, fnmatch, time, uuid
import json, asyncio
from datetime import datetime

from airflow import DAG
from airflow import settings
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow import settings
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models import Connection, XCom
from airflow.models.param import Param
from airflow.utils.edgemodifier import Label
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import (
    DbtCloudJobRunSensor
)
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

KEYVAULT_URI = "https://kv-datalink-dp-pilot.vault.azure.net"
KEYVAULT_DBT_SECRET = "DBTSecret"
KEYVAULT_SNOWFLAKE_SECRET = "SnowflakeSecret"

DBT_CONN_ID = "datalink_dbt_cloud_conn"
SNOWFLAKE_CONN_ID = "datalink_snowflake_conn"


ENV_ID = "DEV"
DAG_ID = "af_trigger_dbt_jobs"

default_args = {
    "owner": "Airflow User",
    "start_date": datetime(2022, 2, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

session = settings.Session()

az_credential = AzureCliCredential()
kv_client = SecretClient(vault_url=KEYVAULT_URI, credential=az_credential)


@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


def _check_job_not_running(job_id):
    """
    Retrieves the last run for a given dbt Cloud job and checks to see if the job is not currently running.
    """
    hook = DbtCloudHook(DBT_CONN_ID)
    runs = hook.list_job_runs(job_definition_id=job_id, order_by="-id")
    latest_run = runs[0].json()["data"][0]
    print(
        "status============================",
        DbtCloudJobRunStatus.is_terminal(latest_run["status"]),
    )
    return DbtCloudJobRunStatus.is_terminal(latest_run["status"])


def get_status(run_id, **context):
    dbt_job_id = context["params"]["dbt_cloud_job_id"]
    print("message=====================", run_id)
    job_status = "IN PROGRESS"
    job_error_details = ""
    update_audit_table(run_id, dbt_job_id, job_status, job_error_details, context)
    try:
        job_sensor = DbtCloudJobRunSensor(
            task_id="dbt_job_run_sensor",
            dbt_cloud_conn_id=DBT_CONN_ID,
            # poll_interval=10,
            run_id=run_id,
        ).execute(context=context)

        job_status = "SUCCESS"
        # job_error_details =

    except Exception as err:
        print("job_status===============", err)
        job_status = "FAILURE"
        job_error_details = str(err)

    finally:
        update_audit_table(run_id, dbt_job_id, job_status, job_error_details, context)


def update_audit_table(run_id, dbt_job_id, job_status, job_error_details, context):

    hook = DbtCloudHook(DBT_CONN_ID)
    runs = hook.list_job_runs(job_definition_id=dbt_job_id, order_by="-id")
    latest_run = runs[0].json()["data"][0]

    print("temp_run=========================", latest_run)

    if latest_run["is_error"] == True and latest_run["is_complete"] == True:
        if latest_run["status_message"] != None:
            job_error_details = latest_run["status_message"]

        err_obj = {
            "error_message": job_error_details,
            "job_debug_url": latest_run["href"],
        }
        job_error_details = err_obj

    job_audit_details = {
        "file_ingestion_details_id":"",
        "job_run_id": str(run_id),
        "job_id": dbt_job_id,
        "job_run_duration": latest_run["run_duration"],
        "status": job_status,
        "error_details": job_error_details,
        "start_date": latest_run["created_at"],
        "end_date": latest_run["finished_at"],
    }
    print("job_audit_details=========================", job_audit_details)
    db = context["params"]["config_db"]
    schema = context["params"]["config_schema"]
    table = "DBT_JOB_DETAILS"
    sql_statement=""

    if job_status=="IN PROGRESS":
        sql_statement = """insert into {}.{}.{} (
            FILE_INGESTION_DETAILS_ID,
            JOB_RUN_ID,
            JOB_ID,
            JOB_RUN_DURATION,
            STATUS,
            ERROR_DETAILS,
            START_DATE
        ) values ('{}','{}','{}','{}','{}','{}','{}')
            """.format(
                db,schema,table,
                job_audit_details["file_ingestion_details_id"],
                job_audit_details["job_run_id"],
                job_audit_details["job_id"],
                job_audit_details["job_run_duration"],
                job_audit_details["status"],
                job_audit_details["error_details"],
                job_audit_details["start_date"]     
            )
    else:
        sql_statement = """update {}.{}.{} set 
                JOB_RUN_DURATION='{}', STATUS='{}', ERROR_DETAILS='{}', START_DATE='{}',END_DATE='{}'
            """.format(
                db,schema,table,
                job_audit_details["job_run_duration"],
                job_audit_details["status"],
                json.dumps(job_audit_details["error_details"]),
                job_audit_details["start_date"],
                job_audit_details["end_date"]
            )
    
    print("sql_statement===========================",sql_statement)

    snf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    snf_hook.run(sql=sql_statement)


def get_kv_secret(secret_name):
    fetched_secret = kv_client.get_secret(secret_name)
    # print(fetched_secret.value)
    return fetched_secret.value


def create_connection():
    dbt_details = json.loads(get_kv_secret(KEYVAULT_DBT_SECRET))
    snowflake_details = json.loads(get_kv_secret(KEYVAULT_SNOWFLAKE_SECRET))

    snowflake_conn = Connection(
        conn_id=SNOWFLAKE_CONN_ID,
        conn_type="snowflake",
        login=snowflake_details["username"],
        password=snowflake_details["password"],
        extra=json.dumps(
            {
                "account": snowflake_details["account"],
                "warehouse": snowflake_details["warehouse"],
                "role": snowflake_details["role"],
            }
        ),
    )

    snowflake_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == SNOWFLAKE_CONN_ID)
        .first()
    )

    dbt_conn = Connection(
        conn_id=DBT_CONN_ID,
        conn_type="dbt_cloud",
        host=dbt_details["host"],
        login=dbt_details["username"],
        password=dbt_details["password"],
    )

    dbt_conn_obj = (
        session.query(Connection).filter(Connection.conn_id == DBT_CONN_ID).first()
    )

    print("snowflake - ", type(snowflake_conn_obj))
    print("DBT - ", type(dbt_conn))

    if snowflake_conn_obj is None:
        print("Creating snowflake connection....")
        session.add(snowflake_conn)
        session.commit()
        print("Snowflake conn established....")

    if dbt_conn_obj is None:
        print("Creating dbt cloud connection....")
        session.add(dbt_conn)
        session.commit()
        print("dbt cloud conn established....")


def delete_connection():
    snowflake_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == SNOWFLAKE_CONN_ID)
        .first()
    )

    dbt_conn_obj = (
        session.query(Connection).filter(Connection.conn_id == DBT_CONN_ID).first()
    )

    if snowflake_conn_obj is not None:
        print("deleting snowflake connection....")
        session.delete(snowflake_conn_obj)

    if dbt_conn_obj is not None:
        print("deleting dbt cloud connection....")
        session.delete(dbt_conn_obj)

    session.commit()
    session.close()


"""
## Initialize Dag
"""
with DAG(
    dag_id=DAG_ID,
    schedule=None,
    default_args=default_args,
    params={
        "email": Param(default="abhilash.p@anblicks.com", type="string"),
        "customer_id": Param(default=120, type=["integer", "string"], min=1, max=255),
        "config_db": Param(default="DEV_OPS_DB", type=["string"], min=1, max=255),
        "config_schema": Param(default="CONFIG", type=["string"], min=1, max=255),
        "dbt_cloud_job_id": Param(default="263406", type=["string"], min=1, max=255),
    }
    # max_active_tasks=2
) as dag:
    # print(SFTP_FILE_COMPLETE_PATH)
    create_conn = PythonOperator(
        task_id="create_connections", python_callable=create_connection
    )

    with TaskGroup(group_id="dbt_job_run") as dbt_job_run_tg:
        check_dbt_job = ShortCircuitOperator(
            task_id="check_job_is_not_running",
            python_callable=_check_job_not_running,
            op_kwargs={"job_id": """{{params.dbt_cloud_job_id}}"""},
        )

        dbt_job_run_silver_to_gold_zone = DbtCloudRunJobOperator(
            task_id="dbt_job_run_silver_to_gold_zone",
            dbt_cloud_conn_id=DBT_CONN_ID,
            job_id="""{{params.dbt_cloud_job_id}}""",
            check_interval=10,
            wait_for_termination=False
            # timeout=200
        )

        get_job_status = PythonOperator(
            task_id="get_job_status",
            python_callable=get_status,
            op_kwargs={"run_id": dbt_job_run_silver_to_gold_zone.output},
        )

        # dbt_job_run_sensor = DbtCloudJobRunSensor(
        #     task_id="dbt_job_run_sensor",
        #     dbt_cloud_conn_id=DBT_CONN_ID,
        #     # poll_interval=10,
        #     run_id=dbt_job_run_silver_zone.output,
        # )

        check_dbt_job >> dbt_job_run_silver_to_gold_zone >> get_job_status
        # >> dbt_job_run_sensor

    with TaskGroup(group_id="cleanup") as cleanup:
        del_conn = PythonOperator(
            task_id="delete_connections", python_callable=delete_connection
        )

        clean_xcom = PythonOperator(
            task_id="clean_xcom",
            python_callable=cleanup_xcom,
            provide_context=True,
            dag=dag,
        )

    create_conn >> dbt_job_run_tg >> cleanup
