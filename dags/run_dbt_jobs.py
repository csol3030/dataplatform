import os, fnmatch, time, uuid
import json
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
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator, DbtCloudJobRunException
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

KEYVAULT_URI = "https://kv-datalink-dp-pilot.vault.azure.net"
KEYVAULT_DBT_SECRET = "DBTSecret"

DBT_CONN_ID = "datalink_dbt_cloud_conn"
# AIRBYTE_JOB_CONN_ID = "b92860b1-46c1-4098-9903-92ebfdef2985"

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

def get_kv_secret(secret_name):
    fetched_secret = kv_client.get_secret(secret_name)
    # print(fetched_secret.value)
    return fetched_secret.value

def create_connection():
    dbt_details = json.loads(get_kv_secret(KEYVAULT_DBT_SECRET))

    dbt_conn = Connection(
        conn_id=DBT_CONN_ID,
        conn_type="dbt_cloud",
        host=dbt_details["host"],
        login=dbt_details["username"],
        password=dbt_details["password"]
    )

    dbt_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == DBT_CONN_ID)
        .first()
    )

    print("DBT - ", type(dbt_conn))

    if dbt_conn_obj is None:
        print("Creating dbt cloud connection....")
        session.add(dbt_conn)
        session.commit()
        print("dbt cloud conn established....")


def delete_connection():
    dbt_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == DBT_CONN_ID)
        .first()
    )

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
        "dbt_cloud_job_id": Param(default="263406", type=["string"], min=1, max=255)
    }
    # max_active_tasks=2
) as dag:
    # print(SFTP_FILE_COMPLETE_PATH)
    create_conn = PythonOperator(
        task_id="create_connections", python_callable=create_connection
    )

    with TaskGroup(group_id="dbt_job_run") as dbt_job_run_tg:

        dbt_job_run_silver_zone = DbtCloudRunJobOperator(
            task_id="dbt_job_run_data_vault",
            dbt_cloud_conn_id=DBT_CONN_ID,
            job_id="""{{params.dbt_cloud_job_id}}""",
            check_interval=10,
            wait_for_termination=False
            # timeout=200
        )

        dbt_job_run_sensor = DbtCloudJobRunSensor(
                task_id="dbt_job_run_sensor",
                dbt_cloud_conn_id=DBT_CONN_ID,
                run_id=dbt_job_run_silver_zone.output
        )

        dbt_job_run_silver_zone >> dbt_job_run_sensor

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

    create_conn >> dbt_job_run_tg  >> cleanup