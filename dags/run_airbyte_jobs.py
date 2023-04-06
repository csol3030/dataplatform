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
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

KEYVAULT_URI = "https://kv-datalink-dp-pilot.vault.azure.net"
KEYVAULT_AIRBYTE_SECRET = "AirbyteSecret"

AIRBYTE_CONN_ID = "datalink_airbyte_conn"
# AIRBYTE_JOB_CONN_ID = "b92860b1-46c1-4098-9903-92ebfdef2985"

ENV_ID = "DEV"
DAG_ID = "af_trigger_airbyte_jobs"

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
    airbyte_details = json.loads(get_kv_secret(KEYVAULT_AIRBYTE_SECRET))

    airbyte_conn = Connection(
        conn_id=AIRBYTE_CONN_ID,
        conn_type="airbyte",
        host=airbyte_details["host"],
        port=airbyte_details["port"],
        login=airbyte_details["username"],
        password=airbyte_details["password"]
    )

    airbyte_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == AIRBYTE_CONN_ID)
        .first()
    )

    print("Airbyte - ", type(airbyte_conn))

    if airbyte_conn_obj is None:
        print("Creating airbyte connection....")
        session.add(airbyte_conn)
        session.commit()
        print("airbyte conn established....")


def delete_connection():
    airbyte_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == AIRBYTE_CONN_ID)
        .first()
    )

    if airbyte_conn_obj is not None:
        print("deleting airbyte connection....")
        session.delete(airbyte_conn_obj)
    
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
        "airbyte_job_id": Param(default="", type=["string"], min=1, max=255)
    }
    # max_active_tasks=2
) as dag:
    # print(SFTP_FILE_COMPLETE_PATH)
    create_conn = PythonOperator(
        task_id="create_connections", python_callable=create_connection
    )

    airbyte_job_snowflake_postgres = AirbyteTriggerSyncOperator(
        task_id="airbyte_job_snowflake_postgres",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id="""{{params.airbyte_job_id}}""",
        asynchronous=True
    )

    airbyte_sensor_snowflake_postgres = AirbyteJobSensor(
        task_id="airbyte_sensor_snowflake_postgres",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        airbyte_job_id=airbyte_job_snowflake_postgres.output
    )

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

    create_conn >> airbyte_job_snowflake_postgres >> airbyte_sensor_snowflake_postgres >> cleanup