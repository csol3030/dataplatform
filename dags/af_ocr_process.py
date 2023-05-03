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
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

import sys
sys.path.append('/usr/local/airflow/include/scripts')

import ocr_utils

KEYVAULT_URI = "https://kv-datalink-dp-pilot.vault.azure.net"
KEYVAULT_ADLS_BLOB_SECRET = "ADLSBlobConnSTR"
KEYVAULT_SNOWFLAKE_SECRET = "SnowflakeSecret"

ADLS_CONN_ID = "datalink_adls_conn"
SNOWFLAKE_CONN_ID = "datalink_snowflake_conn"

ENV_ID = "DEV"
DAG_ID = "af_ocr_process"

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

adls_details = json.loads(get_kv_secret(KEYVAULT_ADLS_BLOB_SECRET))
blob_service_client = BlobServiceClient.from_connection_string(
    conn_str=adls_details["connection_string"]
)
container_name = 'cont-datalink-dp-shared' 
folder_path = 'OCR_DATA/'

def download_blob_to_file(blob_service_client: BlobServiceClient, container_name): 
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="OCR_DATA/AetnaMA_Smith_Jane_12.17.1950_OMWb.pdf") 
    download_stream = blob_client.download_blob() 
    bytes_data=download_stream.readall() 
    return bytes_data

def get_ocr_details():

    print("============inside get_ocr_details================")
    # formUrl = r"C:\Anblicks\Projects\Datalink\Utilities\org_file\AetnaMA_Smith_Jane_12.17.1950_OMWb.pdf"

    # with open(formUrl,'rb') as pdf_file:
    #     bytes_data = pdf_file.read()
    bytes_data = download_blob_to_file(blob_service_client,container_name)

    ocr_utils.get_ocr_output(ocr_mode="cloud",document=bytes_data)


def create_connection():
    adls_details = json.loads(get_kv_secret(KEYVAULT_ADLS_BLOB_SECRET))
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

    adls_conn = Connection(
        conn_id=ADLS_CONN_ID,
        conn_type="wasb",
        description="Azure Blob Storage",
        host=adls_details["host"],
        extra=json.dumps(
            {
                "tenant_id": adls_details["tenant_id"],
                "connection_string": adls_details["connection_string"],
            }
        ),
    )

    snowflake_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == SNOWFLAKE_CONN_ID)
        .first()
    )
    adls_conn_obj = (
        session.query(Connection).filter(Connection.conn_id == ADLS_CONN_ID).first()
    )

    print("snowflake - ", type(snowflake_conn_obj))
    print("adls - ", type(adls_conn_obj))

    if snowflake_conn_obj is None:
        print("Creating snowflake connection....")
        session.add(snowflake_conn)
        session.commit()
        print("Snowflake conn established....")

    if adls_conn_obj is None:
        print("Creating adls connection....")
        session.add(adls_conn)
        session.commit()
        print("ADLS conn established....")


def delete_connection():
    snowflake_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == SNOWFLAKE_CONN_ID)
        .first()
    )
    adls_conn_obj = (
        session.query(Connection).filter(Connection.conn_id == ADLS_CONN_ID).first()
    )
    # sftp_conn_obj = session.query(Connection).filter(Connection.conn_id == SFTP_CONN_ID).first()

    if snowflake_conn_obj is not None:
        print("deleting snowflake connection....")
        session.delete(snowflake_conn_obj)

    if adls_conn_obj is not None:
        print("deleting adls connection....")
        session.delete(adls_conn_obj)

    session.commit()
    session.close()

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    default_args=default_args,
    params={
        "email": Param(default="abhilash.p@anblicks.com", type="string"),
        "customer_id": Param(default=120, type=["integer", "string"], min=1, max=255),
        "config_db": Param(default="DEV_OPS_DB", type=["string"], min=1, max=255),
        "config_schema": Param(default="CONFIG", type=["string"], min=1, max=255),
        "dest_container_name": Param(
            default="cont-datalink-dp-shared", type=["string"], min=3, max=255
        ),
        "source_folder_name": Param(default="OCR_DATA", type=["string"], min=3, max=255),
    },
    # max_active_tasks=2
) as dag:
    
    exec_ocr_process = PythonOperator(
            task_id="exec_ocr_process",
            python_callable=get_ocr_details
        )

    exec_ocr_process


