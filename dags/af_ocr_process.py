import os, fnmatch, time, uuid, re
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

from azure.storage.blob import BlobServiceClient
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

import sys
sys.path.append("/usr/local/airflow/include/scripts")

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
    return fetched_secret.value

adls_details = json.loads(get_kv_secret(KEYVAULT_ADLS_BLOB_SECRET))
blob_service_client = BlobServiceClient.from_connection_string(
    conn_str=adls_details["connection_string"]
)

def download_blob_to_file(
    blob_service_client: BlobServiceClient, container_name, blob_name
):
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    download_stream = blob_client.download_blob()
    bytes_data = download_stream.readall()
    return bytes_data

def get_files_from_adls(**context):

    if context["params"]:
        blob_list = []
        try:
            print("============inside get_files_from_adls================", context["params"])
            
            container_name = context["params"]["container_name"]
            container_client = blob_service_client.get_container_client(container_name) 
            folder_path = context["params"]["folder_path"]
            blobs = container_client.list_blobs(folder_path+"/")
            blob_name = context["params"]["file_name"]
            if blob_name != "":
                file_name = os.path.join(folder_path,blob_name)
                blob_list.append([file_name])
            else:
                for blob in blobs: 
                    if not blob.name.endswith('/'): 
                        blob_details = {
                            "blob_path" : blob.name,
                            "blob_name" : blob.name.split("/")[-1]
                        }
                        print(blob_details)
                        blob_list.append([blob_details])

        except Exception as err:
            print("Error Occured ",err)
        finally:
            print("===="*5,blob_list)
            return blob_list


def escape_special_chars(my_string):
    escaped_string = ""
    for char in my_string:
        if char in ['\\', '\'', '\"', '\t', '@', '_', '!', '#', '$', '%', '^', '&', '*', '(', ')', '<', '>', '?', '/', '|', '}', '{', '~', ':']:
            escaped_string += '\\' + char
        else:
            escaped_string += char
    return escaped_string


def get_ocr_details(data, **context):
    print("============inside get_ocr_details================",data, context["params"])

    if context["params"]:
        mode = context["params"]["ocr_mode"]
        container_name = context["params"]["container_name"]
        folder_path = context["params"]["folder_path"]
        table_name = context["params"]["ocr_table"]

        if data:
            blob_name = data["blob_name"]
            file_path = os.path.join(folder_path, blob_name)
            print("path==================", file_path)
            bytes_data = download_blob_to_file(
                blob_service_client, container_name, data["blob_path"]
            )
            result = ocr_utils.get_ocr_output(ocr_mode=mode, document=bytes_data)
            result["file_name"] = blob_name
            result["content"] = result["content"].replace("\n",r"\n")
            result["content"] = escape_special_chars(result["content"])
            print(result["content"])
            write_output_to_snowflake({"extracted_content": result, "table_name":table_name})
    else:
        return None



def write_output_to_snowflake(data):

    print("=========data==========",data)

    if data is not None:

        doc_content = data["extracted_content"]
        snf_table = data["table_name"]

        sql_statement = (
            rf"insert into {snf_table} (DOC_DETAILS) (select PARSE_JSON('"
            + json.dumps(doc_content).replace("'", r"\'").replace("(", r"\(").replace(")", r"\)")
            + "') ) "
        )

        snf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snf_hook.run(sql=sql_statement)


def create_connection():
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

    print("snowflake - ", type(snowflake_conn_obj))

    if snowflake_conn_obj is None:
        print("Creating snowflake connection....")
        session.add(snowflake_conn)
        session.commit()
        print("Snowflake conn established....")


def delete_connection():
    snowflake_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == SNOWFLAKE_CONN_ID)
        .first()
    )

    if snowflake_conn_obj is not None:
        print("deleting snowflake connection....")
        session.delete(snowflake_conn_obj)

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
        "container_name": Param(
            default="cont-datalink-dp-shared", type=["string"], min=3, max=255
        ),
        "folder_path": Param(default="OCR_DATA", type=["string"], min=3, max=255),
        "file_name": Param(
            default="",
            type=["string"],
            min=3,
            max=255,
        ),
        "ocr_table": Param(default="SAMPLE_DB.PUBLIC.OCR_DETAILS", type=["string"], min=1, max=255),
        "ocr_mode": Param(
            default="open_source",
            description="Modes = cloud or open_source",
            type=["string"],
            min=3,
            max=255,
        )
    },
    # max_active_tasks=2
) as dag:
    create_conn = PythonOperator(
        task_id="create_connections", python_callable=create_connection
    )

    get_files_adls = PythonOperator(
        task_id="get_files_from_adls", python_callable=get_files_from_adls
    )
    
    exec_ocr_process = PythonOperator.partial(
        task_id="exec_ocr_process", python_callable=get_ocr_details
    ).expand(op_args=get_files_adls.output)

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

    create_conn >> get_files_adls >> exec_ocr_process >> cleanup
