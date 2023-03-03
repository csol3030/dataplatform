import os
import json
import paramiko
from datetime import datetime

from airflow import DAG
from airflow import settings
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SFTPToWasbOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

# TENANT_ID = '18c3b4f7-e526-4f98-939a-19118361cac0'
KEYVAULT_URI = "https://kv-datalink-dp-pilot.vault.azure.net"
KEYVAULT_ADLS_BLOB_SECRET = "ADLSBlobConnSTR"
KEYVAULT_SNOWFLAKE_SECRET = "SnowflakeSecret"
# KEYVAULT_SFTP_SECRET = "SFTPSecret"

ADLS_CONN_ID = "datalink_adls_conn"
SNOWFLAKE_CONN_ID = "datalink_snowflake_conn"
# SFTP_CONN_ID = "datalink_sftp_conn"

# ADLS_HOST_NAME = "https://dlsdatalinkdppilot.blob.core.windows.net"
# AZURE_CONTAINER_NAME = "cont-datalink-dp-shared/TEMP"
# BLOB_PREFIX = ""
# SFTP_SRC_PATH = "./*Sample.txt"
# SFTP_SRC_PATH = "/member*"

ENV_ID = "DEV"
DAG_ID = "af_transfer_files_sftp_to_azure"

default_args = {
    'owner':'Airflow User',
    'start_date':datetime(2022,2,7),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':0
}


session = settings.Session()

az_credential = AzureCliCredential()
kv_client = SecretClient(vault_url=KEYVAULT_URI, credential=az_credential)

def get_kv_secret(secret_name):    
    fetched_secret = kv_client.get_secret(secret_name)
    # print(fetched_secret.value)
    return fetched_secret.value

def transfer_files_sftp_azure(row,params):

    print("row",row, params)
    if "KV_PWD_KEY" in row.keys():
        DYN_SFTP_CONN_ID = "datalink_"+row["KV_PWD_KEY"].lower()+"_conn"
        SFTP_FILEPATH = row["FILE_NAME_PATTERN"]+row["FILE_WILD_CARD_EXT"]
        sftp_conn_obj = session.query(Connection).filter(Connection.conn_id == DYN_SFTP_CONN_ID).first()
        print("DYN_SFTP_CONN_ID", DYN_SFTP_CONN_ID, SFTP_FILEPATH, sftp_conn_obj)
        AZURE_CONTAINER_NAME = params["dest_container_name"]
        BLOB_PREFIX = ""

        if sftp_conn_obj is None:
            print("Creating sftp connection....",paramiko.SFTPFile.MAX_REQUEST_SIZE)
            paramiko.SFTPFile.MAX_REQUEST_SIZE = 1024
            sftp_details=json.loads(get_kv_secret(row["KV_PWD_KEY"]))
            
            sftp_conn = Connection(
                conn_id=DYN_SFTP_CONN_ID,
                conn_type="SFTP",
                description="SFTP Connection",
                host=sftp_details["host"],
                login=sftp_details["username"],
                port=sftp_details["port"],
                password=sftp_details["password"],
                extra=json.dumps({
                    "conn_timeout":10
                })
            )

            session.add(sftp_conn)
            session.commit() 
            print("SFTP conn established....", paramiko.SFTPFile.MAX_REQUEST_SIZE)
        else:
            print("Using Existing connection")
        
        transfer_files_to_azure = SFTPToWasbOperator(
            task_id="transfer_files_from_sftp_to_azure",
            sftp_conn_id=DYN_SFTP_CONN_ID,
            sftp_source_path=SFTP_FILEPATH,
            move_object=False,
            wasb_conn_id=ADLS_CONN_ID,
            container_name=AZURE_CONTAINER_NAME,
            blob_prefix=BLOB_PREFIX,
            wasb_overwrite_object=True,
            create_container=False
        )

        try:
            transfer_files_to_azure.execute({})
        except Exception as e:
            print("error occured in uploading the file",e)
            pass

        if sftp_conn_obj is not None:
            session.delete(sftp_conn_obj)
            session.commit()
            print("deleting sftp connection....")


def get_snowflake_config_data(**context):
    print(context["snowflake_sql"])
    query = context["snowflake_sql"]
    snf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    # result = snf_hook.get_records(query)
    df = snf_hook.get_pandas_df(query)

    for index, row in df.iterrows():
        transfer_files_sftp_azure(row,context["params"])

    print("Result Rows  - %s", len(df))
    print("Result Type  - ", type(df))
    print("Result - \n", df)

def create_connection():

    adls_details=json.loads(get_kv_secret(KEYVAULT_ADLS_BLOB_SECRET))
    snowflake_details=json.loads(get_kv_secret(KEYVAULT_SNOWFLAKE_SECRET))

    snowflake_conn = Connection(
        conn_id=SNOWFLAKE_CONN_ID,
        conn_type="snowflake",
        login=snowflake_details["username"],
        password=snowflake_details["password"],
        extra=json.dumps({
            "account": snowflake_details["account"],
            "warehouse": snowflake_details["warehouse"],
            "role":snowflake_details["role"]
        })
    )

    adls_conn = Connection(
        conn_id=ADLS_CONN_ID,
        conn_type="wasb",
        description="Azure Blob Storage",
        host=adls_details["host"],
        extra=json.dumps({
            'tenant_id':adls_details["tenant_id"],
            'connection_string':adls_details["connection_string"]
        })
    )

    snowflake_conn_obj = session.query(Connection).filter(Connection.conn_id == SNOWFLAKE_CONN_ID).first()
    adls_conn_obj = session.query(Connection).filter(Connection.conn_id == ADLS_CONN_ID).first()
  
    print("snowflake - ",type(snowflake_conn_obj))
    print("adls - ",type(adls_conn_obj))

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
    snowflake_conn_obj = session.query(Connection).filter(Connection.conn_id == SNOWFLAKE_CONN_ID).first()
    adls_conn_obj = session.query(Connection).filter(Connection.conn_id == ADLS_CONN_ID).first()
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
        "email":Param(
            default="abhilash.p@anblicks.com",
            type="string"
        ),
        "customer_id":Param(
            default=120,
            type=["integer","string"],
            min=1,
            max=255
        ),
        "config_db":Param(
            default="DEV_OPS_DB",
            type=["string"],
            min=1,
            max=255
        ),
        "config_schema":Param(
            default="CONFIG",
            type=["string"],
            min=1,
            max=255
        ),
        "dest_container_name":Param(
            default="cont-datalink-dp-shared",
            type=["string"],
            min=3,
            max=255
        ),
        "dest_folder_name":Param(
            default="LANDING",
            type=["string"],
            min=3,
            max=255
        )
    }
) as dag:

    # print(SFTP_FILE_COMPLETE_PATH)
    create_conn = PythonOperator(
        task_id="create_connections",
        python_callable=create_connection
    )

    upload_data_sftp_to_azure = PythonOperator(
        task_id="upload_data_sftp_to_azure",
        python_callable=get_snowflake_config_data,
        op_kwargs={
            "snowflake_sql":"""select ftp_dtl.*,
                            file_dtl.file_name_pattern,file_dtl.file_wild_card_ext,file_dtl.source 
                            from DEV_OPS_DB.CONFIG.FILE_DETAILS file_dtl
                        inner join
                        (select ftp_details_id,customer_id,directory,host,port,kv_pwd_key from DEV_OPS_DB.CONFIG.FTP_DETAILS) as ftp_dtl
                        on( file_dtl.customer_id = ftp_dtl.customer_id and file_dtl.source_dtls_id = ftp_dtl.ftp_details_id )
                        where file_dtl.customer_id={{params.customer_id}} 
                        order by ftp_dtl.host;"""
        }
    )

    del_conn = PythonOperator(
        task_id="delete_connections",
        python_callable=delete_connection
    )

    process_files_adls_to_snowflake = TriggerDagRunOperator(
        task_id="process_files_adls_to_snowflake",
        trigger_dag_id="Process_ADLS_to_Snowflake",
        wait_for_completion=True,
        conf={
            "email":"abhilash.p@anblicks.com",
            "customer_id":"""{{params.customer_id}}""",
            "container_name":"""{{params.dest_container_name}}""",
            "root_folder_name":"""{{params.dest_folder_name}}"""
        },
        # execution_date=datetime(2022,2,7) 
    )

    create_conn >> upload_data_sftp_to_azure >> del_conn >> process_files_adls_to_snowflake