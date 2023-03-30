import os, fnmatch, time, uuid
import json
import paramiko
import asyncio, zipfile
from datetime import datetime

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
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient, BlobBlock

KEYVAULT_URI = "https://kv-datalink-dp-pilot.vault.azure.net"
KEYVAULT_ADLS_BLOB_SECRET = "ADLSBlobConnSTR"
KEYVAULT_SNOWFLAKE_SECRET = "SnowflakeSecret"

ADLS_CONN_ID = "datalink_adls_conn"
SNOWFLAKE_CONN_ID = "datalink_snowflake_conn"

ENV_ID = "DEV"
DAG_ID = "af_sftp_to_adls_parallel_test"

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


# asyncio function
# decorator for running in background
def background(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


def get_kv_secret(secret_name):
    fetched_secret = kv_client.get_secret(secret_name)
    # print(fetched_secret.value)
    return fetched_secret.value


def callback(current: int, total: int):
    print("processed ", round(current / (1024 * 1024), ndigits=3))
    print("total ", round(total / (1024 * 1024), ndigits=3))


def read_parts(file_obj, offset, fsize=10000):
    # while True:
    file_obj.seek(offset)
    chunked_data = file_obj.read(fsize)
    if chunked_data is not None:
        yield chunked_data


def get_sftp_conn_config(kv_value):
    sftp_details = json.loads(get_kv_secret(kv_value))
    # paramiko.SFTPFile.MAX_REQUEST_SIZE = 32768
    transport = paramiko.Transport((sftp_details["host"], sftp_details["port"]))
    # transport.start_client(timeout=100)
    transport.connect(
        username=sftp_details["username"], password=sftp_details["password"]
    )

    transport.default_window_size = paramiko.common.MAX_WINDOW_SIZE
    transport.packetizer.REKEY_BYTES = pow(2, 40)
    transport.packetizer.REKEY_PACKETS = pow(2, 40)

    transport.set_keepalive(10)
    # window_size = pow(4, 12)#about ~16MB chunks
    # max_packet_size = pow(4, 12)
    window_size = 30000  # about ~16MB chunks
    max_packet_size = 30000

    sftp_client = paramiko.SFTPClient.from_transport(
        transport,
        # window_size=window_size, max_packet_size=max_packet_size
    )
    sftp_client.get_channel().settimeout(120)

    return sftp_client, transport


adls_details = json.loads(get_kv_secret(KEYVAULT_ADLS_BLOB_SECRET))
blob_service_client = BlobServiceClient.from_connection_string(
    conn_str=adls_details["connection_string"]
)


def upload_file_async(
    container_name,
    blob,
    filename,
    kv_secret,
    sftp_config=None,
    file_metadata: str = None,
):
    """
    File Upload Function

    Upload the file chunks in async mode

    Source = SFTP, Target = AzureBlobStorage
    """
    block_list = []
    blob_name = blob
    # blob_name = "arbcbs_Membership_Manual_DePhi_Full_220620.txt"
    print(" upload_file_async params ===", container_name, blob, filename, sftp_config)
    sftp_client, transport = get_sftp_conn_config(kv_secret)
    # sftp_client, transport = sftp_config

    @background
    def upload_parallel_blob(chunk, chunk_id=1, blob_client: BlobClient = None):
        block_id = str(uuid.uuid4())
        blob_client.stage_block(block_id=block_id, data=chunk)
        block_list.append((chunk_id, BlobBlock(block_id=block_id)))
        # print("inside upload_parallel_blob", (BlobBlock(block_id=block_id)))
        # print("inside upload_parallel_blob", block_id)

    # upload data to blob using asyncio
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    start_time = time.time()
    loop = asyncio.get_event_loop()

    # Logic to process SFTP Files
    if file_metadata.find("txt") > -1:
        try:
            with sftp_client.open(filename=filename) as data:
                filesize = data.stat().st_size
                # data.MAX_REQUEST_SIZE = pow(2,10)
                data.MAX_REQUEST_SIZE = 30000
                chunksize = 30000  # <-- adjust this and benchmark speed
                # chunksize = 10*1024  #<-- adjust this and benchmark speed
                # chunksize = 1024  #<-- adjust this and benchmark speed
                chunk_list = [
                    (offset, chunksize) for offset in range(0, filesize, chunksize)
                ]

                try:
                    grp = asyncio.gather(
                        *[
                            upload_parallel_blob(item, index, blob_client)
                            for index, item in enumerate(data.readv(chunks=chunk_list))
                        ]
                    )

                    loop.run_until_complete(grp)

                    blob_upload_list = sorted(block_list, key=lambda x: x[0])
                    block_list = list(list(zip(*blob_upload_list))[1])
                    blob_client.commit_block_list(block_list=block_list)

                except EOFError as eof:
                    print("stream ended/EOF reached", eof)
                finally:
                    print(
                        "filesize ",
                        filesize,
                        round(filesize / (1024 * 1024), ndigits=3),
                        " MB",
                    )
                    print("chunk_list ", len(chunk_list))
                    print("block_list ", len(block_list))

        except (EOFError, paramiko.ssh_exception.SSHException, OSError) as x:
            print(x)
        except Exception as e:
            print("Exception := ", e)
        finally:
            print(
                "done uploading regular files ",
                filename,
                round(time.time() - start_time, ndigits=3),
            )

    # Logic to process Downloaded Zip Files
    elif file_metadata.find("zip") > -1:
        try:
            with open(file=filename, mode="rb") as zip_file_obj:
                filesize = os.stat(path=filename).st_size
                zip_file_obj.MAX_REQUEST_SIZE = 30000
                chunksize = 30000  # <-- adjust this and benchmark speed
                # chunksize = 10*1024  #<-- adjust this and benchmark speed
                chunk_list = [
                    (offset, chunksize) for offset in range(0, filesize, chunksize)
                ]

                try:
                    data = [
                        read_parts(file_obj=zip_file_obj, offset=offset, fsize=size)
                        for offset, size in chunk_list
                    ]
                    grp = asyncio.gather(
                        *[
                            upload_parallel_blob(item, index, blob_client)
                            for index, item in enumerate(data)
                        ]
                    )

                    loop.run_until_complete(grp)

                    blob_upload_list = sorted(block_list, key=lambda x: x[0])
                    block_list = list(list(zip(*blob_upload_list))[1])
                    blob_client.commit_block_list(block_list=block_list)

                except EOFError as eof:
                    print("stream ended/EOF reached", eof)
                finally:
                    print(
                        "filesize ",
                        filesize,
                        round(filesize / (1024 * 1024), ndigits=3),
                        " MB",
                    )
                    print("chunk_list ", len(chunk_list))
                    print("block_list ", len(block_list))
            pass
        except Exception as e:
            print("Exception := ", e)
        finally:
            print(
                "done uploading zip data files",
                filename,
                round(time.time() - start_time, ndigits=3),
            )
            try:
                os.remove(filename)
                print("% s removed successfully" % filename)
            except OSError as error:
                print(error)
                print("File path can not be removed")
        # loop.close()
        sftp_client.close()
        transport.close()


def parse_config_data(data, params):
    file_info_arr = []
    file_info_path = []

    for index, row in data:
        print("row", index, row, params)
        if "KV_PWD_KEY" in row.keys():
            DYN_SFTP_CONN_ID = "datalink_" + row["KV_PWD_KEY"].lower() + "_conn"
            SFTP_FILE = str(row["FILE_NAME_PATTERN"] + row["FILE_WILD_CARD_EXT"])
            print("DYN_SFTP_CONN_ID", DYN_SFTP_CONN_ID, SFTP_FILE)

            sftp_client, transport = get_sftp_conn_config(row["KV_PWD_KEY"])
            dir_info = [x.filename for x in sftp_client.listdir_iter()]
            file_list = fnmatch.filter(dir_info, SFTP_FILE)

            for item in file_list:
                blob_name = (
                    params["dest_folder_name"]
                    + "/"
                    + str(params["customer_id"])
                    + "/"
                    + str(item)
                )
                print(blob_name)
                details = {
                    "dest_container_name": params["dest_container_name"],
                    "blob_name": blob_name,
                    "filename": item,
                    "kv_secret": row["KV_PWD_KEY"],
                    "sftp_obj": None,
                    "file_ext": row["FILE_WILD_CARD_EXT"],
                }
                # details = json.dumps(details)
                # file_info_path.append(blob_name)
                file_info_arr.append([details])

            sftp_client.close()
            transport.close()

    # print("file_info_arr ================", file_info_arr)
    # print("file_info_path ================", file_info_path)
    # print("file_info_path ================", fnmatch.filter(file_info_path,"*.zip"))
    # print("file_info_path ================", fnmatch.filter(file_info_path,"*.txt"))

    return file_info_arr


def extract_zip_files(details, **context):
    print(
        "===================extract_zip_files==============",
        details,
        "\n\n\n",
        context["params"],
    )
    contextParams = context["params"]
    file_info_arr = []

    if details["file_ext"].find("zip") > -1:
        sftp_client, transport = get_sftp_conn_config(details["kv_secret"])

        with sftp_client.open(filename=details["filename"]) as zip_file_data:
            zip_obj = zipfile.ZipFile(zip_file_data)
            folder_id = str(uuid.uuid4())
            path = "/home/astro/airflow/zipfiles/" + folder_id

            zip_obj.extractall(path)

        file_details = []
        for current_dir, dirs, files in os.walk(path):
            print("current_dir", current_dir)
            print("dirs", dirs)
            print("files", files)
            print("path", path)
            file_details = [(i, os.path.join(current_dir, i)) for i in files]

        for file_item in file_details:
            blob_name = (
                contextParams["dest_folder_name"]
                + "/"
                + str(contextParams["customer_id"])
                + "/"
                + str(file_item[0])
            )
            print(blob_name)
            details = {
                "dest_container_name": details["dest_container_name"],
                "blob_name": blob_name,
                "filename": file_item[1],
                "kv_secret": details["kv_secret"],
                "sftp_obj": None,
                "file_ext": details["file_ext"],
            }

            # file_info_arr.append({details["filename"]: details})
            file_info_arr.append(details)

        sftp_client.close()
        transport.close()

        print("List of Files in zip ", file_info_arr)

    return file_info_arr


def get_zip_file_info(**context):

    ti = context["ti"]
    details = ti.xcom_pull(
        key="return_value",
        task_ids="upload_sftp_to_azure.upload_zip_files.extract_zip_files",
    )

    file_list = []
    for item in details:
        if type(item) == list:
            for val in item:
                file_list.append([val])
        else:
            file_list.append([item])  
    
    # print("get_zip_file_info", type(details), file_list)
    return file_list


def process_config_data(**context):
    ti = context["ti"]
    file_ext = context["file_ext"]
    data = ti.xcom_pull(key="return_value", task_ids="get_snowflake_config_data")
    print("===================data==============", data)
    details = [item for item in data if item[0]["file_ext"] == file_ext]
    print(details)
    print("op_args=====", file_ext)
    return details


def upload_data_azure(details, **context):
    print("===================upload_data_azure==============", details)

    params = details
    upload_file_async(
        params["dest_container_name"],
        params["blob_name"],
        params["filename"],
        params["kv_secret"],
        params["sftp_obj"],
        params["file_ext"],
    )



def get_snowflake_config_data(**context):
    print(context["snowflake_sql"])
    query = context["snowflake_sql"]
    snf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    # result = snf_hook.get_records(query)
    df = snf_hook.get_pandas_df(query)

    print("Result Rows  - %s", len(df))
    print("Result Type  - ", type(df))
    print("Result - \n", df)

    # file_result = transfer_files_sftp_azure(df.iterrows(), context["params"])
    file_result = parse_config_data(df.iterrows(), context["params"])

    print("file_result =========================================", file_result)

    return file_result


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
        "dest_folder_name": Param(default="LANDING", type=["string"], min=3, max=255),
    },
    # max_active_tasks=2
) as dag:
    # print(SFTP_FILE_COMPLETE_PATH)
    create_conn = PythonOperator(
        task_id="create_connections", python_callable=create_connection
    )

    get_config_data_snowflake = PythonOperator(
        task_id="get_snowflake_config_data",
        python_callable=get_snowflake_config_data,
        op_kwargs={
            "snowflake_sql": """select ftp_dtl.*,
                            file_dtl.file_name_pattern,file_dtl.file_wild_card_ext,file_dtl.source 
                            from DEV_OPS_DB.CONFIG.FILE_DETAILS file_dtl
                        inner join
                        (select ftp_details_id,customer_id,directory,host,port,kv_pwd_key from DEV_OPS_DB.CONFIG.FTP_DETAILS) as ftp_dtl
                        on( file_dtl.customer_id = ftp_dtl.customer_id and file_dtl.source_dtls_id = ftp_dtl.ftp_details_id )
                        where file_dtl.customer_id={{params.customer_id}} order by ftp_dtl.host;"""
        },
    )

    with TaskGroup(group_id="upload_sftp_to_azure") as upload_sftp_to_azure:

        with TaskGroup(group_id="upload_text_files") as upload_text_files:

            process_txt_config = PythonOperator(
                task_id="process_txt_config",
                python_callable=process_config_data,
                op_kwargs={"file_ext":"*.txt"}
            )

            process_text_files = PythonOperator.partial(
                task_id="process_text_files",
                python_callable=upload_data_azure
            ).expand(op_args=process_txt_config.output)

            process_txt_config >> process_text_files

        with TaskGroup(group_id="upload_zip_files") as upload_zip_files:
           
            process_zip_config = PythonOperator(
                task_id="process_zip_config",
                python_callable=process_config_data,
                op_kwargs={"file_ext": "*.zip"},
            )

            extract_zip = PythonOperator.partial(
                task_id="extract_zip_files",
                python_callable=extract_zip_files,
            ).expand(op_args=process_zip_config.output)


            get_zip_files = PythonOperator(
                task_id="get_zip_files",
                python_callable=get_zip_file_info
            )
            
            process_zip_text_files = PythonOperator.partial(
                task_id="process_zip_text_files", python_callable=upload_data_azure
            ).expand(op_args=get_zip_files.output)

            process_zip_config >> extract_zip >> get_zip_files >> process_zip_text_files

        # Parallel in groups
        upload_zip_files, upload_text_files

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

    # process_files_adls_to_snowflake = TriggerDagRunOperator(
    #     task_id="process_files_adls_to_snowflake",
    #     trigger_dag_id="Process_ADLS_to_Snowflake",
    #     wait_for_completion=True,
    #     conf={
    #         "email":"""{{params.email}}""",
    #         "customer_id":"""{{params.customer_id}}""",
    #         "container_name":"""{{params.dest_container_name}}""",
    #         "root_folder":"""{{params.dest_folder_name}}""",
    #         "config_db":"""{{params.config_db}}""",
    #         "config_schema":"""{{params.config_schema}}"""
    #     },
    #     # execution_date=datetime(2022,2,7)
    # )

    create_conn >> get_config_data_snowflake >> upload_sftp_to_azure >> cleanup
    # >> process_files_adls_to_snowflake
