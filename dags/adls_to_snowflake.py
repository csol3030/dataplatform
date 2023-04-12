from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timedelta
import pandas as pd
import urllib.parse
import fnmatch
import re
import ntpath
import json


# Snowflake configuration
DATABASE = "DEV_OPS_DB"
SCHEMA = "CONFIG"
STAGE_NAME = 'DEV_BCBSAR_RAW_DB.PUBLIC.DATALINK_DP_AZURE_RAWFILE_STAGE'
META_DATA_COLUMNS = ['FILENAME', 'FILE_ROW_NUMBER']
LOAD_DATE = 'LOAD_DATE'
# Key vault configuration
KEYVAULT_URI = 'https://kv-datalink-dp-pilot.vault.azure.net'


def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value

def get_snowflake_connection():
    # connects to snowflake and return snowpark session
    snowflake_connection_parameters = json.loads(get_kv_secret("SnowflakeSecret"))
    user = snowflake_connection_parameters.pop("username")
    snowflake_connection_parameters.update({
        "database": DATABASE,
        "schema": SCHEMA,
        "user": user
    })
    # create session using snowflake connector
    session = Session.builder.configs(snowflake_connection_parameters).create()
    return session

snowflake_session = get_snowflake_connection()

def get_azure_connection(context):
    container_name =  context["params"]["container_name"]
    # create a client to interact with blob storage
    blob_details = json.loads(get_kv_secret("ADLSBlobConnSTR"))
    connection_str = blob_details.get('connection_string')
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)
    account_name = urllib.parse.urlsplit(blob_details.get('host')).hostname.split('.')[0]
    # use the client to connect to the container
    container_client = blob_service_client.get_container_client(container_name)
    sas = connection_str.split('SharedAccessSignature=')[1]
    connection = (blob_service_client, container_client, account_name, sas)
    return connection


def get_file_details(snowflake_session, customer_id):
    # fetch records from FILE_DETAILS Config table
    file_details = snowflake_session.table("FILE_DETAILS").filter(col("CUSTOMER_ID") == customer_id)
    pd_df_file_details = file_details.to_pandas()
    return pd_df_file_details

def get_file_col_details(snowflake_session, file_details_id):
    # fetch records from FILE_COL_DETAILS Config table
    file_col_details = snowflake_session.table("FILE_COL_DETAILS").\
        filter(col("FILE_DETAILS_ID") == file_details_id).sort(col("COL_POSITION"))
    pd_df_file_col_details = file_col_details.to_pandas()
    return pd_df_file_col_details

def insert_file_ingestion_details(snowflake_session, file_path):
    # creates a record in FILE_INGESTION_DETAILS table
    file_name = ntpath.basename(file_path)
    status = 'IN_PROGRESS'
    error_found = False
    updated_by = snowflake_session.sql("SELECT CURRENT_USER").collect()
    updated_by = f"{updated_by}".split("=")[1].strip("')]'")
    load_timestamp = str(datetime.now())
    schema = ["FILE_NAME", "INGESTION_STATUS",
        "START_DATE", "UPDATED_BY"]
    data = (file_name, status, load_timestamp, updated_by)
    query = f"""insert into {DATABASE}.{SCHEMA}.FILE_INGESTION_DETAILS
                ({','.join(schema)}) values {str(data)}
            """
    resp = snowflake_session.sql(query).collect()
    print(resp)
    return file_name, load_timestamp

def update_file_ingestion_details(snowflake_session, file_details_id, file_path,
                                  ingestion_details, handle_schema_drift,
                                  schema_drift_columns, file_name, load_timestamp):
    # updates FILE_INGESTION_DETAILS table with status
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    file_name = ntpath.basename(file_path)
    error_details = ""
    error_found = False
    updated_by=snowflake_session.sql("SELECT CURRENT_USER").collect()
    updated_by = f"{updated_by}".split("=")[1].strip("')]'")

    # file key will be in the response if copy into command is executed
    if 'file' in ingestion_details:
        if ingestion_details['first_error']:
            error_details = {
                'first_error': ingestion_details['first_error'],
                'error_limit': ingestion_details['error_limit'],
                'errors_seen': ingestion_details['errors_seen'],
                'first_error_line': ingestion_details['first_error_line'],
                'first_error_character': ingestion_details['first_error_character'],
                'first_error_column_name': ingestion_details['first_error_column_name']
            }
            error_details = json.dumps(error_details)
            error_found = True
        status = ingestion_details['status']
        rows_loaded = ingestion_details['rows_loaded']
    # if schema drift can be handled but file key is not present
    elif handle_schema_drift:
        status = "FAILED"
        rows_loaded = 0
        error_details = ingestion_details['status']
        error_found = True
    # if schema drift cannot be handled
    else:
        status = ingestion_details['status']
        rows_loaded = 0
        error_details = ingestion_details.get('error_details','')
        error_found = True
    if schema_drift_columns.get('added_columns') or schema_drift_columns.get('deleted_columns'):
        schema_drift_columns_str = json.dumps(schema_drift_columns)
    else:
        schema_drift_columns_str = ''
    # update FILE_INGESTION_DETAILS table
    if status == "LOADED":
        status = SUCCESS
    elif status in ("LOAD_FAILED", "ERROR"):
        status = FAILED
    query=f"""UPDATE {DATABASE}.{SCHEMA}.FILE_INGESTION_DETAILS
          SET FILE_DETAILS_ID = '{file_details_id}',
          FILE_NAME = '{file_name}',
          FILE_PATH = '{file_path}',
          INGESTION_STATUS = '{status}',
          END_DATE = '{str(datetime.now())}',
          ERROR_DETAILS = '{error_details}',
          INGESTED_ROW_COUNT = '{rows_loaded}',
          UPDATED_BY = '{updated_by}',
          WARNING_DETAILS = '',
          SCHEMA_DRIFT_COLUMNS = '{schema_drift_columns_str}'
          WHERE START_DATE = to_timestamp('{load_timestamp}')
          """
    resp = snowflake_session.sql(query).collect()
    print(resp)
    return error_found

def update_bronze_to_sliver_details(snowflake_session, file_dict,
                                    file_name, load_timestamp):
    # creates a record in BRONZE_TO_SILVER_DETAILS table with status as PENDING
    error_found = False
    updated_by=snowflake_session.sql("SELECT CURRENT_USER").collect()
    updated_by = f"{updated_by}".split("=")[1].strip("')]'")
    target_schema = f"{file_dict.get('target_db')}.{file_dict.get('target_schema')}".upper()
    target_table = file_dict.get('target_table').upper()

    query=f"""INSERT INTO {DATABASE}.{SCHEMA}.BRONZE_TO_SILVER_DETAILS
            (FILE_INGESTION_DETAILS_ID,
            SOURCE_SCHEMA,SOURCE_TABLE,
            SOURCE_LOAD_DATE,
            ERROR_DETAILS,
            UPDATED_BY,
            TRANSFORMATION_STATUS)
            SELECT FILE_INGESTION_DETAILS_ID,
            '{target_schema}' AS SOURCE_SCHEMA,
            '{target_table}' AS SOURCE_TABLE,
            to_timestamp('{load_timestamp}') AS SOURCE_LOAD_DATE,
            ERROR_DETAILS,
            '{updated_by}' AS UPDATED_BY,
            'PENDING' AS TRANSFORMATION_STATUS
            FROM FILE_INGESTION_DETAILS
            WHERE INGESTION_STATUS = 'SUCCESS' AND
            START_DATE = to_timestamp('{load_timestamp}')
            AND FILE_NAME = '{file_name}'
            """
    resp = snowflake_session.sql(query).collect()
    print(resp)
    return error_found

def add_columns(snowflake_session, target_table, columns):
    # alters the target table and add's specified columns
    columns_dtype = ','.join([f"{col} VARCHAR" for col in columns])
    snowflake_session.sql(f"alter table {target_table} add {columns_dtype}").collect()

def format_columns(columns):
    # replace special characters with underscore for the columns
    col_list = []
    special_char_set = "[@_!#$%^&*()<>?/\|}{~:]"
    for col in columns:
        if col in META_DATA_COLUMNS + [LOAD_DATE]:
            continue
        if isinstance(col, (list, tuple)):
            col_list.append((col[0], re.sub(special_char_set,"_", col[1]).upper()))
        else:
            col_list.append(re.sub(special_char_set,"_", col).upper())
    return col_list

def pattern_matching(azure_session, file_dict, context):
    # returns a list of files if file_name_pattern matches
    file_name_pattern = file_dict["file_name_pattern"]
    customer_id = context["params"]["customer_id"]
    root_folder = context["params"]["root_folder"]
    blob_list = []
    for blob_i in azure_session.list_blobs(name_starts_with=f"{root_folder}/{str(customer_id)}"):
        file_name = blob_i.name.lower()
        file_name_pattern = file_name_pattern.lower()
        if fnmatch.fnmatch(file_name, file_dict['file_wild_card_ext'].lower()):
            if "*" in file_name_pattern:
                if '/' in file_name:
                    file_name = file_name.split('/')[-1]
                if fnmatch.fnmatch(file_name, file_name_pattern):
                    blob_list.append(blob_i.name)
            else:
                if file_name_pattern in file_name:
                    blob_list.append(blob_i.name)
    return blob_list

def get_file_columns(blob_service_client, src_file_path, field_delimiter, context):
    # returns header rows from a file
    container_name =  context["params"]["container_name"]
    file_columns = []
    try:
        blob_client = blob_service_client.get_blob_client(container_name, src_file_path)
        data = blob_client.download_blob(offset=0, length=1024*1024).read()
        data = data.decode("utf-8").splitlines()
        if data:
            file_columns = format_columns(data[0].split(field_delimiter))
        else:
            print("File is Empty")
        return file_columns
    except Exception as e:
        if "INVALID_RANGE" in str(e.error_code).upper():
            print("File is Empty")
    return file_columns

def read_blob(azure_connection, file_dict, context):
    # returns a list of tupple containing file_name, file_columns and respective sas_url
    blob_service_client, azure_session, account_name, sas = azure_connection
    blob_list = pattern_matching(azure_session, file_dict, context)
    blob_with_sas_list = []
    for blob_i in blob_list:
        file_columns = get_file_columns(blob_service_client,
                                        blob_i, file_dict['field_delimiter'], context)
        blob_with_sas_list.append((blob_i, file_columns))
    return blob_with_sas_list

def copy_blob(blob_service_client, account_name,
              src_file_path, sas, target_file_path, context):
    container_name =  context["params"]["container_name"]
    # copies file to target path
    source_blob_sas = 'https://' + account_name +'.blob.core.windows.net/' + \
                      container_name + '/' + src_file_path + '?' + sas
    copied_blob = blob_service_client.get_blob_client(container_name, target_file_path)
    copied_blob.start_copy_from_url(source_blob_sas)

def del_blob(blob_service_client, container_name, src_file_path):
    # deletes the specified file
    source_blob_client = blob_service_client.get_blob_client(container_name, src_file_path)
    source_blob_client.delete_blob()

def move_blob(azure_connection, src_file_path, error_found, context):
    # copies file to target file path
    # deletes the source file
    archive_folder = 'ARCHIVE'
    error_folder = 'ERROR'
    container_name =  context["params"]["container_name"]
    customer_id =  context["params"]["customer_id"]
    blob_service_client, azure_session, account_name, sas = azure_connection
    src_file = ntpath.basename(src_file_path)
    datetime_now = datetime.now()
    src_file_split = src_file.split('.')
    src_file = f"{src_file_split[0]}_{datetime_now.strftime('%H%m%S%f')}.{src_file_split[-1]}"
    if error_found:
        target_file_path = f"{error_folder}/{str(customer_id)}/{str(datetime_now.year)}_{str(datetime_now.month)}/{src_file}"
    else:
        target_file_path = f"{archive_folder}/{str(customer_id)}/{str(datetime_now.year)}_{str(datetime_now.month)}/{src_file}"
    copy_blob(blob_service_client, account_name,
              src_file_path, sas, target_file_path, context)
    del_blob(blob_service_client, container_name, src_file_path)

def create_table(snowflake_session, columns, target_table):
    # creates a specific target table
    columns_list = [ StructField(item, StringType()) for item in columns ]
    columns_list.extend([StructField(col, StringType()) for col in META_DATA_COLUMNS+[LOAD_DATE]])
    schema_log = StructType(columns_list)
    log_df = snowflake_session.create_dataframe([], schema=schema_log)
    log_df.write.mode('overwrite').save_as_table(target_table)

def get_or_create_target_table(snowflake_session, file_dict, file_columns):
    # creates table if not present, validate table columns and source file columns
    header_row = file_dict.get('contains_header_row')
    target_schema = file_dict.get('target_schema')
    target_table = file_dict.get('target_table')
    target_db = file_dict.get('target_db')
    file_details_id = file_dict.get('file_details_id')
    query = f"""select column_name
                from {target_db}.information_schema.columns
                where table_schema ilike '{target_schema}'
                and table_name ilike '{target_table}'
                order by ordinal_position;"""
    table_columns = snowflake_session.sql(query).collect()
    created = False
    if not table_columns:
        # create database, schema, table if not present
        snowflake_session.sql(f"create database if not exists {target_db};").collect()
        snowflake_session.sql(f"create schema if not exists {target_db}.{target_schema};").collect()
        target_table_ntp = f"{target_db}.{target_schema}.{target_table}"
        if not header_row:
            file_config_columns = get_file_col_details(
                snowflake_session, file_details_id)[['COL_POSITION', 'COL_NAME']].values.tolist()
            file_config_columns = format_columns(file_config_columns)
            # creates table only if length of file columns and config table columns are equal
            if not len(file_config_columns) == len(file_columns):
                return (created, file_config_columns)
            indexes, file_columns = zip(*file_config_columns)
            create_table(snowflake_session, file_columns, target_table_ntp)
            created = True
            return (created, file_config_columns)
        create_table(snowflake_session, file_columns, target_table_ntp)
        created = True
        return (created, file_columns)
    else:
        # get table columns
        if not header_row:
            table_columns = get_file_col_details(
                snowflake_session, file_details_id)[['COL_POSITION', 'COL_NAME']].values.tolist()
        else:
            pd_df = snowflake_session.create_dataframe(table_columns).to_pandas()
            table_columns = pd_df.to_dict(orient='list').get('COLUMN_NAME')
        table_columns = format_columns(table_columns)
        return (created, table_columns)

def get_schema_drift_columns(snowflake_session, target_table, file_columns, table_columns):
    # returns schema drift status and columns, if added or deleted
    table_columns_zip = []
    added_columns = []
    deleted_columns = []
    for table_col in table_columns:
        if table_col in file_columns:
            table_columns_zip.append((file_columns.index(table_col)+1, table_col))
        else:
            table_columns_zip.append(("", table_col))
            deleted_columns.append(table_col)
    for file_ind, file_col in enumerate(file_columns, start=1):
        if file_col not in table_columns:
            added_columns.append(file_col)
            table_columns_zip.append((file_ind, file_col))
    if added_columns:
        add_columns(snowflake_session, target_table, added_columns)
    schema_drift_columns = {'deleted_columns': deleted_columns,
                            'added_columns': added_columns}
    return (True, table_columns_zip, schema_drift_columns)

def check_schema_drift(snowflake_session, file_dict, created, file_columns, table_columns):
    # check for combination of added, dropped, repositioned columns
    target_table = f"{file_dict.get('target_db')}.{file_dict.get('target_schema')}.{file_dict.get('target_table')}"
    if file_dict.get('contains_header_row'):
        if created or file_columns == table_columns:
            return (True, list(enumerate(file_columns, start=1)), dict())
        else:
            return get_schema_drift_columns(snowflake_session, target_table,
                                            file_columns, table_columns)
    else:
        # using FILE_COL_DETAILS columns
        if created or len(file_columns) == len(table_columns):
            return (True, table_columns, dict())
        else:
            print('Error: Cannot Handle Schema Drift Without Header Row')
            return (False, 'ERROR:Cannot Handle Schema Drift Without Header Row', dict())

def copy_into_snowflake(snowflake_session, file_dict, src_file_name, data,
                        root_folder, load_timestamp):
    # use COPY INTO to load data into snowflake
    field_delimiter = file_dict.get('field_delimiter')
    file_wild_card_ext = file_dict.get('file_wild_card_ext')
    record_delimiter = file_dict.get('record_delimiter')
    table_name = f"{file_dict.get('target_db')}.{file_dict.get('target_schema')}.{file_dict.get('target_table')}".upper()
    src_file = src_file_name.replace(f"{root_folder}/","")
    if 'txt' or 'csv' in file_wild_card_ext.lower():
        file_wild_card_ext = 'CSV'
    if '{CR}{LF}' in record_delimiter:
        record_delimiter = '\r\n'
    indexes, columns = map(list, zip(*data))
    columns.extend(META_DATA_COLUMNS+[LOAD_DATE])
    indexes_str = ','.join(['t.$'+str(ind) if str(ind)!="" else 'NULL' for ind in indexes])
    indexes_str = indexes_str + ',' + ','.join([f"METADATA${col}" for col in META_DATA_COLUMNS])
    indexes_str = indexes_str + ',' + f"'{load_timestamp}'"
    columns_str = ','.join([col for col in columns])
    file_format_str = f'type = {file_wild_card_ext} field_delimiter = "{field_delimiter}" record_delimiter = "{record_delimiter}" EMPTY_FIELD_AS_NULL = False '
    if file_dict.get('contains_header_row'):
        file_format_str = file_format_str + ' SKIP_HEADER = 1'
    copy_into_str = f"""copy into {table_name} ({columns_str}) from (select {indexes_str}
                    from '@{STAGE_NAME}/{src_file}' as t) FILE_FORMAT = ({file_format_str})
                    on_error='skip_file' FORCE = TRUE"""
    resp_copy_into = snowflake_session.sql(copy_into_str).collect()
    response = resp_copy_into[0].as_dict() if resp_copy_into else dict()
    print(response)
    return response


def process(**context):
    try:
        # container_name =  context["params"]["container_name"]
        azure_connection = get_azure_connection(context)
        df_file_details = get_file_details(snowflake_session,  context["params"]["customer_id"])
        file_dtls_blb_lst = []
        for ind in df_file_details.index:
            try:
                file_dict = {}
                file_dict['file_details_id'] = int(df_file_details['FILE_DETAILS_ID'][ind])
                file_dict['customer_id'] = int(df_file_details['CUSTOMER_ID'][ind])
                file_dict['file_name_pattern'] = df_file_details['FILE_NAME_PATTERN'][ind]
                file_dict['file_wild_card_ext'] = df_file_details['FILE_WILD_CARD_EXT'][ind]
                file_dict['field_delimiter'] = df_file_details['FIELD_DELIMITER'][ind]
                file_dict['record_delimiter'] = df_file_details['RECORD_DELIMITER'][ind]
                file_dict['contains_header_row'] = df_file_details['CONTAINS_HEADER_ROW'][ind]
                file_dict['target_table'] = df_file_details['TARGET_TABLE'][ind]
                file_dict['target_db'] = df_file_details['TARGET_DB'][ind]
                file_dict['target_schema'] = df_file_details['TARGET_SCHEMA'][ind]
                blob_list = read_blob(azure_connection, file_dict, context)
                if blob_list:
                    file_dtls_blb_lst.append((blob_list, file_dict))
        
            except Exception as e:
                print(e)
        blob_i_col_lst=[]
        for blob_list, file_dict in file_dtls_blb_lst:
            for blob_index,blob_i_col in enumerate(blob_list):
                blob_i,file_columns=blob_i_col
                if not file_columns:
                    continue
                blob_i_col_lst.append((blob_i, file_columns, file_dict))
        ls=[(file_dict, blob_i, file_columns) for blob_i, file_columns, file_dict  in blob_i_col_lst]
        return ls
    except Exception as e:
        print(e)
        

def handle_blob_list(file_dict, blob_i, file_columns, **context):
    # main function
    # iterates over FILE_DETIALS records
    # gets pattern matched files and respective file columns
    # checks if schema dirft is present
    # uses COPY INTO to snowflake
    # updates FILE_INGESTION_DETAILS table
    # updates BRONZE_TO_SILVER_DETAILS table
    
    # container_name =  context["params"]["container_name"]
    azure_connection = get_azure_connection(context)
    
    try:
        created, table_columns = get_or_create_target_table(
        snowflake_session, file_dict, file_columns)
        handle_schema_drift, columns_zip, schema_drift_columns = check_schema_drift(
            snowflake_session, file_dict, created, file_columns, table_columns)
        file_name, load_timestamp = insert_file_ingestion_details(snowflake_session, blob_i)
        if handle_schema_drift:
            response = copy_into_snowflake(
                snowflake_session, file_dict, blob_i, columns_zip,
                 context["params"]["root_folder"], load_timestamp)
        else:
            response = {'status': 'ERROR',
                        'error_details': 'Cannot Handle Schema Drift without Header Row'}
        error_found = update_file_ingestion_details(
            snowflake_session, file_dict['file_details_id'],
            blob_i, response, handle_schema_drift, schema_drift_columns,
            file_name, load_timestamp)
        error_found = update_bronze_to_sliver_details(snowflake_session, file_dict,
                                                      file_name, load_timestamp)
        move_blob(azure_connection, blob_i, error_found,  context)
    except Exception as e:
            if load_timestamp:
                error_message = str(e).replace("'", "")
                error_details = f"ERROR: Could not Process File {blob_i}, {error_message}"
                update_file_ingestion_details(
                    snowflake_session, file_dict['file_details_id'],
                    blob_i, {"status": "ERROR", "error_details": error_details},
                    handle_schema_drift, schema_drift_columns,
                    file_name, load_timestamp)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='af_adls_to_snowflake',
    default_args=default_args,
    description='A DAG to process data stored in Azure Datalake Storage and write to Snowflake',
    schedule_interval=timedelta(days=30),
    catchup=False,
    params={
        "customer_id":Param(
            default=120,
            type=["integer","string"],
            min=1,
            max=255
        ),
        "container_name" :Param(
            default='cont-datalink-dp-shared',
            type=["string"]
        ),
        "root_folder" :Param(
            default='LANDING',
            type=["string"]
        ),
        "database":Param(
            default='DEV_OPS_DB',
            type=["string"]
        ),
        "schema":Param(
            default='CONFIG',
            type=["string"]
        ),
        "run_failed":Param(
            default=True
        )
    }
)as dag:

    Process_Files_ADLS_Snowflake = PythonOperator(
        task_id='Process_Files_ADLS_Snowflake',
        python_callable=process
    )

    with TaskGroup(group_id='Process_Files', dag=dag) as Process_Files:
        
        handle_blob_list_operator = PythonOperator.partial(
            task_id='Process_Files',
            python_callable=handle_blob_list,
            dag=dag
        ).expand(op_args=Process_Files_ADLS_Snowflake.output)

        
Process_Files_ADLS_Snowflake >> handle_blob_list_operator 
