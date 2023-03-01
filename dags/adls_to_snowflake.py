from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient
import pandas as pd
import sys
import fnmatch
import os
import re
import ntpath
import json
import urllib.parse

# Snowflake configuration
DATABASE = "DEV_OPS_DB"
SCHEMA = "CONFIG"
STRG_INT_NAME = 'DEV_BCBSAR_RAW_DB.PUBLIC.DATALINK_DP_AZURE_RAWFILE_STAGE'
META_DATA_COLUMNS = ['FILENAME', 'FILE_ROW_NUMBER', 'START_SCAN_TIME']

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

def get_azure_connection(container_name):
    # create a client to interact with blob storage
    blob_details = json.loads(get_kv_secret("ADLSBlobConnSTR"))
    connection_str = blob_details.get('connection_string')
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)
    account_name = urllib.parse.urlsplit(blob_details.get('host')).hostname.split('.')[0]
    #use the client to connect to the container
    container_client = blob_service_client.get_container_client(container_name)
    sas = connection_str.split('SharedAccessSignature=')[1]
    connection = (container_client, account_name, sas)
    return connection

def get_file_details(snowflake_session, customer_id):
    # Fetch records from FILE_DETAILS Config table
    file_details = snowflake_session.table("FILE_DETAILS").filter(col("CUSTOMER_ID") == customer_id)
    pd_df_file_details = file_details.to_pandas()
    # pd_df_file_details = pd_df_file_details.groupby(["TARGET_DB", "TARGET_SCHEMA", "TARGET_TABLE"])
    # pd_df_file_details = pd_df_file_details.groupby("TARGET_TABLE")
    return pd_df_file_details

def get_file_col_details(snowflake_session, file_details_id):
    # Fetch records from FILE_COL_DETAILS Config table
    file_col_details = snowflake_session.table("FILE_COL_DETAILS").filter(col("FILE_DETAILS_ID") == file_details_id).sort(col("COL_POSITION"))
    pd_df_file_col_details = file_col_details.to_pandas()
    return pd_df_file_col_details

def update_file_ingestion_details(snowflake_session, file_details_id, ingestion_details, container_name):
    # creates a record in FILE_INGESTION_DETAILS table from COPY INTO command response
    schema = ["FILE_DETAILS_ID", "FILE_NAME", "FILE_PATH", "INGESTION_STATUS",
            "DATE_CREATED", "DATE_INGESTED", "ERROR_DETAILS", "INGESTED_ROW_COUNT",
            "UPDATED_BY", "WARNING_DETAILS"]
    file_name = ntpath.basename(ingestion_details['file'])
    file_path = ingestion_details['file'].split(container_name+'/')[-1]
    error_details = ""
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
    data = (file_details_id, file_name, file_path, ingestion_details['status'],
        str(datetime.now()), str(datetime.now()), error_details, ingestion_details['rows_loaded'],
        '', '')
    query = f"insert into {DATABASE}.{SCHEMA}.FILE_INGESTION_DETAILS ({','.join(schema)}) values {str(data)}"
    resp = snowflake_session.sql(query).collect()
    print(resp)

def add_columns(snowflake_session, target_table, columns):
    columns_dtype = ','.join([f"{col} VARCHAR" for col in columns])
    snowflake_session.sql(f"alter table {target_table} add {columns_dtype}").collect()

def format_columns(columns):
    # Replace special characters with underscore for the columns
    col_list = []
    special_char_set = "[@_!#$%^&*()<>?/\|}{~:]"
    for col in columns:
        if col in META_DATA_COLUMNS:
            continue
        if isinstance(col, (list, tuple)):
            col_list.append((col[0], re.sub(special_char_set,"_", col[1]).upper()))
        else:    
            col_list.append(re.sub(special_char_set,"_", col).upper())
    return col_list

def pattern_matching(azure_session, file_name_pattern, file_wild_card_ext):
    # Returns a list of files if file_name_pattern matches
    blob_list = []
    for blob_i in azure_session.list_blobs():
        file_name = blob_i.name.lower()
        file_name_pattern = file_name_pattern.lower()
        if fnmatch.fnmatch(file_name, file_wild_card_ext.lower()):
            if "*" in file_name_pattern:
                if '/' in file_name:
                    file_name = file_name.split('/')[-1]
                if fnmatch.fnmatch(file_name, file_name_pattern):
                    blob_list.append(blob_i.name)
            else:
                if file_name_pattern in file_name:
                    blob_list.append(blob_i.name)
    return blob_list

def read_blob(azure_connection, file_dict, container_name):
    # Returns a list of tupple containing file_name, file_columns and respective sas_url 
    azure_session, account_name, sas = azure_connection
    blob_list = pattern_matching(azure_session, file_dict['file_name_pattern'], file_dict['file_wild_card_ext'])
    blob_with_sas_list = []
    for blob_i in blob_list:
        sas_url = 'https://' + account_name +'.blob.core.windows.net/' + container_name + '/' + blob_i + '?' + sas
        file_columns = format_columns(pd.read_csv(sas_url, sep=file_dict['field_delimiter'], nrows=0).columns.to_list())
        blob_with_sas_list.append((blob_i, file_columns))
    return blob_with_sas_list

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
                order by ordinal_position;
            """
    table_columns = snowflake_session.sql(query).collect()
    created = False
    if not table_columns:
        # create table if not present
        snowflake_session.sql(f"create schema if not exists {target_db}.{target_schema};").collect()
        if not header_row:
            file_columns = get_file_col_details(snowflake_session, file_details_id)[['COL_POSITION', 'COL_NAME']].values.tolist()
            file_columns = format_columns(file_columns)
            indexes, columns_to_create = zip(*file_columns)
            columns_list = [ StructField(item, StringType()) for item in columns_to_create ]
        else:
            columns_list = [ StructField(item, StringType()) for item in file_columns ]
        columns_list.extend([StructField(col, StringType()) for col in META_DATA_COLUMNS])
        schema_log = StructType(columns_list)
        log_df = snowflake_session.create_dataframe([], schema=schema_log)
        log_df.write.mode('overwrite').save_as_table(f"{target_db}.{target_schema}.{target_table}")
        created = True
        return (created, file_columns)
    else:
        # validate the columns
        if not header_row:
            table_columns = get_file_col_details(snowflake_session, file_details_id)[['COL_POSITION', 'COL_NAME']].values.tolist()
        else:
            pd_df = snowflake_session.create_dataframe(table_columns).to_pandas()
            table_columns = pd_df.to_dict(orient='list').get('COLUMN_NAME')
        table_columns = format_columns(table_columns)
        return (created, table_columns)

def get_schema_drift_columns(snowflake_session, target_table, file_columns, table_columns):
    table_columns_zip = []
    added_columns = []
    for table_col in table_columns:
        if table_col in file_columns:
            table_columns_zip.append((file_columns.index(table_col)+1, table_col))
        else:
            table_columns_zip.append(("", table_col))
    for file_ind, file_col in enumerate(file_columns, start=1):
        if file_col not in table_columns:
            added_columns.append(file_col)
            table_columns_zip.append((file_ind, file_col))
    if added_columns: 
        add_columns(snowflake_session, target_table, added_columns)
    return (True, table_columns_zip)

def check_schema_drift(snowflake_session, file_dict, created, file_columns, table_columns):
    # check for combination of added, dropped, repositioned columns
    target_table = f"{file_dict.get('target_db')}.{file_dict.get('target_schema')}.{file_dict.get('target_table')}"
    if file_dict.get('contains_header_row'):
        if created or file_columns == table_columns:
            return (False, list(enumerate(file_columns, start=1)))
        else:
            return get_schema_drift_columns(snowflake_session, target_table, file_columns, table_columns)
    else:
        # using FILE_COL_DETAILS columns
        if created or len(file_columns) == len(table_columns):
            return (False, table_columns)
        else:
            print(True, 'Error: Having schema drift without header row')
            return (True, 'Error: Having schema drift without header row')

def copy_into_snowflake(snowflake_session, file_dict, src_file_name, data):
    # use COPY INTO to load data into snowflake
    field_delimiter = file_dict.get('field_delimiter')
    file_wild_card_ext = file_dict.get('file_wild_card_ext')
    table_name = f"{file_dict.get('target_db')}.{file_dict.get('target_schema')}.{file_dict.get('target_table')}".upper()
    src_file = ntpath.basename(src_file_name)
    if 'txt' or 'csv' in file_wild_card_ext.lower():
        file_wild_card_ext = 'CSV'
    indexes, columns = map(list, zip(*data))
    columns.extend(META_DATA_COLUMNS)
    indexes_str = ','.join(['t.$'+str(ind) if str(ind)!="" else 'NULL' for ind in indexes])
    indexes_str = indexes_str + ',' + ','.join([f"METADATA${col}" for col in META_DATA_COLUMNS])
    columns_str = ','.join([col for col in columns])
    file_format_str = f'type = {file_wild_card_ext} field_delimiter = "{field_delimiter}" EMPTY_FIELD_AS_NULL = False '
    if file_dict.get('contains_header_row'):
        file_format_str = file_format_str + ' SKIP_HEADER = 1'
    copy_into_str = f"copy into {table_name} ({columns_str}) from (select {indexes_str} from '@{STRG_INT_NAME}/{src_file}' as t) FILE_FORMAT = ({file_format_str}) on_error='skip_file'"
    resp_copy_into = snowflake_session.sql(copy_into_str).collect()
    response = resp_copy_into[0].as_dict() if resp_copy_into else dict()
    print(response)
    return response

def process(**context):
    # Main function
    # Iterates over FILE_DETIALS records
    # Gets pattern matched files and respective file columns
    # Checks if schema dirft is present
    # Uses COPY INTO to snowflake
    # Updates FILE_INGESTION_DETAILS table
    try:
        container_name = context["params"]["container_name"]
        snowflake_session = get_snowflake_connection()
        azure_connection = get_azure_connection(container_name)
        df_file_details = get_file_details(snowflake_session,context['params']["customer_id"])
        # for target_table, df_file_details in pd_df_file_details:
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
                blob_list = read_blob(azure_connection, file_dict, container_name)
                for item in blob_list:
                    blob_i, file_columns = item
                    created, table_columns = get_or_create_target_table(
                        snowflake_session, file_dict, file_columns)
                    is_schema_drift, columns_zip = check_schema_drift(
                        snowflake_session, file_dict, created, file_columns, table_columns)
                    response = copy_into_snowflake(
                        snowflake_session, file_dict, src_file_name=blob_i, data=columns_zip)
                    update_file_ingestion_details(
                        snowflake_session, file_dict['file_details_id'], response,container_name)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)
        

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
    dag_id='Process_ADLS_to_Snowflake',
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
            default='cont-datalink-dp',
            type=["string"]
        ) 
    }
)as dag:

    Process_Files_ADLS_Snowflake = PythonOperator(
        task_id='Process_Files_ADLS_Snowflake',
        python_callable=process
    )

Process_Files_ADLS_Snowflake