from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timedelta
import json
import hashlib


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 14),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Transformation_dag_parallel',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    max_active_tasks=5,
    params={
        'database': 'SAMPLE_DB',
        'schema': 'PUBLIC',
    }
)


# Constants
OBJECT_CONSTRUCT_COLUMN = "ADDITIONAL_DETAILS"
LOAD_DATE_COLUMN = "LOAD_DATE"
REC_SRC = "REC_SRC"

# Key vault configuration
KEYVAULT_URI = 'https://kv-datalink-dp-pilot.vault.azure.net'

def print_log(title, data, status):
    print("-----"*2+title)
    print(data)
    print(status)
    print("-----"*2)

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
        "database": dag.params.get('database'),
        "schema": dag.params.get('schema'),
        "user": user
    })
    # create session using snowflake connector
    session = Session.builder.configs(snowflake_connection_parameters).create()
    return session

def insert_into_table(snowflake_session, target_table, source_table, source_columns,
                      target_columns, load_date):
    # Snowflake Insert into statment from a source table
    target_columns_str = ','.join([col for col in target_columns])
    source_columns_str = ','.join([col if "$$" not in col else f"'{col.split('$$')[-1]}'"
                                   for col in source_columns])
    insert_into_str = f"""insert into {target_table}({target_columns_str})
                        select {source_columns_str} from {source_table}
                        where {LOAD_DATE_COLUMN}=to_timestamp('{load_date}')
                        """
    print(insert_into_str)
    resp_insert_into = snowflake_session.sql(insert_into_str).collect()
    print_log(f"insert_into_table | {target_table}", resp_insert_into, "")
    return resp_insert_into

def update_brnz_slvr_details(snowflake_session, brnz_slvr_dtls_id, db, schema,
                             status, error_details, date_update_str):
    # Update STATUS and TIMESTAMP in BRONZE_TO_SILVER_DETAILS
    query = f"""update {db}.{schema}.BRONZE_TO_SILVER_DETAILS
                set TRANSFORMATION_STATUS = '{status}'{date_update_str},
                ERROR_DETAILS = '{error_details}'
                where BRONZE_TO_SILVER_DETAILS_ID = {brnz_slvr_dtls_id}
            """
    resp = snowflake_session.sql(query).collect()
    print_log(f"update_brnz_slvr_details | {db}.{schema}.BRONZE_TO_SILVER_DETAILS",
               resp, status)

def create_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id, file_ing_dtls_id,
                                  db, schema, target_schema, target_table):
    updated_by = snowflake_session.sql("SELECT CURRENT_USER").collect()
    updated_by = f"{updated_by}".split("=")[1].strip("')]'")
    created_date = str(datetime.now())
    table_schema = ["BRONZE_TO_SILVER_DETAILS_ID", "FILE_INGESTION_DETAILS_ID",
        "TARGET_SCHEMA", "TARGET_TABLE", "STATUS", "CREATED_DATE", "UPDATED_BY"]
    data = (brnz_slvr_dtls_id, file_ing_dtls_id, target_schema,
            target_table, "PENDING", created_date, updated_by)
    query = f"""insert into {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS
                ({','.join(table_schema)}) values {str(data)}
            """
    resp = snowflake_session.sql(query).collect()
    print_log(f"create_brnz_slvr_step_details | {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS",
               resp, target_table)
    return created_date

def update_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id, file_ing_dtls_id,
                                  db, schema, target_schema, target_table, created_date,
                                  status, error_details, date_update_str):
    query=f"""UPDATE {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS
          SET STATUS = '{status}',
          ERROR_DETAILS = '{error_details}'{date_update_str}
          where TARGET_SCHEMA = '{target_schema}'
          AND TARGET_TABLE = '{target_table}'
          AND BRONZE_TO_SILVER_DETAILS_ID = '{brnz_slvr_dtls_id}'
          AND FILE_INGESTION_DETAILS_ID = '{file_ing_dtls_id}'
          AND CREATED_DATE = {created_date}
          """
    resp = snowflake_session.sql(query).collect()
    print_log(f"update_brnz_slvr_step_details | {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS",
               resp, f"{target_table}\n{status}")

def get_brnz_slvr_details(snowflake_session):
    # Fetch PENDING status records from BRONZE_TO_SILVER_DETAILS Config table
    brnz_slvr_details = snowflake_session.table("BRONZE_TO_SILVER_DETAILS").filter(
        col("TRANSFORMATION_STATUS")=="PENDING")
    pd_df_brnz_slvr_dtls = brnz_slvr_details.to_pandas()
    return pd_df_brnz_slvr_dtls

def get_stm_brnz_slvr_details(snowflake_session, source_schema, source_table):
    # Fetch records from STM_BRONZE_TO_SILVER table
    # Group by TARGET_SCHEMA, TARGET_TABLE
    stm_details = snowflake_session.table("STM_BRONZE_TO_SILVER").filter(
        (col("SOURCE_SCHEMA")==source_schema) & (col("SOURCE_TABLE")==source_table)
        # & (col("TARGET_TABLE") == "LINK_MEMBER_CLAIM")
        )
    pd_df_stm_details = stm_details.to_pandas()
    pd_df_stm_details_grp = pd_df_stm_details.groupby(["TARGET_SCHEMA", "TARGET_TABLE"])
    return pd_df_stm_details_grp

def get_file_ingestion_details(snowflake_session, file_ing_dtls_id):
    # Get FILE_INGESTION_DETAILS record based on FILE_INGESTION_DETAILS_ID
    file_ing_dtls = snowflake_session.table("FILE_INGESTION_DETAILS").filter(
        col("FILE_INGESTION_DETAILS_ID") == file_ing_dtls_id)
    file_ing_dtls = file_ing_dtls.to_pandas()
    return file_ing_dtls

def get_unmapped_columns(snowflake_session, deleted_columns, source_schema,
                         source_table, source_target_zip, source_config_columns):
    # Compares source table columns with target columns and returns the unmapped columns
    data = dict(source_target_zip)
    source_db, source_schema = source_schema.split('.')
    query = f"""select column_name
                from {source_db}.information_schema.columns
                where table_schema ilike '{source_schema}'
                and table_name ilike '{source_table}'
                order by ordinal_position;"""
    source_columns = snowflake_session.sql(query).to_pandas().values.tolist()
    source_columns_flatten = [item for sublist in source_columns for item in sublist]
    unmapped_columns = [scol for scol in source_columns_flatten
                        if scol not in source_config_columns and scol not in deleted_columns]
    return unmapped_columns

def get_deleted_columns(snowflake_session, file_ing_dtls_id):
    # Get deleted columns from FILE_INGESTION_DETAILS (SCHEMA_DRIFT_COLUMNS)
    file_ing_dtls = get_file_ingestion_details(snowflake_session, file_ing_dtls_id)
    if file_ing_dtls.empty:
        return list()
    deleted_columns = []
    schema_drift_cols = file_ing_dtls[['SCHEMA_DRIFT_COLUMNS']].values.tolist()[0][0]
    if schema_drift_cols:
        deleted_columns = json.loads(schema_drift_cols).get('deleted_columns')
    return deleted_columns

def transformation(snowflake_session, brnz_slvr_dtls_dict):
    # Refers STM_BRONZE_TO_SILVER table with Config data to get Columns
    # Creates Object Construct if OBJECT_CONSTRUCT_COLUMN is present
    # Insert records into repsective HUB, Satellite and Link tables
    try:
        source_schema = brnz_slvr_dtls_dict['source_schema']
        source_table = brnz_slvr_dtls_dict['source_table']
        src_ld_dt = brnz_slvr_dtls_dict['src_ld_dt']
        file_ing_dtls_id = brnz_slvr_dtls_dict['file_ing_dtls_id']
        brnz_slvr_dtls_id = brnz_slvr_dtls_dict['brnz_slvr_dtls_id']
        trans_status = "FAILED"
        error_details = ""
        db = dag.params.get('database')
        schema = dag.params.get('schema')

        update_brnz_slvr_details(snowflake_session, brnz_slvr_dtls_id,
                                 db, schema, "IN_PROGRESS", error_details,
                                 f", START_DATE = to_timestamp('{str(datetime.now())}')")
        stm_details_df = get_stm_brnz_slvr_details(snowflake_session, source_schema, source_table)
        file_ing_dtls_id = int(file_ing_dtls_id)
        deleted_columns = get_deleted_columns(snowflake_session, file_ing_dtls_id)
        to_be_inserted_count = len(stm_details_df)
        inserted_count = 0
        lst_brnz_slvr_step_dtls = []
        for stm_details_grp, stm_dtls in stm_details_df:
            target_schema, target_table = stm_details_grp
            created_date = create_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id,
                                                       file_ing_dtls_id, db, schema,
                                                       target_schema, target_table)
            lst_brnz_slvr_step_dtls.append((target_schema, target_table, stm_dtls, created_date))
        for target_schema, target_table, stm_dtls, created_date in lst_brnz_slvr_step_dtls:
            try:
                step_start_date = str(datetime.now())
                step_status = "FAILED"
                step_error_details = ""
                update_brnz_slvr_step_details(snowflake_session,
                                            brnz_slvr_dtls_id, file_ing_dtls_id,
                                            db, schema, target_schema, target_table,
                                            f"to_timestamp('{created_date}')", "IN_PROGRESS", "",
                                            f", START_DATE=to_timestamp('{step_start_date}')")
                source_columns, target_columns, source_column_fn = zip(*stm_dtls[
                    ['SOURCE_COLUMN', 'TARGET_COLUMN', 'SOURCE_COLUMN_FN']].values.tolist())
                source_columns = list(source_columns)
                source_column_fn = list(source_column_fn)
                if OBJECT_CONSTRUCT_COLUMN in target_columns:
                    add_dtls_column_index, add_dtls_columns = (
                        target_columns.index(OBJECT_CONSTRUCT_COLUMN), get_unmapped_columns(
                        snowflake_session, deleted_columns, source_schema, source_table,
                        zip(source_columns, target_columns), source_columns ))
                    obj_const_columns_str = ','.join([f"'{col}',{col}" for col in add_dtls_columns])
                    obj_const_str = f"OBJECT_CONSTRUCT ({obj_const_columns_str}) as {OBJECT_CONSTRUCT_COLUMN}"
                    source_columns[add_dtls_column_index] = obj_const_str
                if REC_SRC in target_columns:
                    source_columns[target_columns.index(REC_SRC)] = str(file_ing_dtls_id)
                if LOAD_DATE_COLUMN in target_columns:
                    source_columns[target_columns.index(LOAD_DATE_COLUMN)] = f"to_timestamp('{str(datetime.now())}')"
                for indx, item in enumerate(source_column_fn):
                    if item and item.upper() != 'NULL':
                        source_columns[indx] = source_column_fn[indx]
                source_table_path = f"{source_schema}.{source_table}"
                target_table_path = f"{target_schema}.{target_table}"
                response = insert_into_table(snowflake_session, target_table_path,
                                source_table_path, source_columns, target_columns, src_ld_dt)
                if response:
                    row = response[0].as_dict()
                    if 'number of rows inserted' in row:
                        inserted_count += 1
                        step_status = "SUCCESS"
                if step_status == "FAILED":
                    step_error_details = f"ERROR: Could not Update {target_schema}.{target_table}"
            except Exception as e:
                step_status == "FAILED"
                step_message = str(e).replace("'", "")
                step_error_details = f"ERROR: Could not Update {target_schema}.{target_table}, {step_message}"
            finally:
                update_brnz_slvr_step_details(snowflake_session,
                                            brnz_slvr_dtls_id, file_ing_dtls_id,
                                            db, schema, target_schema, target_table,
                                            f"to_timestamp('{created_date}')", step_status,
                                            step_error_details,
                                            f", END_DATE=to_timestamp('{str(datetime.now())}')")
        if inserted_count and inserted_count == to_be_inserted_count:
            trans_status = "SUCCESS"
        else:
            trans_status = "FAILED"
            error_details = "Could not Update Mapped Target Table(s)"
    except Exception as e:
        trans_status = "FAILED"
        trans_message = str(e).replace("'", "")
        error_details = f"ERROR: {trans_message}"
    finally:
        update_brnz_slvr_details(snowflake_session, brnz_slvr_dtls_id,
                                 db, schema, trans_status, error_details,
                                 f", END_DATE = to_timestamp('{str(datetime.now())}')")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

snowflake_session = get_snowflake_connection()
pd_brnz_slvr_dtls = get_brnz_slvr_details(snowflake_session)
with TaskGroup(group_id='transform' , dag= dag) as transform:
    for ind in pd_brnz_slvr_dtls.index:
        brnz_slvr_dtls_dict = {}
        brnz_slvr_dtls_dict['source_schema'] = pd_brnz_slvr_dtls['SOURCE_SCHEMA'][ind]
        brnz_slvr_dtls_dict['source_table'] = pd_brnz_slvr_dtls['SOURCE_TABLE'][ind]
        brnz_slvr_dtls_dict['src_ld_dt'] = pd_brnz_slvr_dtls['SOURCE_LOAD_DATE'][ind]
        brnz_slvr_dtls_dict['file_ing_dtls_id'] = pd_brnz_slvr_dtls['FILE_INGESTION_DETAILS_ID'][ind]
        brnz_slvr_dtls_dict['brnz_slvr_dtls_id'] = pd_brnz_slvr_dtls['BRONZE_TO_SILVER_DETAILS_ID'][ind]
        task_id = f"transformation_bronze_to_silver_{brnz_slvr_dtls_dict['brnz_slvr_dtls_id']}"
        print(task_id)
        transformation_operator = PythonOperator(
            task_id=task_id,
            python_callable=transformation,
            op_kwargs={
            'snowflake_session':snowflake_session,
            'brnz_slvr_dtls_dict':brnz_slvr_dtls_dict
            },
            dag=dag
        )
<<<<<<< HEAD
        transform
        
        
        
        
        
# def process(**context):
#     # Main function
#     # Gets Config details of PENDING records from BRONZE_TO_SILVER_DETAILS table
#     try:
#         db = context['params']['database']
#         schema = context['params']['schema']
#         snowflake_session = get_snowflake_connection(db, schema)
#         pd_brnz_slvr_dtls = get_brnz_slvr_details(snowflake_session)
#         for ind in pd_brnz_slvr_dtls.index:
#             brnz_slvr_dtls_dict = {}
#             brnz_slvr_dtls_dict['source_schema'] = pd_brnz_slvr_dtls['SOURCE_SCHEMA'][ind]
#             brnz_slvr_dtls_dict['source_table'] = pd_brnz_slvr_dtls['SOURCE_TABLE'][ind]
#             brnz_slvr_dtls_dict['src_ld_dt'] = pd_brnz_slvr_dtls['SOURCE_LOAD_DATE'][ind]
#             brnz_slvr_dtls_dict['file_ing_dtls_id'] = pd_brnz_slvr_dtls['FILE_INGESTION_DETAILS_ID'][ind]
#             brnz_slvr_dtls_dict['brnz_slvr_dtls_id'] = pd_brnz_slvr_dtls['BRONZE_TO_SILVER_DETAILS_ID'][ind]
#             transformation(snowflake_session, brnz_slvr_dtls_dict, db, schema)
#     except Exception as e:
#         print(e)
=======
        transform
>>>>>>> 2c2e1189beab8a5b78955fa1ca55f5c573225223
