from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timedelta
import json


# Constants
OBJECT_CONSTRUCT_COLUMN = "ADDITIONAL_DETAILS"
LOAD_DATE_COLUMN = "LOAD_DATE"
REC_SRC = "REC_SRC"

# Key vault configuration
KEYVAULT_URI = 'https://kv-datalink-dp-pilot.vault.azure.net'


def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value

def get_snowflake_connection(database, schema):
    # connects to snowflake and return snowpark session
    snowflake_connection_parameters = json.loads(get_kv_secret("SnowflakeSecret"))
    user = snowflake_connection_parameters.pop("username")
    snowflake_connection_parameters.update({
        "database": database,
        "schema": schema,
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
    resp_insert_into = snowflake_session.sql(insert_into_str).collect()
    print(resp_insert_into)
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
    print(resp)

def create_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id, file_ing_dtls_id,
                                  db, schema, target_schema, target_table):
    updated_by = snowflake_session.sql("SELECT CURRENT_USER").collect()
    updated_by = f"{updated_by}".split("=")[1].strip("')]'")
    table_schema = ["BRONZE_TO_SILVER_DETAILS_ID", "FILE_INGESTION_DETAILS_ID",
        "TARGET_SCHEMA", "TARGET_TABLE", "START_DATE", "STATUS", "UPDATED_BY"]
    start_date = str(datetime.now())
    data = (brnz_slvr_dtls_id, file_ing_dtls_id, target_schema, target_table,
            start_date, "PENDING", updated_by)
    query = f"""insert into {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS
                ({','.join(table_schema)}) values {str(data)}
            """
    resp = snowflake_session.sql(query).collect()
    print(resp)
    return start_date

def update_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id, file_ing_dtls_id,
                                  db, schema, target_schema, target_table,
                                  status, error_details, date_update_str):
    query=f"""UPDATE {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS
          SET STATUS = '{status}',
          ERROR_DETAILS = '{error_details}'{date_update_str}
          where TARGET_SCHEMA = '{target_schema}'
          AND TARGET_TABLE = '{target_table}'
          AND BRONZE_TO_SILVER_DETAILS_ID = '{brnz_slvr_dtls_id}'
          AND FILE_INGESTION_DETAILS_ID = '{file_ing_dtls_id}'
          """
    resp = snowflake_session.sql(query).collect()
    print(resp)

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

def transformation(snowflake_session, brnz_slvr_dtls_dict, db, schema):
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
            lst_brnz_slvr_step_dtls.append((target_schema, target_table, stm_dtls))
            create_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id, file_ing_dtls_id,
                                      db, schema, target_schema, target_table)
        for target_schema, target_table, stm_dtls in lst_brnz_slvr_step_dtls:
            try:
                update_brnz_slvr_step_details(snowflake_session,
                                            brnz_slvr_dtls_id, file_ing_dtls_id,
                                            db, schema, target_schema, target_table,
                                            "IN_PROGRESS", "",
                                            f", START_DATE=to_timestamp('{str(datetime.now())}')")
                step_status = "FAILED"
                step_error_details = ""
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
                    if item:
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
                    step_error_details = f"ERROR: Couldn't Update {target_schema}.{target_table}"
            except Exception as e:
                step_status == "FAILED"
                step_error_details = f"ERROR: Couldn't Update {target_schema}.{target_table}, {str(e)}"
            finally:
                update_brnz_slvr_step_details(snowflake_session,
                                            brnz_slvr_dtls_id, file_ing_dtls_id,
                                            db, schema, target_schema, target_table,
                                            step_status, step_error_details,
                                            f", END_DATE=to_timestamp('{str(datetime.now())}')")
        if inserted_count and inserted_count == to_be_inserted_count:
            trans_status = "SUCCESS"
        else:
            trans_status = "FAILED"
            error_details = "Couldn't Udpdate Mapped Target Tables"
    except Exception as e:
        trans_status = "FAILED"
        error_details = f"ERROR: Couldn't Udpdate Mapped Target Tables, {str(e)}"
    finally:
        update_brnz_slvr_details(snowflake_session, brnz_slvr_dtls_id,
                                 db, schema, trans_status, error_details,
                                 f", END_DATE = to_timestamp('{str(datetime.now())}')")

def process(**context):
    # Main function
    # Gets Config details of PENDING records from BRONZE_TO_SILVER_DETAILS table
    try:
        db = context['params']['database']
        schema = context['params']['schema']
        snowflake_session = get_snowflake_connection(db, schema)
        pd_brnz_slvr_dtls = get_brnz_slvr_details(snowflake_session)
        for ind in pd_brnz_slvr_dtls.index:
            brnz_slvr_dtls_dict = {}
            brnz_slvr_dtls_dict['source_schema'] = pd_brnz_slvr_dtls['SOURCE_SCHEMA'][ind]
            brnz_slvr_dtls_dict['source_table'] = pd_brnz_slvr_dtls['SOURCE_TABLE'][ind]
            brnz_slvr_dtls_dict['src_ld_dt'] = pd_brnz_slvr_dtls['SOURCE_LOAD_DATE'][ind]
            brnz_slvr_dtls_dict['file_ing_dtls_id'] = pd_brnz_slvr_dtls['FILE_INGESTION_DETAILS_ID'][ind]
            brnz_slvr_dtls_dict['brnz_slvr_dtls_id'] = pd_brnz_slvr_dtls['BRONZE_TO_SILVER_DETAILS_ID'][ind]
            transformation(snowflake_session, brnz_slvr_dtls_dict, db, schema)
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
    dag_id='transform_bronze_to_silver',
    default_args=default_args,
    description='A DAG to process data from Snowflake Bronze to Silver Raw Vault',
    schedule_interval=timedelta(days=30),
    catchup=False,
    params={
        "database" :Param(
            default='DEV_OPS_DB',
            type=["string"]
        ),
        "schema" :Param(
            default='CONFIG',
            type=["string"]
        ),
    }
)as dag:

    transform_bronze_to_silver = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=process
    )

transform_bronze_to_silver