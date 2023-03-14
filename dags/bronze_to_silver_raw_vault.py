from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timedelta
import json

# Key vault configuration
KEYVAULT_URI = 'https://kv-datalink-dp-pilot.vault.azure.net'

OBJECT_CONSTRUCT_COLUMN = "ADDITIONAL_DETAILS"
LOAD_DATE_COLUMN = "LOAD_DATE"

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
                      target_columns, add_dtls_columns, load_date):
    # Snowflake Insert into statment from a source table including Object Construct
    if add_dtls_columns:
        add_dtls_column_index, add_dtls_columns = add_dtls_columns
        obj_const_columns_str = ','.join([f"'{col}',{col}" for col in add_dtls_columns])
        obj_const_str = f"OBJECT_CONSTRUCT ({obj_const_columns_str}) as {OBJECT_CONSTRUCT_COLUMN}"
        source_columns = list(source_columns)
        source_columns[add_dtls_column_index] = obj_const_str
    target_columns_str = ','.join([col for col in target_columns])
    source_columns_str = ','.join([col if "$$" not in col else f"'{col.split('$$')[-1]}'"
                                   for col in source_columns])
    insert_into_str = f"""insert into {target_table}({target_columns_str})
                        select {source_columns_str} from {source_table}
                        where {LOAD_DATE_COLUMN}=to_timestamp('{load_date}')
                        """
    resp_insert_into = snowflake_session.sql(insert_into_str).collect()
    print(resp_insert_into)

def get_brnz_slvr_details(snowflake_session):
    # Fetch records from BRONZE_TO_SILVER_DETAILS Config table
    # Group by SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_LOAD_DATE
    brnz_slvr_details = snowflake_session.table("BRONZE_TO_SILVER_DETAILS").filter(
        col("TRANSFORMATION_STATUS")=="PENDING")
    pd_df_brnz_slvr_dtls = brnz_slvr_details.to_pandas()
    pd_df_drnz_slvr_dtls_grp = pd_df_brnz_slvr_dtls.groupby([
        "SOURCE_SCHEMA", "SOURCE_TABLE", "SOURCE_LOAD_DATE"])
    return pd_df_drnz_slvr_dtls_grp

def get_stm_brnz_slvr_details(snowflake_session, source_schema, source_table):
    # Fetch records from STM_BRONZE_TO_SILVER table
    stm_details = snowflake_session.table("STM_BRONZE_TO_SILVER").filter(
        (col("SOURCE_SCHEMA")==source_schema) & (col("SOURCE_TABLE")==source_table)
        # & (col("TARGET_TABLE") == "LINK_MEMBER_CLAIM")
        )
    pd_df_stm_details = stm_details.to_pandas()
    pd_df_stm_details_grp = pd_df_stm_details.groupby(["TARGET_SCHEMA", "TARGET_TABLE"])
    return pd_df_stm_details_grp

def get_unmapped_columns(snowflake_session, source_schema, source_table,
                         source_target_zip, source_config_columns):
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
                        if scol not in source_config_columns]
    return unmapped_columns

def transformation(snowflake_session, brnz_slvr_dtls_grp):
    # Refers STM_BRONZE_TO_SILVER table with Config data to get Columns
    # Insert records into repsective HUB, Satellite tables
    try:
        source_schema, source_table, src_ld_dt = brnz_slvr_dtls_grp
        stm_details_df = get_stm_brnz_slvr_details(snowflake_session, source_schema, source_table)
        for stm_details_grp, stm_dtls in stm_details_df:
            add_dtls_columns = []
            target_schema, target_table = stm_details_grp
            source_columns, target_columns = zip(*stm_dtls[['SOURCE_COLUMN', 'TARGET_COLUMN']]
                                                .values.tolist())
            if OBJECT_CONSTRUCT_COLUMN in target_columns:
                add_dtls_columns = (target_columns.index(OBJECT_CONSTRUCT_COLUMN),
                                    get_unmapped_columns(
                    snowflake_session, source_schema, source_table,
                    zip(source_columns, target_columns), source_columns
                    ))
            source_table_path = f"{source_schema}.{source_table}"
            target_table_path = f"{target_schema}.{target_table}"
            insert_into_table(snowflake_session, target_table_path, source_table_path,
                            source_columns,target_columns, add_dtls_columns, src_ld_dt)
    except Exception as e:
        print(e)

def process(**context):
    # Main function
    # Gets Config details of PENDING records from BRONZE_TO_SILVER_DETAILS table
    try:
        snowflake_session = get_snowflake_connection(
            context['params']['database'], context['params']['database'])
        brnz_slvr_dtls_df = get_brnz_slvr_details(snowflake_session)
        for brnz_slvr_dtls_grp, brnz_slvr_dtls in brnz_slvr_dtls_df:
            transformation(snowflake_session, brnz_slvr_dtls_grp)
    except Exception as e:
        raise
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
    dag_id='process_data_bronze_silver_raw_vault',
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

    process_data_bronze_silver_raw_vault = PythonOperator(
        task_id='process_data_bronze_silver_raw_vault',
        python_callable=process
    )

process_data_bronze_silver_raw_vault