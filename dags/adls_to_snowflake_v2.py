from airflow import DAG
from airflow.operators.python import PythonOperator, ExternalPythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def load_adls_snowflake(**context):
    import sys
    sys.path.append('/usr/local/airflow/include/scripts')
    import adls_snowflake_ingest
    file_list = adls_snowflake_ingest.process(context)
    print("=========file_list========",len(file_list),file_list)
    return file_list


def move_adls_blob(file_dict, blob_i, file_columns, created, table_columns,**context):
    print("==============adls_blob_list===============",file_dict, blob_i, file_columns, created, table_columns,context)
    import sys
    sys.path.append('/usr/local/airflow/include/scripts')
    import adls_snowflake_ingest
    adls_snowflake_ingest.handle_blob_list(file_dict, blob_i, file_columns, created, table_columns, context)

with DAG(
    dag_id="af_adls_to_snowflake_v2",
    default_args=default_args,
    description="A DAG to process data stored in Azure Datalake Storage and write to Snowflake",
    schedule_interval=timedelta(days=30),
    # catchup=False,
    params={
        "customer_id": Param(default=120, type=["integer", "string"], min=1, max=255),
        "container_name": Param(default="cont-datalink-dp-shared", type=["string"]),
        "root_folder": Param(default="LANDING", type=["string"]),
        "database": Param(default="DEV_OPS_DB", type=["string"]),
        "schema": Param(default="CONFIG", type=["string"]),
        "run_failed": Param(default=True),
    },
) as dag:

    copy_from_adls_snowflake = ExternalPythonOperator(
        provide_context=True,
        task_id="copy_from_adls_snowflake",
        python_callable=load_adls_snowflake,
        python="/home/astro/.pyenv/versions/snowpark_env/bin/python",
        op_kwargs={
            "params": {
                "customer_id": """{{params.customer_id}}""",
                "container_name": """{{params.container_name}}""",
                "root_folder": """{{params.root_folder}}""",
                "database": """{{params.database}}""",
                "schema": """{{params.schema}}""",
                "run_failed": """{{params.run_failed}}""",
            }
        },
    )

    with TaskGroup(group_id='process_files') as process_files:

        handle_blob_list = ExternalPythonOperator.partial(
            task_id='load_adls_snowflake',
            python_callable=move_adls_blob,
            python="/home/astro/.pyenv/versions/snowpark_env/bin/python",
            op_kwargs={
            "params": {
                "customer_id": """{{params.customer_id}}""",
                "container_name": """{{params.container_name}}""",
                "root_folder": """{{params.root_folder}}""",
                "database": """{{params.database}}""",
                "schema": """{{params.schema}}""",
                "run_failed": """{{params.run_failed}}""",
            }
        },
        ).expand(op_args=copy_from_adls_snowflake.output)
    

    copy_from_adls_snowflake >> process_files
