from airflow import DAG
from airflow.operators.python import PythonOperator, ExternalPythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

ENV_ID = "DEV"
DAG_ID = "af_bronze_to_silver"

def load_bronze_silver_raw_vault(**context):
    import sys
    sys.path.append('/usr/local/airflow/include/scripts')
    import bronze_silver_raw_transform
    file_list = bronze_silver_raw_transform.process(context)
    print("=========file_list========",len(file_list),file_list)
    return file_list


def transform(brnz_slvr_dtls_dict, **context):
    print("==============adls_blob_list===============", brnz_slvr_dtls_dict, context)
    import sys
    sys.path.append('/usr/local/airflow/include/scripts')
    import bronze_silver_raw_transform
    bronze_silver_raw_transform.transformation(brnz_slvr_dtls_dict, context)

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="A DAG to transform data to Bronze Silver Raw Vault in Snowflake",
    schedule_interval=None,
    catchup=False,
    params={
        "database":Param(default='DEV_OPS_DB', type=["string"]),
        "schema":Param(default='CONFIG', type=["string"]),
        "run_failed":Param(default=True)
    }
) as dag:


    process_operator=ExternalPythonOperator(
        provide_context=True,
        task_id='process_operator',
        python_callable=load_bronze_silver_raw_vault,
        python="/home/astro/.pyenv/versions/snowpark_env/bin/python",
        op_kwargs={
            "params": {
                "database": """{{params.database}}""",
                "schema": """{{params.schema}}""",
                "run_failed": """{{params.run_failed}}""",
            }
        }
    )

    with TaskGroup(group_id='transform_files') as transform_files:
        transformation_operator = ExternalPythonOperator.partial(
            task_id='transformation_bronze_to_silver',
            python_callable=transform,
            python="/home/astro/.pyenv/versions/snowpark_env/bin/python",
            op_kwargs={
                "params": {
                    "database": """{{params.database}}""",
                    "schema": """{{params.schema}}""",
                    "run_failed": """{{params.run_failed}}""",
                }
            }
        ).expand(op_args=process_operator.output)

    process_operator >> transform_files