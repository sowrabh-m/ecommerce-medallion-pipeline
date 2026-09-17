from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DBT_BIN = "/opt/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/ecommerce_medallion"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
S3_BUCKET = "de-ecommerce-medallion"

default_args = {
    "owner": "sowrabh",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def load_to_bronze(ds, **context):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    sql = f"""
    COPY INTO DE_ECOMMERCE_DB.BRONZE.ORDERS
    FROM @DE_ECOMMERCE_DB.BRONZE.RAW_STAGE/{ds}/orders.csv
    FILE_FORMAT = (FORMAT_NAME = DE_ECOMMERCE_DB.BRONZE.CSV_FORMAT)
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO DE_ECOMMERCE_DB.BRONZE.ORDER_ITEMS
    FROM @DE_ECOMMERCE_DB.BRONZE.RAW_STAGE/{ds}/order_items.csv
    FILE_FORMAT = (FORMAT_NAME = DE_ECOMMERCE_DB.BRONZE.CSV_FORMAT)
    ON_ERROR = 'ABORT_STATEMENT';
    """
    hook.run(sql)


with DAG(
    dag_id="ecommerce_medallion_daily",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_orders = S3KeySensor(
        task_id="wait_for_orders_file",
        bucket_name=S3_BUCKET,
        bucket_key="{{ ds }}/orders.csv",
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=30,
    )

    wait_for_order_items = S3KeySensor(
        task_id="wait_for_order_items_file",
        bucket_name=S3_BUCKET,
        bucket_key="{{ ds }}/order_items.csv",
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=30,
    )

    load_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_to_bronze,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    success_alert = EmptyOperator(task_id="success_alert")
    failure_alert = EmptyOperator(task_id="failure_alert", trigger_rule="one_failed")

    [wait_for_orders, wait_for_order_items] >> load_bronze >> dbt_run >> dbt_test
    dbt_test >> success_alert
    dbt_test >> failure_alert