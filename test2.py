from datetime import datetime
from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook

# Định nghĩa DAG
with DAG(
    dag_id="example_trino_operator",
    schedule_interval="@daily",   # chạy hàng ngày
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["trino", "example"],
) as dag:

    # Task chạy query trên Trino
    run_trino_query = TrinoOperator(
        task_id="run_trino_query",
        trino_conn_id="trino_default_oauth2",  # connection ID cấu hình trong Airflow
        sql="""
            show catalogs;
        """,
    )

    run_trino_query
