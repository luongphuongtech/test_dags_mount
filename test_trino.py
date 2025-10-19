from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook

with DAG(
    dag_id="test_trino_oauth2_show_catalogs",
    description="DAG test TrinoOAuth2Operator với lệnh SHOW CATALOGS",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["trino", "oauth2", "test"]
) as dag:
    show_catalogs = TrinoOperator(
        task_id="show_trino_catalogs",
        sql="show schemas from tpch",  # câu lệnh Trino
        trino_conn_id="trino_default_oauth2"
    )
    show_catalogs
