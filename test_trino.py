from datetime import datetime, timedelta
from airflow import DAG
from trino_oauth2_operator import TrinoOAuth2Operator  # import operator bạn đã tạo

with DAG(
    dag_id="test_trino_oauth2_show_catalogs",
    description="DAG test TrinoOAuth2Operator với lệnh SHOW CATALOGS",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["trino", "oauth2", "test"]
) as dag:

    show_catalogs = TrinoOAuth2Operator(
        task_id="show_trino_catalogs",
        sql="select 1",  # câu lệnh Trino
        trino_conn_id="trino_default_oauth2"
    )

    show_catalogs
