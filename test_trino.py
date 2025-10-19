from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.operators.python import PythonOperator

def run_trino_query():
    """Hàm chạy SQL query bằng TrinoHook"""
    hook = TrinoHook(trino_conn_id="trino_default_oauth2")
    sql = "SHOW CATALOGS"
    records = hook.get_records(sql)
    for row in records:
        print(row)
    return records

with DAG(
    dag_id="test_trino_oauth2_show_catalogs",
    description="DAG test TrinoOAuth2Operator với lệnh SHOW CATALOGS",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["trino", "oauth2", "test"]
) as dag:
    show_catalogs = PythonOperator(
        task_id="run_show_catalogs",
        python_callable=run_trino_query,
    )
    show_catalogs
