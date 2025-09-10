from datetime import datetime
from airflow import DAG
from trino_oauth2_operator import TrinoOAuth2Operator3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag = DAG(
    'trino_oauth2_test_dag',
    description='Test Trino OAuth2 Hook và Operator',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 9, 10),
    catchup=False,
)

sql_query = "SHOW CATALOGS"

run_trino_sql = TrinoOAuth2Operator(
    task_id='run_trino_sql_task',
    sql=sql_query,
    trino_conn_id='trino_default_oauth2',  # Sử dụng kết nối Trino đã được tạo trong Airflow UI
    dag=dag
)

run_trino_sql
