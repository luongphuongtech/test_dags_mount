from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Hàm Python đơn giản sẽ được gọi bởi PythonOperator
def hello_world():
    print("Hello, world! This is a simple Airflow DAG.")

# Định nghĩa các tham số DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
dag = DAG(
    'simple_airflow_dag',
    default_args=default_args,
    description='Một DAG đơn giản cho Airflow 3.0.2',
    schedule=timedelta(days=1),  # Chạy mỗi ngày
)

# Tác vụ bắt đầu (dummy task)
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Tác vụ in thông báo
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)

# Tác vụ kết thúc (dummy task)
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Xác định thứ tự thực thi các tác vụ
start_task >> hello_task >> end_task
