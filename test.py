from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Định nghĩa hàm Python sẽ được gọi trong DAG
def print_hello():
    print("Hello, Airflow!")

# Định nghĩa DAG
dag = DAG(
    'simple_print_dag',  # Tên DAG
    description='A simple DAG that prints Hello',
    schedule_interval=timedelta(days=1),  # DAG này chỉ chạy khi được trigger thủ công
    start_date=datetime(2023, 9, 9),  # Ngày bắt đầu DAG
    catchup=False,  # Không thực hiện catchup cho những ngày đã qua
)

# Định nghĩa task sử dụng PythonOperator
print_hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,  # Hàm Python sẽ được gọi
    dag=dag,
)

# Thiết lập task order (ở đây chỉ có một task)
print_hello_task
