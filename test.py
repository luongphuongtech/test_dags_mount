from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Hàm Python đơn giản sẽ được gọi bởi PythonOperator
def start_task():
    print("Starting the DAG...")

def hello_task():
    print("Hello, world! This is a simple Airflow DAG.")

def end_task():
    print("Ending the DAG...")

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

# Tạo các tác vụ PythonOperator
start_task_operator = PythonOperator(
    task_id='start_task',
    python_callable=start_task,
    dag=dag,
)

hello_task_operator = PythonOperator(
    task_id='hello_task',
    python_callable=hello_task,
    dag=dag,
)

end_task_operator = PythonOperator(
    task_id='end_task',
    python_callable=end_task,
    dag=dag,
)

# Xác định thứ tự thực thi các tác vụ
start_task_operator >> hello_task_operator >> end_task_operator
