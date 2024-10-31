from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 간단한 Python 함수 정의
def print_hello():
    print("Hello from Airflow!")

# DAG 기본 설정
default_args = {
    'start_date': datetime(2023, 10, 1),
    'catchup': False
}

with DAG(
    'test_python_operator',
    default_args=default_args,
    schedule_interval=None,  # 수동으로 실행
    description='A simple test DAG with PythonOperator',
    tags=['test']
) as dag:

    # PythonOperator를 사용한 간단한 Task
    python_task = PythonOperator(
        task_id='run_python_function',
        python_callable=print_hello  # 실행할 함수
    )
