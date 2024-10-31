
from airflow import DAG
from airflow.operators.python import PythonOperator  # 최신 Airflow 버전에서는 이 경로를 사용합니다.
from datetime import datetime

def hello_jupyter():
    print("go going")

default_args = {
    'owner': 'airflow',  # owner 추가
    'start_date': datetime(2023, 10, 21),
}

dag = DAG('gogo_dag', default_args=default_args, schedule_interval='@once')

hello_task = PythonOperator(
    task_id='go',
    python_callable=hello_jupyter,
    dag=dag
)
