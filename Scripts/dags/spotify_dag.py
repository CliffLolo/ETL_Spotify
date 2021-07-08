import datetime 
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from etl import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 7, 8),
    'email': ['cliffordfrimpong69@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description="This is my very first DAG. I'm getting there!",
    schedule_interval=timedelta(days=30),
)


run_etl = PythonOperator(
    task_id='final_spotify_etl',
    python_callable=run_etl,
    dag=dag,
)

run_etl