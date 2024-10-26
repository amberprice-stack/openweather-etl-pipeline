from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from openweather_etl import run_weather_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'openweather_dag',
    default_args=default_args,
    description='My first DAG with ETL process',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_openweather_etl',
    python_callable=run_weather_etl,
    dag=dag, 
)
run_etl