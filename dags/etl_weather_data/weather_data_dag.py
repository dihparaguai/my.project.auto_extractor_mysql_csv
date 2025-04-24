from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum  # Used to set a date on Airflow

import sys
import os

path = os.path.abspath(os.path.dirname(__file__))
sys.path.append(path)
from etl_weather_data.extrair_dados import extrair_dados

# Creates a DAG schedule
with DAG(
    dag_id="weather_data",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"),
    schedule_interval="0 0 * * 1",  # Every Monday at midnight
) as dag:

    # Task to create a directory for the weekly weather data
    task_1 = BashOperator(
        task_id="criar_pasta",
        bash_command=f'mkdir -p {path}' + '/weather_data/weather_week={{ data_interval_end.strftime("%Y-%m-%d") }}'
    )

    # Task para extrair dados
    task_2 = PythonOperator(
        task_id="extrair_dados",
        python_callable=extrair_dados,
        op_kwargs={
            'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}',
            'data_path': path + '/weather_data/weather_week={{ data_interval_end.strftime("%Y-%m-%d") }}'
        }
    )

    task_1 >> task_2
