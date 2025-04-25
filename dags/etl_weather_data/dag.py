from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum  # Used to set a date on Airflow

import sys, os

# Define the path to the directory where the script is located
# This is necessary to import the 'extrair_dados' function from the 'etl_weather_data' module
path = os.path.abspath(os.path.dirname(__file__))
sys.path.append(path)
from etl_weather_data.main import extrair_dados

# Creates a DAG schedule
with DAG(
    dag_id="_etl_weather_data",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"), # Set the start date schedule
    schedule_interval="0 0 * * 1",  # Every Monday at midnight
) as dag:

    # Task to create a directory for the weekly weather data
    task_1 = BashOperator(
        task_id="criar_pasta",
        bash_command=f'mkdir -p {path}' + '/data/weather_week={{ data_interval_end.strftime("%Y-%m-%d") }}'
    )

    # Task to extract weather data from the API and save it to the created directory
    task_2 = PythonOperator(
        task_id="extrair_dados",
        python_callable=extrair_dados,
        op_kwargs={
            'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}',
            'data_path': path + '/data/weather_week={{ data_interval_end.strftime("%Y-%m-%d") }}'
        } # Pass the schedule date and the path to the function, like a parameter
    )

    task_1 >> task_2 # Set the task dependencies, defining the order in which tasks should be executed
