from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add # Used to add days in a Airflow date
import pendulum # Used to set a date on Airflow

import os
from os.path import join
import pandas as pd

# Creates a DAG schedule
with DAG(
    dag_id="weather_data",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"),
    schedule_interval="0 0 * * 1",  # Every Monday at midnight
) as dag:


    def extrair_dados(data_interval_end): # 'data_interval_end' is the schedule date process
        # use the schdule date to define de interval date in API request
        date_ini = data_interval_end 
        date_end = ds_add(data_interval_end, 7)

        city = 'SaoPaulo'
        key = os.getenv('WEATHER_API_KEY')

        # Define the URL for the API request
        main_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
        parameters_url = f'{city}/{date_ini}/{date_end}?unitGroup=metric&contentType=csv&include=days&key={key}'
        url = join(main_url, parameters_url)

        # Read the CSV data from the URL
        data = pd.read_csv(url)

        # Save the data to a local directory
        path = f'/home/diego/Documents/schedule_weather_data_pipeline/weather_week={data_interval_end}'
        data.to_csv(f'{path}/data_raw.csv')
        data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(f'{path}/temperatures.csv')
        data[['datetime', 'description', 'icon']].to_csv(f'{path}/conditions.csv')


    # Task to create a directory for the weekly weather data
    task_1 = BashOperator(
        task_id="criar_pasta",
        bash_command="mkdir -p /home/diego/Documents/schedule_weather_data_pipeline/weather_week={{ data_interval_end.strftime('%Y-%m-%d') }}",
    )

    task_2 = PythonOperator(
        task_id="extrair_dados",
        python_callable=extrair_dados,
        op_kwargs={'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}'}
    )

task_1 >> task_2