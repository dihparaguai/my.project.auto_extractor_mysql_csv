import pandas as pd

import os
from dotenv import load_dotenv
load_dotenv(override=True) # Used to load the environment variables from .env file

from airflow.macros import ds_add # Used to add days in a Airflow date


def extrair_dados(data_interval_end, data_path): # 'data_interval_end' is the schedule date process
    # use the schdule date to define de interval date in API request
    date_ini = data_interval_end 
    date_end = ds_add(data_interval_end, 7)

    city = 'SaoPaulo'
    key_api = os.getenv('WEATHER_KEY') # Get the API key from environment variable on .env file

    # Define the URL for the API request
    main_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
    parameters_url = f'{city}/{date_ini}/{date_end}?unitGroup=metric&contentType=csv&include=days&key={key_api}'
    url = main_url + parameters_url

    # Read the CSV data from the URL
    data = pd.read_csv(url)

    # Save the data to a local directory
    data.to_csv(f'{data_path}/data_raw.csv')
    data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(f'{data_path}/temperatures.csv')
    data[['datetime', 'description', 'icon']].to_csv(f'{data_path}/conditions.csv')
