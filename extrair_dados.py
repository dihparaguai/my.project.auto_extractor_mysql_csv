import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

# Define the date range for the data extraction
date_ini = datetime.today()
date_end = date_ini + timedelta(days=7)

# Format the dates to 'YYYY-MM-DD'
date_ini = date_ini.strftime('%Y-%m-%d')
date_end = date_end.strftime('%Y-%m-%d')

city = 'SaoPaulo'
key = os.getenv('WEATHER_API_KEY')

# Define the URL for the API request
main_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
parameters_url = f'{city}/{date_ini}/{date_end}?unitGroup=metric&contentType=csv&include=days&key={key}'
url = join(main_url, parameters_url)

# Read the CSV data from the URL
data = pd.read_csv(url)

# Save the data to a local directory
path = f'weather_week={date_ini}'
os.makedirs(path, exist_ok=True)
data.to_csv(f'{path}/data_raw.csv')
data[['datetime','tempmin', 'temp', 'tempmax']].to_csv(f'{path}/temperatures.csv')
data[['datetime', 'description', 'icon']].to_csv(f'{path}/conditions.csv')
