from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys, os

# Define the path to the directory where the script is located
# This is necessary to import the 'extrair_dados' function from the 'etl_weather_data' module
path = os.path.abspath(os.path.dirname(__file__))
sys.path.append(path)
from etl_mysql_csv.main import subir_e_extrair_dados

# Configuração da DAG
default_args = {
    "start_date": datetime(2024, 4, 1),  # Data de início da DAG 
    "retries": 3,                        # Número de tentativas em caso de falha
    "retry_delay": timedelta(minutes=3), # Tempo de espera entre tentativas
}

with DAG(
    dag_id="_etl_mysql_csv",            # ID da DAG (serve para identificar a DAG no Airflow)
    default_args=default_args,          # Argumentos padrão da DAG (serve para definir o comportamento padrão da DAG)
    schedule_interval="*/1 * * * *",    # Roda a cada 1 minuto
) as dag:
    task_1 = PythonOperator(                    # Operador Python para executar o script Python (serve para executar funções Python)
        task_id="subir_e_extrair_dados",        # ID da tarefa (serve para identificar a tarefa no Airflow)
        python_callable=subir_e_extrair_dados   # Função a ser chamada quando a tarefa for executada (neste caso, a função 'executar_script')
    )

    task_1 # Define a tarefa a ser executada na DAG
    # Define a ordem de execução das tarefas (neste caso, apenas uma tarefa)

