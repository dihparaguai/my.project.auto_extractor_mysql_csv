from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# Executa o script Python 'main.py' localizado no mesmo diretório
def executar_script():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_to_run = os.path.join(script_dir, "main.py")
    subprocess.run(["python", script_to_run], check=True)

# Configuração da DAG
default_args = {
    "owner": "Diego",                    # Nome do responsável pela DAG, (pode ser qualquer nome)
    "depends_on_past": False,            # Não depende de execuções passadas (ou seja, não espera que a execução anterior tenha sido concluída)
    "start_date": datetime(2024, 4, 1),  # Data de início da DAG 
    "retries": 3,                        # Número de tentativas em caso de falha
    "retry_delay": timedelta(minutes=3), # Tempo de espera entre tentativas
}

with DAG(
    dag_id="rodar_script",              # ID da DAG (serve para identificar a DAG no Airflow)
    default_args=default_args,          # Argumentos padrão da DAG (serve para definir o comportamento padrão da DAG)
    schedule_interval="*/1 * * * *",    # Roda a cada 1 minuto
    catchup=False,                      # Não executa execuções passadas (ou seja, não tenta executar a DAG para datas anteriores à data de início)
) as dag:
    rodar_script = PythonOperator(      # Operador Python para executar o script Python (serve para executar funções Python)
        task_id="executar_script",      # ID da tarefa (serve para identificar a tarefa no Airflow)
        python_callable=executar_script # Função a ser chamada quando a tarefa for executada (neste caso, a função 'executar_script')
    )

    rodar_script # Define a tarefa a ser executada na DAG
    # Define a ordem de execução das tarefas (neste caso, apenas uma tarefa)

