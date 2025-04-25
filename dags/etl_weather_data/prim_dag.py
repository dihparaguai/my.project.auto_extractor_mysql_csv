from airflow.models import DAG
from airflow.utils.dates import days_ago # Importing days_ago to set the start date
from airflow.operators.empty import EmptyOperator # Importing EmptyOperator to create empty tasks
from airflow.operators.bash import BashOperator # Importing BashOperator to execute bash commands

# Creating a DAG instance
with DAG(
    dag_id='_prim_dag', # Unique identifier for the DAG
    start_date=days_ago(1), # Setting the start date to one day ago
    schedule_interval='@daily', # Setting the schedule interval to daily at 00:00
) as dag:
    task_1 = EmptyOperator(task_id='task_1') # Creating an empty task with ID 'task_1'
    task_2 = EmptyOperator(task_id='task_2')
    task_3 = EmptyOperator(task_id='task_3')
    task_4 = BashOperator(
        task_id='task_4',
        bash_command='mkdir -p /home/diego/Documents/airflow/pasta_teste',
    ) # Creating a task that executes a bash command to create a directory
    

    # Setting up task dependencies, defining the order in which tasks should be executed
    task_1 >> [task_2, task_3]
    task_3 >> task_4