from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator  # Import PythonOperator
import pandas as pd
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


# Default arguments for the DAG
# added owner in args
default_args = {
    'owner': 'shashidhar'
}


# Define the DAG
dag = DAG(
    dag_id='sample_1_dag',
    description='sample dummy dag',  # Fixed 'description' spelling
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1)
)

# File location for reading the CSV

name=Variable.get('Name')

def read_file(name):
    return f'hello {name} ,this is sample python callable'


# Function to read the CSV file
start=DummyOperator(task_id='start')
end=DummyOperator(task_id='end')


# Task 1: BashOperator to print a welcome message
task_1 = BashOperator(
    task_id='welcome_command',  # Fixed task_id (no spaces)
    bash_command='echo Hello, welcome to Airflow',
    dag=dag
)

# Task 2: PythonOperator to read the file
task_2 = PythonOperator(
    task_id='using_python_operatores',  # Fixed 'task_id'
    python_callable=read_file,
    op_args=[name], # Pass the function name, not the function call
    dag=dag
)

# Set task dependencies
start>>task_1 >> task_2>>end
