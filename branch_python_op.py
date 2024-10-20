from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import random as r
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule  # Import TriggerRule

default_args = {
    'owner': 'shashidhar'
}

def get_num(start_num, end_num):
    return r.randint(start_num, end_num)

def check_num(ti, **kwargs):
    num = ti.xcom_pull(task_ids='creating_branch_oper')
    if int(num) >= 0:
        return "postive_num"
    else:
        return 'negative_num'

dag = DAG(
    dag_id='Branchpythonoperator',
    description='Sample branch operator testing',
    schedule_interval=None,
    default_args=default_args,
    start_date=days_ago(1),
    tags=['branch_operator', 'new_pipeline']
)

task_1 = PythonOperator(
    task_id='creating_branch_oper',
    python_callable=get_num,
    op_args=[-10, 10],
    dag=dag
)

task_2 = BranchPythonOperator(
    task_id='check_the_number',
    python_callable=check_num,
    dag=dag
)

task3 = DummyOperator(task_id='start', dag=dag)
task6 = DummyOperator(
    task_id='end', 
    dag=dag, 
    trigger_rule=TriggerRule.ONE_SUCCESS  # Ensures task6 runs when either branch succeeds
)

task5 = BashOperator(
    task_id='postive_num',
    bash_command='echo Positive Number',
    dag=dag
)

task4 = BashOperator(
    task_id='negative_num',
    bash_command='echo Negative Number',
    dag=dag
)

dummy_tasks = DummyOperator(
    task_id='join_task', 
    dag=dag, 
    trigger_rule=TriggerRule.ONE_SUCCESS  # Ensures the join task runs when either branch succeeds
)

# Set dependencies
task3 >> task_1 >> task_2 >> [task5, task4]
task5 >> dummy_tasks
task4 >> dummy_tasks
dummy_tasks >> task6
