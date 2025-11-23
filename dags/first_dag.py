from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'Nikhil',
    'retries' : 5,
    'retry_delay' :  timedelta(minutes=2)
}

with DAG(
    dag_id = 'first_dag_v4',
    default_args = default_args,
    description = 'first airflow dag',
    start_date = datetime(2025, 11, 17, 14),
    schedule = '@daily'
) as dag:
    task1= BashOperator(
        task_id = 'first_task',
        bash_command= 'echo Hello world, first task'
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command= 'echo This is second task, it runs after first task'
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command= 'echo This is the third task, runs after the second task'
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    
    task1 >> task2
    task1 >> task3