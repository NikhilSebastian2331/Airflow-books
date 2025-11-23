from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner' : 'Nikhil',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id = 'dag_taskflow_v1',
     default_args= default_args,
     start_date = datetime(2025, 11, 18, 4),
     schedule = '@daily')
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return{
            'first_name': 'Tom',
            'last_name': 'Fridman'
        }
    
    @task
    def get_age():
        return 21
    
    @task()
    def greet(first_name, last_name, age):
        print(f"Hello world!! Name is {first_name} {last_name}, "
              f"and age is {age}")
        
    name_dict = get_name()
    age = get_age()
    greet(first_name = name_dict['first_name'], 
          last_name = name_dict['last_name'], 
          age = age)
    
greet_dag = hello_world_etl()


