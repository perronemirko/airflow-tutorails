from airflow.models import DAG
from airflow.utils.dates import days_ago as dt
from airflow.utils.dates import timedelta
from airflow.operators.python_operator import PythonOperator as py
import random
args = {
    'owner': 'Mirko',
    'start_date': dt(1)
}

dag = DAG(dag_id='my_simple_dag', default_args=args, schedule_interval=None)

def return_this_func(**context):
    print("Hi man!")

def always_fail(**context):
    raise Exception("Exception")

def randomly_fail(**context):
    if random.random()>0.7:
        raise Exception("Exception")
    print("All Ok!!!")

def push_to_xcom(**context): 
    random_value=random.random()
    context['ti'].xcom_push(key='random_value', value = random_value) #ti task instance
    print("All Ok push XCOM!!!")

def pull_from_xcom(**context): 
    recived_value = context['ti'].xcom_pull(key='random_value') #ti task instance
    print("All Ok pull XCOM!!!"+ str(recived_value))


with dag:
    t1 = run_this_task = py(
        task_id='First',
        python_callable=return_this_func,
        provide_context=True
    )
    t2 = run_this_task2 = py(
        task_id='Second',
        python_callable=return_this_func,
        provide_context=True
    )
    t3 = run_this_task3 = py(
        task_id='Failing-task',
        python_callable=always_fail,
        provide_context=True
    )
    t4 = run_this_task4 = py(
        task_id='Failing-randomly-task',
        python_callable=randomly_fail,
        retries=10,
        retry_delay=timedelta(seconds=1),
        provide_context=True
    )
    t5 = run_this_task5 = py(
        task_id='Push-task',
        python_callable=push_to_xcom,
        retries=10,
        retry_delay=timedelta(seconds=1),
        provide_context=True
    )
    t6 = run_this_task6 = py(
        task_id='Pull-task',
        python_callable=pull_from_xcom,
        retries=10,
        retry_delay=timedelta(seconds=1),
        provide_context=True
    )
t1 >> t2 >> t5 >> t6 
# [t1, t2]