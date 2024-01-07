# Task Group using task_group decorator
# Task를 그룹화하는 기능으로, 그룹 안에도 다른 그룹을 중첩하여 사용 가능하다.
# 참고. 그룹간의 task_id는 동일해도 상관없다.

from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 8, tz="Asia/Seoul"),
    catchup=False,
    tags=['test']
) as dag:
    
    def inner_func(**kwargs):
        msg=kwargs.get('msg') or ''
        print(msg)

    # 그룹정의(방법1)
    @task_group(group_id='first_group')
    def group_1():  
        ''' task_group 데커레이터를 이용한 첫 번째 그룹입니다. '''

        # 그룹내 task정의(task decorator을 이용한 PythonOperator 방법)
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')

        # 그룹내 task정의(PythonOperator 방법)
        inner_function2=PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 TaskGroup내 두 번째 task입니다.'}
        )

        inner_func1() >> inner_function2 # task Flow
    
    # ----------------------------------------------------------------------------------- #
    # 그룹정의(방법2)
    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다.') as group_2:
     
        # 그룹내 task정의
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task입니다.')

        # 그룹내 task정의
        inner_function2=PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'두 번째 TaskGroup내 두 번째 task입니다.'}
        )

        inner_func1() >> inner_function2 # task Flow

    # group Flow
    group_1() >> group_2
