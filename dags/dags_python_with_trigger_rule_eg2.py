# Trigger Rule
# 여러개의 상위 Task가 하나의 하위 Task로 가기 위한 조건
# 상-하위 Task 간의 실행 조건을 설정(ex. 3개의 상위 Task가 모두 성공했을 때만 하위 Task 수행)
# --> Trigger 실행 조건을 원하는 방식으로 설정할 수 있으며 본 실습에서는 'none_skipped'으로 진행

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg2',
    start_date=pendulum.datetime(2024, 1, 8, tz='Asia/Seoul'),
    schedule=None,
    catchup=False,
    tags=['test']
) as dag:
    
    @task.branch(task_id='branching')
    def random_branch():
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)

        if selected_item == 'A':
            return 'task_a'
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'

    # branch(task-1)
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    )

    # branch(task-2)
    @task(task_id='task_b')
    def task_b():
        print('정상 처리')

    # branch(task-3)
    @task(task_id='task_c')
    def task_c():
        print('정상 처리')

    # none_skipped : 상위Task가 모두 skip처리 되지 않아야 수행된다.
    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상 처리')

    # task Flow
    # 참고. Task-(a,b,c)중에서 일부를 skip처리하여 task_d가 수행되지 않는 것이 정상
    random_branch() >> [task_a, task_b(), task_c()] >> task_d()
    