# Trigger Rule
# 여러개의 상위 Task가 하나의 하위 Task로 가기 위한 조건
# 상-하위 Task 간의 실행 조건을 설정(ex. 3개의 상위 Task가 모두 성공했을 때만 하위 Task 수행)
# --> Trigger 실행 조건을 원하는 방식으로 설정할 수 있으며 본 실습에서는 상위 Task가 모두 성공시에만 수행될 수 있는 'all_done'으로 진행

from airflow import DAG 
from airflow.decorators import task 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg1',
    start_date=pendulum.datetime(2024, 1, 8, tz='Asia/Seoul'),
    schedule=None,
    catchup=None,
    tags=['test']
) as dag:
    
    # 상위 Task-1
    bash_upstream_1=BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    # 상위 Task-2
    # (Task실패확인을 위해)에러발생시킴
    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')
    
    # 상위 Task-3
    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상 처리')

    # 하위 Task
    # all_done : 모든 상위 Task 실행 완료(원하는 방식으로 'trigger_rule' 설정 가능)
    @task(task_id='python_downstream_1', trigger_rule='all_done')
    def python_downstream_1():
        print('정상 처리')

    # task Flow
    # 참고.Task-2에서 고의로 에러를 발생해도 모두 수행되었으므로 하위Task는 실행되는 것이 정상
    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()
    