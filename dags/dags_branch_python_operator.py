# 실습주제: Task 분기 처리 with BranchPythonOperator
# 선후행 Task 간에 분기 처리를 진행하는 방법
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id='dags_branch_python_operator',
    start_date=pendulum.datetime(2024, 1, 7, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False,
    tags=["test"]
) as dag:
    
    def select_random():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst) # A,B,C 중에서 랜덤으로 값 선택

        if selected_item == 'A':
            return 'task_a' # 후행으로 실행할 task_id 반환(string or list)
        elif selected_item in ['B','C']:
            return ['task_b','task_c']
    
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random   # python_random 함수실행해서 반환한 값
    )
    
    # task_a, task_b, task_c에 대한 내용 정의
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    # task Flow
    python_branch_task >> [task_a, task_b, task_c]
