# BaseBranchOperator을 이용한 Task 분기 처리

# --------------------------------------------------------------- #
# Task 분기처리 방법 정리
# 1. BranchPythonOperator
# 2. task.branch decorator
# 3. BaseBranchOperator 상속 후 class 내에서 choose_branch 재정의
# --------------------------------------------------------------- #

from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2024, 1, 8, tz='Asia/Seoul'),
    schedule=None,
    catchup=False,
    tags=['test']
) as dag:
    
    # class 클래스명(상속클래스)
    # 부모 --> 자식 : BaseBranchOperator --> CustomBranchOperator
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random
            # print(context)

            item_lst=['A', 'B', 'C']
            selected_item=random.choice(item_lst)

            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B', 'C']:
                return ['task_b', 'task_c']
            
    # 선행 Task with BaseBranchOperator(class에 대한 객체 정의)
    custom_branch_operator=CustomBranchOperator(task_id='python_branch_task')

    # 후행 Task(task_a, task_b, task_c)
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a=PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b=PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c=PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    # task Flow
    custom_branch_operator >> [task_a, task_b, task_c]
