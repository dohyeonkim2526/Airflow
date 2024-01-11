'''
Dataset을 이용한 Dag 트리거
* Produce/Consume 구조를 이용 --> 실제 queue가 아닌 DB에 Produce/Consume 내역 기록
* Task 완료를 알리기 위해 특정 key로 Produce하고, 해당 key를 Consume하는 DAG을 트리거 하는 기능

(참고)
* Produce : queue에 메세지 입력
* Consume : queue에서 메세지를 받아감
'''

from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dags_dataset_producer_1 = Dataset("dataset_dags_dataset_producer_1")    # Produce할때 사용하는 key값

with DAG(
    dag_id='dags_dataset_producer_1',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 1, 12, tz='Asia/Seoul'),
    catchup=False,
    tags=['test']
) as dag:
    
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_dags_dataset_producer_1],
        bash_command='echo "producer_1 수행 완료"'
    )
    