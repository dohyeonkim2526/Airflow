from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")    #'dags_dataset_producer_1'에서 Produce한 값을 가져옴

with DAG(
    dag_id='dags_dataset_consumer_1',
    schedule=[dataset_dags_dataset_producer_1],
    start_date=pendulum.datetime(2024, 1, 12, tz='Asia/Seoul'),
    catchup=False,
    tags=['test']
) as dag:
    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1이 완료되면 수행"'
    )
