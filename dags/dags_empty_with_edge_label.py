# Edge Label : Task 연결에 대한 comment를 의미
from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id="dags_empty_with_edge_label",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 8, tz="Asia/Seoul"),
    catchup=False,
    tags=['test']
) as dag:
    
    empty_1 = EmptyOperator(
        task_id='empty_1'
    )

    empty_2 = EmptyOperator(
        task_id='empty_2'
    )

    empty_3 = EmptyOperator(
        task_id='empty_3'
    )

    empty_4 = EmptyOperator(
        task_id='empty_4'
    )

    empty_5 = EmptyOperator(
        task_id='empty_5'
    )

    empty_6 = EmptyOperator(
        task_id='empty_6'
    )

    # label test-1
    empty_1 >> Label('1과 2사이') >> empty_2

    # label test-2(모든 edge 위에 comment 확인)
    empty_2 >> Label('Start Branch') >> [empty_3,empty_4,empty_5] >> Label('End Branch') >> empty_6