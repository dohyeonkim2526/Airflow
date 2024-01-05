import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",    # 일반적으로 file명과 dag_id는 일치
    schedule="0 0 * * *",   # 매일 0시 0분에 실행
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,  # 미수행된 과거 일자를 소급할지에 대한 여부
    tags=["test"]
) as dag:
    # 객체선언
    bash_t1 = BashOperator(
        task_id="bash_t1",  # 일반적으로 객체명과 task_id는 일치
        bash_command="echo whoami",
    )
    
    # 객체선언
    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    # 실행순서정의
    bash_t1 >> bash_t2
    