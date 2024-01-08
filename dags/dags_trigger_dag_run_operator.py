# -------------------------------------------------------------- #
# DAG간 의존관계 설정하는 법
# 1. TriggerDagRun Operators : 다른 dag을 실행시키는 operator
# 2. ExternalTask Sensor : 다른 dag내의 task 완료를 기다리는 센서
# -------------------------------------------------------------- #

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2024, 1, 8, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False,
    tags=['test']
) as dag:
    
    start_task=BashOperator(
        task_id='start_task',
        bash_command='echo "start!"'
    )

    trigger_dag_task=TriggerDagRunOperator(
        # 필수옵션
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',  # trigger할 dag_id 명시
        # 선택옵션(추가)
        trigger_run_id=None,    # dag 수행방식, 시간을 유일하게 식별해주는 key
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,  # trigger된 dag이 완료되는 것을 기다릴지 여부
        poke_interval=60,   # trigger된 dag 모니터링 주기
        allowed_states=['success'], # trigger된 dag 완료상태기준
        failed_states=None
    )
    
    # task Flow
    start_task >> trigger_dag_task
