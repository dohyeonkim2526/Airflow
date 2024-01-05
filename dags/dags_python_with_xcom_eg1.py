# Xcom(Cross Communication) - dag task간에 데이터 공유를 위해 사용하는 기술
# 주로 작은 규모의 데이터 공유를 위해 사용
# 참고. 1GB 이상의 대용량 데이터 공유를 위해서는 aws s3, hdfs 등의 기술이 필요

from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 1, 5, tz="Asia/Seoul"),
    catchup=False,
    tags=["test"]
) as dag:
    
    # task1
    @task(task_id="python_xcom_push_task1")
    def xcom_push1(**kwargs):
        ti=kwargs['ti']
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1,2,3])

    # task2
    @task(task_id="python_xcom_push_task2")
    def xcom_push2(**kwargs):
        ti=kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    # task3
    @task(task_id="python_xcom_pull_task")
    def xcom_pull(**kwargs):
        ti=kwargs['ti']
        value1=ti.xcom_pull(key="result1")  # key값만 알려주는 경우(key가 동일한 경우, 가장 마지막에 push된 value 반환)
        value2=ti.xcom_pull(key="result2", task_ids="python_xcom_push_task1")   # key, task_id를 모두 알려주는 경우
        print(value1)
        print(value2)

    # task 실행 순서 
    xcom_push1() >> xcom_push2() >> xcom_pull()
