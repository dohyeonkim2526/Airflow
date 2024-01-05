from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 1, 5, tz="Asia/Seoul"),
    catchup=False,
    tags=["test"]
) as dag:
    
    # task1
    @task(task_id="python_xcom_push_by_return")
    def xcom_push_result(**kwargs):
        return 'Success'    # key: return_value, value: Success
    
    # task2
    @task(task_id="python_xcom_pull_1")
    def xcom_pull_1(**kwargs):
        ti=kwargs['ti']
        value1=ti.xcom_pull(task_ids="python_xcom_push_by_return") # task_id가 return한 값 반환
        print('xcom_pull 메서드로 찾은 리턴 값:' + value1)

    # task3
    @task(task_id="python_xcom_pull_2")
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)

    # task 실행 순서(실행 후 graph 확인해보기)
    python_xcom_push_by_return=xcom_push_result()   # python_xcom_push_by_return은 'Success'라는 텍스트 반환
    xcom_pull_2(python_xcom_push_by_return)     # status 변수에 'Success' 입력
    python_xcom_push_by_return >> xcom_pull_1()
