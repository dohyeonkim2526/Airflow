from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 1, 4, tz="Asia/Seoul"),
    catchup=False,
    tags=["test"]
) as dag:
    
    # case1
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1=PythonOperator(
        task_id="python_t1",
        python_callable=python_function1
        op_kwargs={'start_date':'{{data_interval_start | ds}}'  # jinja template variable
                 , 'end_date':'{{data_interval_end | ds}}'}
    )

    # case2(opkwargs)
    # 함수 실행만으로 task 생성 가능한 방법
    @task(task_id="python_t2")
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))

    python_t1 >> python_function2()
