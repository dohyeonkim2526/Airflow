import pendulum
from airflow import DAG 
from airflow.decorators import task

with DAG(
    dag_id="example_python_operator",
    schedule=" 0 2 * * 1",  # 매주 월요일 2시 실행
    start_date=pendulum.datetime(2024, 1, 4, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(input):
        print(input)

    python_task_1=print_context('task_decorator 실행 테스트')
