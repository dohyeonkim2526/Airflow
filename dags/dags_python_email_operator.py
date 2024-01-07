# python -> email operator xom 전달해보는 실습
from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_python_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2024, 1, 7, tz="Asia/Seoul"),
    catchup=False, 
    tags=["test"]
) as dag:
    
    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice 
        return choice(['Success','Fail'])   # choice : list, tuple, string 에서 랜덤값을 출력해주는 기능

    send_email = EmailOperator(
        task_id='send_email',
        to='xx_yy_zz@gmail.com',   # 전달받을 email 주소 입력
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
                    {{ti.xcom_pull(task_ids="something_task")}} 했습니다 <br>'
    )

    some_logic() >> send_email
