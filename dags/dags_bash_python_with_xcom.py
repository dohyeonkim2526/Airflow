# python, bash 간에 xcom 전달해보는 실습
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 1, 7, tz="Asia/Seoul"),
    catchup=False,
    tags=["test"]
) as dag:

    # case1. python -> bash operator xcom 전달
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'Good','data':[1,2,3],'options_cnt':100}
        return result_dict  # python의 경우 함수 return값은 자동으로 xcom에 올라간다.

    bash_pull = BashOperator(   # bash의 경우 (ti.xom_pull or ti.xcom_push) template 문법 기능을 사용한다.
        task_id='bash_pull',
        env={
            'STATUS':'{{ti.xcom_pull(task_ids="python_push")["status"]}}', # python_push가 return한 값 이용
            'DATA':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',
            'OPTIONS_CNT':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'
        },
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )

    python_push_xcom() >> bash_pull

    # -----------------------------------------------------------------------------------------------------
    # case2. bash -> python operator xcom 전달
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command='echo PUSH_START '
                 '{{ti.xcom_push(key="bash_pushed",value=200)}} && '
                 'echo PUSH_COMPLETE'   # 마지막으로 echo 출력된 값이 return_value로 취급되어 xcom에 올라간다.
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key='bash_pushed')  # 200
        return_value = ti.xcom_pull(task_ids='bash_push')   # PUSH_COMPLETE
        print('status_value:' + str(status_value))
        print('return_value:' + return_value)

    bash_push >> python_pull_xcom()
