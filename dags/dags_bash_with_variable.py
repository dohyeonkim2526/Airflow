# 모든 dag이 공유할 수 있는 '전역 변수'를 활용해보는 실습
# (참고)xcom : 특정 dag, schedule에 수행되는 task 간에만 공유되는 데이터

# ----------------------------------------- #
# 사전준비(variable 추가하여 메타에 저장)
# airflow --> (admin)menu --> variable 추가
# ----------------------------------------- #

# ----------------------------------------- #
# 전역변수 사용하는 방법
# case1. variable 라이브러리를 이용하여 python 문법 사용 --> 비추천 --> scheduler가 주기적으로 dag parsing 하면서 불필요한 DB연결을 일으켜 부하가 발생하므로 비추천하는 방법
# case2. jinja 템플릿을 이용하여 operator 내부에서 가져오기 --> 추천
# ----------------------------------------- #

# ----------------------------------------- #
# 전역변수 사용이 필요한 경우
# --> 대부분의 협업 환경에서 표준화된 dag를 만들기 위해 사용
# --> 변수를 상수(const)로 지정하여 셋팅
# ----------------------------------------- #

from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2024, 1, 7, tz="Asia/Seoul"),
    catchup=False, 
    tags=["test"]
) as dag:
    
    # case1. variable 라이브러리를 이용하여 python 문법 사용(비추천)
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
    )

    # case2. jinja 템플릿을 이용하여 operator 내부에서 가져오기(추천)
    bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"   # operator 내부에서 가져오기
    )
   