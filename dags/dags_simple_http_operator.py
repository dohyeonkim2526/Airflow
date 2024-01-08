# SimpleHttpOperator을 이용하여 공공데이터 api 활용
# 참고. 실습전 Airflow connection 생성필요([Admin]-->[Connections])

# ------------------------------------------------------------------------ #
# SimpleHttpOperator
# * Http 요청을 하고 결과로 text를 리턴 받는 operator(리턴 값은 xcom에 저장)
# * Http를 이용하여 api를 처리하는 RestAPI 호출시 사용 가능
# ------------------------------------------------------------------------ #

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2024, 1, 8, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    tags=['test']
) as dag:
    
    ''' 서울시 공공자전거 대여소 정보 '''
    tb_cycle_station_info=SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr', # airflow connection 정보
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',  # key는 변수로 사용
        method='GET',
        headers={'Content-Type' : 'application/json',
                 'charset' : 'utf-8',
                 'Accept' : '*/*'
                 }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        import json
        from pprint import pprint
        ti=kwargs['ti']
        rslt=ti.xcom_pull(task_ids='tb_cycle_station_info') # SimpleHttpOperator 리턴값
        pprint(json.loads(rslt))

    # task Flow
    tb_cycle_station_info >> python_2
