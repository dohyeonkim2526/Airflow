from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_openapi_test3',
    start_date=pendulum.datetime(2024, 1, 29, tz='Asia/Seoul'),
    schedule=None,
    catchup=False,
    tags=['project']
) as dag:
    
    tb_cycle_station_info=SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr', # airflow connection 정보
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',  # key는 변수로 사용
        method='GET',
        headers={'Content-Type': 'application/json',
                 'charset': 'utf-8',
                 'Accept': '*/*'
                }
    )

    tb_cycle_station_info
