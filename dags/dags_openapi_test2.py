from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum

with DAG(
    dag_id='dags_openapi_test2',
    start_date=pendulum.datetime(2024, 1, 29, tz='Asia/Seoul'),
    schedule=None,
    catchup=False,
    tags=['project']
) as dag:
    
    # getRTMS_openapi_data = SimpleHttpOperator(
    #     task_id='getRTMS_openapi_data',
    #     http_conn_id='openapi.molit.go.kr',
    #     endpoint='OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade',
    #     method='GET',
    #     headers={'Content-Type':'application/xml'},
    #     data={'LAWD_CD':'11110',
    #           'DEAL_YMD':'201512',
    #           'serviceKey':'{{ var.value.apikey_getRTMS_openapi_molit }}'
    #           },
    #     log_response=True,
    #     dag=dag
    # )

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
    # getRTMS_openapi_data
