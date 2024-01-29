# 데이터셋: 국토교통부 실거래가 정보 OpenAPI
# 데이터 설명: 지역코드와 기간을 설정하여 해당지역, 해당기간의 아파트 매매 신고 자료를 제공하는 아파트 매매 신고 정보
# 데이터 갱신주기: 일 1회

# 요청 메시지 명세
# 지역코드(LAWD_CD) / 계약월(DEAL_YMD) / 인증키(serviceKey)

# SimpleHttpOperator
# * Http 요청을 하고 결과로 text를 리턴 받는 operator(리턴 값은 xcom에 저장)
# * Http를 이용하여 api를 처리하는 RestAPI 호출시 사용 가능

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum # python에서 timezone을 쉽게 사용할 수 있도록 도와주는 라이브러리

with DAG(
    dag_id='dags_openapi_test',
    start_date=pendulum.datetime(2024, 1, 29, tz='Asia/Seoul'),
    schedule=None,
    catchup=False, # dag가 실행되지 않았던 과거 시점 task를 실행할지에 대한 여부
    tags=['project']
) as dag:
    
    # apiKey 정보 : var.value.apikey_getRTMS_openapi_molit
    # 지역코드 정보 : var.value.lawdcd_getRTMS_openapi_molit
    # 계약월 정보 : var.value.dealymd_getRTMS_openapi_molit

    task_getRTMS_data = SimpleHttpOperator(
        task_id='task_getRTMS_data',
        http_conn_id='openapi.molit.go.kr', # Connection ID 정보
        #endpoint='OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?serviceKey={{var.value.apikey_getRTMS_openapi_molit}}&LAWD_CD=11110&DEAL_YMD=201512', # Endpoint URL
        #endpoint='OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?LAWD_CD={{var.value.lawdcd_getRTMS_openapi_molit}}&DEAL_YMD={{var.value.dealymd_getRTMS_openapi_molit}}&serviceKey={{var.value.apikey_getRTMS_openapi_molit}}', # Endpoint URL
        endpoint='/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade',
        method='GET', # HTTP method
        headers={'Content-Type':'application/xml'},
        data={'LAWD_CD':'11110',
              'DEAL_YMD':'201512',
              'serviceKey':'{{ var.value.apikey_getRTMS_openapi_molit }}'}
    )

    @task(task_id='getData')
    def getData(**kwargs):
        ti=kwargs['ti']
        result=ti.xcom_pull(task_ids='task_getRTMS_data')
        print(result)

    # task
    task_getRTMS_data >> getData()
    