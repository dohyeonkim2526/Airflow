# 데이터셋: 국토교통부 실거래가 정보 OpenAPI
# 데이터 설명: 지역코드와 기간을 설정하여 해당지역, 해당기간의 아파트 매매 신고 자료를 제공하는 아파트 매매 신고 정보
# 데이터 갱신주기: 일 1회

# 요청 메시지 명세
# 지역코드(LAWD_CD) / 계약월(DEAL_YMD) / 인증키(serviceKey)

# SimpleHttpOperator
# * Http 요청을 하고 결과로 text를 리턴 받는 operator(리턴 값은 xcom에 저장)
# * Http를 이용하여 api를 처리하는 RestAPI 호출시 사용 가능

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
import pendulum # python에서 timezone을 쉽게 사용할 수 있도록 도와주는 라이브러리

def get_openapi_data():
    import requests
    import xmltodict
    import json
    import pandas as pd 

    # http_conn_id = 'openapi.molit.go.kr', # Connection ID 정보
    # endpoint = 'OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade', # Endpoint URL
    # headers = {'Content-Type':'application/xml'},
    # params = {'LAWD_CD':'11110',
    #           'DEAL_YMD':'201512',
    #           'serviceKey':'{{ var.value.apikey_getRTMS_openapi_molit }}'}

    # connection = BaseHook.get_connection(http_conn_id)
    # request_url = f'http://{connection.host}:{connection.port}/{endpoint}'
    request_url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?LAWD_CD=11110&DEAL_YMD=201512&serviceKey=H2IR0IidJiL8%2BelzLkLrCd5jxthjDayM22614UIUSyu7kHXEs8fKxzz43B6MshNDf4uWZ1WeAjieAXMOG6h1VA%3D%3D'

    # response = requests.get(request_url, headers=headers, params=params)
    response = requests.get(request_url, headers={'Content-Type':'application/xml'})
    content = response.content

    dic = xmltodict.parse(content)
    items = dic['response']['body']['items']['item']

    json_string = json.dumps(items)
    df = pd.read_json(json_string, orient='records')
    
    return df

with DAG(
    dag_id = 'dags_openapi_test',
    start_date = pendulum.datetime(2024, 1, 29, tz='Asia/Seoul'),
    schedule = None,
    catchup = False, # dag가 실행되지 않았던 과거 시점 task를 실행할지에 대한 여부
    tags = ['project']
) as dag:

    getRTMS_task = PythonOperator(
        task_id = 'getRTMS_task',
        python_callable = get_openapi_data,
        dag = dag
    )

    getRTMS_task

    