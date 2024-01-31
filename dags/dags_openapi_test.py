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
    import bs4
    import pandas as pd 

    base_url = 'http://openapi.molit.go.kr'
    port = '8081'
    endpoint = 'OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
    # service_key = '{{var.value.apikey_getRTMS_openapi_molit}}'
    service_key = 'H2IR0IidJiL8%2BelzLkLrCd5jxthjDayM22614UIUSyu7kHXEs8fKxzz43B6MshNDf4uWZ1WeAjieAXMOG6h1VA%3D%3D'
    headers = {'Content-Type':'application/xml'}

    request_url = f'{base_url}:{port}/{endpoint}?serviceKey={service_key}&LAWD_CD=11110&DEAL_YMD=201512'
    response = requests.get(request_url, headers=headers)
    
    content = response.text
    xml_obj = bs4.BeautifulSoup(content,'lxml-xml')
    rows = xml_obj.findAll('item')
    
    name_list, value_list, row_list = list(), list(), list()

    for i in range(0, len(rows)):
        columns = rows[i].find_all()
        for j in range(0,len(columns)):
            if i ==0:
                name_list.append(columns[j].name)
            value_list.append(columns[j].text)
        row_list.append(value_list)
        value_list=[]
   
    df = pd.DataFrame(row_list, columns=name_list)
    print(df)
    
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

    