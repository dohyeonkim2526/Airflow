# --------------------------------------------------------------------- #
# 데이터셋: 국토교통부 실거래가 정보 OpenAPI
# 데이터 설명: 지역코드와 기간을 설정하여 해당지역, 해당기간의 아파트 매매 신고 자료를 제공하는 아파트 매매 신고 정보
# 데이터 갱신주기: 일 1회
# 요청 메시지 명세 : 지역코드(LAWD_CD) / 계약월(DEAL_YMD) / 인증키(serviceKey)
# --------------------------------------------------------------------- #

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pendulum # python에서 timezone을 쉽게 사용할 수 있도록 도와주는 라이브러리

# 공공데이터 수집
def get_openapi_data(lawd_cd, **kwargs): #ti: task instance
    import requests
    import bs4
    import pandas as pd 

    # 공공데이터 OpenAPI 커넥션 정보
    base_url = 'http://openapi.molit.go.kr'
    port = '8081'
    endpoint = 'OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
    service_key = Variable.get('apikey_getRTMS_openapi_molit') # 공공데이터 활용 인증키
    # lawd_cd = kwargs['lawd_cd'] # 법정동 코드
    deal_ymd = Variable.get('DEAL_YMD') # 계약년월
    headers = {'Content-Type':'application/xml'} # xml 데이터 활용

    # OpenAPI 수집요청 
    request_url = f'{base_url}:{port}/{endpoint}?serviceKey={service_key}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}'
    response = requests.get(request_url, headers=headers)
    
    # xml parsing --> DataFrame 변환
    content = response.text
    xml_obj = bs4.BeautifulSoup(content,'lxml-xml')
    rows = xml_obj.findAll('item')
    
    name_list, value_list, row_list = list(), list(), list()

    for i in range(0, len(rows)):
        columns = rows[i].find_all()
        for j in range(0,len(columns)):
            if i ==0:
                name_list.append(columns[j].name)   # 컬럼리스트
            value_list.append(columns[j].text)  # 데이터리스트
        row_list.append(value_list)
        value_list=[]
   
    df = pd.DataFrame(row_list, columns=name_list) # 컬럼별 데이터 정보 --> DataFrame 변환
    kwargs['ti'].xcom_push(key=f'openapi_data_{lawd_cd}', value=df.to_dict()) # Xcom을 사용하여 DataFrame 저장


# 수집한 데이터 전체 merge
def merge_data(lawd_params, **kwargs):
    import pandas as pd
    ti = kwargs['ti']
    all_data = []

    for param in lawd_params:
        data = ti.xcom_pull(key=f'openapi_data_{param}', task_ids=f'getOpenapi_task_{param}')
        df = pd.DataFrame(data)
        all_data.append(df)

    result = pd.concat(all_data, ignore_index=True)
    ti.xcom_push(key='merge_data', value=result.to_dict())
        

# 데이터베이스 적재
def load_data(**kwargs):
    import pymysql
    import pandas as pd
    from sqlalchemy import create_engine

    ti = kwargs['ti']
    data = ti.xcom_pull(key='merge_data') # Xcom에서 저장된 DataFrame 가져오기
    df = pd.DataFrame(data)
    
    conn = BaseHook.get_connection('mysql-conn') # airflow에서 connection 하는 mysql ID
    mysql_host = 'mariadb-container'
    mysql_port = 3306
    mysql_database = conn.schema
    mysql_user = conn.login
    mysql_password = conn.password
        
    engine_msg = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}'
    engine = create_engine(engine_msg)

    # DB 데이터 적재
    df.to_sql(name = 'test_table_240614',
              con = engine,
              if_exists = 'append',
              index = False
    )
    

# DAG 정의
with DAG(
    dag_id = 'dags_getApt_transdata',
    start_date = pendulum.datetime(2024, 1, 29, tz='Asia/Seoul'),
    schedule = None,
    catchup = False, # dag가 실행되지 않았던 과거 시점 task를 실행할지에 대한 여부
    tags = ['project']
) as dag:

    # 수집할 지역코드 리스트
    lawd_params = ['11110','11140','11170','11200','11215','11230','11260','11290','11305','11320','11350','11380','11410','11440','11470','11500','11530','11545','11560','11590','11620','11650','11680','11710','11740']

    # 데이터 수집 task
    for param in lawd_params:
        getOpenapi_task = PythonOperator(
            task_id = f'getOpenapi_task_{param}',
            python_callable = get_openapi_data,
            op_kwargs = {'lawd_cd':param},
            dag = dag
        )

    # 데이터 merge task
    merge_task = PythonOperator(
        task_id = 'merge_data',
        python_callable = merge_data,
        op_kwargs = {'lawd_params':lawd_params},
        dag = dag
    )

    # 데이터 적재 task
    load_task = PythonOperator(
        task_id = 'load_task',
        python_callable = load_data,
        dag = dag
    )

    # task 실행
    getOpenapi_task >> merge_task >> load_task
