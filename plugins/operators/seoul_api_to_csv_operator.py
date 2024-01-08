from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr', # airflow connection 정보
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm     # key는 변수로 사용
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row, end_row = 1, 1000

        while True:
            self.log.info(f'시작: {start_row}')
            self.log.info(f'끝: {end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])

            # 모든데이터를 가져왔으면 while문 중단
            # 1천개씩 가져오므로 마지막은 1천개 미만의 데이터가 들어오게됨
            if len(row_df) < 1000:
                break

            else:
                start_row = end_row + 1
                end_row += 1000

        # os path 경로가 없으면 생성
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')

        # 데이터 저장한 DataFrame --> csv 저장
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding = 'utf-8', index = False)

    ''' api를 통해 공공데이터를 수집하여 DataFrame에 저장 '''
    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {'Content-Type' : 'application/json',
                   'charset' : 'utf-8',
                   'Accept' : '*/*'
                   }
        
        request_url = f'{base_url}/{start_row}/{end_row}/'

        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'

        response = requests.get(request_url, headers) # dictionary를 string으로 반환
        contents = json.loads(response.text)

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)

        return row_df
    