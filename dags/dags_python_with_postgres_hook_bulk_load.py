# --------------------------------------------- # 
# Bulk_load 사용법
# * delimiter : Tab으로 변경 필요
# * file header 삭제 후 업로드 필요
# * 특수문자가 있는 경우 전처리 후 업로드 필요
# --------------------------------------------- # 

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id = 'dags_python_with_postgres_hook_bulk_load',
    start_date = pendulum.datetime(2024, 1, 9, tz='Asia/Seoul'),
    schedule = '0 7 * * *',
    catchup = False,
    tags = ['test']
) as dag:
    
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)

    insrt_postgres = PythonOperator(
            task_id='insrt_postgres',
            python_callable=insrt_postgres,
            op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                    'tbl_nm':'test_bulk',
                    'file_nm':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
        )
    