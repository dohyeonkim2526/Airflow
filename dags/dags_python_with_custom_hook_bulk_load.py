# Custom Hook으로 bulk_load 진행
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook  # custom Hook 만든 것 import 

with DAG(
        dag_id='dags_python_with_custom_hook_bulk_load',
        start_date=pendulum.datetime(2024, 1, 8, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False,
        tags=['test']
) as dag:
    
    def instr_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm,
                                       file_name=file_nm,
                                       delimiter=',',
                                       is_header=True,
                                       is_replace=True)
        
        instr_postgres = PythonOperator(
            task_id='instr_postgres',
            python_callable=instr_postgres,
            op_kwargs={'postgres_conn_id' : 'conn-db-postgres-custom',
                        'tbl_nm' : 'test_bulk',
                        'file_nm' : '/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'
                        }
        )
