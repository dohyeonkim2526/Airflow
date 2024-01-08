from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'dags_python_with_postgres',
    start_date = pendulum.datetime(2024, 1, 9, tz='Asia/Seoul'),
    schedule = None,
    catchup = False,
    tags = ['test']
) as dag:
    
    def instr_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2 # postgres DB에 접속해서 sql을 수행하고 데이터를 가져오는 library
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn: # session(DB server와의 연결) 정보
            with closing(conn.cursor()) as cursor:  # session을 통해서 cursor을 만들어 sql을 수행하고 결과를 받아온다. 
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                message = 'insert 수행'
                sql = 'insert into test values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, message)) # sql 실행
                conn.commit()

    instr_postgres = PythonOperator(
        task_id = 'instr_postgres',
        python_callable = instr_postgres,
        op_args = ['172.28.0.3', '5432', 'dhkim', 'dhkim', 'dhkim']
    )

    instr_postgres
