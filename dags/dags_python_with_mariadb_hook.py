# Connection : Airflow UI 화면에 등록한 커넥션 정보
# Hook : Airflow에서 외부 솔루션 기능을 사용할 수 있도록 구현된 클래스

# ------------------------------------------------- #
# Hook
# * Connection 정보를 통해 생성되는 객체
# * Connection을 통해 접속 정보를 받아오므로 DB 접속정보가 코드상 노출되지 않음
# * Hook은 task를 만들지 못하여 Custom, Python Operator내 함수에서 사용된다.
# ------------------------------------------------- #

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'dags_python_with_mariadb_hook',
    start_date = pendulum.datetime(2024, 1, 17, tz='Asia/Seoul'),
    schedule = None,
    catchup = False,
    tags = ['test']
) as dag:
    
    def instr_mariadb(mysql_conn_id, **kwargs):
        # pip install apache-airflow-providers-mysql
        from airflow.providers.mysql.operators.mysql import MysqlOperator, MySqlHook
        from contextlib import closing

        mariadb_hook = MySqlHook.get_hook(mysql_conn_id)
        with closing(mariadb_hook.get_conn()) as conn: # session(DB server와의 연결) 정보
            with closing(conn.cursor()) as cursor:  # session을 통해서 cursor을 만들어 sql을 수행하고 결과를 받아온다. 
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                message = 'insert 수행'
                sql = 'insert into test values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, message)) # sql 실행
                conn.commit()

    instr_mariadb_with_hook = PythonOperator(
        task_id = 'instr_mariadb_with_hook',
        python_callable = instr_mariadb,
        op_kwargs = {'mysql_conn_id' : 'conn-db-mysql'}
    )

    instr_mariadb_with_hook
