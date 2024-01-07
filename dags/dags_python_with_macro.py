from airflow import DAG
import pendulum 
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 1, 4, tz="Asia/Seoul"),
    catchup=False,
    tags=["test"]
) as dag:

    # macro 연산 활용
    @task(task_id="task_using_macros",
        templates_dict={'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
                        'end_date':'{{ data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
            
        }
    )

    def get_datetime_macro(**kwargs):
        templates_dict=kwargs.get('templates_dict') or {}
        
        if templates_dict:
            start_date=templates_dict.get('start_date') or 'no_exists_start_date'
            end_date=templates_dict.get('end_date') or 'no_exists_end_date'
            print(start_date)
            print(end_date)

    # 날짜를 직접 연산
    @task(task_id="task_direct_calc")
    def get_datetime_calc(**kwargs):
        # 스케줄러는 주기적으로 dag를 파싱하여 문법적 오류를 확인한다.
        # 이때, 스케줄러 부하 경감을 위해서 python library를 task operator 내부에 넣게 된다. 
        from dateutil.relativedelta import relativedelta

        data_interval_end=kwargs['data_interval_end']
        prev_month_day_first=data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last=data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()
