# sensor 모드 유형
# 1) poke 모드 : dag 수행되는 내내 running slot을 차지하며 slot 안에서 sleep, active 반복 --> 센싱해야되는 주기가 초 단위로 짧은 경우
# 2) reschedule 모드 : 센서가 동작하는 시기에만 slot을 차지하며, 그 외에는 slot을 점유하지 않음 --> 센싱해야되는 주기가 분 단위로 긴 경우

from airflow.sensors.bash import BashSensor
from airflow.operators.bash import BashOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_bash_sensor',
    start_date=pendulum.datetime(2024, 1, 11, tz='Asia/Seoul'),
    schedule='0 6 * * *',
    catchup=False,
    tags=['test']
) as dag:
    
    sensor_task_by_poke = BashSensor(
        task_id='sensor_task_by_poke',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command=f'''echo $FILE && 
                        if [ -f $FILE ]; then 
                              exit 0
                        else 
                              exit 1
                        fi''',
        poke_interval=30,
        timeout=60*2,
        mode='poke',
        soft_fail=False        
    )

    sensor_task_by_reschedule = BashSensor(
        task_id='sensor_task_by_reschedule',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command=f'''echo $FILE && 
                        if [ -f $FILE ]; then 
                              exit 0
                        else 
                              exit 1
                        fi''',
        poke_interval=60*3,
        timeout=60*9,
        mode='reschedule',
        soft_fail=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
        env={'FILE': '/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command='echo "건수: `cat $FILE | wc -l`"'
    )

    [sensor_task_by_poke,sensor_task_by_reschedule] >> bash_task
    