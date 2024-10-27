from airflow.decorators import dag,task
from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="xcom",
    start_date=datetime(2022,12,30,tz="UTC")
)

@task(task_id='xcom_pushing_task',dag=dag)
def push(ti=None):
    print("triggering pushing task")
    ti.xcom_push(key='abhijeet',value="tarkase")

task1= BashOperator(
    task_id="xcom_pull_task"
    ,dag=dag,
    bash_command="echo {{ti.xcom_pull(key='abhijeet',task_ids='xcom_pushing_task')}}"
)

push() >> task1
