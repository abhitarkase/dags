from airflow.decorators import dag,task
from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="xcomDag",
    start_date=datetime(2022,12,30,tz="UTC"),
    catchup=False
)

@task(task_id='xcom_pushing_task',dag=dag,multiple_outputs=True)
def push(ti=None):# we're setting ti=None because it's a timeInstance value and auto recognized by airflow we don't need to pass it to method explicitly
    print("triggering pushing task")
    ti.xcom_push(key='abhijeet',value="tarkase")

task1= BashOperator(
    task_id="xcom_pull_task",
    dag=dag,
    bash_command="echo {{ti.xcom_pull(key='abhijeet',task_ids='xcom_pushing_task')}}"
    #pull multiple values from one task id
    #t1,t2=ti.xcom_pull(key=['key1','key2'],task_ids='task_id')

    #pull multiple values from different tasks
    #t1,t2=ti.xcom_pull(key=None,task_id=['task1','task2'])
)

push() >> task1
