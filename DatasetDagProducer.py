from airflow import DAG
from airflow.datasets import Dataset
from pendulum.datetime import DateTime
from airflow.operators.bash import BashOperator
from airflow.decorators import task

ds=Dataset("/home/abhitarkase/dags/files/abc.txt")
dag=DAG(
    dag_id="DatasetDAGProducer",
    schedule="@daily",
    catchup=False,
    start_date=DateTime(2022,12,30)
)
#outlets config tells interpreter that this method/task returns/update a dataset 
@task(task_id="Producer",outlets=[ds],dag=dag)
def producer(**kwargs):
    print("abc file is updating")
    with open(ds.uri,"+a") as f:
        f.write(f"\nwriting abc file as {kwargs['ds']}")

producer()
