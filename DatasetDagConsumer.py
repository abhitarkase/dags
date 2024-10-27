from airflow import DAG
from airflow.datasets import Dataset
from pendulum.datetime import DateTime
from airflow.operators.bash import BashOperator
from airflow.decorators import task

ds=Dataset("/home/abhitarkase/dags/files/abc.txt")
#dataset is a logical representation of a data in file

dag=DAG(
    dag_id="DatasetDAGConsumer",
    #this schedule represents that this dag will be instantiated if the file present in dataset in updated
    schedule=[ds],
    catchup=False,
    start_date=DateTime(2022,12,30)
)
#outlets in config tells interpreter that this method/task returns/update a dataset 
#don't use outlets config in a task that triggers after the dataset in modified 
#if we do that then the task will never ends.it will trigger in a while loop
@task(task_id="Consumer",dag=dag)
def consumer(**kwargs):
    print("abc file is reading")
    with open(ds.uri,"r") as f:
        print(f.read())


consumer()

