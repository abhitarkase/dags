from airflow.decorators import dag,task
from datetime import datetime, timedelta
import logging
from airflow.models import Variable

#we can use constant variables whose value we can store in airflow UI under Admin->Variable
name1 = Variable.get('name')
sirname = Variable.get("sirname")

default_args = {
    "ower":"AbhijeetTarkase",
    'retry_delay':timedelta(minutes=5),
    'retries':3
        }
@dag(
        dag_id="decorator_dag",
        start_date=datetime(2022,12,30),
        catchup=False,
        schedule="* * * * *",
        default_args=default_args
)
def dag_initiator():

    @task(task_id="firstDecoredTask",)
    def firstTask():
        ARGS={"ABHIJEET":"ABHIJEET"}
        logging.info("first Decorted task executed")
        rn=generateName(ARGS["ABHIJEET"])
        logging.info(f"return value is {rn['ABHIJEET']}")


    def generateName(name):

        print(f"checking the constant variables {name1} {sirname},")
        dctName={
            "abhijeet":"tarkase",
            "ABHIJEET":"TARKASE"
        }
        return dctName

    firstTask()

dag_initiator()

         

