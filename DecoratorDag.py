from airflow.decorators import dag,task
from datetime import datetime, timedelta
import logging

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
        dctName={
            "abhijeet":"tarkase",
            "ABHIJEET":"TARKASE"
        }
        return dctName

    firstTask()

dag_initiator()

         

