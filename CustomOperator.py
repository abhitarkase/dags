from CustomOperatorBase import CustomOperatorBase
from airflow import DAG
from pendulum import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    dag_id="CustomOperator",
    start_date=datetime(2022,12,30),
    schedule="* * * * *"
) as dag:
    
    def kwargsTAsk(company,**kwargs):
        print("initializing custom operator class")
        print("testing passing argument with template")
        print(f"op Kwargs are :company is = {company}")
        print(f"tempalates dict is : location is = {kwargs['templates_dict']['location']}")

# kwargs: are nothing but the configuration values that are present in key value pairs in kwargs dict
#         in above case we have used a template in task1 as 'templates_dict' and the key values provided in that will set into config dict

#op_kwargs: by using this we can directly pass the command line argument to the python function
        
    task1=PythonOperator(
        task_id="Custom_Task_Initiating",
        python_callable=kwargsTAsk,
        op_kwargs={"company":"credit_suisse"},
        templates_dict = {"location":"Kharadi"}
    )

    task2=CustomOperatorBase(
        task_id="customerOperator",
        name="abhijeet",
        sirname="tarkase",
    )

    task3=BashOperator(
        task_id="end_custom_OPerator",
        bash_command="echo 'Custom Operator class ended successfully'"
    )

    task1 >> task2 >> task3

