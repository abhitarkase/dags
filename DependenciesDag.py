from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

#Trigger Rules to see all the rule go to /home/abhitarkase/.local/lib/python3.10/site-packages/airflow/utils/trigger_rule.py this file
with DAG (
    dag_id="878firstdag123",
    description="this is the first dag",
    start_date = datetime(2020,1,1),
    catchup=True,
    schedule="@daily"
) as dag:
    task1=   BashOperator(
        task_id="first_task",
        bash_command="echo 'My name is Abhijeet Baburao Tarkase'"
    )

    task2= BashOperator(
        task_id="second_task",
        bash_command="echo asdf'this task depends on first task'"
    )

    task3= BashOperator(
        task_id="third_task",
        bash_command="echo 'this task depends on first task'"
    )

    task4= BashOperator(
        task_id="fourth_task",
        bash_command="echo 'this task depends on 2nd task only'",
        trigger_rule=TriggerRule.ALL_SUCCESS
        # this task only trigger if all the upstream task successeds
    )


    task_5= BashOperator(
        task_id="fifth_task",
        bash_command="echo 'this task is depends on 3rd task'"
    )


    task_6= BashOperator(
        task_id="sixth_task",
        bash_command="echo 'this task depends on 4th task",
        trigger_rule=TriggerRule.ALL_FAILED
        # this task triggers only all the upstream task fails
    )

    task_7= BashOperator(
        task_id="seventh_task",
        bash_command="echo 'this task depends on 4th and 6th task'",
        trigger_rule=TriggerRule.ONE_SUCCESS
        #this task will trigger if atlease one upstream task succeeds
    )

    task1 >> [task2,task3]
    task2 >> [task4,task_6] >> task_7
    task3 >> task_5