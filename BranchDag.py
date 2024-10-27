from airflow.decorators import dag,task
from airflow.operators.bash import BashOperator
from datetime import datetime

#Branching

#For a Dag there can be multiple downstreams and in some cases we want to trigger those downstream tasks based on certains conditions
#so for ex. below we have two tasks A and B after bd task we can write a logic in branchInit(in which we have used task.branch decorator to show this 
# method is a branching method to decide downstream task) method to return the task name.
@dag(dag_id="BranchDag",start_date=datetime(2022,12,30),catchup=False)
def branchDag():
    bd=BashOperator(task_id="Branch_Task_started",bash_command="echo 'branching task started'")

    @task.branch(task_id="Branching_is_started_for_tasks")
    def branchInit():
        return "task_A"
    #we can can return the list of tasks names as well based on our requirement
    
    @task(task_id="task_A")
    def taskA():
        print("the task A is executed")
    
    @task(task_id="task_B")
    def taskB():
        print("the task B is executed")

    branch=branchInit()
    Ta=taskA()
    Tb=taskB()

    bd >> branch >> [Ta,Tb]

branchDag()