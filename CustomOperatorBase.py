from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator

#to create custom operator we have extend BaseOperator and we have to implement two methods
#1.constructor
#2.execute 
#context in execute method will have all the config parameters in form of dict that you can fetch 
class CustomOperatorBase(BaseOperator):

    def __init__(self,name,sirname,**kwargs):

        super().__init__(**kwargs)
        print("the customOpertorBase initiated")
        self.name=name
        self.sirname=sirname

    def execute(self,context):
        print("execute method of the customOperator is started")
        
    
    