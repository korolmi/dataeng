"""
Тестовый граф использования спарка через Livy - простая загрузка таблицы
"""

from airflow.models import DAG
from airflow.models import Variable
#from airflow.models.xcom import XCom # так и не смог найти всех концов - как этим пользоваться...
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator

import requests, json, subprocess, datetime, time

import livy_tools

# настройки

DAG_NAME = "livy_test"
SESS_VAR = DAG_NAME + "_sess"

dag = DAG( dag_id=DAG_NAME, schedule_interval=None, start_date=datetime.datetime(2019, 7, 23) )

createSess = PythonOperator( task_id='createSess', python_callable=livy_tools.createSess, op_args = [SESS_VAR], dag=dag)
createSparkSess = PythonOperator( task_id='createSparkSess', python_callable=livy_tools.createSparkSession, op_args = [SESS_VAR], dag=dag)
closeSess = PythonOperator( task_id='closeSess', python_callable=livy_tools.closeSess, op_args = [SESS_VAR], dag=dag)

loadTab = PythonOperator( task_id='loadTable', python_callable=livy_tools.loadTable, op_args = [SESS_VAR,"address_area"], dag=dag)
storeTab = PythonOperator( task_id='storeTable', python_callable=livy_tools.storeTable, op_args = [SESS_VAR,"addr_ar"], dag=dag)

# создаем DAG - последовательность операторов, подумаем, как лучше обобщить
createSess >> createSparkSess >> loadTab >> storeTab >> closeSess
