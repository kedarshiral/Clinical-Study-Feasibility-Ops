#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import sys
import imp
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# ssh script path
sh_file_path = "/usr/local/airflow/dags/plugins/qa/ctfo/code/adaptor/common_utilities/"
sys.path.insert(1, sh_file_path)


# Adapter Name and sh_file_path
adapter_name = "EudraCT"

# Name of the Dag
DAG_NAME = "ZS-CTFO-DataEngineering-L0-EudraCT-Monthly-QA"
STARTED = "STARTED"
SUCCEEDED = "SUCCEEDED"

# Bash Command
adapter_bash_command = "sh " + sh_file_path + "RunAdapter.sh " + adapter_name

# Set default dag properties
default_args = {
    "owner": "ZS Associates",
    "start_date": datetime.today().strftime('%Y-%m-%d'),
    "provide_context": True
}
# Define the dag object
dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
)


start = DummyOperator(
    task_id="start",
    dag=dag)

end = DummyOperator(
    task_id="end",
    dag=dag)

DataExtraction = BashOperator(
    task_id=adapter_name + "DataExtraction",
    bash_command=adapter_bash_command,
    dag=dag)
DataExtraction.set_upstream(start)
end.set_upstream(DataExtraction)