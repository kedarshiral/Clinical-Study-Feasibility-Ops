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
from airflow.utils.dates import days_ago
# ssh script path
sh_file_path = "/usr/local/airflow/dags/plugins/dev/ctfo/code/adaptor/common_utilities/"
sys.path.insert(1, sh_file_path)
import EmrManagerUtility

EmrManagerUtility = imp.reload(EmrManagerUtility)


# Adapter Name and sh_file_path
adapter_name = "Organizationhierarchy"

# Name of the Dag
DAG_NAME = "ZS-CTFO-DataEngineering-L0-CitelineOrganizationhierarchy-Monthly-DEV"
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

emr_terminate = PythonOperator(
    task_id="terminate_cluster",
    trigger_rule="all_done",
    python_callable=EmrManagerUtility.terminate_adapter, provide_context=True,
    op_kwargs={"source_name": adapter_name},
    dag=dag)

DataExtraction.set_upstream(start)
emr_terminate.set_upstream(DataExtraction)
end.set_upstream(emr_terminate)
