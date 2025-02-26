#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/qa/ctfo/code")
from datetime import datetime
import imp
import DagUtils

dagutils = imp.reload(DagUtils)

# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import EmrManagerUtility

EmrManagerUtility = imp.reload(EmrManagerUtility)

# Name of the Dag

PROCESS_ID = 10000

DAG_NAME = "ZS-CTFO-DataEngineering-Validation-Adhoc-QA"

# Set default dag properties
default_args = {
    "owner": "ZS Associates",
    "start_date": datetime.now(),
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


emr_launch = PythonOperator(
    task_id="launch_cluster",
    python_callable=dagutils.launch_emr, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID},
    dag=dag)

emr_terminate = PythonOperator(
    task_id="terminate_cluster",
    trigger_rule="all_done",
    python_callable=EmrManagerUtility.terminate_adhoc_cluster, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID},
    dag=dag)

emr_launch.set_upstream(start)

emr_terminate.set_upstream(emr_launch)

end.set_upstream(emr_terminate)
