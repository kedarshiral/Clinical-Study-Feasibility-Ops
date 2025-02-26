#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/dev/hct/code")
from datetime import datetime
import imp
import DagUtils

dagutils = imp.reload(DagUtils)

# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Name of the Dag


PROCESS_ID = 1800
FREQUENCY = "monthly"
DAG_NAME = "ZS-PatientExpBank-DataEngineering-BulkIngestion-Monthly-DEV"

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

start = PythonOperator(
    task_id="start",
    python_callable=dagutils.send_dw_email, provide_context=True,
    op_kwargs={"email_type": "cycle_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
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
    python_callable=dagutils.terminate_emr, provide_context=True,
    op_kwargs={"email_type": "cycle_status",
               "process_id": PROCESS_ID, "dag_name": DAG_NAME},
    dag=dag)
    
process_dependency_list = PythonOperator(
    task_id="process_dependency_list",
    python_callable=dagutils.populate_dependency_details, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)



BulkData = PythonOperator(
    task_id="BULK_INGESTION",
    python_callable=dagutils.call_job_executor, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "BULK_INGESTION"},
    dag=dag)


process_dependency_list.set_upstream(start)
emr_launch.set_upstream(process_dependency_list)
BulkData.set_upstream(emr_launch)
#BulkData.set_upstream(emr_launch)
emr_terminate.set_upstream(BulkData)
end.set_upstream(emr_terminate)
