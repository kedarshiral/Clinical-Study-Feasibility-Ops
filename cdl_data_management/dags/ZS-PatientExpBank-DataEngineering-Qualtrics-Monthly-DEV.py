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


PROCESS_ID = 1700
FREQUENCY = "monthly"


DAG_NAME = "ZS-PatientExpBank-DataEngineering-Qualtrics-Monthly-DEV"

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
    op_kwargs={"email_type": "cycle_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
    dag=dag)


process_dependency_list = PythonOperator(
    task_id="process_dependency_list",
    python_callable=dagutils.populate_dependency_details, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)



QualtricsData = PythonOperator(
    task_id="QUALTRICS_INGESTION",
    python_callable=dagutils.call_job_executor, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "QUALTRICS_INGESTION"},
    dag=dag)

#
# create_glue_tables = PythonOperator(
#     task_id="create_glue_tables",
#     python_callable=dagutils.update_catalog_data_dw,
#     op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
#     dag=dag
# )

update_cycle_status = PythonOperator(
    task_id="update_cycle_status",
    python_callable=dagutils.update_cycle_status_and_cleaup, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)



process_dependency_list.set_upstream(start)
emr_launch.set_upstream(process_dependency_list)
QualtricsData.set_upstream(emr_launch)
#create_glue_tables.set_upstream(QualtricsData)

update_cycle_status.set_upstream(QualtricsData)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)
