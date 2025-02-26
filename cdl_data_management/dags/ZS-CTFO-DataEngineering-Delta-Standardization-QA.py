#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/qa/ctfo/code")
from datetime import datetime
import DagUtils
from imp import reload
import EmrManagerUtility
import imp

EmrManagerUtility = imp.reload(EmrManagerUtility)
dagutils = reload(DagUtils)

# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# Set default dag properties
default_args = {
    "owner": "ZS Associates",
    "start_date": datetime.today().strftime('%Y-%m-%d'),
    "provide_context": True
}

DAG_NAME = "ZS-CTFO-DataEngineering-Delta-Standardization-QA"
PROCESS_ID = 12000
FREQUENCY = "monthly"

# Define the dag object
dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
)

start = PythonOperator(
    task_id="start",
    python_callable=dagutils.trigger_notification_utility, provide_context=True,
    op_kwargs={"email_type": "batch_status", "process_id":PROCESS_ID, "dag_name": DAG_NAME},
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
    op_kwargs={"process_id": PROCESS_ID},
    dag=dag)

Generic_Delta_Calculation = PythonOperator(
    task_id="Generic_Delta_Calculation",
    python_callable=dagutils.call_delta_calc_std,
    op_kwargs={"file_master_id":9999,"process_id": PROCESS_ID,"type":"Generic"},
    dag=dag)

Disease_Delta_Standardization = PythonOperator(
    task_id="Disease_Delta_Standardization",
    python_callable=dagutils.call_delta_calc_std,
    op_kwargs={"file_master_id":9999 ,"process_id": PROCESS_ID,"type":"Disease"},
    dag=dag)

Sponsor_Delta_Standardization = PythonOperator(
    task_id="Sponsor_Delta_Standardization",
    python_callable=dagutils.call_delta_calc_std,
    op_kwargs={"file_master_id":9999 ,"process_id": PROCESS_ID,"type":"Sponsor"},
    dag=dag)

Drug_Delta_Standardization = PythonOperator(
    task_id="Drug_Delta_Standardization",
    python_callable=dagutils.call_delta_calc_std,
    op_kwargs={"file_master_id":9999 ,"process_id": PROCESS_ID,"type":"Drug"},
    dag=dag)

City_Delta_Standardization = PythonOperator(
    task_id="City_Delta_Standardization",
    python_callable=dagutils.call_delta_calc_std,
    op_kwargs={"file_master_id":9999 ,"process_id": PROCESS_ID,"type":"City"},
    dag=dag)

emr_launch.set_upstream(start)



Disease_Delta_Standardization.set_upstream(emr_launch)
Sponsor_Delta_Standardization.set_upstream(emr_launch)
Drug_Delta_Standardization.set_upstream(emr_launch)
City_Delta_Standardization.set_upstream(emr_launch)
Generic_Delta_Calculation.set_upstream(emr_launch)



emr_terminate.set_upstream(Disease_Delta_Standardization)
emr_terminate.set_upstream(Sponsor_Delta_Standardization)
emr_terminate.set_upstream(Drug_Delta_Standardization)
emr_terminate.set_upstream(City_Delta_Standardization)
emr_terminate.set_upstream(Generic_Delta_Calculation)

end.set_upstream(emr_terminate)