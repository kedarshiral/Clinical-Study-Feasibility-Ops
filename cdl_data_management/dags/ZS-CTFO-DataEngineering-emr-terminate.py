#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

sys.path.append("/usr/local/airflow/dags/plugins/dev/ctfo/code")
from datetime import datetime
import imp
import DagUtils

dagutils = imp.reload(DagUtils)

# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Name of the Dag
DAG_NAME = "Sanofi-DataEngineering-Emr-Terminate"

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




emr_terminate = PythonOperator(
    task_id="terminate_cluster",
    trigger_rule="all_done",
    python_callable=dagutils.terminate_emr_adhoc, provide_context=True,
    dag=dag)



##Set task ordering
emr_terminate.set_upstream(start)
end.set_upstream(emr_terminate)