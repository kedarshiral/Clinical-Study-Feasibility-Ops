#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
sys.path.insert(1, "/usr/local/airflow/dags/plugins/dev/ctfo/code")
# sys.path.insert(1, "/appdata/dev/clinical_data_lake/v0_0_5/code")
from datetime import datetime, timedelta
#import DagUtils
import AutomationWrapperMWAA
from DagTriggerUtility import DagTriggerUtility
from imp import reload

#dagutils = reload(DagUtils)
data_date = reload(AutomationWrapper)

# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

yesterday_date = datetime.today() - timedelta(days=1)

# Set default dag properties
default_args = {
    "owner": "ZS Associates",
    "start_date": yesterday_date,
    "provide_context": True
}

config_file_name = "/usr/local/airflow/dags/plugins/dev/ctfo/code/DataEngineering-test-V1.json"
automated_dag_name = "/usr/local/airflow/dags/plugins/dev/ctfo/code/DataEngineering-test-V1.template"
DAG_NAME = "super_dag"
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
    dag=dag
    , trigger_rule=TriggerRule.ALL_SUCCESS)

child_dag_t1 = PythonOperator(
    task_id="Ingestion-Dag",
    python_callable=data_date.insert_data_date, provide_context=True,
    op_kwargs={"automated_dag_name": automated_dag_name, "dag_id": "CC_glue_cata_testing_100088",
               "process_type": "Ingestion", "config_file_name": config_file_name},
    dag=dag)

child_dag_t2 = PythonOperator(
    task_id="DW-Dag",
    python_callable=data_date.insert_data_date, provide_context=True,
    op_kwargs={"automated_dag_name": automated_dag_name, "dag_id": "cc_dw_enhancement_v4",
               "process_type": "DW", "config_file_name": config_file_name},
    dag=dag)



# Set task ordering
child_dag_t1.set_upstream(start)
child_dag_t2.set_upstream(child_dag_t1)
end.set_upstream(child_dag_t2)

