#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

import sys

sys.path.insert(1, " ") #Directory path of the codes should be provided in the quotes
from datetime import datetime
import imp
import launch_emr

launch_emr_obj = imp.reload(launch_emr)

# Airflow level imports
import airflow
from airflow.models import DAG
from airflow import AirflowException
from airflow.models import DAG, TaskInstance, BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

#please insert rdbms datasource name accordingly
Datasource_name=""


# Name of the Dag
DAG_NAME = " "


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

start = PythonOperator(
    task_id="Start",
    python_callable=launch_emr_obj.trigger_notification_utility, provide_context=True,
    op_kwargs={"email_type":"extractor_status","execution_point":"start", "dag_name": DAG_NAME},
    dag=dag)

emr_launch = PythonOperator(
    task_id="launch_cluster",
    python_callable=launch_emr_obj.call_launch_cluster, provide_context=True,
    op_kwargs={"data_source_name":Datasource_name , "dag_name": DAG_NAME},
    dag=dag)

database_extractor = PythonOperator(
    task_id="database_extractor",
    python_callable=launch_emr_obj.execute_extractor_utility, provide_context=True,
    op_kwargs={ "dag_name": DAG_NAME},
    dag=dag)

emr_terminate = PythonOperator(
    task_id="terminate_cluster",
    python_callable=launch_emr_obj.call_terminate_cluster, provide_context=True,
    op_kwargs={ "dag_name": DAG_NAME},
    dag=dag)

end = PythonOperator(
    task_id="End",
    python_callable=launch_emr_obj.trigger_notification_utility, provide_context=True,
    op_kwargs={ "email_type":"extractor_status","execution_point":"end","dag_name": DAG_NAME},
    dag=dag)


emr_launch.set_upstream(start)
database_extractor.set_upstream(emr_launch)
emr_terminate.set_upstream(database_extractor)
end.set_upstream(emr_terminate)
