#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/dev/ctfo/code")
from datetime import datetime
import imp
import DagUtils

dagutils = imp.reload(DagUtils)

# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Name of the Dag

FILE_MASTER_IDS = ["601"]

PROCESS_ID = 600

DAG_NAME = "ZS-CTFO-DataEngineering-L1-Data_Monitor-Monthly-600-DEV"

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
    task_id="start",
    python_callable=dagutils.trigger_notification_utility, provide_context=True,
    op_kwargs={"email_type": "batch_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
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
    op_kwargs={"email_type": "batch_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
    dag=dag)

emr_launch.set_upstream(start)

for file_master_id in FILE_MASTER_IDS:
    pre_landing = PythonOperator(
        task_id="pre_landing_" + str(file_master_id),
        python_callable=dagutils.call_pre_landing, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    file_check = PythonOperator(
        task_id="file_check_" + str(file_master_id),
        python_callable=dagutils.call_file_check, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    landing = PythonOperator(
        task_id="landing_" + str(file_master_id),
        python_callable=dagutils.call_landing, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    pre_dqm = PythonOperator(
        task_id="pre_dqm_" + str(file_master_id),
        python_callable=dagutils.call_pre_dqm, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    dqm = PythonOperator(
        task_id="dqm_" + str(file_master_id),
        python_callable=dagutils.call_dqm, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    dqm_filter = PythonOperator(
        task_id="dqm_filter_" + str(file_master_id),
        python_callable=dagutils.call_dqm_filter, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    publish = PythonOperator(
        task_id="publish_" + str(file_master_id),
        python_callable=dagutils.publish, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    archive = PythonOperator(
        task_id="archive_" + str(file_master_id),
        python_callable=dagutils.archive, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    update_catalog = PythonOperator(
        task_id="update_catalog_" + str(file_master_id),
        python_callable=dagutils.update_catalog_data, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    update_status = PythonOperator(
        task_id="update_status_" + str(file_master_id),
        python_callable=dagutils.call_staging, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

    # Set task ordering
    pre_landing.set_upstream(emr_launch)
    file_check.set_upstream(pre_landing)
    landing.set_upstream(file_check)
    pre_dqm.set_upstream(landing)
    dqm.set_upstream(pre_dqm)
    dqm_filter.set_upstream(dqm)
    publish.set_upstream(dqm_filter)
    archive.set_upstream(publish)
    #update_status.set_upstream(archive)
    update_catalog.set_upstream(archive)
    update_status.set_upstream(update_catalog)
    emr_terminate.set_upstream(update_status)
end.set_upstream(emr_terminate)
