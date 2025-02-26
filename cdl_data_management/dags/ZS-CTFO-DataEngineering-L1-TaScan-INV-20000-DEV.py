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

FILE_MASTER_IDS = [14001, 14004, 14007, 14009, 14010, 14012, 14015, 16001, 16003, 18016, 19001, 19002, 19004, 19006, 19009, 19010, 19013, 19014, 19016]
#FILE_MASTER_IDS = [19001, 14001, 16001, 19014]
PROCESS_ID = 14000

DAG_NAME = "ZS-CTFO-DataEngineering-L1-TaScan-INV-20000-DEV"

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




# tdec_to_internal = PythonOperator(
#     task_id="tdec_to_internal",
#     trigger_rule="all_done",
#     python_callable=dagutils.tascan_tdec_to_internal, provide_context=True,
#     op_kwargs={"email_type": "batch_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
#     dag=dag)

for file_master_id in FILE_MASTER_IDS:
    pre_ingestion = PythonOperator(
        task_id="pre_ingestion_" + str(file_master_id),
        python_callable=dagutils.call_pre_ingestion_tascan, provide_context=True,
        op_kwargs={"file_master_id": file_master_id, "dag_name": DAG_NAME, "process_id": PROCESS_ID},
        dag=dag)

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
    # emr_launch.set_upstream(tdec_to_internal)
    # tdec_to_internal.set_upstream(start)
    # emr_launch.set_upstream(tdec_to_internal)
    emr_launch.set_upstream(start)
    # tdec_to_internal.set_upstream(pre_ingestion)
    pre_ingestion.set_upstream(emr_launch)
    pre_landing.set_upstream(pre_ingestion)
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
