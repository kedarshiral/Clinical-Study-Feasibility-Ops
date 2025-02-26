import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/qa/ctfo/code")
from datetime import datetime
import DagUtils
import imp

dagutils = imp.reload(DagUtils)
# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Name of the Dag
DAG_NAME = "ZS-CTFO-DataEngineering-L2-BridgingFile-Weekly-QA"
PROCESS_ID = 8000
FREQUENCY = "monthly"

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
    python_callable=dagutils.send_dw_email, provide_context=True,
    op_kwargs={"email_type": "cycle_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
    dag=dag)

# Create a dummy operator task
end = DummyOperator(
    task_id="end",
    dag=dag)

process_dependency_list = PythonOperator(
    task_id="process_dependency_list",
    python_callable=dagutils.populate_dependency_details, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

emr_launch = PythonOperator(
    task_id="launch_cluster",
    python_callable=dagutils.launch_cluster_dw, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

copy_data_s3_hdfs = PythonOperator(
    task_id="copy_data_s3_hdfs",
    python_callable=dagutils.copy_data_s3_hdfs, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

launch_ddl_creation = PythonOperator(
    task_id="ddl_creation",
    python_callable=dagutils.launch_ddl_creation, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

publish = PythonOperator(
    task_id="publish",
    python_callable=dagutils.publish_step_dw,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

create_glue_tables = PythonOperator(
    task_id="create_glue_tables",
    python_callable=dagutils.update_catalog_data_dw,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag
)


# steps needs to be added. This will vary for each process
# #<---------------------->
future_bridging_file_site = PythonOperator(
    task_id="future_bridging_file_site",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "future_bridging_file_site"},
    dag=dag)

future_bridging_file_trial = PythonOperator(
    task_id="future_bridging_file_trial",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "future_bridging_file_trial"},
    dag=dag)

future_bridging_file_inv = PythonOperator(
    task_id="future_bridging_file_inv",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "future_bridging_file_inv"},
    dag=dag)



# <---------------------->

update_cycle_status = PythonOperator(
    task_id="update_cycle_status",
    python_callable=dagutils.update_cycle_status_and_cleaup, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

emr_terminate = PythonOperator(
    task_id="terminate_cluster",
    trigger_rule="all_done",
    python_callable=dagutils.terminate_emr, provide_context=True,
    op_kwargs={"email_type": "cycle_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
    dag=dag)

# Set task ordering
process_dependency_list.set_upstream(start)
emr_launch.set_upstream(process_dependency_list)
copy_data_s3_hdfs.set_upstream(emr_launch)
launch_ddl_creation.set_upstream(copy_data_s3_hdfs)

future_bridging_file_site.set_upstream(launch_ddl_creation)
future_bridging_file_trial.set_upstream(future_bridging_file_site)
future_bridging_file_inv.set_upstream(future_bridging_file_trial)
publish.set_upstream(future_bridging_file_inv)


create_glue_tables.set_upstream(publish)
update_cycle_status.set_upstream(create_glue_tables)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)
