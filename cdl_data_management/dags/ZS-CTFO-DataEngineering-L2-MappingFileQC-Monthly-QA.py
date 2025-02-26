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
DAG_NAME = "ZS-CTFO-DataEngineering-L2-MappingFileQC-Monthly-QA"
PROCESS_ID = 9000
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

#steps needs to be added. This will vary for each process
# #<---------------------->
Drug_Mapping_File = PythonOperator(
    task_id="Drug_Mapping_File",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Drug_Mapping_File"},
    dag=dag)

Sponsor_Mapping_File = PythonOperator(
    task_id="Sponsor_Mapping_File",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Sponsor_Mapping_File"},
    dag=dag)

Mapping_QC_Check = PythonOperator(
    task_id="Mapping_QC_Check",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Mapping_QC_Check"},
    dag=dag)

#<---------------------->
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

Drug_Mapping_File.set_upstream(launch_ddl_creation)
Sponsor_Mapping_File.set_upstream(launch_ddl_creation)
Mapping_QC_Check.set_upstream(Drug_Mapping_File)
Mapping_QC_Check.set_upstream(Sponsor_Mapping_File)

update_cycle_status.set_upstream(Mapping_QC_Check)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)