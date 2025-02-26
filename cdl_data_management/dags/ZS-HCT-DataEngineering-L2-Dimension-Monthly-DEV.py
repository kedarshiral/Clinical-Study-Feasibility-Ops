import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/dev/hct/code")
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
DAG_NAME = "ZS-HCT-DataEngineering-L2-Dimension-Weekly-DEV"
PROCESS_ID = 2000
FREQUENCY = "monthly"

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

# steps needs to be added. This will vary for each process
# <---------------------->

DFullLoad = PythonOperator(
    task_id="d_full_load", 
    python_callable=dagutils.moments_dimension_full_load, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_full_load"},
    dag=dag)


DQCCheck = PythonOperator(
    task_id="d_qc_checks", 
    python_callable=dagutils.init_dimension_qc_check, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_qc_checks"},
    dag=dag)



DLovMapping = PythonOperator(
    task_id="d_lov_mapping", 
    python_callable=dagutils.call_job_executor, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_lov_mapping"},
    dag=dag)


DDateMapping = PythonOperator(
    task_id="d_date_mapping", 
    python_callable=dagutils.call_job_executor, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_date_mapping"},
    dag=dag)


DMomentsLov = PythonOperator(
    task_id="d_moments_lov", 
    python_callable=dagutils.call_job_executor, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_moments_lov"},
    dag=dag)


DMomentsDate = PythonOperator(
    task_id="d_moments_date", 
    python_callable=dagutils.call_job_executor, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_moments_date"},
    dag=dag)

DNormalization = PythonOperator(
    task_id="d_normalization", 
    python_callable=dagutils.moments_dimension_normalization, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_normalization"},
    dag=dag)

DAggClassifiers = PythonOperator(
    task_id="d_agg_classifiers", 
    python_callable=dagutils.moments_dimension_agg_classifiers, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_agg_classifiers"},
    dag=dag)

DimensionQcChecks = PythonOperator(
    task_id="dimension_qc_checks", 
    python_callable=dagutils.final_dimension_qc_checks, provide_context=True,
    op_kwargs={"dag_name": DAG_NAME, "process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "dimension_qc_checks"},
    dag=dag)

# <---------------------->

HDFStoS3 = PythonOperator(
    task_id="HDFS_to_S3",
    python_callable=dagutils.call_hdfs_to_s3,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "HDFS_to_S3"},
    dag=dag)

publish = PythonOperator(
    task_id="publish",
    python_callable=dagutils.publish_step_dw,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

RDSConfig = PythonOperator(
    task_id="RDS_Pre_Config",
    python_callable=dagutils.rds_pre_config,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

S3toRDS = PythonOperator(
    task_id="S3toRDS",
    python_callable=dagutils.call_import_export_utility,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "S3toRDS", "conf_file_path": "import_export_config.json"},
    dag=dag)

create_glue_tables = PythonOperator(
    task_id="create_glue_tables",
    python_callable=dagutils.update_catalog_data_dw,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
    dag=dag
)

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

DFullLoad.set_upstream(launch_ddl_creation)
DQCCheck.set_upstream(DFullLoad)
DLovMapping.set_upstream(DQCCheck)
# DDateMapping.set_upstream(DQCCheck)
DDateMapping.set_upstream(DLovMapping)
DMomentsLov.set_upstream(DDateMapping)
DMomentsDate.set_upstream(DMomentsLov)
# DMomentsDate.set_upstream(DDateMapping)


DNormalization.set_upstream(DMomentsDate)
DAggClassifiers.set_upstream(DNormalization)


DimensionQcChecks.set_upstream(DAggClassifiers)

HDFStoS3.set_upstream(DimensionQcChecks)
publish.set_upstream(HDFStoS3)
RDSConfig.set_upstream(publish)
S3toRDS.set_upstream(RDSConfig)


create_glue_tables.set_upstream(S3toRDS)
update_cycle_status.set_upstream(create_glue_tables)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)
