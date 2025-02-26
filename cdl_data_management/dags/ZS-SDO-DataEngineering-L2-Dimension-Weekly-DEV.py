import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/dev/sdo/code")
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
DAG_NAME = "ZS-SDO-DataEngineering-L2-Dimension-Weekly-DEV"
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

DTrialAmendments = PythonOperator(
    task_id="d_trial_amendments",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_trial_amendments"},
    dag=dag)

DTrailMetadata = PythonOperator(
    task_id="d_trial_metadata",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_trial_metadata"},
    dag=dag)

DTrailCommon = PythonOperator(
    task_id="d_trial_common",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_trial_common"},
    dag=dag)

Dendpoints = PythonOperator(
    task_id="d_endpoints",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_endpoints"},
    dag=dag)

Dprocedures = PythonOperator(
    task_id="d_procedures",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_procedures"},
    dag=dag)

DTrailInclusionExclusion = PythonOperator(
    task_id="d_trial_inclusion_exclusion",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_trial_inclusion_exclusion"},
    dag=dag)

DTrialArm = PythonOperator(
    task_id="d_trial_arm",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_trial_arm"},
    dag=dag)

Ddisease = PythonOperator(
    task_id="d_disease",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_disease"},
    dag=dag)

RTdisease = PythonOperator(
    task_id="r_trial_disease",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "r_trial_disease"},
    dag=dag)

Ddrug = PythonOperator(
    task_id="d_drug",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_drug"},
    dag=dag)

RTdrug = PythonOperator(
    task_id="r_trial_drug",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "r_trial_drug"},
    dag=dag)

# RArmEndpointProcedures = PythonOperator(
#     task_id="RArmEndpointProcedures",
#     python_callable=dagutils.call_job_executor,
#     op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RArmEndpointProcedures"},
#     dag=dag)

DTrialArmProcedures = PythonOperator(
    task_id="d_trial_arm_procedures",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_trial_arm_procedures"},
    dag=dag)

DTrialEndpoints = PythonOperator(
    task_id="d_trial_endpoint",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_trial_endpoint"},
    dag=dag)

DTimeVisitDetails = PythonOperator(
    task_id="d_time_and_visit_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "d_time_and_visit_details"},
    dag=dag)

DProcedureBurden = PythonOperator(
    task_id="d_procedure_burden",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_procedure_burden"},
    dag=dag)

DimensionQcChecks = PythonOperator(
    task_id="dimension_qc_checks",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "dimension_qc_checks"},
    dag=dag)

create_glue_tables = PythonOperator(
    task_id="create_glue_tables",
    python_callable=dagutils.update_catalog_data_dw,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
    dag=dag
)


# <---------------------->

publish = PythonOperator(
    task_id="publish",
    python_callable=dagutils.publish_step_dw,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

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

DTrialAmendments.set_upstream(launch_ddl_creation)
DTrailMetadata.set_upstream(launch_ddl_creation)
Dendpoints.set_upstream(launch_ddl_creation)
DProcedureBurden.set_upstream(launch_ddl_creation)
Dprocedures.set_upstream(DProcedureBurden)

DTrailCommon.set_upstream(DTrialAmendments)
DTrailCommon.set_upstream(DTrailMetadata)
DTrailInclusionExclusion.set_upstream(DTrailCommon)
DTrialArm.set_upstream(DTrailCommon)
Ddisease.set_upstream(DTrailCommon)
RTdisease.set_upstream(Ddisease)
Ddrug.set_upstream(DTrailCommon)
RTdrug.set_upstream(Ddrug)
DTrialEndpoints.set_upstream(Dendpoints)
DTrialArmProcedures.set_upstream(Dprocedures)
DTrialArmProcedures.set_upstream(DTrialArm)
DTimeVisitDetails.set_upstream(DTrialArmProcedures)
DTrialEndpoints.set_upstream(DTrailCommon)




#publish.set_upstream(RArmEndpointProcedures)
#publish.set_upstream(Dprocedures)
#publish.set_upstream(Dendpoints)
publish.set_upstream(DTimeVisitDetails)
publish.set_upstream(DTrailInclusionExclusion)
#publish.set_upstream(DTrialArm)
publish.set_upstream(RTdisease)
publish.set_upstream(RTdrug)
publish.set_upstream(DTrialEndpoints)
#publish.set_upstream(DProcedureBurden)

DimensionQcChecks.set_upstream(publish)
create_glue_tables.set_upstream(DimensionQcChecks)
update_cycle_status.set_upstream(create_glue_tables)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)
