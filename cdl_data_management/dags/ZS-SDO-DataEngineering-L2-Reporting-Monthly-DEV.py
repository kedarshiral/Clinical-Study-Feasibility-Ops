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
DAG_NAME = "ZS-SDO-DataEngineering-L2-Reporting-Monthly-DEV"
PROCESS_ID = 3000
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

create_glue_tables = PythonOperator(
    task_id="create_glue_tables",
    python_callable=dagutils.update_catalog_data_dw,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
    dag=dag
)
#steps needs to be added. This will vary for each process
#<---------------------->

RptStudyDesignDetails= PythonOperator(
    task_id="f_rpt_study_design_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_study_design_details"},
    dag=dag)

RptFilterDetails = PythonOperator(
    task_id="f_rpt_filter_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_filter_details"},
    dag=dag)

RptStudyArmDetails = PythonOperator(
    task_id="f_rpt_study_arm_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_study_arm_details"},
    dag=dag)

RptEndpoints = PythonOperator(
    task_id="d_rpt_endpoints",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_endpoints"},
    dag=dag)

RptProcedures = PythonOperator(
    task_id="d_rpt_procedures",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_procedures"},
    dag=dag)

RptProcedureDetails = PythonOperator(
    task_id="f_rpt_procedure_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_procedure_details"},
    dag=dag)


RptTuftsProcedureMappingDetails = PythonOperator(
    task_id="f_rpt_tufts_procedure_mapping_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_tufts_procedure_mapping_details"},
    dag=dag)


RptEndpointDetails = PythonOperator(
    task_id="f_rpt_endpoint_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_endpoint_details"},
    dag=dag)

RptArmStageDetails = PythonOperator(
    task_id="f_rpt_arm_stage_details",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_arm_stage_details"},
    dag=dag)

RptDisease = PythonOperator(
    task_id="d_rpt_disease",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_disease"},
    dag=dag)

RptDrug = PythonOperator(
    task_id="d_rpt_drug",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_drug"},
    dag=dag)

RptProcedureCareEndpointMapping = PythonOperator(
    task_id="f_rpt_procedure_care_endpoint_mapping",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "f_rpt_procedure_care_endpoint_mapping"},
    dag=dag)

RptProcedureBurden = PythonOperator(
    task_id="d_rpt_procedure_burden",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_procedure_burden"},
    dag=dag)

RptStatus = PythonOperator(
    task_id="d_rpt_status",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_status"},
    dag=dag)

RptPhase = PythonOperator(
    task_id="d_rpt_phase",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_phase"},
    dag=dag)

RptSponsor = PythonOperator(
    task_id="d_rpt_sponsor",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_sponsor"},
    dag=dag)

RptObjective = PythonOperator(
    task_id="d_rpt_objective",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_objective"},
    dag=dag)

RptLot = PythonOperator(
    task_id="d_rpt_lot",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "d_rpt_lot"},
    dag=dag)

ReportingQcChecks = PythonOperator(
    task_id="reporting_qc_checks",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "reporting_qc_checks"},
    dag=dag)

#<---------------------->

publish = PythonOperator(
    task_id="publish",
    python_callable=dagutils.publish_step_dw,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
    dag=dag)

S3toRDS = PythonOperator(
    task_id="S3toRDS",
    python_callable=dagutils.call_import_export_utility,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "S3toRDS", "conf_file_path": "import_export_config.json"},
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
#launch_ddl_creation.set_upstream(emr_launch)
RptStudyDesignDetails.set_upstream(launch_ddl_creation)
RptFilterDetails.set_upstream(launch_ddl_creation)
RptStudyArmDetails.set_upstream(RptProcedureDetails)
RptProcedures.set_upstream(launch_ddl_creation)
RptEndpoints.set_upstream(launch_ddl_creation)
RptProcedureDetails.set_upstream(launch_ddl_creation)
RptEndpointDetails.set_upstream(launch_ddl_creation)
RptArmStageDetails.set_upstream(RptProcedureDetails)
RptTuftsProcedureMappingDetails.set_upstream(launch_ddl_creation)
RptDisease.set_upstream(launch_ddl_creation)
RptDrug.set_upstream(launch_ddl_creation)
RptProcedureCareEndpointMapping.set_upstream(launch_ddl_creation)
RptProcedureBurden.set_upstream(launch_ddl_creation)
RptPhase.set_upstream(launch_ddl_creation)
RptObjective.set_upstream(launch_ddl_creation)
RptStatus.set_upstream(launch_ddl_creation)
RptLot.set_upstream(launch_ddl_creation)
RptSponsor.set_upstream(launch_ddl_creation)


publish.set_upstream(RptArmStageDetails)
publish.set_upstream(RptEndpointDetails)
#publish.set_upstream(RptProcedureDetails)
publish.set_upstream(RptProcedures)
publish.set_upstream(RptEndpoints)
publish.set_upstream(RptStudyDesignDetails)
publish.set_upstream(RptFilterDetails)
publish.set_upstream(RptStudyArmDetails)
publish.set_upstream(RptTuftsProcedureMappingDetails)
publish.set_upstream(RptDisease)
publish.set_upstream(RptDrug)
publish.set_upstream(RptProcedureCareEndpointMapping)
publish.set_upstream(RptProcedureBurden)
publish.set_upstream(RptPhase)
publish.set_upstream(RptObjective)
publish.set_upstream(RptStatus)
publish.set_upstream(RptLot)
publish.set_upstream(RptSponsor)


ReportingQcChecks.set_upstream(publish)

#update_cycle_status.set_upstream(publish)
create_glue_tables.set_upstream(ReportingQcChecks)
S3toRDS.set_upstream(create_glue_tables)
update_cycle_status.set_upstream(S3toRDS)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)