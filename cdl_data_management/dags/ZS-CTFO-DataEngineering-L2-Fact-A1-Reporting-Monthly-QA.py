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
DAG_NAME = "ZS-CTFO-DataEngineering-L2-Fact-A1-Reporting-Monthly-QA"
PROCESS_ID = 3000
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

Validation_utility = PythonOperator(
    task_id="Validation_utility",
    trigger_rule="all_done",
    python_callable=dagutils.call_validation_utility, provide_context=True,
    op_kwargs={"dag_id": PROCESS_ID},
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
#<---------------------->


MCE_TrialCountry = PythonOperator(
    task_id="MCE_TrialCountry",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialCountry"},
    dag=dag)


MCE_TrialSite = PythonOperator(
    task_id="MCE_TrialSite",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialSite"},
    dag=dag)


MCE_TrialInv = PythonOperator(
    task_id="MCE_TrialInv",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialInv"},
    dag=dag)

MCE_TrialUniverse = PythonOperator(
    task_id="MCE_TrialUniverse",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialUniverse"},
    dag=dag)

MCE_TrialCountry_KPI = PythonOperator(
    task_id="MCE_TrialCountry_KPI",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialCountry_KPI"},
    dag=dag)


MCE_TrialSite_KPI = PythonOperator(
    task_id="MCE_TrialSite_KPI",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialSite_KPI"},
    dag=dag)


MCE_TrialInv_KPI = PythonOperator(
    task_id="MCE_TrialInv_KPI",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialInv_KPI"},
    dag=dag)

MCE_TrialUniverse_KPI = PythonOperator(
    task_id="MCE_TrialUniverse_KPI",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "MCE_TrialUniverse_KPI"},
    dag=dag)

TrialSite = PythonOperator(
    task_id="TrialSite",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "TrialSite"},
    dag=dag)


RptSiteStudyDetails = PythonOperator(
    task_id="RptSiteStudyDetails",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RptSiteStudyDetails"},
    dag=dag)

FactInvestigator = PythonOperator(
    task_id="FactInvestigator",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "FactInvestigator"},
    dag=dag)

RptInvestigatorSiteStudyDetails = PythonOperator(
    task_id="RptInvestigatorSiteStudyDetails",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RptInvestigatorSiteStudyDetails"},
    dag=dag)

RptCountry = PythonOperator(
    task_id="RptCountry",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RptCountry"},
    dag=dag)

RptFiltersTrial = PythonOperator(
    task_id="RptFiltersTrial",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RptFiltersTrial"},
    dag=dag)


RptFiltersSetupAll = PythonOperator(
    task_id="RptFiltersSetupAll",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RptFiltersSetupAll"},
    dag=dag)

RptDrugDetails = PythonOperator(
    task_id="RptDrugDetails",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RptDrugDetails"},
    dag=dag)

batch_id_maintain_future= PythonOperator(
    task_id="batch_id_maintain_future",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "batch_id_maintain_future"},
    dag=dag)




RptFiltersSiteInv = PythonOperator(
    task_id="RptFiltersSiteInv",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "RptFiltersSiteInv"},
    dag=dag)




RptPatientDetails = PythonOperator(
    task_id="RptPatientDetails",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "RptPatientDetails"},
    dag=dag)

RptPatientDiversity = PythonOperator(
    task_id="RptPatientDiversity",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "RptPatientDiversity"},
    dag=dag)

RptTrialUniverse = PythonOperator(
    task_id="RptTrialUniverse",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "RptTrialUniverse"},
    dag=dag)

#<---------------------->

publish = PythonOperator(
    task_id="publish",
    python_callable=dagutils.publish_step_dw,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
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

MCE_TrialCountry.set_upstream(RptFiltersSiteInv)
MCE_TrialSite.set_upstream(RptFiltersSiteInv)
MCE_TrialInv.set_upstream(RptFiltersSiteInv)
MCE_TrialUniverse.set_upstream(RptFiltersSiteInv)


MCE_TrialCountry_KPI.set_upstream(MCE_TrialCountry)
MCE_TrialSite_KPI.set_upstream(MCE_TrialSite)
MCE_TrialInv_KPI.set_upstream(MCE_TrialInv)
MCE_TrialUniverse_KPI.set_upstream(MCE_TrialUniverse)


RptCountry.set_upstream(MCE_TrialCountry_KPI)
TrialSite.set_upstream(MCE_TrialSite_KPI)
FactInvestigator.set_upstream(MCE_TrialInv_KPI)
RptTrialUniverse.set_upstream(MCE_TrialUniverse_KPI)
RptSiteStudyDetails.set_upstream(TrialSite)
RptInvestigatorSiteStudyDetails.set_upstream(FactInvestigator)




RptFiltersSetupAll.set_upstream(launch_ddl_creation)
RptDrugDetails.set_upstream(launch_ddl_creation)
RptPatientDetails.set_upstream(launch_ddl_creation)
RptPatientDiversity.set_upstream(launch_ddl_creation)
RptFiltersTrial.set_upstream(RptFiltersSetupAll)
RptFiltersSiteInv.set_upstream(RptFiltersTrial)



batch_id_maintain_future.set_upstream(RptInvestigatorSiteStudyDetails)
batch_id_maintain_future.set_upstream(RptPatientDiversity)
batch_id_maintain_future.set_upstream(RptDrugDetails)
batch_id_maintain_future.set_upstream(RptPatientDetails)
batch_id_maintain_future.set_upstream(RptSiteStudyDetails)
batch_id_maintain_future.set_upstream(RptCountry)
batch_id_maintain_future.set_upstream(RptTrialUniverse)
publish.set_upstream(batch_id_maintain_future)
create_glue_tables.set_upstream(publish)
update_cycle_status.set_upstream(create_glue_tables)
Validation_utility.set_upstream(update_cycle_status)
emr_terminate.set_upstream(Validation_utility)
end.set_upstream(emr_terminate)


