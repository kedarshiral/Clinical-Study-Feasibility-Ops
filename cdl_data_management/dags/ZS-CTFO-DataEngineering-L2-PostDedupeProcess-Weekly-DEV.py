import sys
sys.path.insert(1, "/usr/local/airflow/dags/plugins/dev/ctfo/code")
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
DAG_NAME = "ZS-CTFO-DataEngineering-L2-PostDedupeProcess-Weekly-DEV"
PROCESS_ID = 2000
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
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
    dag=dag)

create_glue_tables = PythonOperator(
    task_id="create_glue_tables",
    python_callable=dagutils.update_catalog_data_dw,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY},
    dag=dag
)

#steps needs to be added. This will vary for each process
# #<---------------------->
AllInvUniqueIDGeneration = PythonOperator(
    task_id="AllInvUniqueIDGeneration",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "AllInvUniqueIDGeneration"},
    dag=dag)

AllInvPrecedence = PythonOperator(
    task_id="AllInvPrecedence",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "AllInvPrecedence"},
    dag=dag)

AllSiteUniqueIDGeneration = PythonOperator(
    task_id="AllSiteUniqueIDGeneration",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "AllSiteUniqueIDGeneration"},
    dag=dag)

AllSitePrecedence = PythonOperator(
    task_id="AllSitePrecedence",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "AllSitePrecedence"},
    dag=dag)

AllTrialUniqueIDGeneration = PythonOperator(
    task_id="AllTrialUniqueIDGeneration",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "AllTrialUniqueIDGeneration"},
    dag=dag)

AllTrialPrecedence = PythonOperator(
    task_id="AllTrialPrecedence",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "AllTrialPrecedence"},
    dag=dag)

Investigator = PythonOperator(
    task_id="Investigator",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Investigator"},
    dag=dag)

Site = PythonOperator(
    task_id="Site",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Site"},
    dag=dag)

Trial = PythonOperator(
    task_id="Trial",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Trial"},
    dag=dag)


Drug = PythonOperator(
    task_id="Drug",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Drug"},
    dag=dag)

Disease = PythonOperator(
    task_id="Disease",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Disease"},
    dag=dag)

Geo = PythonOperator(
    task_id="Geo",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Geo"},
    dag=dag)
	
PatientDetails = PythonOperator(
    task_id="PatientDetails",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "PatientDetails"},
    dag=dag)	

Sponsor = PythonOperator(
    task_id="Sponsor",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "Sponsor"},
    dag=dag)

SubsidiarySponsor = PythonOperator(
    task_id="SubsidiarySponsor",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "SubsidiarySponsor"},
    dag=dag)  

R_Trial_Site = PythonOperator(
    task_id="R_Trial_Site",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "TrialSite"},
    dag=dag)

R_Trial_Investigator = PythonOperator(
    task_id="R_Trial_Investigator",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "TrialInvestigator"},
    dag=dag)

R_Site_Investigator = PythonOperator(
    task_id="R_Site_Investigator",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "SiteInvestigator"},
    dag=dag)

R_Trial_Drug = PythonOperator(
    task_id="R_Trial_Drug",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "TrialDrug"},
    dag=dag)

R_Trial_Disease = PythonOperator(
    task_id="R_Trial_Disease",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "TrialDisease"},
    dag=dag)

R_Trial_Sponsor = PythonOperator(
    task_id="R_Trial_Sponsor",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "TrialSponsor"},
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

Drug.set_upstream(launch_ddl_creation)
Geo.set_upstream(launch_ddl_creation)
Sponsor.set_upstream(launch_ddl_creation)
PatientDetails.set_upstream(launch_ddl_creation)


AllInvUniqueIDGeneration.set_upstream(launch_ddl_creation)
AllSiteUniqueIDGeneration.set_upstream(launch_ddl_creation)
AllTrialUniqueIDGeneration.set_upstream(launch_ddl_creation)

Disease.set_upstream(AllTrialUniqueIDGeneration)

R_Trial_Site.set_upstream(AllSiteUniqueIDGeneration)
R_Trial_Site.set_upstream(AllTrialUniqueIDGeneration)

R_Trial_Investigator.set_upstream(AllTrialUniqueIDGeneration)
R_Trial_Investigator.set_upstream(AllInvUniqueIDGeneration)

R_Site_Investigator.set_upstream(AllSiteUniqueIDGeneration)
R_Site_Investigator.set_upstream(AllInvUniqueIDGeneration)

R_Trial_Drug.set_upstream(Drug)
R_Trial_Drug.set_upstream(AllTrialUniqueIDGeneration)

R_Trial_Disease.set_upstream(Disease)


SubsidiarySponsor.set_upstream(Sponsor)
R_Trial_Sponsor.set_upstream(SubsidiarySponsor)
R_Trial_Sponsor.set_upstream(AllTrialUniqueIDGeneration)


R_Trial_Disease.set_downstream(publish)
R_Trial_Drug.set_downstream(publish)
R_Trial_Site.set_downstream(publish)
R_Trial_Sponsor.set_downstream(publish)
R_Trial_Investigator.set_downstream(publish)
R_Site_Investigator.set_downstream(publish)
PatientDetails.set_downstream(publish)

AllInvPrecedence.set_upstream(AllInvUniqueIDGeneration)
AllSitePrecedence.set_upstream(AllSiteUniqueIDGeneration)
AllTrialPrecedence.set_upstream(AllTrialUniqueIDGeneration)
Investigator.set_upstream(AllInvPrecedence)
Site.set_upstream(AllSitePrecedence)
Site.set_upstream(Geo)
Investigator.set_upstream(Geo)
Trial.set_upstream(AllTrialPrecedence)

publish.set_upstream(Investigator)
publish.set_upstream(Site)
publish.set_upstream(Trial)

create_glue_tables.set_upstream(publish)
update_cycle_status.set_upstream(create_glue_tables)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)
