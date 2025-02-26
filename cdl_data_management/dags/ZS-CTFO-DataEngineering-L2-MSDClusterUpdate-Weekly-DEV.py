#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

sys.path.insert(1, "/usr/local/airflow/dags/plugins/dev/ctfo/code")
from datetime import datetime
import DagUtils
from imp import reload
import EmrManagerUtility
import imp
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from airflow.utils.dates import days_ago


EmrManagerUtility = imp.reload(EmrManagerUtility)
dagutils = reload(DagUtils)

# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# Set default dag properties
default_args = {
    "owner": "ZS Associates",
    "start_date": datetime.today().strftime('%Y-%m-%d'),
    "provide_context": True
}

DAG_NAME = "ZS-CTFO-DataEngineering-L2-MSDClusterUpdate"
PROCESS_ID = 6000
FREQUENCY = "monthly"

# Define the dag object
dag = DAG(
    DAG_NAME,
    default_args=default_args,
    #start_date = days_ago(7),
    #schedule_interval='30  06 * * *',
    catchup=False
)


start = PythonOperator(
    task_id="start",
    python_callable=dagutils.send_dw_email, provide_context=True,
    op_kwargs={"email_type": "cycle_status", "process_id": PROCESS_ID, "dag_name": DAG_NAME},
    dag=dag)


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
    python_callable=dagutils.launch_emr, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID},
    dag=dag)

emr_terminate = PythonOperator(
    task_id="terminate_cluster",
    trigger_rule="all_done",
    python_callable=dagutils.terminate_emr, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID},
    dag=dag)

site_cluster_update = PythonOperator(
    task_id="site_cluster_update",
    python_callable=dagutils.call_job_executor,
    op_kwargs={ "process_id": PROCESS_ID,"frequency":FREQUENCY,  "step_name": "site_cluster_update"},
    dag=dag)

trial_cluster_update = PythonOperator(
    task_id="trial_cluster_update",
    python_callable=dagutils.call_job_executor,
    op_kwargs={ "process_id": PROCESS_ID,"frequency":FREQUENCY, "step_name": "trial_cluster_update"},
    dag=dag)

investigator_cluster_update = PythonOperator(
    task_id="investigator_cluster_update",
    python_callable=dagutils.call_job_executor,
    op_kwargs={ "process_id": PROCESS_ID,"frequency":FREQUENCY,  "step_name": "investigator_cluster_update"},
    dag=dag)

MSD_email_notification = PythonOperator(
    task_id="MSD_email_notification",
    python_callable=dagutils.call_job_executor,
    op_kwargs={"file_master_id": 6999, "process_id": PROCESS_ID,"frequency":FREQUENCY,  "step_name": "MSD_email_notification"},
    dag=dag)
    
update_cycle_status = PythonOperator(
    task_id="update_cycle_status",
    python_callable=dagutils.update_cycle_status_and_cleaup, provide_context=True,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY},
    dag=dag)

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
s3_buckname = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])
s3_locat = 'clinical-data-lake/applications/commons/temp/km_validation/MSD_cluster_update/identified_records/identified_records.csv'
'''  
s3_sensor = S3KeySensor(
    task_id='s3_file_check',
    poke_interval=300,
    timeout=43200,
    retries=4,
    retry_delay=43200,
    bucket_key=s3_locat,
    bucket_name=s3_buckname,
    aws_conn_id='aws_default',
    dag=dag)

#s3_sensor &gt;&gt; start 
s3_sensor >> start
'''    
process_dependency_list.set_upstream(start)

emr_launch.set_upstream(process_dependency_list)

site_cluster_update.set_upstream(emr_launch)
trial_cluster_update.set_upstream(emr_launch)
investigator_cluster_update.set_upstream(emr_launch)

MSD_email_notification.set_upstream(site_cluster_update)
MSD_email_notification.set_upstream(trial_cluster_update)
MSD_email_notification.set_upstream(investigator_cluster_update)
update_cycle_status.set_upstream(MSD_email_notification)
emr_terminate.set_upstream(update_cycle_status)
end.set_upstream(emr_terminate)