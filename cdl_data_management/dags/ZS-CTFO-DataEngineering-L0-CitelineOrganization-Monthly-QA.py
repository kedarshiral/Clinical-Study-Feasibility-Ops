#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import sys
import imp
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# ssh script path
sh_file_path = "/usr/local/airflow/dags/plugins/qa/ctfo/code/adaptor/common_utilities/"
sys.path.insert(1, sh_file_path)

import EmrManagerUtility
import AdaptorDagUtils
EmrManagerUtility = imp.reload(EmrManagerUtility)
dagutils = imp.reload(AdaptorDagUtils)


# Adapter Name and sh_file_path
adapter_name = "Organization"
adapter_name_org = "Organization"
adapter_name_orgtrial = "OrganizationTrials"
adapter_name_orgdataprep = "OrganizationDataPrep"

# Name of the Dag
DAG_NAME = "ZS-CTFO-DataEngineering-L0-CitelineOrganization-Monthly-QA"
STARTED = "STARTED"
SUCCEEDED = "SUCCEEDED"

# Bash Command
orgadapter_bash_command = "sh " + sh_file_path + "RunAdapter.sh " + adapter_name_org
trialorgadapter_bash_command = "sh " + sh_file_path + "RunAdapter.sh " + adapter_name_orgtrial
OrgDataPrepAdapter_bash_command = "sh " + sh_file_path + "RunAdapter.sh " + adapter_name_orgdataprep

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


start = DummyOperator(
    task_id="start",
    dag=dag)

end = DummyOperator(
    task_id="end",
    dag=dag)

DataExtraction = BashOperator(
    task_id=adapter_name_org + "DataExtraction",
    bash_command=orgadapter_bash_command,
    dag=dag)
OrganizationTrialDataExtraction = BashOperator(
    task_id=adapter_name_orgtrial + "DataExtraction",
    bash_command=trialorgadapter_bash_command,
    dag=dag)
OrganizationDataPrep = PythonOperator(
    task_id=adapter_name + "DataPrep",
    python_callable=AdaptorDagUtils.call_OrganizationDataPrep,
    dag=dag)

emr_terminate = PythonOperator(
    task_id="terminate_cluster",
    trigger_rule="all_done",
    python_callable=EmrManagerUtility.terminate_adapter, provide_context=True,
    op_kwargs={"source_name": adapter_name},
    dag=dag)

DataExtraction.set_upstream(start)
OrganizationTrialDataExtraction.set_upstream(DataExtraction)
OrganizationDataPrep.set_upstream(OrganizationTrialDataExtraction)
emr_terminate.set_upstream(OrganizationDataPrep)
end.set_upstream(emr_terminate)

