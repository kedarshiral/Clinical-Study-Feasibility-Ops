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
DAG_NAME = "ZS-CTFO-DataEngineering-L2-Fact-A1-RedshiftRptRefresh-QA"
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


# Create a dummy operator task
end = DummyOperator(
    task_id="end",
    dag=dag)

S3toRedshift = PythonOperator(
    task_id="S3toRedshift",
    python_callable=dagutils.call_redshift_load_utility,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "S3toRedshift",
               "conf_file_path": "redshiftUtilityInputConfig.json"},
    dag=dag)

Retention = PythonOperator(
    task_id="Retention",
    python_callable=dagutils.call_retention,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "Retention"},
    dag=dag)



# Set task ordering

S3toRedshift.set_upstream(start)
Retention.set_upstream(S3toRedshift)
end.set_upstream(Retention)
