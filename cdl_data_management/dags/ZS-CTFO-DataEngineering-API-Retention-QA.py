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
DAG_NAME = "ZS-CTFO-DataEngineering-API-Retention-QA.py"
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



Retention = PythonOperator(
    task_id="Retention",
    python_callable=dagutils.call_retention_api,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "Retention"},
    dag=dag)

inv_json_prior=PythonOperator(
    task_id="inv_json_prior",
    python_callable=dagutils.call_retention_api_inv_prior,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "inv_json_prior"},
    dag=dag)

inv_json_opti=PythonOperator(
    task_id="inv_json_opti",
    python_callable=dagutils.call_retention_api_inv_optimized,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "inv_json_opti"},
    dag=dag)


batch_job_retention = PythonOperator(
    task_id="batch_job_retention",
    python_callable=dagutils.call_retention_api_batch_job,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "batch_job_retention"},
    dag=dag)

retaining_TU = PythonOperator(
    task_id="retaining_TU",
    python_callable=dagutils.call_retention_api_batch_job,
    op_kwargs={"process_id": PROCESS_ID, "frequency": FREQUENCY, "step_name": "retaining_TU"},
    dag=dag)

Retention.set_upstream(start)
batch_job_retention.set_upstream(start)
end.set_upstream(batch_job_retention)
# retaining_TU.set_upstream(start)
# end.set_upstream(retaining_TU)
inv_json_prior.set_upstream(start)
end.set_upstream(Retention)
inv_json_opti.set_upstream(inv_json_prior)
end.set_upstream(inv_json_opti)
