
from MySQLConnectionManager import MySQLConnectionManager
from DagTriggerUtility import DagTriggerUtility
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from smart_open import smart_open
from LogSetup import logger
import CommonConstants
import subprocess
import DagUtils
import boto3
import json
import os


dag_utils = DagUtils
MODULE_NAME = "Automation_Wrapper"

execution_context = ExecutionContext()
execution_context.set_context({"module_name": MODULE_NAME})
env_configs = JsonConfigUtility(
    os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
s3_bucket = env_configs.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])


def insert_data_date(automated_dag_name, dag_id, process_type, config_file_name, **kwargs):
    try:
        trigger_dag = DagTriggerUtility()
        audit_db = env_configs.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        if process_type is None:
            logger.error("Process type cant be none", extra=execution_context.get_context())
            raise Exception
        logger.info("preparing Config file", extra=execution_context.get_context())
        config_json = prepare_config_file(config_file_name, automated_dag_name)
        if process_type.lower() == "dw":
            dw_config = config_json["DW"]
            logger.info("Starting DW process for " + dag_id, extra=execution_context.get_context())
            enable_flag = dw_config[dag_id]["enable_flag"]
            data_date = dw_config[dag_id]["data_date"]
            process_id = dw_config[dag_id]["process_id"]
            frequency = dw_config[dag_id]["frequency"]
            user_name = dw_config[dag_id]["user"]
            process_interruption_flag = dw_config[dag_id]["process_interruption_flag"]
            logger.info("running DW process from " + dag_id + " with data date" + data_date,
                        extra=execution_context.get_context())
            if enable_flag.lower() == "y":
                if data_date is not None:
                    query = "Insert into {audit_db}.{ctl_process_date_mapping} (process_id, frequency, data_date, " \
                            "process_interruption_flag, insert_by, insert_date ) values ({process_id},'{frequency}'," \
                            "'{data_date}','{process_interruption_flag}','{user}',now());".format(
                             audit_db=audit_db,
                             frequency=frequency,
                             data_date=data_date,
                             process_id=process_id,
                             user=user_name,
                             ctl_process_date_mapping=CommonConstants.PROCESS_DATE_TABLE,
                             process_interruption_flag=process_interruption_flag)
                    logger.debug(query, extra=execution_context.get_context())
                    logger.info("inserting data date :" + data_date, extra=execution_context.get_context())
                    MySQLConnectionManager().execute_query_mysql(query, False)
                    try:
                        logger.info("Triggering DW process for: " + dag_id, extra=execution_context.get_context())
                        trigger_dag.execute_dag(dag_id=dag_id, airflow_db_config=env_configs.get_configuration(
                            [CommonConstants.AIRFLOW_CONFIG]), sync=True)
                    except:
                        logger.error(dag_id + " Failed", extra=execution_context.get_context())
                        raise
                else:
                    logger.error("Data data is not updated in config file", extra=execution_context.get_context())
                    raise Exception
        elif process_type.lower() == "ingestion":
            logger.info("Starting Ingestion process for " + dag_id)
            ing_config = config_json["Ingestion"]
            enable_flag = ing_config[dag_id]["enable_flag"]
            if enable_flag.lower() == "y":
                try:
                    logger.info("triggering the ingestion dag " + dag_id, extra=execution_context.get_context())
                    trigger_dag.execute_dag(dag_id=dag_id, airflow_db_config=env_configs.get_configuration(
                        [CommonConstants.AIRFLOW_CONFIG]), sync=True)
                except:
                    raise Exception
            else:
                logger.debug(str(dag_id) + "dag is disabled")
        elif process_type.lower() == "adaptor":
            logger.info("Starting Ingestion process for " + dag_id)
            adaptor_config = config_json["Adaptor"]
            enable_flag = adaptor_config[dag_id]["enable_flag"]
            if enable_flag.lower() == "y":
                try:
                    logger.info("triggering the adaptor dag " + dag_id, extra=execution_context.get_context())
                    trigger_dag.execute_dag(dag_id=dag_id, airflow_db_config=env_configs.get_configuration(
                        [CommonConstants.AIRFLOW_CONFIG]), sync=True)
                except:
                    raise Exception
            else:
                logger.debug(str(dag_id) + "dag is disabled")

    except Exception as e:
        logger.error("Failed to trigger dag: " + dag_id, extra=execution_context.get_context())
        logger.error(e, extra=execution_context.get_context())
        #dag_utils.trigger_notification(automated_dag_name=automated_dag_name, email_type="central_dag",
         #                              dag_status=CommonConstants.STATUS_FAILED)
        raise


def prepare_config_file(config_file_name, automated_dag_name):
    try:
        template_file_name = CommonConstants.AIRFLOW_CODE_PATH + "/DataEngineering-L1-L2-A1-Super-Dag-Weekly-Full.template"
        logger.info(automated_dag_name)
        with open(template_file_name, 'r') as f:
                template_file_json = f.read()
        logger.info( config_file_name)
        logger.info(template_file_json)
        with smart_open(config_file_name,
                        'r') as data_date_string:
            data_date_json = json.load(data_date_string)
            for key, val in data_date_json.items():
                if key in str(template_file_json):
                    template_file_json = template_file_json.replace(str(key), str(val))

                else:
                    raise Exception
            logger.info("configuration created", extra=execution_context.get_context())
        return json.loads(template_file_json)

    except:
        raise


def remove_configfile(config_file_name, automated_dag_name, **kwargs):
    try:
        logger.info("removing the config file from EC2 machine", extra=execution_context.get_context())
        #with smart_open(CommonConstants.S3_PREFIX + s3_bucket + os.path.join(CommonConstants.S3_PATH, config_file_name),
        #                'r') as data_date_string:
        #    data_date_json = json.load(data_date_string)
        #    for key, val in data_date_json.items():
        #        data_date_json[key] = None
        #with smart_open(CommonConstants.S3_PREFIX + s3_bucket + os.path.join(CommonConstants.S3_PATH,
        #                                                                     config_file_name), "wb") as f:
        #    f.write(json.dumps(data_date_json).encode('utf-8'))
        #logger.info("updated config Jason in S3", extra=execution_context.get_context())
        #dag_utils.trigger_notification(automated_dag_name=automated_dag_name,
         #                              email_type=CommonConstants.CENTRAL_DAG_EMAIL_TYPE,
          #                             dag_status=CommonConstants.STATUS_SUCCEEDED)
    except:
        raise

