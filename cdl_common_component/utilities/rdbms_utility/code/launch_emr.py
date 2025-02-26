

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

# Importing required python packages.
import ast
import json
import logging
import os
from os import path
import sys
import traceback
import boto3
import time
import socket
import copy


start = time.time()


#Path of the json files should be provided here
CLUSTER_CONFIG = " " #Path of the rdbms_cluster_variables_oracle.json file
CONFIGURATION_FILE = " " #Path of the rdbms_application_conf.json file
with open(CONFIGURATION_FILE, "r") as file:
    app_config = file.read()


# Importing Files
import CommonConstants
from SystemManagerUtility import SystemManagerUtility
from LogSetup import logger as logging
from EmrManagerUtility import EmrManagerUtility
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
from NotificationUtility import NotificationUtility

s3_client = boto3.client("s3")
cluster_id = ""
cluster_launch_flag = "N"
execution_context = ExecutionContext()

configuration_rdbms = JsonConfigUtility(
os.path.join(CommonConstants.AIRFLOW_CODE_PATH,CommonConstants.ENVIRONMENT_CONFIG_FILE))
rdbms_connection_details=configuration_rdbms.get_configuration(["rdbms_configs"])


for key in rdbms_connection_details:
    app_config = app_config.replace(key + "$$", rdbms_connection_details[key])
app_config = json.loads(app_config)
configuration=app_config



d = dict()
status_code = 0
step_name = ""


def generate_extractor_status_start():
    try:
        """This function generates batch status html"""
        logging.info("Starting function to generate Extractor status HTML Table")
        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))
        audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        # Might need to parameterize the ctl extractor detail table
        query = "select distinct(database_name) as database_name,count(*) as No_of_object  from " + audit_db + \
                ".ctl_db_extractor_dtl where active_flag='Y' group by database_name"
        logging.info("Query to retrieve the names of configured : " + query)
        dtls_log = MySQLConnectionManager().execute_query_mysql(query, False)
        html_table_header = '"<table class="tg" align="center"><tr><th class="tg-yw4l">Database name' \
                            '</th><th class="tg-yw4l">No of object(s)</th></tr>'
        html_table_end = "</table>"
        final_row = ""
        logging.info("Parsing each record from the status")
        for var in dtls_log:
            row = dict(var)
            row_str = ('<tr><td class="tg-yw4l">' + str(row["database_name"]
                                                    ) + '</td>').lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["No_of_object"]
                                                    ) + '</td>').lstrip('"').rstrip('"')
            row_str = (row_str + "</tr>").lstrip('"').rstrip('"')
            final_row = final_row + row_str

        html_table = (html_table_header + final_row + html_table_end).lstrip('"').rstrip('"')
        logging.info("HTML Spec generated for the Extractor load details" + str(html_table))
        return html_table
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        logging.error("Failed while triggering Notification Utility. Error: " + e)
        raise e


def generate_extractor_status_end():
    try:
        """This function generates status html"""
        logging.info("Starting function to generate Extractor status HTML Table")
        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))
        audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        # Might need to parameterize the log extractor detail table
        query_to_fetch_cycle_id = "select max(cycle_id) as cycle_id  from " + audit_db + \
                                  ".log_db_extractor_dtl"

        output_cid = MySQLConnectionManager().execute_query_mysql(query_to_fetch_cycle_id, False)
        cycle_id = output_cid[0]['cycle_id']
        # Might need to parameterize the log extractor detail table
        query = "select database_name,status,count(*) as count from " + audit_db + \
                ".log_db_extractor_dtl where cycle_id='" + cycle_id + "' group by database_name,status"
        logging.info("Query to retrieve the names of status and count " + query)
        dtls_log = MySQLConnectionManager().execute_query_mysql(query, False)
        html_table_header = '"<table class="tg" align="center"><tr><th class="tg-yw4l">Database name' \
                            '</th><th class="tg-yw4l">Status of Data Extraction</th>' \
                            '</th><th class="tg-yw4l">Count</th></tr>'
        html_table_end = "</table>"
        final_row = ""
        logging.info("Parsing each record from the status")
        for var in dtls_log:
            row = dict(var)
            row_str = ('<tr><td class="tg-yw4l">' + str(row["database_name"]
                                                    ) + '</td>').lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["status"]
                                                    ) + '</td>').lstrip('"').rstrip('"')+ \
                      ('<td class="tg-yw4l">' + str(row["count"]
                                                    ) + '</td>').lstrip('"').rstrip('"')
            row_str = (row_str + "</tr>").lstrip('"').rstrip('"')
            final_row = final_row + row_str

        html_table = (html_table_header + final_row + html_table_end).lstrip('"').rstrip('"')
        logging.info("HTML Spec generated for the Extractor load details" + str(html_table))
        return html_table
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        logging.error("Failed while triggering Notification Utility. Error: " + e)
        raise e


def fetch_extractor_email_common_info(dag_name, execution_point):
    try:
        """This method fetches ingestion email common information"""
        logging.info("Starting function to retrieve Common Ingestion email information for" +
                     " and DAG Name: " + str(dag_name))
        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))
        common_metadata = {"dag_name": str(dag_name)}

        # Retrieving the Airflow Link
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)

        airflow_url = "http://" + ip_address + ":8080/admin/airflow/graph?root=&dag_id=" + str(dag_name)

        logging.info("Airflow URL Generated:" + str(airflow_url))
        common_metadata["airflow_link"] = str(airflow_url)
        # Retrieving Environment

        env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])
        logging.info("Environment retrieved from Configurations is: " + str(env))
        common_metadata["env"] = str(env)


        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))
        audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        logging.info("Final Batch Ingestion common information extracted for the email: " + str(
            common_metadata))
        return common_metadata
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        logging.error("Failed while triggering Notification Utility. Error: " + e)
        raise e


def trigger_notification_utility(email_type,execution_point,dag_name, **kwargs):
    """This method triggers notification utility"""
    try:
        replace_variables = {}
        logging.info("Staring function to trigger Notification Utility")

        logging.info("Preparing the common information to be sent in all emails")
        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))

        email_types = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                       "email_type_configurations"])
        if email_types:
            email_types = list(email_types.keys())
            if email_type in email_types:
                logging.info("Email type: " + str(email_type) + " is configured")
            else:
                logging.info("Email type: " + str(email_type) + " is not configured, cannot proceed")
                raise Exception("Email type: " + str(email_type) + " is not configured,"
                                                                   " cannot proceed")
        else:
            raise Exception("No email types configured, cannot proceed")

        notification_flag = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "email_type_configurations",
                                                             str(email_type),
                                                             "notification_flag"])

        logging.info("Notification Flag: " + str(notification_flag))

        if notification_flag.lower() != 'y':
            logging.info("Notification is disabled hence skipping notification"
                         " trigger and exiting function")

        else:
            logging.info("Notification is enabled hence starting notification trigger")

            # Common configurations

            email_ses_region = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "ses_region"])
            logging.info("SES Region retrieved from Configurations is: " + str(email_ses_region))
            email_template_path = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_template_path"])
            logging.info("Email template path retrieved from Configurations is: " + str(
                email_template_path))

            # Email type specific configurations

            email_sender = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                            "email_type_configurations",
                                                            str(email_type),
                                                            "ses_sender"])
            logging.info("Email Sender retrieved from Configurations is: " + str(email_sender))

            email_recipient_list = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                 str(email_type), "ses_recipient_list"])
            logging.info("Email Recipient retrieved from Configurations is: " + str(
                email_recipient_list))

            email_subject = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "email_type_configurations",
                                                             str(email_type),
                                                             "subject"])
            logging.info("Email Subject retrieved from Configurations is: " + str(email_subject))

            email_template_name = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                 str(email_type), "template_name"])
            logging.info("Email Template Name retrieved from Configurations is: "
                         + str(email_template_name))

            email_template_path = (email_template_path + "/" + email_template_name
                                   ).replace("//", "/")
            logging.info("Final Email template path is: " + str(email_template_path))
            if email_type == "extractor_status":
                common_info = fetch_extractor_email_common_info(dag_name, "")
                replace_variables = copy.deepcopy(common_info)
                logging.info("Common email information for extrctor status in starting" + str(replace_variables))
            if execution_point.lower() == 'end':
                replace_variables["status"] = "Completed"
                html_table = generate_extractor_status_end()
            else:
                replace_variables["status"] = "Started"
                html_table = generate_extractor_status_start()
            replace_variables["detailed_status"] = html_table
            logging.info("Variables prepared: " + str(replace_variables))
            logging.info("Final list of replacement variables for Email: " + str(replace_variables))

            # Preparing email details
            notif_obj = NotificationUtility()
            output = notif_obj.send_notification(email_ses_region, email_subject, email_sender,
                                                 email_recipient_list,
                                                 email_template=email_template_path,
                                                 replace_param_dict=replace_variables)
            logging.info("Output of Email Notification Trigger: " + str(output))

    except Exception as e:
        logging.error(str(traceback.format_exc()))
        logging.error("Failed while triggering Notification Utility. Error: " + e)
        raise e

def call_terminate_cluster(dag_name=None, **kwargs):
    """
     Purpose      :   This module performs below operation:
                      a. Terminate the Launched cluster.
     """
    try:
        logging.info("Terminate_cluster method called")
        ti = kwargs["ti"]
        cluster_id = ti.xcom_pull(key='cluster_id', task_ids="database_extractor", dag_id=dag_name)
        region_name = ti.xcom_pull(key='region_name', task_ids="database_extractor", dag_id=dag_name)
        status = ti.xcom_pull(key='status', task_ids="database_extractor", dag_id=dag_name)
        execution_status="send_notification"
        if status.lower() == "success":
            if CommonConstants.TERMINATION_ON_SUCCESS_FLAG.upper() == "Y":
                EmrManagerUtility().terminate_cluster(cluster_id, region_name)
            else:
                logging.info("Not terminated the cluster due TERMINATION_ON_SUCCESS_FLAG is N")
        else:
            if CommonConstants.TERMINATION_ON_FAILURE_FLAG.upper() == "Y":
                EmrManagerUtility().terminate_cluster(cluster_id, region_name)
            else:
                logging.info("Not terminated the cluster due TERMINATION_ON_FAILURE_FLAG is N")

        logging.info("Cluster terminated")
    except Exception:
        raise Exception(str(traceback.format_exc()))



def call_launch_cluster( data_source_name, **kwargs):
    """
      Purpose      :   This module performs below operation:
                       a. Launch the cluster.
                       b. Run the Bootstrap script.
      """
    try:
        cluster_type = configuration['generic_config']["cluster_type"]
        file_read = open(CLUSTER_CONFIG, "r")
        json_data = json.load(file_read)
        file_read.close()

    except Exception:
        raise Exception("File not found error")

    try:
        logging.info("launch_cluster method called for dag")

        emr_payload = json_data["cluster_configurations"][cluster_type]
        region_name = emr_payload["Region"]

        returned_json = EmrManagerUtility().launch_emr_cluster(
            emr_payload, region_name, data_source_name)

        logging.debug("Json for EMR --> " + str(returned_json))
        cluster_id = returned_json[CommonConstants.RESULT_KEY]
        logging.debug("cluster_id --> " + str(cluster_id))
        logging.info("Cluster launched")
        kwargs["ti"].xcom_push(key='cluster_id', value=str(cluster_id))
        kwargs["ti"].xcom_push(key='datasource_name', value=str(data_source_name))

    except Exception as e:
        error_message = str(traceback.format_exc())
        logging.error("Cluster is not launched " + error_message)
        raise e


def execute_extractor_utility(dag_name=None, **kwargs):
    """
    SSh the rdbms extractor
    :return:
    """
    # Reading values from application json
    try:
        ti = kwargs["ti"]
        cluster_id = ti.xcom_pull(key='cluster_id', task_ids="launch_cluster", dag_id=dag_name)
        datasource_name = ti.xcom_pull(key='datasource_name', task_ids="launch_cluster", dag_id=dag_name)
        emr_region_name = configuration["generic_config"]["aws_region"]
        emr_keypair_path =configuration["generic_config"]["private_key_loaction"]
        if datasource_name == "" or datasource_name is None or datasource_name == " ":
            command = "/usr/lib/spark/bin/spark-submit --jars" + " " + CommonConstants.AIRFLOW_CODE_PATH + "/ojdbc7.jar " + " " + CommonConstants.AIRFLOW_CODE_PATH + "/rdbms_extractor.py"
        else:
            command = "/usr/lib/spark/bin/spark-submit --jars" + " " + CommonConstants.AIRFLOW_CODE_PATH + "/ojdbc7.jar  --executor-cores 3 --driver-cores 3  --conf spark.port.maxRetries=200 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=10000000" + " " + CommonConstants.AIRFLOW_CODE_PATH + "/rdbms_extractor.py -sc" + " " + str(datasource_name)

        logging.info("Executing command using sshEMRUtility")
        output_status = SystemManagerUtility().execute_command(cluster_id,command)
        status_message = "Final output: " + str(output_status)
        logging.debug(str(status_message))
        status=output_status["status"]
        logging.info("Completed main function")
        kwargs["ti"].xcom_push(key='cluster_id', value=str(cluster_id))
        kwargs["ti"].xcom_push(key='region_name', value=str(emr_region_name))
        kwargs["ti"].xcom_push(key='status', value=str(status))
    except Exception as e:
        error_message = str(traceback.format_exc())
        logging.error("Error in data extraction " + error_message)
        raise e

