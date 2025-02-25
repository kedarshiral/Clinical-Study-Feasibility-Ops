"""This module fetches batch status."""
# !/usr/bin/python3
# -*- coding: utf-8 -*-
import socket
import json
import copy
from EmrClusterLaunchWrapper import EmrClusterLaunchWrapper
from SystemManagerUtility import SystemManagerUtility
from CustomS3Utility import CustomS3Utility
from ImportExportUtility import ImportExportUtility
from MySQLConnectionManager import MySQLConnectionManager
from Get_EMR_Host_IP import Get_EMR_Host_IP
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from CommonUtils import CommonUtils
from PopulateDependency import PopulateDependency
import logging
import traceback
import subprocess
from NotificationUtility import NotificationUtility
import random
import os
import sys
import datetime as dt
import time
from urllib.parse import urlparse
import boto3
import pandas
import pyarrow.parquet as pq
import ast
import base64
# from ExecuteSSHCommand import ExecuteSSHCommand
from DatabaseUtility import DatabaseUtility
import RedShiftUtilityConstants
from RedshiftUtility import RedShiftUtility

sys.path.insert(0, os.getcwd())

logger = logging.getLogger('__name__')
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


def fetch_batch_status(batch_id, dataset_ids, process_id, cluster_id):
    """This method fetched batch status"""
    logger.info("Starting function to retrieve the batch status")
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "select process_id, batch_id, dataset_id, batch_status," \
            " cast(batch_start_time as CHAR) as batch_start_time," \
            " cast(batch_end_time as CHAR) as batch_end_time from " + audit_db + \
            "." + CommonConstants.BATCH_TABLE + " where dataset_id in (" + str(
        dataset_ids) + ") and batch_id in (" + str(batch_id) + \
            ") and process_id = " + str(process_id) + " and cluster_id = '" + str(cluster_id) + \
            "' order by batch_start_time"
    logger.info("Query to retrieve the batch status: " + query)
    query_output = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Output" + str(query_output))
    if "FAIL" in str(query_output).upper():
        final_batch_status = CommonConstants.STATUS_FAILED.upper()
    elif CommonConstants.IN_PROGRESS_DESC.upper() in str(query_output).upper():
        final_batch_status = CommonConstants.IN_PROGRESS_DESC.upper()
    elif CommonConstants.STATUS_RUNNING.upper() in str(query_output).upper():
        final_batch_status = CommonConstants.IN_PROGRESS_DESC.upper()
    else:
        final_batch_status = CommonConstants.STATUS_SUCCEEDED
    return {"final_batch_status": final_batch_status, "batch_dtls": query_output}


def fetch_batch_id(cluster_id, process_id):
    """This method fetches batch id"""
    logger.info("Starting function to retrieve the Batch ID of current execution")
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "select group_concat(batch_id separator ',') as batch_id from " + audit_db + \
            "." + CommonConstants.BATCH_TABLE + " where process_id = " + str(
        process_id) + " and cluster_id = '" + str(cluster_id) + "'"
    logger.info("Query to retrieve the batch ID: " + query)
    query_output = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Output : " + str(query_output))
    return query_output[0]["batch_id"]


def generate_batch_status_html(batch_status):
    """This function generates batch status html"""
    logger.info("Starting function to generate batch status HTML Table")
    logger.info("Batch Status recieved: " + str(batch_status))
    dtls_log = batch_status["batch_dtls"]

    html_table_header = '"<table class="tg" align="center"><tr><th class="tg-yw4l">process_id' \
                        '</th><th class="tg-yw4l">batch_id</th><th class="tg-yw4l">' \
                        'dataset_id</th><th class="tg-yw4l">batch_status</th>' \
                        '<th class="tg-yw4l">batch_start_time</th>' \
                        '<th class="tg-yw4l">batch_end_time</th></tr>'
    html_table_end = "</table>"
    final_row = ""
    logger.info("Parsing each record from the status")
    for var in dtls_log:
        row = dict(var)
        row_str = ('<td class="tg-yw4l">' + str(row["process_id"]
                                                ) + '</td>').lstrip('"').rstrip('"') + \
                  ('<td class="tg-yw4l">' + str(row["batch_id"]
                                                ) + '</td>').lstrip('"').rstrip('"') + \
                  ('<td class="tg-yw4l">' + str(row["dataset_id"]
                                                ) + '</td>').lstrip('"').rstrip('"') + \
                  ('<td class="tg-yw4l">' + str(row["batch_status"]
                                                ) + '</td>').lstrip('"').rstrip('"') + \
                  ('<td class="tg-yw4l">' + str(row["batch_start_time"]
                                                ) + '</td>').lstrip('"').rstrip('"') + \
                  ('<td class="tg-yw4l">' + str(row["batch_end_time"]
                                                ) + '</td>').lstrip('"').rstrip('"')
        row_str = (row_str + "</tr>").lstrip('"').rstrip('"')
        final_row = final_row + row_str

    html_table = (html_table_header + final_row + html_table_end).lstrip('"').rstrip('"')
    logger.info("HTML Spec generated for the Batch load details" + str(html_table))
    return html_table


def fetch_dqm_status(batch_id, dataset_ids):
    """This method fetches dqm status"""
    logger.info("Starting function to retrieve the DQM Summary status")
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "select application_id, dataset_id, batch_id, file_id, column_name," \
            " qc_id,  error_count, " \
            "qc_status, qc_message from " + audit_db + ".log_dqm_smry where dataset_id in (" + \
            str(dataset_ids) + ") and batch_id in (" + str(batch_id) + ")"
    logger.info("Query to retrieve the DQM status: " + query)
    query_output = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Output" + str(query_output))
    if query_output:
        return {"dqm_dtls": query_output}
    else:
        return {"dqm_dtls": ""}


def generate_dqm_status_html(dqm_status):
    """This method generates dqm status html"""
    logger.info("DQM Status retrieved: " + str(dqm_status))
    dqm_log = dqm_status["dqm_dtls"]
    if dqm_log:
        html_table_header = '"<table class="tg" align="center"><tr>' \
                            '<th class="tg-yw4l">application_id</th>' \
                            '<th class="tg-yw4l">dataset_id</th>' \
                            '<th class="tg-yw4l">batch_id</th>' \
                            '<th class="tg-yw4l">file_id</th>' \
                            '<th class="tg-yw4l">column_name</th>' \
                            '<th class="tg-yw4l">error_count</th>' \
                            '<th class="tg-yw4l">qc_status</th>' \
                            '<th class="tg-yw4l">qc_message</th>' \
                            '</tr>'
        html_table_end = "</table>"
        final_row = ""
        for var in dqm_log:
            row = dict(var)
            row_str = ('<td class="tg-yw4l">' + str(row["application_id"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["dataset_id"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["batch_id"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["file_id"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["column_name"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["error_count"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["qc_status"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["qc_message"]) + '</td>'
                       ).lstrip('"').rstrip('"')
            row_str = (row_str + "</tr>").lstrip('"').rstrip('"')
            final_row = final_row + row_str
        html_table = (html_table_header + final_row + html_table_end).lstrip('"').rstrip('"')
    else:
        html_table = ""
    logger.info("HTML Spec generated for the DQM details" + str(html_table))
    return html_table


def get_dataset_ids(process_id):
    """This method get dataset ids"""
    """
    :param process_id: Process ID as configured in the table
    :return: data set id's list
    """
    logger.info("Starting function to get the Dataset IDs based on process ID recieved: " +
                str(process_id))
    configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                   CommonConstants.ENVIRONMENT_CONFIG_FILE))

    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    dataset_id_list = []
    try:

        query = "Select distinct(" + CommonConstants.MYSQL_DATASET_ID + ") from " + audit_db + "." \
                + CommonConstants.EMR_PROCESS_WORKFLOW_MAP_TABLE + " where " + \
                CommonConstants.EMR_PROCESS_WORKFLOW_COLUMN + "='" + str(
            process_id) + "' and " + CommonConstants.EMR_PROCESS_WORKFLOW_ACTIVE_COLUMN + \
                "='" + str(
            CommonConstants.WORKFLOW_DATASET_ACTIVE_VALUE) + "'"
        logger.info("Query to retrieve the dataset ID: " + str(query))
        dataset_ids = MySQLConnectionManager().execute_query_mysql(query, False)
        logger.info("Output of query: " + str(dataset_ids))
        for id in dataset_ids:
            dataset_id_list.append(str(int(id[CommonConstants.MYSQL_DATASET_ID])))
        logger.info("List of Dataset IDs  extracted: " + str(dataset_id_list))
        return dataset_id_list
    except Exception as exception:
        logger.error(str(traceback.format_exc()))
        logger.error(str(exception))
        raise exception


def fetch_ingestion_email_dqm_info(process_id, dataset_id):
    """This method fetches ingestion email dqm information"""
    logger.info("Starting function to retrieve DQM email information for Process ID: " + str(
        process_id) +
                " and dataset ID: " + str(dataset_id))
    replace_dict = {}

    logger.info("Retrieving Cluster ID for the Process ID: " + str(process_id))

    cluster_id = get_cluster_id(process_id)
    # Fetching Batch status since Batch ID is assigned
    logger.info("Retrieving Batch ID based on Cluster ID - " + str(cluster_id
                                                                   ) + " configured Process ID: " +
                str(process_id) + " and configured Dataset ID - " + str(dataset_id))
    batch_id = fetch_batch_id(cluster_id, process_id)

    if batch_id:
        logger.info("Batch ID present hence retrieving detailed status")
        batch_id_str = batch_id.replace(",", ",\n")
        replace_dict["batch_id"] = batch_id_str
        logger.info("Batch ID: " + str(batch_id_str))

        batch_status = fetch_batch_status(batch_id, dataset_id, process_id, cluster_id)
        dqm_status = fetch_dqm_status(batch_id, dataset_id)
        html_spec = generate_dqm_status_html(dqm_status)

        replace_dict["status"] = batch_status["final_batch_status"]
        replace_dict["detailed_status"] = html_spec

    else:
        logger.info("Batch ID is Not Assigned however the Cluster launch is successful. "
                    "Hence marking status of run as failed as process failed at Prelanding step")
        replace_dict["batch_id"] = "Not Assigned"
        replace_dict["status"] = CommonConstants.STATUS_FAILED
        replace_dict["detailed_status"] = ""

    logger.info("Final DQM details extracted for the email: " + str(replace_dict))
    return replace_dict


def fetch_ingestion_email_batch_info(process_id, dataset_id):
    """This method fetches ingestion email batch information"""
    logger.info("Starting function to retrieve Batch Ingestion email information for Process"
                " ID: " + str(process_id) +
                " and dataset ID: " + str(dataset_id))
    replace_dict = {}

    logger.info("Retrieving Cluster ID for the Process ID: " + str(process_id))
    cluster_id = get_cluster_id(process_id)
    # Fetching Batch status since Batch ID is assigned
    logger.info("Retrieving Batch ID based on Cluster ID - " + str(cluster_id) + " configured "
                                                                                 "Process ID: " +
                str(process_id) + " and configured Dataset ID - " + str(dataset_id))
    batch_id = fetch_batch_id(cluster_id, process_id)

    if batch_id:
        batch_id_str = batch_id.replace(",", ",\n")
        replace_dict["batch_id"] = batch_id_str
        logger.info("Batch ID: " + str(batch_id_str))
        batch_status = fetch_batch_status(batch_id, dataset_id, process_id, cluster_id)
        html_spec = generate_batch_status_html(batch_status)
        replace_dict["status"] = batch_status["final_batch_status"]
        replace_dict["detailed_status"] = html_spec

    else:
        logger.info("Batch ID is Not Assigned however the Cluster launch is successful. "
                    "Hence marking status of run as failed as process failed at Prelanding step")
        replace_dict["batch_id"] = "Not Assigned"
        replace_dict["status"] = CommonConstants.STATUS_FAILED
        replace_dict["detailed_status"] = ""
    logger.info("Final Batch Ingestion details extracted for the email: " + str(replace_dict))
    return replace_dict


def fetch_ingestion_email_common_info(process_id, dag_name):
    """This method fetches ingestion email common information"""
    logger.info("Starting function to retrieve Common Ingestion email information for"
                " Process ID: " + str(process_id) +
                " and DAG Name: " + str(dag_name))
    configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                   CommonConstants.ENVIRONMENT_CONFIG_FILE))
    common_metadata = {"process_id": str(process_id), "dag_name": str(dag_name)}
    # ***** Mandatory Information Reported in every email *****
    # 1. Process ID
    # 2. Dataset Names
    # 3. DAG Name
    # 4. Batch ID
    # 5. Status

    # Retrieving the Airflow Link
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)

    airflow_url = "http://" + ip_address + ":8080/admin/airflow/graph?root=&dag_id=" + str(dag_name)

    logger.info("Airflow URL Generated:" + str(airflow_url))
    common_metadata["airflow_link"] = str(airflow_url)
    # Retrieving Environment

    env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])
    logger.info("Environment retrieved from Configurations is: " + str(env))
    common_metadata["env"] = str(env)

    # Fetching the dataset names from the Control tables

    dataset_id_list = get_dataset_ids(process_id)
    final_dataset_id = ",".join(map(str, dataset_id_list))

    logger.info("List of Dataset IDs to report status in this email: " + str(final_dataset_id))

    configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                   CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "select group_concat(dataset_name separator ',') as dataset_names from " + audit_db + \
            ".ctl_dataset_master where dataset_id in (" + final_dataset_id + ")"
    logger.info("Query to retrieve the names of configured Dataset IDs: " + query)
    query_output = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Output" + str(query_output))
    dataset_names = query_output[0]['dataset_names']
    common_metadata["dataset_name"] = str(dataset_names)
    common_metadata["dataset_id"] = str(final_dataset_id)
    logger.info("Final Batch Ingestion common information extracted for the email: " + str(
        common_metadata))
    return common_metadata


def get_cycle_step_status(cycle_id):
    """This method gets cycle step status"""
    logger.info("Starting function to retrieve the batch status")
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "select process_id, frequency, cycle_id, step_name, step_status, cast(" \
            "step_start_time as CHAR)" \
            " as step_start_time, cast(step_end_time as CHAR) as step_end_time from " + audit_db + \
            "." + CommonConstants.LOG_STEP_DTL + " where cycle_id = '" + str(cycle_id) + \
            "' order by step_start_time"
    logger.info("Query to retrieve the cycle steps status: " + query)
    query_output = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Output" + str(query_output))

    query = "select cycle_status from " + audit_db + \
            "." + CommonConstants.LOG_CYCLE_DTL + " where cycle_id = " + str(cycle_id)
    logger.info("Query to retrieve the cycle status: " + query)
    cycle_query_output = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Output" + str(cycle_query_output))
    final_cycle_status = cycle_query_output[0]["cycle_status"]
    return {"status": final_cycle_status, "cycle_dtls": query_output}


def generate_cycle_status_html(cycle_status):
    """This method generates cycle status html"""
    logger.info("Starting function to generate cycle status HTML Table")
    logger.info("Cycle Status recieved: " + str(cycle_status))
    dtls_log = cycle_status["cycle_dtls"]
    if dtls_log:
        html_table_header = '"<table class="tg" align="center"><tr><th class="tg-yw4l">process_id' \
                            '</th><th class="tg-yw4l">frequency</th><th class="tg-yw4l">' \
                            'cycle_id</th><th class="tg-yw4l">step_name</th><th class="tg-yw4l">' \
                            'step_status</th>' \
                            '<th class="tg-yw4l">step_start_time</th><th class="tg-yw4l">' \
                            'step_end_time</th></tr>'
        html_table_end = "</table>"
        final_row = ""
        logger.info("Parsing each record from the status")
        for var in dtls_log:
            row = dict(var)
            row_str = ('<td class="tg-yw4l">' + str(row["process_id"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["frequency"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["cycle_id"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["step_name"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["step_status"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["step_start_time"]) + '</td>'
                       ).lstrip('"').rstrip('"') + \
                      ('<td class="tg-yw4l">' + str(row["step_end_time"]) + '</td>'
                       ).lstrip('"').rstrip('"')
            row_str = (row_str + "</tr>").lstrip('"').rstrip('"')
            final_row = final_row + row_str

        html_table = (html_table_header + final_row + html_table_end).lstrip('"').rstrip('"')
        logger.info("HTML Spec generated for the Cycle details" + str(html_table))
        return html_table
    else:
        return ""


def fetch_dw_email_cycle_info(process_id):
    """This method fetches dw email cycle information"""
    logger.info("Starting function to fetch the cycle information for DW email")
    email_details = {}
    cycle_details = get_cycle_details(process_id, cycle_assigned_flag=True)
    logger.info("Cycle Details retrieved: " + str(cycle_details))
    email_details = copy.deepcopy(cycle_details)
    step_details = get_cycle_step_status(cycle_details["cycle_id"])
    email_details["detailed_status"] = generate_cycle_status_html(step_details)
    email_details["status"] = step_details["status"]
    logger.info("Final cycle information retrieved for DW email: " + str(email_details))
    return email_details


def fetch_dw_email_common_info(process_id, dag_name):
    """This method fetches dw email common information"""
    logger.info("Starting function to retrieve Common Ingestion email information for "
                "Process ID: " + str(process_id) +
                " and DAG Name: " + str(dag_name))
    configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                   CommonConstants.ENVIRONMENT_CONFIG_FILE))
    common_metadata = {"process_id": str(process_id), "dag_name": str(dag_name)}
    # ***** Mandatory Information Reported in every email *****
    # 1. Process ID
    # 2. Cycle ID
    # 3. DAG Name
    # 4. DAG URL
    # 5. Data Date
    # 6. Status

    # Retrieving the Airflow Link
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)

    airflow_url = "http://" + ip_address + ":8080/admin/airflow/graph?root=&dag_id=" + str(dag_name)

    logger.info("Airflow URL Generated:" + str(airflow_url))
    common_metadata["airflow_link"] = str(airflow_url)
    # Retrieving Environment

    env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])
    logger.info("Environment retrieved from Configurations is: " + str(env))
    common_metadata["env"] = str(env)

    # Fetching the dataset names from the Control tables
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    # audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
    #  "mysql_db"])
    # Return - Cycle ID, Frequency, Data date
    cycle_details = get_cycle_details(process_id, cycle_assigned_flag=False)
    common_metadata["data_date"] = str(cycle_details["data_date"])
    logger.info("Final DW common information extracted for the email: " + str(common_metadata))
    return common_metadata


def trigger_notification_utility(email_type, process_id, dag_name, emr_status=None, **kwargs):
    """This method triggers notification utility"""
    try:
        replace_variables = {}
        logger.info("Staring function to trigger Notification Utility")

        logger.info("Preparing the common information to be sent in all emails")
        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))

        email_types = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                       "email_type_configurations"])
        if email_types:
            email_types = list(email_types.keys())
            if email_type in email_types:
                logger.info("Email type: " + str(email_type) + " is configured")
            else:
                logger.info("Email type: " + str(email_type) + " is not configured, cannot proceed")
                raise Exception("Email type: " + str(email_type) + " is not configured,"
                                                                   " cannot proceed")
        else:
            raise Exception("No email types configured, cannot proceed")

        notification_flag = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "email_type_configurations",
                                                             str(email_type),
                                                             "notification_flag"])

        logger.info("Notification Flag: " + str(notification_flag))

        if notification_flag.lower() != 'y':
            logger.info("Notification is disabled hence skipping notification"
                        " trigger and exiting function")

        else:
            logger.info("Notification is enabled hence starting notification trigger")

            # Common configurations

            email_ses_region = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "ses_region"])
            logger.info("SES Region retrieved from Configurations is: " + str(email_ses_region))
            email_template_path = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_template_path"])
            logger.info("Email template path retrieved from Configurations is: " + str(
                email_template_path))

            # Email type specific configurations

            email_sender = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                            "email_type_configurations",
                                                            str(email_type),
                                                            "ses_sender"])
            logger.info("Email Sender retrieved from Configurations is: " + str(email_sender))

            email_recipient_list = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                 str(email_type), "ses_recipient_list"])
            logger.info("Email Recipient retrieved from Configurations is: " + str(
                email_recipient_list))

            email_subject = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "email_type_configurations",
                                                             str(email_type),
                                                             "subject"])
            logger.info("Email Subject retrieved from Configurations is: " + str(email_subject))

            email_template_name = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                 str(email_type), "template_name"])
            logger.info("Email Template Name retrieved from Configurations is: "
                        + str(email_template_name))

            email_template_path = (email_template_path + "/" + email_template_name
                                   ).replace("//", "/")
            logger.info("Final Email template path is: " + str(email_template_path))

            if email_type == "batch_status" or email_type == "dqm":
                common_info = fetch_ingestion_email_common_info(str(process_id), dag_name)
                replace_variables = copy.deepcopy(common_info)
                logger.info("Common email information for Batch/DQM" + str(replace_variables))
            elif email_type == "cycle_status":
                common_info = fetch_dw_email_common_info(str(process_id), dag_name)
                replace_variables = copy.deepcopy(common_info)
                logger.info("Common Email information for DW: " + str(replace_variables))
            replace_variables["status"] = CommonConstants.STATUS_NOT_STARTED
            logger.info("Variables prepared: " + str(replace_variables))

            if str(emr_status) == "None":
                replace_variables["detailed_status"] = ""
                replace_variables["batch_id"] = "Not Assigned"
                replace_variables["cycle_id"] = "Not Assigned"
                logger.info("Either failed during EMR launch or Cluster is not yet launched, "
                            "hence Batch/Cycle ID will be unassigned")
                logger.info("Variables prepared: " + str(replace_variables))
            else:
                replace_variables["status"] = str(emr_status)
                logger.info("Variables prepared: " + str(replace_variables))
                if str(emr_status) == CommonConstants.STATUS_SUCCEEDED:
                    if email_type == "batch_status":
                        dynamic_variables = fetch_ingestion_email_batch_info(
                            str(process_id), replace_variables["dataset_id"])
                    elif email_type == "dqm":
                        dynamic_variables = fetch_ingestion_email_dqm_info(
                            str(process_id), replace_variables["dataset_id"])
                    elif email_type == "cycle_status":
                        dynamic_variables = fetch_dw_email_cycle_info(str(process_id))
                    else:
                        #### Add other email type conditionals here and call the functions to
                        # fetch the variables for that email type
                        dynamic_variables = {}
                    replace_variables.update(dynamic_variables)
                    logger.info(str(replace_variables))
                else:
                    replace_variables["detailed_status"] = ""
                    replace_variables["batch_id"] = "Not Assigned"
                    replace_variables["cycle_id"] = "Not Assigned"
                    logger.info(
                        "Failed during EMR launch, hence Batch/Cycle ID will be unassigned")
                    logger.info("Variables prepared: " + str(replace_variables))

            logger.info("Final list of replacement variables for Email: " + str(replace_variables))

            # Preparing email details
            notif_obj = NotificationUtility()
            output = notif_obj.send_notification(email_ses_region, email_subject, email_sender,
                                                 email_recipient_list,
                                                 email_template=email_template_path,
                                                 replace_param_dict=replace_variables)
            logger.info("Output of Email Notification Trigger: " + str(output))

    except Exception as e:
        logger.error(str(traceback.format_exc()))
        logger.error("Failed while triggering Notification Utility. Error: " + e.message)


def execute_shell_command(command):
    """This method executes shell command"""
    status_message = ""
    try:
        status_message = "Started executing shell command " + command
        logger.info(status_message)
        command_output = subprocess.Popen(command, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE, shell=True)
        standard_output, standard_error = command_output.communicate()
        logger.info(standard_output)
        logger.info(standard_error)
        if command_output.returncode == 0:
            return True
        else:
            status_message = "Error occurred while executing command on shell :" + command
            raise Exception(status_message)
    except Exception as e:
        logger.error(str(traceback.format_exc()))
        logger.error(status_message)
        raise e


def get_cluster_id(process_id, **kwargs):
    """This method gets cluster id"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "Select cluster_id from {audit_db}.{emr_cluster_details_table} where " \
            "process_id='{process_id}' and cluster_status='{state}'".format(
        audit_db=audit_db,
        emr_cluster_details_table=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
        process_id=process_id, state=CommonConstants.CLUSTER_ACTIVE_STATE)
    logger.info("Query to retrieve the Cluster ID: " + str(query))
    cluster_ids = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Result: " + str(cluster_ids))

    # TODO: Handle cases where blank is returned by MySQL
    return cluster_ids[0]['cluster_id']


def get_spark_configurations(dataset_id, process_id, **kwargs):
    """This method gets spark configurations"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "Select spark_config from {audit_db}.{emr_process_workflow_map_table} where " \
            "dataset_id='{dataset_id}' and process_id = '{process_id}'".format(
        audit_db=audit_db,
        emr_process_workflow_map_table=CommonConstants.EMR_PROCESS_WORKFLOW_MAP_TABLE,
        dataset_id=dataset_id, process_id=process_id)
    logger.info("Query to retrieve the Spark Configurations: " + str(query))
    spark_config = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Result: " + str(spark_config))
    if str(spark_config) == None or str(spark_config) == 'NULL':
        return ''
    else:
        return spark_config[0]['spark_config']
        # TODO: Handle cases where blank is returned by MySQL


def get_host(process_id, instance_group_list, **kwargs):
    """This method gets host"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    logger.info("The Audit Database name extracted from configuration: " + str(audit_db))
    ip_list = []
    query = "Select cluster_id from {audit_db}.{emr_cluster_details_table} where " \
            "process_id='{process_id}' and cluster_status='{state}'".format(
        audit_db=audit_db,
        emr_cluster_details_table=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
        process_id=process_id,
        state=CommonConstants.CLUSTER_ACTIVE_STATE)
    logger.info("Query for retrieving the Cluster host name: " + str(query))
    cluster_ids = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Result: " + str(cluster_ids))
    if len(cluster_ids) > 1:
        raise Exception("More then one clusters are in Waiting state")
    elif len(cluster_ids) == 0:
        raise Exception("No Cluster in Waiting state")
    else:
        emrClusterLaunch = EmrClusterLaunchWrapper()
        cluster_configs = emrClusterLaunch.extractClusterConfiguration(process_id)
        logger.info("Cluster Configuration Provided : ", cluster_configs[0])
        cluster_configs = cluster_configs[0]
        region_string = str(cluster_configs["region_name"])
        logger.info("Cluster ID retrieved: " + str(cluster_ids[0]["cluster_id"]))
        cluster_id_str = str(cluster_ids[0]["cluster_id"])
        obj = Get_EMR_Host_IP()
        ip_list = obj.get_running_instance_ips(cluster_id_str, region_string,
                                               instance_group_list)
        logger.info("List of IP addresses retrived for instances: " + str(instance_group_list) + " is: " + str(ip_list))
        random_selection = random.choice(ip_list)
        if random_selection is None:
            query = "Select host_ip_address from {audit_db}.{emr_cluster_host_details} where  cluster_id='{cluster_id}'".format(
                audit_db=audit_db, emr_cluster_host_details=CommonConstants.EMR_CLUSTER_HOST_DETAILS,
                cluster_id=cluster_ids[0]['cluster_id'])
            cluster_ips = MySQLConnectionManager().execute_query_mysql(query, False)
            for ip in cluster_ips:
                ip_list.append(ip['host_ip_address'])
        random_selection = random.choice(ip_list)
        logger.info("IP address selected randomly: " + str(random_selection))
    return random_selection


def launch_emr(process_id, **kwargs):
    """This method launches emr"""
    kwargs["ti"].xcom_push(key='emr_status', value=CommonConstants.STATUS_FAILED)
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    logger.info(audit_db)
    emrClusterLaunch = EmrClusterLaunchWrapper()
    cluster_configs = emrClusterLaunch.extractClusterConfiguration(process_id)
    logger.info("Cluster Configuration Provided >> ", cluster_configs[0])
    cluster_configs = cluster_configs[0]
    property_json = json.loads(cluster_configs["property_json"])
    query = "Select cluster_id from {audit_db}.{emr_cluster_details_table} where " \
            "process_id={process_id} and cluster_status='{state}'".format(
        audit_db=audit_db,
        emr_cluster_details_table=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
        process_id=process_id,
        state=CommonConstants.CLUSTER_ACTIVE_STATE)
    logger.info(query)
    cluster_ids = MySQLConnectionManager().execute_query_mysql(query, False)
    if CommonConstants.CHECK_LAUNCH_COLUMN not in property_json:
        if cluster_ids:
            kwargs["ti"].xcom_push(key='emr_status', value=CommonConstants.STATUS_FAILED)
            raise Exception("More than one clusters are in Waiting state")
        else:
            emrClusterLaunch.main(process_id, "start")
            # launch_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
            #                                          "EmrClusterLaunchWrapper.py") + " -p " + str(
            #     process_id) + " -a start"
            # execute_shell_command(launch_string)
    else:
        if property_json[CommonConstants.CHECK_LAUNCH_COLUMN
        ] == CommonConstants.CHECK_LAUNCH_ENABLE:
            status_message = "Check flag is present with 'Y' value.Proceeding with existing " \
                             "cluster Id in WAITING state"
            logger.info(status_message)
        elif property_json[CommonConstants.CHECK_LAUNCH_COLUMN
        ] == CommonConstants.CHECK_LAUNCH_DISABLE:
            status_message = "Check flag is present with 'N' value.Proceeding with existing " \
                             "cluster Id in WAITING state"
            logger.info(status_message)
            if not cluster_ids:
                status_message = "There are no clusters with process id " + str(
                    process_id) + " in WAITING state. Trying to launch new cluster"
                logger.info(status_message)
                emrClusterLaunch.main(process_id, "start")
                # launch_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                # "EmrClusterLaunchWrapper.py") + " -p " + str(process_id) + " -a start"
                # execute_shell_command(launch_string)
    cluster_id = get_cluster_id(process_id)
    kwargs["ti"].xcom_push(key='emr_status', value=CommonConstants.STATUS_SUCCEEDED)
    kwargs["ti"].xcom_push(key='cluster_id', value=str(cluster_id))


def get_cycle_details(process_id, cycle_assigned_flag=False):
    """This method gets cycle details"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    result = {"frequency": "Not Configured",
              "data_date": "Not Configured",
              "cycle_id": "Not Configured"}
    try:
        logger.info("Started preparing query to fetch frequency for process id:" + str(process_id))
        query = "Select frequency from " + audit_db + "." + \
                CommonConstants.EMR_CLUSTER_CONFIGURATION_TABLE + \
                " where process_id=" + str(process_id) + " and active_flag='" + \
                CommonConstants.ACTIVE_IND_VALUE + "'"
        logger.info(
            "Completed preparing query to fetch frequency for process id:" + str(process_id))
        frequency = MySQLConnectionManager().execute_query_mysql(query, False)
        logger.info(
            "Completed query execution to fetch frequency for process id:" + str(process_id))
        data_date_flag = False
        if frequency:
            result["frequency"] = frequency[0]['frequency']
            frequency_str = frequency[0]['frequency']
            logger.info("Frequency retrieved : " + str(result["frequency"]))
            if cycle_assigned_flag:
                logger.info("Starting function to fetch latest cycle id")

                fetch_latest_cycle_query = "select * from log_cycle_dtl where cycle_id in (" \
                                           "select max(cycle_id) from log_cycle_dtl where " \
                                           "process_id = {process_id} and frequency = '" \
                                           "{frequency}')" \
                    .format(audit_db=audit_db, cycle_details=CommonConstants.LOG_CYCLE_DTL,
                            process_id=process_id,
                            frequency=frequency_str)
                logger.info("Query String: " + str(fetch_latest_cycle_query))
                cycle_id_result = MySQLConnectionManager().execute_query_mysql(
                    fetch_latest_cycle_query, True)
                cycle_id = cycle_id_result['cycle_id']
                data_date = cycle_id_result['data_date']
                data_date_flag = True
                result["cycle_id"] = str(cycle_id)
                result["data_date"] = str(data_date)
            if not data_date_flag:
                data_date = CommonUtils().fetch_data_date_process(process_id, result["frequency"])
                logger.info("Data date retrieved : " + str(data_date))
                result["data_date"] = str(data_date)
        logger.info("Found cycle details: " + str(result))
    except Exception as e:
        logger.error(str(traceback.format_exc()))
        logger.info("Error: " + str(e.message))
        logger.info("Some metadata information for DW email is not configured")
        logger.info("Found cycle details: " + str(result))
    finally:
        # Return - Cycle ID, Frequency, Data date
        return result


def send_dw_email(email_type, process_id, dag_name, **kwargs):
    """This method send dw email"""
    logger.info("Starting function to send Data Warehouse status email")
    logger.info("Retrieving EMR Launch status to check if Execution has started")
    ti = kwargs["ti"]
    emr_status = ti.xcom_pull(key='emr_status', task_ids="launch_cluster", dag_id=dag_name)
    logger.info("EMR Launch Status:" + str(emr_status))
    trigger_notification_utility(str(email_type), process_id, dag_name, emr_status=str(emr_status))
    logger.info("Completing function to send Data Warehouse notification email")


def terminate_emr(email_type=None, process_id=None, dag_name=None, **kwargs):
    """This method terminates emr"""
    ti = kwargs["ti"]
    emr_status = ti.xcom_pull(key='emr_status', task_ids="launch_cluster", dag_id=dag_name)
    logger.info("EMR Launch Status:" + str(emr_status))
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "SELECT * FROM " + audit_db + "." + CommonConstants.EMR_CLUSTER_CONFIGURATION_TABLE + \
            " WHERE process_id = " + str(process_id) + ";"
    logging.info("Executing query - " + query)
    cluster_configs = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Result of query - " + str(cluster_configs))
    termination_on_failure_flag = ast.literal_eval(cluster_configs[0]["property_json"])["termination_on_failure_flag"]
    logger.info("Value of Termination_on_failure_flag - " + termination_on_failure_flag)
    termination_on_success_flag = ast.literal_eval(cluster_configs[0]["property_json"])["termination_on_success_flag"]
    logger.info("Value of Termination_on_success_flag - " + termination_on_success_flag)

    if str(email_type).lower() == "batch_status":
        trigger_notification_utility(str(email_type), process_id, dag_name,
                                     emr_status=str(emr_status))
        if str(emr_status).lower() == CommonConstants.STATUS_FAILED.lower():
            logger.info("EMR Launch failed hence nothing to terminate")
        if str(emr_status).lower() == CommonConstants.STATUS_SUCCEEDED.lower():
            cluster_id = get_cluster_id(process_id)
            save_ganglia_pdf_to_s3_flag = CommonConstants.SAVE_GANGLIA_PDF_TO_S3
            send_ganglia_pdf_as_attachment_flag = CommonConstants.SEND_GANGLIA_PDF_AS_ATTACHMENT
            save_ganglia_csv_to_s3_flag = CommonConstants.SAVE_GANGLIA_CSV_TO_S3
            if save_ganglia_pdf_to_s3_flag == 'Y' or send_ganglia_pdf_as_attachment_flag == 'Y':
                ganglia_report_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                                  "CreateGangliaMetricsPDF.py"
                                                                  ) + " " + cluster_id
                execute_shell_command(ganglia_report_string)
            configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                           CommonConstants.ENVIRONMENT_CONFIG_FILE))

            s3_bucket_name = "s3://" + configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"]) + CommonConstants.S3_FOLDER
            if save_ganglia_csv_to_s3_flag == 'Y':
                ganglia_csv_string = "cd " + CommonConstants.AIRFLOW_CODE_PATH + ";" + \
                                     'python3 GangliaMetricFetchUtility.py ' \
                                     '--cluster_id "' + cluster_id + '" ' \
                                                                     '--s3_path ' + s3_bucket_name + ' ' \
                                                                                                     '--cluster_ip ' + get_host(
                    process_id, ["MASTER"]) + ' ' \
                                              '--node_level "N"; ' \
                                              'python3 GangliaMetricFetchUtility.py ' \
                                              '--cluster_id ' + cluster_id + ' ' \
                                                                             '--s3_path "' + s3_bucket_name + '" ' \
                                                                                                              '--cluster_ip ' + get_host(
                    process_id, ["MASTER"]) + ' ' \
                                              '--node_level "Y"; '
                logger.debug("Ganglia csv command: " + ganglia_csv_string)
                execute_shell_command(ganglia_csv_string)
            trigger_notification_utility("dqm", process_id, dag_name, emr_status=str(emr_status))
            logger.info("EMR Launch successful hence need to evaluate process status before "
                        "termination")
            dataset_ids = get_dataset_ids(process_id)
            final_dataset_id = ",".join(map(str, dataset_ids))
            batch_status = fetch_ingestion_email_batch_info(process_id, final_dataset_id)
            logger.info("Batch Status retrieved: " + str(batch_status))
            status = batch_status["status"]
            logger.info("Final Batch Status: " + str(status))
            if status == CommonConstants.STATUS_FAILED:
                if termination_on_failure_flag.upper() == "Y":
                    cluster_id = get_cluster_id(process_id)
                    copy_spark_logs_to_s3(cluster_id)
                    logger.info("Termination on Failure flag is set hence terminating EMR")
                    terminate_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                                 "TerminateEmrHandler.py"
                                                                 ) + " " + cluster_id
                    execute_shell_command(terminate_string)
                else:
                    logger.info("Termination on Failure flag is NOT set hence NOT terminating EMR")
            elif status == CommonConstants.STATUS_SUCCEEDED:
                if termination_on_success_flag.upper() == "Y":
                    cluster_id = get_cluster_id(process_id)
                    copy_spark_logs_to_s3(cluster_id)
                    logger.info("Termination on Success flag is set hence terminating EMR")
                    logger.info("python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "TerminateEmrHandler.py") +
                                " " + cluster_id)
                    terminate_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                                 "TerminateEmrHandler.py") + " " + cluster_id
                    execute_shell_command(terminate_string)
                else:
                    logger.info("Termination on Success flag is NOT set hence NOT terminating EMR")
            else:
                logger.info("Batch Status is not logged as FAILED/SUCCEEDED, hence not "
                            "terminating the EMR")

    elif str(email_type).lower() == "cycle_status":
        trigger_notification_utility(str(email_type), process_id, dag_name,
                                     emr_status=str(emr_status))
        if str(emr_status).lower() == CommonConstants.STATUS_FAILED.lower():
            logger.info("EMR Launch failed hence nothing to terminate")
        if str(emr_status).lower() == CommonConstants.STATUS_SUCCEEDED.lower():
            logger.info("EMR Launch successful hence evaluating cycle status")
            cycle_details = get_cycle_details(process_id, cycle_assigned_flag=True)
            cycle_status = get_cycle_step_status(cycle_details["cycle_id"])
            if cycle_status["status"] == CommonConstants.STATUS_FAILED:
                if termination_on_failure_flag.upper() == "Y":
                    logger.info("Termination on Failure flag is set hence terminating EMR")
                    cluster_id = get_cluster_id(process_id)
                    copy_spark_logs_to_s3(cluster_id)
                    terminate_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                                 "TerminateEmrHandler.py"
                                                                 ) + " " + cluster_id
                    execute_shell_command(terminate_string)
                else:
                    logger.info("Termination on Failure flag is NOT set hence NOT terminating EMR")

            elif cycle_status["status"] == CommonConstants.STATUS_SUCCEEDED:
                if termination_on_success_flag.upper() == "Y":
                    logger.info("Termination on Success flag is set hence terminating EMR")

                    cluster_id = get_cluster_id(process_id)
                    copy_spark_logs_to_s3(cluster_id)
                    terminate_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                                 "TerminateEmrHandler.py"
                                                                 ) + " " + cluster_id
                    execute_shell_command(terminate_string)
                else:
                    logger.info("Termination on Success flag is NOT set hence NOT terminating EMR")
            else:
                logger.info("Cycle status is neither SUCCESS not FAILED hence not "
                            "terminating the EMR")
    else:
        logger.info("No Email type given as input hence no email will be sent at this step")
        ti = kwargs["ti"]
        emr_status = ti.xcom_pull(key='emr_status', task_ids="launch_cluster", dag_id=dag_name)
        logger.info("EMR Launch Status:" + str(emr_status))
        if str(emr_status).lower() == CommonConstants.STATUS_FAILED.lower():
            logger.info("EMR Launch failed hence nothing to terminate")
        else:
            logger.info("Terminating EMR")
            cluster_id = get_cluster_id(process_id)
            terminate_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                         "TerminateEmrHandler.py") + " " + cluster_id
            execute_shell_command(terminate_string)


def copy_spark_logs_to_s3(cluster_id=None):
    """This method copies spark logs to s3"""
    try:
        status_message = "Starting to execute copy_spark_logs_to_s3 for cluster id: " + \
                         str(cluster_id)
        logging.info(status_message)

        if cluster_id is None:
            raise Exception("Cluster id can not None")

        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))

        spark_logs_s3_path = configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "spark_logs_s3_path"])
        private_key_location = configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])

        copy_to_s3_flag = True
        if spark_logs_s3_path is None:
            copy_to_s3_flag = False
        if spark_logs_s3_path is "":
            copy_to_s3_flag = False
        if spark_logs_s3_path == "":
            copy_to_s3_flag = False

        logging.info("spark_logs_s3_path is : " + str(spark_logs_s3_path))

        logging.info("copy_to_s3_flag is : " + str(copy_to_s3_flag))

        if copy_to_s3_flag:
            logs_s3_path = str(spark_logs_s3_path).rstrip("/") + "/"
            master_ip = CommonUtils().get_master_ip_from_cluster_id(cluster_id)
            default_spark_logs_location = CommonConstants.SPARK_LOGS_DEFAULT_PATH_EMR

            command_to_execute = "cd " + CommonConstants.AIRFLOW_CODE_PATH + \
                                 "; python3 ClusterToS3LoadHandler.py  -s " + \
                                 default_spark_logs_location + " -t " + logs_s3_path

            status_message = "Command created for log transfer: " + str(command_to_execute)
            logging.info(status_message)

            # ssh = ExecuteSSHCommand(remote_host=master_ip, username=CommonConstants.EMR_USER_NAME,
            #                         key_file=private_key_location)
            # output = ssh.execute_command(command_to_execute)
            output = SystemManagerUtility().execute_command(cluster_id, command_to_execute)
            logging.info("Output generated by job executor " + str(output))

        status_message = "Completing function execute copy_spark_logs_to_s3"
        logging.info(status_message)

    except Exception as exception:
        status_message = "Exception in copy_spark_logs_to_s3 due to  " + str(exception)
        logger.error(str(traceback.format_exc()))
        logger.error(status_message)
        raise exception


def call_pre_landing(file_master_id, dag_name, process_id, **kwargs):
    """This method calls pre-landing"""
    kwargs["ti"].xcom_push(key='emr_status', value=CommonConstants.STATUS_SUCCEEDED)
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])

    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    context = kwargs
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_file_path = str('/tmp/batches/').__add__(
        dt.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')).__add__('_').__add__(
        str(file_master_id)).__add__('.txt')
    max_try_limit = 3
    count = 0
    if conf_value.find('--deploy-mode cluster') != -1:
        logger.info("================Executing job in Spark cluster mode================")
        conf_value = conf_value + " --name PreLanding_" + file_master_id
        logger.info(
            "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh download.sh " + " " + str(file_master_id) + " " + str(
                cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + batch_file_path + " " + '"' + str(
                conf_value) + '"')
        try:
            # output = ssh.execute_command(
            #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh download.sh " + " " + str(file_master_id) + " " + str(
            #         cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + batch_file_path + " " + '"' + str(
            #         conf_value) + '"')
            output = SystemManagerUtility().execute_command(cluster_id,
                                                            "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh download.sh " + " " + str(
                                                                file_master_id) + " " + str(
                                                                cluster_id) + " " + str(dag_id) + " " + str(
                                                                process_id) + " " + batch_file_path + " " + '"' + str(
                                                                conf_value) + '"')
            while True:
                try:
                    batches = fetch_batch_id_prelanding(cluster_id, process_id, file_master_id, dag_id)
                    logging.info("Output generated by pre landing task - " + str(output))
                    kwargs["ti"].xcom_push(key='batch_ids', value=batches)
                except:
                    if count != max_try_limit:
                        count = count + 1
                        logging.info("No batches has been returned so retrying again in 5 seconds for %s ATTEMPT",
                                     count)
                        time.sleep(5)
                        continue
                    else:
                        logging.info("Max attempts have been reached.")
                        time.sleep(2)
                        logging.info("Unexpected error: %s ", sys.exc_info()[0])
                        raise
                break
        except Exception as exception:
            status_message = "Spark Submit failed due to" + str(exception)
            logger.error(status_message)
        finally:
            # # noinspection PyInterpreter
            # yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep PreLanding_" + file_master_id + "| awk '{print $1;}' | sort -r"
            # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
            # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
            # yarn_app_id = yarn_app_id.decode('utf-8')
            # yarn_app_id = yarn_app_id.split('\n')
            # yarn_app_id = yarn_app_id[0]
            # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
            # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
            # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
            # yarn_app_state = yarn_app_state.decode('utf-8')
            # yarn_app_state = yarn_app_state.split('\n')
            # yarn_app_state = yarn_app_state[0]
            # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
            # # output1 = ssh.execute_command(output_cmnd)
            # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
            # logging.info("Output generated by task - " + str(output1))
            # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
            #     status_message = "Spark Job Failed"
            #     raise Exception(status_message)
            response = CommonUtils().get_yarn_app_id_status(app_name="PreLanding_" + file_master_id, cluster_ip=ip)
            yarn_app_id = response["app_id"]
            yarn_app_state = response["state"]
            yarn_diagnostics_info = response["diagnostic_message"]
            logger.error("Diagnostics Info:" + yarn_diagnostics_info)
            output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
            logger.info("This RUN was configured in cluster mode, To get full logs run the "
                        "following command on EMR {yarn_log_command}".format(
                yarn_log_command=output_cmnd
            ))
            # logging.info("Output generated by task - " + str(output1))

            if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                status_message = "Spark Job Failed"
                raise Exception(status_message)

    else:
        logger.info("================Executing job in Spark client mode================")
        logger.info(
            "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh download.sh " + " " + str(file_master_id) + " " + str(
                cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + batch_file_path + " " + '"' + str(
                conf_value) + '"')
        # output = ssh.execute_command(
        #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh download.sh " + " " + str(file_master_id) + " " + str(
        #         cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + batch_file_path + " " + '"' + str(
        #         conf_value) + '"')
        output = SystemManagerUtility().execute_command(cluster_id,
                                                        "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh download.sh " + " " + str(
                                                            file_master_id) + " " + str(
                                                            cluster_id) + " " + str(dag_id) + " " + str(
                                                            process_id) + " " + batch_file_path + " " + '"' + str(
                                                            conf_value) + '"')

        while True:
            try:
                batches = fetch_batch_id_prelanding(cluster_id, process_id, file_master_id, dag_id)
                logging.info("Output generated by pre landing task - " + str(output))
                kwargs["ti"].xcom_push(key='batch_ids', value=batches)
            except:
                if count != max_try_limit:
                    count = count + 1
                    logging.info("No batches has been returned so retrying again in 5 seconds for %s ATTEMPT", count)
                    time.sleep(5)
                    continue
                else:
                    logging.info("Max attempts have been reached.")
                    time.sleep(2)
                    logging.info("Unexpected error: %s ", sys.exc_info()[0])
                    raise
            break


def call_file_check(file_master_id, dag_name, process_id, **kwargs):
    """This method calls file check"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    logging.info("IP---------- - " + str(ip))
    ti = kwargs["ti"]
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        if conf_value.find('--deploy-mode cluster') != -1:
            logger.info("================Executing job in Spark cluster mode================")
            conf_value = conf_value + " --name FileCheck_" + file_master_id + "_" + batch_id
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh file_check.sh " +
                        str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
                        str(process_id) + " " + str(batch_id) + " " + '"' + str(conf_value) + '"' + " " + str(batches))
            try:
                # output = ssh.execute_command(
                #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh file_check.sh " +
                #     str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
                #     str(process_id) + " " + str(batch_id) + " " + '"' + str(conf_value) + '"' +
                #     " " + str(batches))
                output = SystemManagerUtility().execute_command(cluster_id,
                                                                "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh file_check.sh " +
                                                                str(file_master_id) + " " + str(cluster_id) + " " + str(
                                                                    dag_id) + " " +
                                                                str(process_id) + " " + str(batch_id) + " " + '"' + str(
                                                                    conf_value) + '"' +
                                                                " " + str(batches))
                logging.info("Output generated by file check task - " + str(output))
            except Exception as exception:
                status_message = "Spark Submit failed due to" + str(exception)
                logger.error(status_message)
            finally:
                # yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep FileCheck_" + file_master_id + "| awk '{print $1;}' | sort -r"
                # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
                # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
                # yarn_app_id = yarn_app_id.decode('utf-8')
                # yarn_app_id = yarn_app_id.split('\n')
                # yarn_app_id = yarn_app_id[0]
                # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
                # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
                # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
                # yarn_app_state = yarn_app_state.decode('utf-8')
                # yarn_app_state = yarn_app_state.split('\n')
                # yarn_app_state = yarn_app_state[0]
                # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                # # output1 = ssh.execute_command(output_cmnd)
                # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
                # logging.info("Output generated by task - " + str(output1))
                # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                #     status_message = "Spark Job Failed"
                #     raise Exception(status_message)
                response = CommonUtils().get_yarn_app_id_status(app_name="FileCheck_" + file_master_id, cluster_ip=ip)
                yarn_app_id = response["app_id"]
                yarn_app_state = response["state"]
                yarn_diagnostics_info = response["diagnostic_message"]
                logger.error("Diagnostics Info:" + yarn_diagnostics_info)

                output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)

                logger.info("This RUN was configured in cluster mode, To get full logs run the "
                            "following command on EMR {yarn_log_command}".format(
                    yarn_log_command=output_cmnd
                ))

                CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_FILE_CHECK,
                                                               batch_id=batch_id)

                if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                    status_message = "Spark Job Failed"
                    raise Exception(status_message)
        else:
            logger.info("================Executing job in Spark client mode================")
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh file_check.sh " +
                        str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
                        str(process_id) + " " + str(batch_id) + " " + '"' + str(conf_value) + '"' + " " + str(batches))
            # output = ssh.execute_command(
            #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh file_check.sh " +
            #     str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
            #     str(process_id) + " " + str(batch_id) + " " + '"' + str(conf_value) + '"' +
            #     " " + str(batches))
            output = SystemManagerUtility().execute_command(cluster_id,
                                                            "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh file_check.sh " +
                                                            str(file_master_id) + " " + str(cluster_id) + " " + str(
                                                                dag_id) + " " +
                                                            str(process_id) + " " + str(batch_id) + " " + '"' + str(
                                                                conf_value) + '"' +
                                                            " " + str(batches))
            logging.info("Output generated by file check task - " + str(output))


def call_landing(file_master_id, dag_name, process_id, **kwargs):
    """This method calls landing"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    logging.info("IP---------- - " + str(ip))
    ti = kwargs["ti"]
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh landing.sh " + str(file_master_id) + " " +
                    str(cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + str(batch_id) + " " +
                    str(batches))
        # output = ssh.execute_command(
        #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh landing.sh " + str(file_master_id) +
        #     " " + str(cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " +
        #     str(batch_id) + " " + str(batches))
        output = SystemManagerUtility().execute_command(cluster_id,
                                                        "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh landing.sh " + str(
                                                            file_master_id) +
                                                        " " + str(cluster_id) + " " + str(dag_id) + " " + str(
                                                            process_id) + " " +
                                                        str(batch_id) + " " + str(batches))
        logging.info("Output generated by landing task - " + str(output))


def call_pre_dqm(file_master_id, dag_name, process_id, **kwargs):
    """This method calls pre dqm"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    logging.info("IP---------- - " + str(ip))
    ti = kwargs["ti"]
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        if conf_value.find('--deploy-mode cluster') != -1:
            logger.info("================Executing job in Spark cluster mode================")
            conf_value = conf_value + " --name PreDQM_" + file_master_id + "_" + batch_id
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh pre_dqm.sh " + str(file_master_id) + " " +
                        str(cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                        " " + '"' + str(conf_value) + '"' + " " + str(batches))
            try:
                # output = ssh.execute_command(
                #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh pre_dqm.sh " +
                #     str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
                #     str(process_id) + " " + str(batch_id) +
                #     " " + '"' + str(conf_value) + '"' + " " + str(batches))
                output = SystemManagerUtility().execute_command(cluster_id,
                                                                "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh pre_dqm.sh " +
                                                                str(file_master_id) + " " + str(cluster_id) + " " + str(
                                                                    dag_id) + " " +
                                                                str(process_id) + " " + str(batch_id) +
                                                                " " + '"' + str(conf_value) + '"' + " " + str(batches))
                logging.info("Output generated by pre_dqm task - " + str(output))
            except Exception as exception:
                status_message = "Spark Submit failed due to" + str(exception)
                logger.error(status_message)
            finally:
                # yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep PreDQM_" + file_master_id + "| awk '{print $1;}' | sort -r"
                # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
                # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
                # yarn_app_id = yarn_app_id.decode('utf-8')
                # yarn_app_id = yarn_app_id.split('\n')
                # yarn_app_id = yarn_app_id[0]
                # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
                # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
                # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
                # yarn_app_state = yarn_app_state.decode('utf-8')
                # yarn_app_state = yarn_app_state.split('\n')
                # yarn_app_state = yarn_app_state[0]
                # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                # # output1 = ssh.execute_command(output_cmnd)
                # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
                # logging.info("Output generated by task - " + str(output1))
                # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                #     status_message = "Spark Job Failed"
                #     raise Exception(status_message)
                response = CommonUtils().get_yarn_app_id_status(app_name="PreDQM_" + file_master_id, cluster_ip=ip)
                yarn_app_id = response["app_id"]
                yarn_app_state = response["state"]
                yarn_diagnostics_info = response["diagnostic_message"]
                logger.error("Diagnostics Info:" + yarn_diagnostics_info)

                output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                logger.info("This RUN was configured in cluster mode, To get full logs run the "
                            "following command on EMR {yarn_log_command}".format(
                    yarn_log_command=output_cmnd
                ))

                CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_PRE_DQM,
                                                               batch_id=batch_id)

                if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                    status_message = "Spark Job Failed"
                    raise Exception(status_message)
        else:
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh pre_dqm.sh " + str(file_master_id) + " " +
                        str(cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                        " " + '"' + str(conf_value) + '"' + " " + str(batches))
            # output = ssh.execute_command(
            #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh pre_dqm.sh " +
            #     str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
            #     str(process_id) + " " + str(batch_id) +
            #     " " + '"' + str(conf_value) + '"' + " " + str(batches))
            output = SystemManagerUtility().execute_command(cluster_id,
                                                            "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh pre_dqm.sh " +
                                                            str(file_master_id) + " " + str(cluster_id) + " " + str(
                                                                dag_id) + " " +
                                                            str(process_id) + " " + str(batch_id) +
                                                            " " + '"' + str(conf_value) + '"' + " " + str(batches))
            logging.info("Output generated by pre_dqm task - " + str(output))


def call_dqm(file_master_id, dag_name, process_id, **kwargs):
    """This method calls dqm"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    logging.info("IP---------- - " + str(ip))
    ti = kwargs["ti"]
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        if conf_value.find('--deploy-mode cluster') != -1:
            logger.info("================Executing job in Spark cluster mode================")
            conf_value = conf_value + " --name DQMCheckHandler_" + file_master_id + "_" + batch_id
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm.sh "
                        + str(file_master_id) + " " + str(cluster_id) + " " +
                        str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                        " " + '"' + str(conf_value) + '"' + " " + str(batches))
            try:
                # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm.sh "
                #                              + str(file_master_id) + " " + str(cluster_id) + " " +
                #                              str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                #                              " " + '"' + str(conf_value) + '"' + " " + str(batches))
                output = SystemManagerUtility().execute_command(cluster_id,
                                                                "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm.sh "
                                                                + str(file_master_id) + " " + str(cluster_id) + " " +
                                                                str(dag_id) + " " + str(process_id) + " " + str(
                                                                    batch_id) +
                                                                " " + '"' + str(conf_value) + '"' + " " + str(batches))
                logging.info("Output generated by dqm task - " + str(output))
            except Exception as exception:
                status_message = "Spark Submit failed due to" + str(exception)
                logger.error(status_message)
            finally:
                # yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep DQMCheckHandler_" + file_master_id + "| awk '{print $1;}' | sort -r"
                # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
                # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
                # yarn_app_id = yarn_app_id.decode('utf-8')
                # yarn_app_id = yarn_app_id.split('\n')
                # yarn_app_id = yarn_app_id[0]
                # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
                # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
                # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
                # yarn_app_state = yarn_app_state.decode('utf-8')
                # yarn_app_state = yarn_app_state.split('\n')
                # yarn_app_state = yarn_app_state[0]
                # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                # # output1 = ssh.execute_command(output_cmnd)
                # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
                # logging.info("Output generated by task - " + str(output1))
                # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                #     status_message = "Spark Job Failed"
                #     raise Exception(status_message)
                response = CommonUtils().get_yarn_app_id_status(app_name="DQMCheckHandler_" + file_master_id,
                                                                cluster_ip=ip)
                yarn_app_id = response["app_id"]
                yarn_app_state = response["state"]
                yarn_diagnostics_info = response["diagnostic_message"]
                logger.error("Diagnostics Info:" + yarn_diagnostics_info)
                output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                logger.info("This RUN was configured in cluster mode, To get full logs run the "
                            "following command on EMR {yarn_log_command}".format(
                    yarn_log_command=output_cmnd
                ))

                CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_DQM,
                                                               batch_id=batch_id)
                if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                    status_message = "Spark Job Failed"
                    raise Exception(status_message)

        else:
            logger.info("================Executing job in Spark client mode================")
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm.sh "
                        + str(file_master_id) + " " + str(cluster_id) + " " +
                        str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                        " " + '"' + str(conf_value) + '"' + " " + str(batches))
            # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm.sh "
            #                              + str(file_master_id) + " " + str(cluster_id) + " " +
            #                              str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
            #                              " " + '"' + str(conf_value) + '"' + " " + str(batches))
            output = SystemManagerUtility().execute_command(cluster_id,
                                                            "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm.sh "
                                                            + str(file_master_id) + " " + str(cluster_id) + " " +
                                                            str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                                                            " " + '"' + str(conf_value) + '"' + " " + str(batches))
            logging.info("Output generated by dqm task - " + str(output))


def call_dqm_filter(file_master_id, dag_name, process_id, **kwargs):
    """This method calls dqm filter"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    logging.info("IP---------- - " + str(ip))
    ti = kwargs["ti"]
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        if conf_value.find('--deploy-mode cluster') != -1:
            logger.info("================Executing job in Spark cluster mode================")
            conf_value = conf_value + " --name DQMFilter_" + file_master_id + "_" + batch_id
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm_filter.sh " + str(file_master_id) + " " +
                        str(cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + str(batch_id) + " " + '"' +
                        str(conf_value) + '"' + " " + str(batches))
            try:
                # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH +
                #                              "; sh dqm_filter.sh " + str(file_master_id) + " " +
                #                              str(cluster_id) + " " + str(dag_id) + " " + str(process_id) +
                #                              " " + str(batch_id) + " " + '"' + str(conf_value) + '"' +
                #                              " " + str(batches))
                output = SystemManagerUtility().execute_command(cluster_id, "cd " + CommonConstants.AIRFLOW_CODE_PATH +
                                                                "; sh dqm_filter.sh " + str(file_master_id) + " " +
                                                                str(cluster_id) + " " + str(dag_id) + " " + str(
                    process_id) +
                                                                " " + str(batch_id) + " " + '"' + str(
                    conf_value) + '"' +
                                                                " " + str(batches))
                logging.info("Output generated by dqm_filter task - " + str(output))
            except Exception as exception:
                status_message = "Spark Submit failed due to" + str(exception)
                logger.error(status_message)
            finally:
                yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep DQMFilter_" + file_master_id + "| awk '{print $1;}' | sort -r"
                # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
                # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
                # yarn_app_id = yarn_app_id.decode('utf-8')
                # yarn_app_id = yarn_app_id.split('\n')
                # yarn_app_id = yarn_app_id[0]
                # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
                # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
                # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
                # yarn_app_state = yarn_app_state.decode('utf-8')
                # yarn_app_state = yarn_app_state.split('\n')
                # yarn_app_state = yarn_app_state[0]
                # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                # # output1 = ssh.execute_command(output_cmnd)
                # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
                # logging.info("Output generated by task - " + str(output1))
                # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                #     status_message = "Spark Job Failed"
                #     raise Exception(status_message)
                response = CommonUtils().get_yarn_app_id_status(app_name="DQMFilter_" + file_master_id, cluster_ip=ip)
                yarn_app_id = response["app_id"]
                yarn_app_state = response["state"]
                yarn_diagnostics_info = response["diagnostic_message"]
                logger.error("Diagnostics Info:" + yarn_diagnostics_info)
                output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                # output1 = SystemManagerUtility().execute_command(cluster_id,output_cmnd)
                # logging.info("Output generated by task - " + str(output1))
                logger.info("This RUN was configured in cluster mode, To get full logs run the "
                            "following command on EMR {yarn_log_command}".format(
                    yarn_log_command=output_cmnd
                ))

                CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_STAGING,
                                                               batch_id=batch_id)
                if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                    status_message = "Spark Job Failed"
                    raise Exception(status_message)
        else:
            logger.info("================Executing job in Spark client mode================")
            logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh dqm_filter.sh " + str(file_master_id) + " " +
                        str(cluster_id) + " " + str(dag_id) + " " + str(process_id) + " " + str(batch_id) + " " + '"' +
                        str(conf_value) + '"' + " " + str(batches))
            # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH +
            #                              "; sh dqm_filter.sh " + str(file_master_id) + " " +
            #                              str(cluster_id) + " " + str(dag_id) + " " + str(process_id) +
            #                              " " + str(batch_id) + " " + '"' + str(conf_value) + '"' +
            #                              " " + str(batches))
            output = SystemManagerUtility().execute_command(cluster_id, "cd " + CommonConstants.AIRFLOW_CODE_PATH +
                                                            "; sh dqm_filter.sh " + str(file_master_id) + " " +
                                                            str(cluster_id) + " " + str(dag_id) + " " + str(
                process_id) +
                                                            " " + str(batch_id) + " " + '"' + str(conf_value) + '"' +
                                                            " " + str(batches))
            logging.info("Output generated by dqm_filter task - " + str(output))


def copyhdfstos3(file_master_id, dag_name, process_id, **kwargs):
    """This method copies from hdfs to s3"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    logging.info("IP---------- - " + str(ip))
    ti = kwargs["ti"]
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        if conf_value.find('--deploy-mode cluster') != -1:
            logger.info("================Executing job in Spark cluster mode================")
            conf_value = conf_value + " --name copyhdfstos3_" + file_master_id + "_" + batch_id
            logging.info("Output generated by copy_to_s3 task - " + str(output))
            try:
                # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH +
                #                              "; sh copyhdfstos3.sh " + str(file_master_id) + " " +
                #                              str(cluster_id) + " " + str(dag_id) + " " + str(process_id) +
                #                              " " + str(batch_id) + " " + '"' + str(conf_value) + '"' + " " +
                #                              str(batches))
                output = SystemManagerUtility().execute_command(cluster_id, "cd " + CommonConstants.AIRFLOW_CODE_PATH +
                                                                "; sh copyhdfstos3.sh " + str(file_master_id) + " " +
                                                                str(cluster_id) + " " + str(dag_id) + " " + str(
                    process_id) +
                                                                " " + str(batch_id) + " " + '"' + str(
                    conf_value) + '"' + " " +
                                                                str(batches))
                logging.info("Output generated by copy_to_s3 task - " + str(output))
            except Exception as exception:
                status_message = "Spark Submit failed due to" + str(exception)
                logger.error(status_message)
            finally:
                # yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep copyhdfstos3_" + file_master_id + "| awk '{print $1;}' | sort -r"
                # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
                # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
                # yarn_app_id = yarn_app_id.decode('utf-8')
                # yarn_app_id = yarn_app_id.split('\n')
                # yarn_app_id = yarn_app_id[0]
                # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
                # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
                # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
                # yarn_app_state = yarn_app_state.decode('utf-8')
                # yarn_app_state = yarn_app_state.split('\n')
                # yarn_app_state = yarn_app_state[0]
                # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                # # output1 = ssh.execute_command(output_cmnd)
                # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
                # logging.info("Output generated by task - " + str(output1))
                # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                #     status_message = "Spark Job Failed"
                #     raise Exception(status_message)
                response = CommonUtils().get_yarn_app_id_status(app_name="copyhdfstos3_" + file_master_id,
                                                                cluster_ip=ip)
                yarn_app_id = response["app_id"]
                yarn_app_state = response["state"]
                yarn_diagnostics_info = response["diagnostic_message"]
                logger.error("Diagnostics Info:" + yarn_diagnostics_info)
                output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                logger.info("This RUN was configured in cluster mode, To get full logs run the "
                            "following command on EMR {yarn_log_command}".format(
                    yarn_log_command=output_cmnd
                ))

                CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_HDFS_TO_S3,
                                                               batch_id=batch_id)
                if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                    status_message = "Spark Job Failed"
                    raise Exception(status_message)
        else:
            logger.info("================Executing job in Spark client mode================")
            # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH +
            #                              "; sh copyhdfstos3.sh " + str(file_master_id) + " " +
            #                              str(cluster_id) + " " + str(dag_id) + " " + str(process_id) +
            #                              " " + str(batch_id) + " " + '"' + str(conf_value) + '"' + " " +
            #                              str(batches))
            output = SystemManagerUtility().execute_command(cluster_id, "cd " + CommonConstants.AIRFLOW_CODE_PATH +
                                                            "; sh copyhdfstos3.sh " + str(file_master_id) + " " +
                                                            str(cluster_id) + " " + str(dag_id) + " " + str(
                process_id) +
                                                            " " + str(batch_id) + " " + '"' + str(
                conf_value) + '"' + " " +
                                                            str(batches))
            logging.info("Output generated by copy_to_s3 task - " + str(output))


def archive(file_master_id, dag_name, process_id, **kwargs):
    """This method archives data"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ti = kwargs["ti"]
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        if conf_value.find('--deploy-mode cluster') != -1:
            logger.info("================Executing job in Spark cluster mode================")
            conf_value = conf_value + " --name Archive_" + file_master_id + "_" + batch_id
            try:
                # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh archive.sh "
                #                              + str(file_master_id) + " " + str(cluster_id) + " " +
                #                              str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                #                              " " + '"' + str(conf_value) + '"' + " " + str(batches))
                output = SystemManagerUtility().execute_command(cluster_id,
                                                                "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh archive.sh "
                                                                + str(file_master_id) + " " + str(cluster_id) + " " +
                                                                str(dag_id) + " " + str(process_id) + " " + str(
                                                                    batch_id) +
                                                                " " + '"' + str(conf_value) + '"' + " " + str(batches))
                logging.info("Output generated by archive task - " + str(output))
            except Exception as exception:
                status_message = "Spark Submit failed due to" + str(exception)
                logger.error(status_message)
            finally:
                # yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep Archive_" + file_master_id + "| awk '{print $1;}' | sort -r"
                # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
                # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
                # yarn_app_id = yarn_app_id.decode('utf-8')
                # yarn_app_id = yarn_app_id.split('\n')
                # yarn_app_id = yarn_app_id[0]
                # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
                # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
                # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
                # yarn_app_state = yarn_app_state.decode('utf-8')
                # yarn_app_state = yarn_app_state.split('\n')
                # yarn_app_state = yarn_app_state[0]
                # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                # # output1 = ssh.execute_command(output_cmnd)
                # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
                # logging.info("Output generated by task - " + str(output1))
                # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                #     status_message = "Spark Job Failed"
                #     raise Exception(status_message)
                response = CommonUtils().get_yarn_app_id_status(app_name="Archive_" + file_master_id, cluster_ip=ip)
                yarn_app_id = response["app_id"]
                yarn_app_state = response["state"]
                yarn_diagnostics_info = response["diagnostic_message"]
                logger.error("Diagnostics Info:" + yarn_diagnostics_info)
                output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                logger.info("This RUN was configured in cluster mode, To get full logs run the "
                            "following command on EMR {yarn_log_command}".format(
                    yarn_log_command=output_cmnd
                ))

                CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_ARCHIVE,
                                                               batch_id=batch_id)
                if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                    status_message = "Spark Job Failed"
                    raise Exception(status_message)
        else:
            logger.info("================Executing job in Spark client mode================")
            # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh archive.sh "
            #                              + str(file_master_id) + " " + str(cluster_id) + " " +
            #                              str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
            #                              " " + '"' + str(conf_value) + '"' + " " + str(batches))
            output = SystemManagerUtility().execute_command(cluster_id,
                                                            "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh archive.sh "
                                                            + str(file_master_id) + " " + str(cluster_id) + " " +
                                                            str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                                                            " " + '"' + str(conf_value) + '"' + " " + str(batches))
            logging.info("Output generated by archive task - " + str(output))


def call_staging(file_master_id, dag_name, process_id, **kwargs):
    """This method calls staging"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    ti = kwargs["ti"]
    ip = get_host(process_id, ["MASTER"])
    cluster_id = get_cluster_id(process_id)
    logging.info("IP---------- - " + str(ip))
    batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                           dag_id=dag_name)
    logging.info("Batch Id fetched from pre landing is - " + str(batches))
    context = kwargs
    # batches=batches.decode('utf-8')
    conf_value = get_spark_configurations(file_master_id, process_id)
    dag_id = str(context['dag_run'].run_id)
    # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
    batch_ids = batches.split(",")
    for batch_id in batch_ids:
        logger.info("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; python3 AuditHandler.py "
                    + str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
                    " " + str(process_id) + " " + str(batch_id) + " " + str(batch_ids))
        # output = ssh.execute_command(
        #     "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; python3 AuditHandler.py "
        #     + str(file_master_id) + " " + str(cluster_id) + " " + str(dag_id) + " " +
        #     " " + str(process_id) + " " + str(batch_id) + " " + str(batch_ids))
        output = SystemManagerUtility().execute_command(cluster_id,
                                                        "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; python3 AuditHandler.py "
                                                        + str(file_master_id) + " " + str(cluster_id) + " " + str(
                                                            dag_id) + " " +
                                                        " " + str(process_id) + " " + str(batch_id) + " " + str(
                                                            batch_ids))
        logging.info("Output generated by audit update  task - " + str(output))


def publish(file_master_id, dag_name, process_id, **kwargs):
    """This method publishes data"""
    s3_backup_flag = False
    try:
        configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        private_key_location = configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
        ti = kwargs["ti"]
        ip = get_host(process_id, ["MASTER"])
        cluster_id = get_cluster_id(process_id)
        batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                               dag_id=dag_name)
        logging.info("Batch Id fetched from pre landing is - " + str(batches))
        context = kwargs
        # batches=batches.decode('utf-8')
        conf_value = get_spark_configurations(file_master_id, process_id)
        dag_id = str(context['dag_run'].run_id)
        # Adding logic to clear s3 path in case of latest load
        logger.info("Checking load type for the dataset_id " + str(file_master_id))
        audit_db = configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        get_load_type_query = "SELECT publish_s3_path,publish_type from" \
                              " {audit_db}.{process_date_table}" \
                              " where dataset_id='{dataset_id}' ".format \
            (audit_db=audit_db,
             process_date_table=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
             dataset_id=file_master_id)
        # Get publish_s3_path and publish_type for the dataset
        query_output = MySQLConnectionManager().execute_query_mysql(get_load_type_query)
        logging.info("Query output" + str(query_output))
        if len(query_output) == 1 and "publish_type" in query_output[0]:
            if query_output[0]["publish_type"].lower() == CommonConstants.LATEST_LOAD_TYPE.lower():
                client = boto3.client('s3')
                parsed_url = urlparse(query_output[0]["publish_s3_path"])
                # Check if the Publish s3 path contains any data, backup data if exists
                file_list = client.list_objects(Bucket=parsed_url.netloc,
                                                Prefix=os.path.join(parsed_url.path, "").lstrip("/"))
                if "Contents" in file_list and len(file_list["Contents"]) > 0:
                    s3_backup_flag = True
                    # s3_mv_cmd = "aws s3 mv " + os.path.join(query_output[0]["publish_s3_path"],
                    #                                         "") + " " + os.path.join(
                    #     query_output[0]["publish_s3_path"].rstrip("/") + "_backup", "") + " --recursive"
                    # loggonUtils().execute_shell_command(s3_mv_cmd)er.info("Taking back-up before deleting data, command: " + s3_mv_cmd)
                    # Comm
                    # logger.info(
                    #     "Backup of publish data is complete and s3_backup_flag is set as True")
                    CustomS3Utility().move_s3_to_s3_recursively(os.path.join(query_output[0]["publish_s3_path"], ""),
                                                                os.path.join(query_output[0]["publish_s3_path"].rstrip(
                                                                    "/") + "_backup", ""))
                    logger.info("Backup of publish data is complete and s3_backup_flag is set as True")
                else:
                    logger.info("No file is found in " + query_output[0]["publish_s3_path"])
        # ssh = ExecuteSSHCommand(remote_host=ip, username="hadoop", key_file=private_key_location)
        batch_ids = batches.split(",")
        for batch_id in batch_ids:
            if conf_value.find('--deploy-mode cluster') != -1:
                logger.info("================Executing job in Spark cluster mode================")
                conf_value = conf_value + " --name Publish_" + file_master_id + "_" + batch_id
                try:
                    # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh publish.sh "
                    #                              + str(file_master_id) + " " + str(cluster_id) + " " +
                    #                              str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                    #                              " " + '"' + str(conf_value) + '"' + " " + str(batches))
                    output = SystemManagerUtility().execute_command(cluster_id,
                                                                    "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh publish.sh "
                                                                    + str(file_master_id) + " " + str(
                                                                        cluster_id) + " " +
                                                                    str(dag_id) + " " + str(process_id) + " " + str(
                                                                        batch_id) +
                                                                    " " + '"' + str(conf_value) + '"' + " " + str(
                                                                        batches))
                    logging.info("Output generated by publish task - " + str(output))
                except Exception as exception:
                    status_message = "Spark Submit failed due to" + str(exception)
                    logger.error(status_message)
                finally:
                    # yarn_app_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep Publish_" + file_master_id + "| awk '{print $1;}' | sort -r"
                    # # yarn_app_id = ssh.execute_command(yarn_app_cmnd)
                    # yarn_app_id = SystemManagerUtility().execute_command(cluster_id, yarn_app_cmnd)
                    # yarn_app_id = yarn_app_id.decode('utf-8')
                    # yarn_app_id = yarn_app_id.split('\n')
                    # yarn_app_id = yarn_app_id[0]
                    # yarn_app_state_cmnd = "yarn application -list -appStates FINISHED FAILED KILLED | grep " + yarn_app_id + " | awk '{print $7;}'"
                    # # yarn_app_state = ssh.execute_command(yarn_app_state_cmnd)
                    # yarn_app_state = SystemManagerUtility().execute_command(cluster_id, yarn_app_state_cmnd)
                    # yarn_app_state = yarn_app_state.decode('utf-8')
                    # yarn_app_state = yarn_app_state.split('\n')
                    # yarn_app_state = yarn_app_state[0]
                    # output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                    # # output1 = ssh.execute_command(output_cmnd)
                    # output1 = SystemManagerUtility().execute_command(cluster_id, output_cmnd)
                    # logging.info("Output generated by task - " + str(output1))
                    # if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                    #     status_message = "Spark Job Failed"
                    #     raise Exception(status_message)
                    response = CommonUtils().get_yarn_app_id_status(app_name="Publish_" + file_master_id,
                                                                    cluster_ip=ip)
                    yarn_app_id = response["app_id"]
                    yarn_app_state = response["state"]
                    yarn_diagnostics_info = response["diagnostic_message"]
                    logger.error("Diagnostics Info:" + yarn_diagnostics_info)
                    output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                    # output1 = SystemManagerUtility().execute_command(cluster_id,output_cmnd)
                    # logging.info("Output generated by task - " + str(output1))
                    logger.info("This RUN was configured in cluster mode, To get full logs run the "
                                "following command on EMR {yarn_log_command}".format(
                        yarn_log_command=output_cmnd
                    ))

                    CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_PUBLISH,
                                                                   batch_id=batch_id)

                    if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                        status_message = "Spark Job Failed"
                        raise Exception(status_message)
            else:
                # output = ssh.execute_command("cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh publish.sh "
                #                              + str(file_master_id) + " " + str(cluster_id) + " " +
                #                              str(dag_id) + " " + str(process_id) + " " + str(batch_id) +
                #                              " " + '"' + str(conf_value) + '"' + " " + str(batches))
                output = SystemManagerUtility().execute_command(cluster_id,
                                                                "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; sh publish.sh "
                                                                + str(file_master_id) + " " + str(cluster_id) + " " +
                                                                str(dag_id) + " " + str(process_id) + " " + str(
                                                                    batch_id) +
                                                                " " + '"' + str(conf_value) + '"' + " " + str(batches))
                logging.info("Output generated by publish task - " + str(output))
    except Exception as e:
        if s3_backup_flag is True:
            logger.info("s3_backup_flag is set as True, moving the backup data")
            client = boto3.client('s3')
            parsed_url = urlparse(query_output[0]["publish_s3_path"])
            # Check if the Publish s3 path contains any data, backup data if exists
            file_list = client.list_objects(Bucket=parsed_url.netloc,
                                            Prefix=os.path.join(parsed_url.path, "").lstrip("/"))
            if "Contents" in file_list and len(file_list["Contents"]) > 0:
                # s3_rm_cmd = "aws s3 rm " + os.path.join(
                #     query_output[0]["publish_s3_path"], " ") + "--recursive"
                # logger.info("Deleting half copied data in publish, command: " + str(s3_rm_cmd))
                logger.info("Deleting half copied data in publish")
                CustomS3Utility().delete_s3_objects_recursively(os.path.join(query_output[0]["publish_s3_path"], ""))
                # CommonUtils().execute_shell_command(s3_rm_cmd)
            else:
                logger.info("No file is found in " + query_output[0]["publish_s3_path"])
            # s3_mv_cmd = "aws s3 mv " + os.path.join(
            #     query_output[0]["publish_s3_path"].rstrip("/") + "_backup", "") + " " + os.path.join(
            #     query_output[0]["publish_s3_path"], " ") + " --recursive"
            # logger.info("Moving backup data to publish, command: " + str(s3_mv_cmd))
            # CommonUtils().execute_shell_command(s3_mv_cmd)
            logger.info("Moving backup data to publish")
            CustomS3Utility().move_s3_to_s3_recursively(
                s3_source_location=os.path.join(query_output[0]["publish_s3_path"].rstrip("/") + "_backup", ""),
                s3_target_location=os.path.join(query_output[0]["publish_s3_path"], ""))

        else:
            logger.info("s3_backup_flag is set as False no action will be performed")
        raise e
    finally:
        if s3_backup_flag is True:
            logger.info("In finally block and s3_backup_flag is True")
            client = boto3.client('s3')
            parsed_url = urlparse(query_output[0]["publish_s3_path"].rstrip("/") + "_backup")
            # Check if the Publish s3 path contains any data, backup data if exists
            file_list = client.list_objects(Bucket=parsed_url.netloc,
                                            Prefix=os.path.join(parsed_url.path, "").lstrip("/"))

            if "Contents" in file_list and len(file_list["Contents"]) > 0:
                CustomS3Utility().delete_s3_objects_recursively(
                    os.path.join(query_output[0]["publish_s3_path"].rstrip("/") + "_backup", ""))
                # CommonUtils().execute_shell_command("aws s3 rm " +
                #                                     os.path.join(query_output[0]["publish_s3_path"].rstrip("/")
                #                                                  + "_backup", " ") + " --recursive")
            else:
                logger.info("No file is found in " + query_output[0]["publish_s3_path"])


def call_job_executor(process_id, frequency, step_name, **kwargs):
    """This method calls job executor"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    if CommonConstants.CLUSTER_MASTER_SUBMIT_FLAG == CommonConstants.ACTIVE_IND_VALUE:
        ip = get_host(process_id, ["MASTER"])
    else:
        ip = get_host(process_id, ["MASTER", "CORE"])
    cluster_id = get_cluster_id(str(process_id))
    data_date = CommonUtils().fetch_data_date_process(process_id, frequency)
    cycle_id = CommonUtils().fetch_cycle_id_for_data_date(process_id, frequency, data_date)
    logging.info("IP---------- - " + str(ip))
    # ssh = ExecuteSSHCommand(remote_host=ip, username=CommonConstants.EMR_USER_NAME,
    #                         key_file=private_key_location)
    command = "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; python3 JobExecutor.py" + " " + \
              str(process_id) + " " + str(frequency) + " " + str(step_name) + " " + \
              str(data_date) + " " + str(cycle_id)
    logging.info("Command to be executed:" + command)
    # output = ssh.execute_command(command)
    output = SystemManagerUtility().execute_command(cluster_id, command)
    logging.info("Output generated by job executor:" + str(output))


def populate_dependency_details(process_id, frequency, **kwargs):
    """This method populates dependency details"""
    logging.info("Started Populate dependency list process for process_id:" +
                 str(process_id) + " " + "and frequency:" + str(frequency))
    context = kwargs
    dag_id = str(context['dag_run'].run_id)
    populate_dependency_handler = PopulateDependency(process_id, frequency, dag_id)
    populate_dependency_handler.populate_dependency_list()
    logging.info("Completed Populate dependency list process for process_id:" +
                 str(process_id) + " " + "and frequency:" + str(frequency))


def copy_data_s3_hdfs(process_id, frequency, **kwargs):
    """This method copies data from s3 to hdfs"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    if CommonConstants.CLUSTER_MASTER_SUBMIT_FLAG == CommonConstants.ACTIVE_IND_VALUE:
        ip = get_host(process_id, ["MASTER"])
    else:
        ip = get_host(process_id, ["MASTER", "CORE"])
    cluster_id = get_cluster_id(str(process_id))
    data_date = CommonUtils().fetch_data_date_process(process_id, frequency)
    cycle_id = CommonUtils().fetch_cycle_id_for_data_date(process_id, frequency, data_date)
    logging.info("IP---------- - " + str(ip))
    # ssh = ExecuteSSHCommand(remote_host=ip, username=CommonConstants.EMR_USER_NAME,
    #                         key_file=private_key_location)
    command = "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; python3 S3ToHdfsCopyDWHandler.py" + \
              " " + str(process_id) + \
              " " + str(frequency) + " " + str(data_date) + " " + str(cycle_id)
    logging.info("Command to be executed:" + command)
    # output = ssh.execute_command(command)
    output = SystemManagerUtility().execute_command(cluster_id, command)
    logging.info("Output generated by S3toHdfsCopyDWHandler:" + str(output))


def launch_ddl_creation(process_id, frequency, **kwargs):
    """This method launches ddl creation"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    if CommonConstants.CLUSTER_MASTER_SUBMIT_FLAG == CommonConstants.ACTIVE_IND_VALUE:
        ip = get_host(process_id, ["MASTER"])
    else:
        ip = get_host(process_id, ["MASTER", "CORE"])
    cluster_id = get_cluster_id(str(process_id))
    data_date = CommonUtils().fetch_data_date_process(process_id, frequency)
    cycle_id = CommonUtils().fetch_cycle_id_for_data_date(process_id, frequency, data_date)
    logging.info("IP---------- - " + str(ip))
    # ssh = ExecuteSSHCommand(remote_host=ip, username=CommonConstants.EMR_USER_NAME,
    #                         key_file=private_key_location)
    command = "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; python3 LaunchDDLCreationHandler.py" + \
              " " + str(process_id) + " " + str(frequency) + " " + str(data_date) + \
              " " + str(cycle_id)
    logging.info("Command to be executed:" + command)
    # output = ssh.execute_command(command)
    output = SystemManagerUtility().execute_command(cluster_id,command)
    logging.info("Output generated by LaunchDDLHandler :" + str(output))


def publish_step_dw(process_id, frequency, **kwargs):
    """This method publishes dw step"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    private_key_location = configuration.get_configuration(
        [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
    if CommonConstants.CLUSTER_MASTER_SUBMIT_FLAG == CommonConstants.ACTIVE_IND_VALUE:
        ip = get_host(process_id, ["MASTER"])
    else:
        ip = get_host(process_id, ["MASTER", "CORE"])
    cluster_id = get_cluster_id(str(process_id))
    data_date = CommonUtils().fetch_data_date_process(process_id, frequency)
    cycle_id = CommonUtils().fetch_cycle_id_for_data_date(process_id, frequency, data_date)
    logging.info("IP---------- - " + str(ip))
    # ssh = ExecuteSSHCommand(remote_host=ip, username=CommonConstants.EMR_USER_NAME,
    #                         key_file=private_key_location)
    command = "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; python3 PublishDWHandler.py" + " " + \
              str(process_id) + " " + str(frequency) + " " + str(data_date) + " " + str(cycle_id)
    logging.info("Command to be executed:" + command)
    # output = ssh.execute_command(command)
    output = SystemManagerUtility().execute_command(cluster_id,command)
    logging.info("Output generated by PublishDWHandler :" + str(output))


def update_cycle_status_and_cleaup(process_id, frequency, **kwargs):
    """This method updates cycle status and clean up"""
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    data_date = CommonUtils().fetch_data_date_process(process_id, frequency)
    cycle_id = CommonUtils().fetch_cycle_id_for_data_date(process_id, frequency, data_date)
    success_status = CommonConstants.STATUS_SUCCEEDED
    CommonUtils().update_cycle_audit_entry(process_id, frequency, data_date, cycle_id,
                                           success_status)
    process_date_delete_query = "DELETE from {audit_db}.{process_date_table} where " \
                                "process_id={process_id} and" \
                                " frequency='{frequency}' ". \
        format(audit_db=audit_db, process_date_table=CommonConstants.PROCESS_DATE_TABLE,
               process_id=process_id, frequency=frequency)
    logging.debug(process_date_delete_query)
    MySQLConnectionManager().execute_query_mysql(process_date_delete_query)
    logging.info("Process date mapping entry deleted for process id:" + str(process_id) + " " +
                 "and frequency:" + str(frequency))


def launch_cluster_dw(process_id, frequency, **kwargs):
    """This method launches dw cluster"""
    try:
        logging.info("Cluster launch invoked for process_id:" + str(process_id) + " " +
                     "and frequency:" + str(frequency))
        launch_emr(str(process_id), **kwargs)
        cluster_id = get_cluster_id(str(process_id))
        logging.info("Cluster launch completed for process_id:" + str(process_id) + " " +
                     "and frequency:" + str(frequency))
        data_date = CommonUtils().fetch_data_date_process(process_id, frequency)
        logging.info("Data date is " + str(data_date))
        cycle_id = CommonUtils().fetch_cycle_id_for_data_date(process_id, frequency, data_date)
        logging.info("Cycle Id is " + str(cycle_id))
        CommonUtils().update_cycle_cluster_id_entry(process_id, frequency, data_date, cycle_id,
                                                    cluster_id)
    except Exception as exception:
        logging.error("Cluster launch failed for process_id:" + str(process_id) + " " +
                      "and frequency:" + str(frequency))
        logger.error(str(traceback.format_exc()))
        failed_status = CommonConstants.STATUS_FAILED
        CommonUtils().update_cycle_audit_entry(process_id, frequency, data_date, cycle_id,
                                               failed_status)
        raise exception


def fetch_batch_id_prelanding(cluster_id, process_id, dataset_id, workflow_id):
    """This method fetches batch id"""
    logger.info("Starting function to retrieve the Batch ID of current execution")
    configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    query = "select group_concat(batch_id separator ',') as batch_id from " + audit_db + \
            "." + CommonConstants.BATCH_TABLE + " where process_id = " + str(
        process_id) + " and cluster_id = '" + str(cluster_id) + "' and dataset_id=" + str(
        dataset_id) + " and workflow_id = '" + str(workflow_id) + "'"
    logger.info("Query to retrieve the batch ID: " + query)
    query_output = MySQLConnectionManager().execute_query_mysql(query, False)
    logger.info("Query Output : " + str(query_output))
    return query_output[0]["batch_id"]


def call_redshift_load_utility(conf_file_path, **kwargs):
    """Method to call Redshift Load Utility"""
    try:
        conf_file_path = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, conf_file_path)
        with open(conf_file_path) as data_file:
            file_str = data_file.read()
        configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       CommonConstants.ENVIRONMENT_CONFIG_FILE))
        env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"]).lower()
        file_str = file_str.replace("$$env", env)
        conf_json = json.loads(file_str)
        table_paths = conf_json['table_paths']
        redshift_utility_obj = RedShiftUtility()
        for redshift_table_name in table_paths:
            conf_json[RedShiftUtilityConstants.REDSHIFT_TARGET_TABLE_KEY] = redshift_table_name
            conf_json[RedShiftUtilityConstants.REDSHIFT_SOURCE_S3_KEY] = table_paths[redshift_table_name]
            # truncate
            conf_json[RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY] = "TRUNCATE"
            logging.info("{} operation to be executed".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY))
            operation_status = redshift_utility_obj.execute_redshift_operation(conf_json)
            if not operation_status:
                raise Exception("Redshift operation failed. Input JSON provided: {}".format(str(conf_json)))
            logging.info("{} operation executed on {}".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY,
                                                              redshift_table_name))
            # load
            conf_json[RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY] = "LOAD"
            logging.info("{} operation to be executed".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY))
            operation_status = redshift_utility_obj.execute_redshift_operation(conf_json)
            if not operation_status:
                raise Exception("Redshift operation failed. Input JSON provided: {}".format(str(conf_json)))
            logging.info("{} operation executed on {}".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY,
                                                              redshift_table_name))
        return True
    except Exception as e:
        logger.error(str(e))
        raise e


def call_import_export_utility(conf_file_path, **kwargs):
    try:
        run_command_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       "ImportExportUtility.py") + " " + "-f " + os.path.join(
            CommonConstants.AIRFLOW_CODE_PATH, "../configs/", conf_file_path)
        execute_shell_command(run_command_string)

    except Exception as e:
        logger.error(str(e))
        raise e


def tag_import_export_utility(conf_file_path, **kwargs):
    try:
        query = "select file_id,batch_id from log_file_smry where batch_id in (select max(batch_id) from log_batch_dtl " \
                "where dataset_id = 1303 and batch_status = 'SUCCEEDED');"
        logger.info("Query for retrieving the Cluster host name: " + str(query))
        cluster_ids = MySQLConnectionManager().execute_query_mysql(query, False)
        df_list = []
        for each in cluster_ids:
            file_id = each["file_id"]
            batch_id = each["batch_id"]
            file_path = "s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/publish/" \
                        "staging/TAG/final_site_tag/pt_batch_id=" + str(batch_id) + "/pt_file_id=" + str(file_id)
            table = pq.read_table(file_path)
            df = table.to_pandas()
            df_list.append(df)
            # print("final df:",len(df_list), df_list)
        final_df = pandas.concat(df_list)
        final_site_tag = final_df.drop(["row_id"], axis=1)
        write_path_csv = "final_tag.csv"
        # print("result df:",final_site_tag)
        final_site_tag.to_csv(write_path_csv, sep="|", header=False, quotechar='"', doublequote=True, escapechar='"',
                              index=False)
        cmd = "aws s3 cp final_tag.csv s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/publish/staging/" \
              "temp/TAG/final_site_tag_csv/"
        CommonUtils().execute_shell_command(cmd)
        run_command_string = "python3 " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                       "ImportExportUtility.py") + " " + "-f " + os.path.join(
            CommonConstants.AIRFLOW_CODE_PATH, "../configs/", conf_file_path)
        execute_shell_command(run_command_string)

    except Exception as e:
        logger.error(str(e))
        raise e


def call_glue_crawler(crawler_name, remove_duplicates="N", drop_tables="Y", **kwargs):
    # ########################################### call_glue_crawler ###########################
    # Purpose            :   Calling glue crawler to update the metadata
    # Input              :   crawler name, remove_duplicates
    # Output             :   NA
    # #########################################################################################
    try:
        logger.info("Starting the Glue crawler")
        configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])

        for value in crawler_name:
            execute_glue_crawler_string = "python " + os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                                   "Glue_Crawler.py -c") + \
                                          " " + value + "_" + env.lower() \
                                          + " " + "-rd " + remove_duplicates + " -dt " + drop_tables
            logger.info(execute_glue_crawler_string)
            execute_shell_command(execute_glue_crawler_string)

            logger.info(value + " Crawler execution completed successfully")
    except Exception as e:
        logger.error("Crawler execution Failed")
        logger.error(str(e))
        raise


def call_indexing_script(flag='Y', **kwargs):
    if flag == 'Y':
        try:
            logger.info("Indexing Started")
            STATUS_SUCCESS = "SUCCESS"
            STATUS_FAILED = "FAILED"
            STATUS_KEY = "status"
            ERROR_KEY = "error"
            RESULT_KEY = "result"
            EMPTY = ""
            env_conf_file_path = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "../configs/environment_config.json")
            with open(env_conf_file_path) as env_conf_file:
                env_conf = json.load(env_conf_file)
            db_host = env_conf.get("host", None)
            db_port = int(env_conf.get("port", None))
            db_username = env_conf.get("username", None)
            password_encoded = env_conf.get("password", None)
            db_password = base64.b64decode(password_encoded.encode()).decode()
            db_name = env_conf.get("database_name", None)
            connection = DatabaseUtility().create_and_get_connection(host=db_host, port=db_port,
                                                                     username=db_username,
                                                                     password=db_password, database_name=db_name)
            logging.info("database connected")
            if connection[RESULT_KEY] == STATUS_FAILED:
                raise Exception(connection[ERROR_KEY])
            conn = connection[RESULT_KEY]
            file_path = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "../configs/indexing_script.sql")
            sql_file = open(file_path, 'r')
            logger.info(file_path)
            output = DatabaseUtility().execute(conn=conn, query=sql_file.read())
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])
            logger.info(output)
        except Exception as e:
            logger.error(str(e))
            raise
    else:
        return


def update_catalog_data(file_master_id, dag_name, process_id, **kwargs):
    try:
        ti = kwargs["ti"]
        batches = ti.xcom_pull(key='batch_ids', task_ids="pre_landing_" + str(file_master_id),
                               dag_id=dag_name)
        batch_ids = batches.split(",")
        logging.info("Batch Id fetched from pre landing is - " + str(batches))

        conf_value = get_spark_configurations(file_master_id, process_id)
        cluster_id = get_cluster_id(process_id)
        ip = get_host(process_id, ["MASTER"])
        logging.info("cluster_id---------- - " + str(cluster_id))

        context = kwargs
        dag_id = str(context['dag_run'].run_id)

        steps = ["landing", "pre_dqm", "staging"]
        if CommonConstants.PUBLISH_TYPE.lower() == 'view':
            steps.append('publish_view')
        else:
            steps.append("publish_table")

        steps = ",".join(steps)
        for batch_id in batch_ids:
            processed_flag = CommonUtils().check_if_processed_batch_entry(file_master_id, batch_id, cluster_id,
                                                                          process_id)
            if processed_flag:
                break
            # crawler_obj = LaunchDDLCreationIngestionHandler(dataset_id=file_master_id, step=step)
            # crawler_obj.get_table_details()
            # crawler_obj.call_add_partition_data(batch_id=batch_id)
            if conf_value.find('--deploy-mode cluster') != -1:
                logger.info("================Executing job in Spark cluster mode================")
                ##Adding mandatory max attempt as 1
                conf_value = conf_value + " --conf spark.yarn.maxAppAttempts=1"
                conf_value = conf_value + " --name LaunchDDLCreationIngestionHandler_" + file_master_id + "_" + batch_id
                cmd = "cd {airflow_code_path};/usr/lib/spark/bin/spark-submit {conf_value} LaunchDDLCreationIngestionHandler.py " \
                      "--dataset_id {file_master_id} --step {step} --batch_id {batch_id}".format(
                    conf_value=conf_value,
                    file_master_id=file_master_id,
                    step=steps,
                    batch_id=batch_id,
                    airflow_code_path=CommonConstants.AIRFLOW_CODE_PATH
                )
                try:
                    output = SystemManagerUtility(log_type='stderr').execute_command(cluster_id,
                                                                                     cmd)
                    logging.info("Output generated by Update catalog task - " + str(output))
                except Exception as exception:
                    status_message = "Spark Submit failed due to" + str(exception)
                    logger.error(status_message)
                finally:
                    response = CommonUtils().get_yarn_app_id_status(
                        app_name="LaunchDDLCreationIngestionHandler_" + file_master_id,
                        cluster_ip=ip)
                    yarn_app_id = response["app_id"]
                    yarn_app_state = response["state"]
                    yarn_diagnostics_info = response["diagnostic_message"]
                    logger.error("Diagnostics Info:" + yarn_diagnostics_info)
                    output_cmnd = "yarn logs -applicationId " + str(yarn_app_id)
                    logger.info("This RUN was configured in cluster mode, To get full logs run the "
                                "following command on EMR {yarn_log_command}".format(
                        yarn_log_command=output_cmnd
                    ))

                    CommonUtils().set_inprogress_records_to_failed(step_name=CommonConstants.FILE_PROCESS_NAME_DQM,
                                                                   batch_id=batch_id)
                    if yarn_app_state == 'FAILED' or yarn_app_state == 'KILLED':
                        status_message = "Spark Job Failed"
                        raise Exception(status_message)

            else:
                logger.info("================Executing job in Spark client mode================")
                cmd = "cd {airflow_code_path};/usr/lib/spark/bin/spark-submit {conf_value} LaunchDDLCreationIngestionHandler.py " \
                      "--dataset_id {file_master_id} --step {step} --batch_id {batch_id}".format(
                    conf_value=conf_value,
                    file_master_id=file_master_id,
                    step=steps,
                    batch_id=batch_id,
                    airflow_code_path=CommonConstants.AIRFLOW_CODE_PATH
                )
                logger.info(cmd)
                output = SystemManagerUtility().execute_command(cluster_id,
                                                                cmd)
                logging.info("Output generated by Update catalog task - " + str(output))
    except Exception as exc:
        logger.error(str(exc))
        raise exc


def update_catalog_data_dw(process_id, frequency, **kwargs):
    try:
        """This method launches ddl creation on glue for dw process"""
        configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        cluster_id = get_cluster_id(str(process_id))
        if CommonConstants.CLUSTER_MASTER_SUBMIT_FLAG == CommonConstants.ACTIVE_IND_VALUE:
            ip = get_host(process_id, ["MASTER"])
        else:
            ip = get_host(process_id, ["MASTER", "CORE"])
        data_date = CommonUtils().fetch_data_date_process(process_id, frequency)
        cycle_id = CommonUtils().fetch_cycle_id_for_data_date(process_id, frequency, data_date)
        logging.info("IP---------- - " + str(ip))

        steps = ["staging_dw"]
        if CommonConstants.PUBLISH_TYPE.lower() == 'view':
            steps.append('publish_dw_view')
        else:
            steps.append("publish_dw_table")

        steps = ",".join(steps)
        ##Get Target Dataset ID's for process_id
        target_datasets_result = CommonUtils().get_target_datasets_by_process_id(process_id=process_id)
        logger.info("target_datasets_result:{0}".format(target_datasets_result))
        target_datasets_list = []
        for row in target_datasets_result:
            target_datasets_list.append(str(row['dataset_id']))

        target_datasets_list = ','.join(target_datasets_list)

        command = "cd " + CommonConstants.AIRFLOW_CODE_PATH + "; /usr/lib/spark/bin/spark-submit LaunchDDLCreationIngestionHandler.py " + \
                  "--dataset_id {file_master_id} --step {step} --cycle_id {cycle_id} --data_dt {data_dt}".format(
                      file_master_id=target_datasets_list,
                      step=steps,
                      cycle_id=cycle_id,
                      data_dt=data_date
                  )
        logging.info("Command to be executed:" + command)
        output = SystemManagerUtility().execute_command(cluster_id, command)
        logging.info("Output generated by LaunchDDLCreationIngestionHandler :" + str(output))
    except Exception as exc:
        logger.error(str(exc))
        raise exc
