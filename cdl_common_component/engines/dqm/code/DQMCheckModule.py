#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

#Importing all required python packages
import sys
import os
import json
import traceback
from datetime import datetime
from datetime import date
from LogSetup import logger
sys.path.insert(0, os.getcwd())
import ForFile
import ForQuery
from MySQLConnectionManager import MySQLConnectionManager
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from NotificationUtility import NotificationUtility
import NotificationUtilityConstants
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor
spark_job_name = "DQM on DW"
spark = SparkSession.builder.appName(spark_job_name).enableHiveSupport().getOrCreate()

sys.path.insert(0, os.getcwd())

MODULE_NAME = "DQMCheckModule"

"""
Module Name         :   DQMCheckModule
Purpose             :   This module will perform Data Quality Checks on DW tables.
Input Parameters    :   process id, frequency , process_name
Output Value        :   None
Pre-requisites      :   None
Last changed on     :   10th May 2021
Last changed by     :   Sukhwinder Singh
Reason for change   :   Updated code to include multiprocessing and logger
"""

class ExecuteDqmCheck:

    def __init__(self, process_id, frequency, master_process_name = None):
        self.process_id = process_id
        self.frequency = frequency
        self.master_process_name = master_process_name
        self.configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.dqm_err_loc = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "dw_dqm_error_location"])
        self.aws_region = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "aws_region"])
        self.data_date = CommonUtils().fetch_data_date_process(self.process_id,self.frequency)
        self.cycle_id =  CommonUtils().fetch_cycle_id_for_data_date(self.process_id,self.frequency,self.data_date)
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_id": self.process_id})
        self.execution_context.set_context({"process_name": self.master_process_name})
        self.execution_context.set_context({"frequency": self.frequency})
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def send_dqm_notification(self, status=None, table_name=None,dataset_name=None, fail_perc=None,
                              dqm_error_location=None, detailed_status=None, qc_id=None, qc_type=None, qc_query=None,
                              threshold_type=None, threshold_value=None, curr_cycle_id=None,
                              data_date=None,status_message=None,dataset_id=None):
        notify = NotificationUtility()

        today = date.today()
        replace_param_dict = {"status": status,    "process_id": str(self.process_id),
                              "qc_date": str(today), "detailed_status": detailed_status,   "cycle_id": curr_cycle_id,
                              "data_date": data_date }
        email_type = CommonConstants.DQM_DW_EMAIL_CONF_KEY
        template_name = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", str(email_type), "template_name"])
        code_path = CommonConstants.AIRFLOW_CODE_PATH
        email_template_path = os.path.join(code_path, template_name)

        status_message = "Starting to fetch email template for DQMs on DW datasets:" + str(self.process_id) + " " \
                         + " process name : " + str(self.master_process_name) + " " \
                         + "and frequency:" + str(self.frequency)
        logger.debug(status_message, extra=self.execution_context.get_context())
        sender = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", str(email_type), "sender"])
        recipient_list = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", str(email_type), "recipient_list"])
        subject_string = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", str(email_type), "subject"])
        env = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])

        replaceables = {"env": str(env), "status": str(status)}
        if "$" in subject_string:
            for param in list(replaceables):
                subject_string = subject_string.replace("$$" + param + "$$", replaceables[param])

        email_notification_res = notify.send_notification(ses_region=self.aws_region, subject=subject_string, sender=sender,
                                 recipient_list=recipient_list, email_template=email_template_path,
                                 replace_param_dict=replace_param_dict)
        if email_notification_res[NotificationUtilityConstants.STATUS_KEY] == NotificationUtilityConstants.STATUS_SUCCESS:
            logger.info(str(email_notification_res[NotificationUtilityConstants.RESULT_KEY]), extra=self.execution_context.get_context())
        else:
            raise Exception(email_notification_res[NotificationUtilityConstants.ERROR_KEY])

    def get_qc_rows(self):
        try:
            query_string = ("select * from {audit_db}.{ctl_dqm_master_dw} where process_id={process_id} and "
                            "lower(active_flag)='y' ").format(
                ctl_dqm_master_dw=CommonConstants.DW_DQM_CONTROL_TABLE,
                audit_db=self.audit_db,
                process_id=self.process_id)
            result = MySQLConnectionManager().execute_query_mysql(query_string)

        except Exception as exception:
            status_message = "Error while querying dqm control table for qc details configured"
            logger.error(status_message)
            raise exception

        if result is None or result == []:
            status_message = "No QCs configured for process ID: " + str(self.process_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
        else:
            try:
                return result
            except Exception as exception:
                raise exception

    def execute_dqm_check(self, each_data):
        response = dict()
        notification_dict = dict()
        process_id = self.process_id
        try:
            row = json.loads(json.dumps(each_data, default = str))
            if row:
                update_check = 0
                dataset_id = row["dataset_id"]
                qc_id = row["qc_id"]
                qc_type = row["qc_type"]
                query_input = row["qc_query"]
                qc_param = json.loads(row["qc_param"])
                criticality = row["criticality"]
                threshold_type = (row["threshold_type"]).lower()
                qc_threshold = row["threshold_value"]
                # qc_threshold = ("%.2f" % qc_threshold)
                error_table_name = row["error_table_name"]
                status_message = row["status_message"]
                application_id = row["application_id"]
                qc_create_by = row["insert_by"]
                qc_create_date = row["insert_date"]
                geography_id=row["geography_id"]
                start_time = datetime.now()
                template_file_location = CommonConstants.AIRFLOW_CODE_PATH

                # validating certain inputs
                if qc_threshold is None or qc_threshold == "" or qc_threshold == " ":
                    raise Exception

                # retrieving query from file/table
                if qc_type.lower() == "query":
                    query = query_input
                    query = query.replace('"', "'").replace(';', '')
                elif qc_type.lower() == "file":
                    status_message = "qc query is a python file"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    file_name = query_input
                    path_to_file = os.path.join(template_file_location, file_name)
                    q = open(path_to_file, "r")
                    query = ""
                    for x in q:
                        query = query + x
                        query = query.replace('"', "'").replace(';', '')
                    q.close()
                else:
                    status_message = "QC type configured is Invalid - Please provide either query/file type configuration"
                    logger.debug(status_message, extra=self.execution_context.get_context())

                # replacing parameters in the configured query
                qc_query = ""
                if "$" in query:
                    for param in list(qc_param):
                        qc_query = query.replace("$" + param, qc_param[param])
                else:
                    qc_query = query
                cycle_id = self.cycle_id
                status_message = "The current latest successful cycle ID is: " + str(cycle_id)
                logger.debug(status_message, extra=self.execution_context.get_context())

                # Inserting initial log entries into summary table
                qc_message = "QC check is in progress for QC ID: " + str(qc_id)
                qc_status = CommonConstants.CYCLE_STATUS_VALUE
                status_message = "QC message : " + str(qc_message)
                logger.debug(status_message, extra=self.execution_context.get_context())
                insert_log_query = "insert into {audit_db}.{log_dqm_smry_dw}(application_id, process_id, dataset_id, qc_id, qc_type," \
                                   "qc_query, qc_param, criticality, threshold_type, threshold_value, error_table_name, " \
                                   "error_count, cycle_id, qc_status, qc_message, qc_start_time, qc_end_time, " \
                                   "qc_create_by, qc_create_date, geography_id)" \
                                   " values({application_id},{process_id},{dataset_id},{qc_id},'{qc_type}'," \
                                   "'{qc_query}','{qc_param}','{criticality}','{threshold_type}',{threshold_value}," \
                                   "'{error_table_name}',{error_count},{cycle_id}," \
                                   "'{qc_status}','{qc_message}','{start_time}','0000-00-00 00:00:00', '{qc_create_by}'," \
                                   " '{qc_create_date}', {geography_id})"
                insert_log_query_string = insert_log_query.format(audit_db=self.audit_db,
                                                                  log_dqm_smry_dw=CommonConstants.DW_DQM_LOG_TABLE,
                                                                  application_id=application_id,
                                                                  process_id=process_id,
                                                                  dataset_id=dataset_id,
                                                                  qc_id=qc_id,
                                                                  qc_type=qc_type,
                                                                  qc_query=qc_query,
                                                                  qc_param=json.dumps(qc_param),
                                                                  criticality=criticality,
                                                                  threshold_type=threshold_type,
                                                                  threshold_value=qc_threshold,
                                                                  error_table_name=error_table_name,
                                                                  error_count='NULL',
                                                                  cycle_id=cycle_id,
                                                                  qc_status=qc_status, qc_message=status_message,
                                                                  start_time=start_time,
                                                                  qc_create_by=qc_create_by,
                                                                  qc_create_date=qc_create_date,
                                                                  geography_id=geography_id)
                MySQLConnectionManager().execute_query_mysql(insert_log_query_string)

                # fetching compute table name from dataset master
                dataset_query = (
                    "select {table_name},{dataset_name}, table_s3_path, table_hdfs_path from "
                    "{audit_db}.{dataset_master} where dataset_id={dataset_id}")\
                    .format(table_name = CommonConstants.TABLE_NAME_COL,
                            dataset_name = CommonConstants.DATASET_NAME_COL,
                            dataset_master = CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                            audit_db=self.audit_db,
                            dataset_id=dataset_id)
                dataset_table_name = MySQLConnectionManager().execute_query_mysql(dataset_query)
                table_name = dataset_table_name[0][CommonConstants.TABLE_NAME_COL]
                dataset_name = dataset_table_name[0][CommonConstants.DATASET_NAME_COL]

                # if no error table is provided - we pick up the table name as error table name
                if error_table_name is None or error_table_name == "" or error_table_name == " ":
                    error_table_name = table_name

                # obtaining dqm error location
                dqm_error_location = str(self.dqm_err_loc)
                if dqm_error_location is None:
                    raise Exception("The 'dw_dqm_error_location' is not provided in environment_params file")

                val_path = dqm_error_location + "/" + str(error_table_name) + "/pt_qc_id=" + str(
                    qc_id) + "/data_date=" + str(self.data_date) + "/pt_cycle_id=" + str(cycle_id)

                # passing required details to sql template to obtain qc result
                if qc_type.lower() == "query":
                    status_message = "Query type input being executed"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    check = ForQuery.dqm(spark, val_path, qc_threshold, qc_query, threshold_type, status_message)
                    check_json = json.loads(json.dumps(check))
                elif qc_type.lower() == "file":
                    check = ForFile.dqm(spark, val_path, qc_threshold, qc_query, threshold_type, status_message)
                    check_json = json.loads(json.dumps(check))
                    logger.info(check_json, extra=self.execution_context.get_context())

                # updating logs for this qc ID
                qc_end_time = datetime.now()
                qc_message = "QC check is completed for QC ID: " + str(qc_id)
                status_message = "QC message : " + str(qc_message)
                logger.debug(status_message, extra=self.execution_context.get_context())

                update_log_string = "update {audit_db}.{log_dqm_smry_dw} set error_count={error_record_count}, " \
                                    "qc_status='{qc_status}', qc_end_time='{qc_end_time}', qc_message = '{qc_message}' " \
                                    " where qc_id={qc_id} " \
                                    "and cycle_id={cycle_id} and qc_status='{run_status}' and " \
                                    "process_id={process_id} and dataset_id={dataset_id}"
                update_log_query = update_log_string\
                    .format(error_record_count=check_json['error_record_count'],
                            audit_db=self.audit_db,
                            log_dqm_smry_dw=CommonConstants.DW_DQM_LOG_TABLE,
                            qc_status=check_json['validation_status'],
                            qc_end_time=qc_end_time, qc_message = qc_message,
                            qc_id=qc_id,
                            cycle_id=cycle_id,
                            run_status=CommonConstants.CYCLE_STATUS_VALUE, process_id=process_id,
                            dataset_id=dataset_id)

                MySQLConnectionManager().execute_query_mysql(update_log_query)
                update_check = 1
                status_message = "QC Check status update: " + str(check_json)
                logger.debug(status_message, extra=self.execution_context.get_context())

                if criticality.lower() == "c" and check_json['validation_status'].lower() == "failed":
                    detailed_status = "QC check for process ID " + str(process_id) + " , dataset_id " + str(
                        dataset_id) + " and QC ID " + str(qc_id) + " with criticality '" + str(criticality) + \
                                    "' and threshold value/percentage '" + str(qc_threshold) + "' has failed. " + \
                             str(check_json['error_record_count']) + " records have failed."
                    status_message = str(detailed_status)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                    notification_dict["status"] = check_json['validation_status']
                    notification_dict["detailed_status"] = detailed_status
                    notification_dict["curr_cycle_id"] = str(cycle_id)
                    notification_dict["data_date"] = str(self.data_date)
                    notification_dict["status_message"] = str(status_message)
                    notification_dict["dataset_id"] = str(dataset_id)
                    response[CommonConstants.RESULT_KEY] = notification_dict
                    error = "Critical QC check has been failed for process ID " + str(
                        process_id) + ", dataset_id " + str(dataset_id) + " and QC ID " + str(qc_id)
                    response[CommonConstants.ERROR_KEY] = error
                    return response
                else:
                    detailed_status = "QC check for process ID " + str(process_id) + " , dataset_id " + str(
                        dataset_id) + " and QC ID " + str(qc_id) + " with criticality '" + str(criticality) + \
                                    "' and threshold value/percentage '" + str(qc_threshold) + "' has passed. " + \
                             str(check_json['error_record_count']) + " records have failed."
                    status_message = str(detailed_status)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                notification_dict["status"] = check_json['validation_status']
                notification_dict["detailed_status"] = detailed_status
                notification_dict["curr_cycle_id"] = str(cycle_id)
                notification_dict["data_date"] = str(self.data_date)
                notification_dict["status_message"] = str(status_message)
                notification_dict["dataset_id"] = str(dataset_id)
                response[CommonConstants.RESULT_KEY] = notification_dict
                return response

        except Exception as exception:
            qc_end_time = datetime.now()
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            if update_check==0:
                qc_message = "QC check is failed for QC ID: " + str(qc_id)
                status_message = "QC message : " + str(qc_message)
                logger.debug(status_message, extra=self.execution_context.get_context())

                update_log_string = "update {audit_db}.{log_dqm_smry_dw} set error_count={err_rec}, qc_status='NOT EXECUTED', " \
                                    "qc_end_time='{qc_end_time}', qc_message ='{qc_message}' where qc_id={qc_id} and " \
                                    "cycle_id={cycle_id} and qc_status='{run_status}' " \
                                    "and process_id={process_id} and dataset_id={dataset_id}"
                update_log_query = update_log_string\
                    .format(qc_end_time=qc_end_time,
                            qc_message = qc_message,
                            audit_db=self.audit_db,
                            log_dqm_smry_dw=CommonConstants.DW_DQM_LOG_TABLE,
                            qc_id=qc_id,
                            cycle_id=cycle_id,
                            run_status=CommonConstants.CYCLE_STATUS_VALUE,
                            process_id=process_id,
                            dataset_id=dataset_id,
                            err_rec=0)

                MySQLConnectionManager().execute_query_mysql(update_log_query)
                status_message = "Error while performing QC check"
                logger.error(status_message, extra=self.execution_context.get_context())
            status = CommonConstants.STATUS_FAILED
            CommonUtils().update_cycle_audit_entry(self.process_id,self.frequency,self.data_date,self.cycle_id,status)
            logger.error(str(exception), extra=self.execution_context.get_context())
            error = "Error occurred while executing the QC check for process ID " + str(self.process_id) + ", dataset_id " + str(
                dataset_id) + " and QC ID " + str(qc_id) + "ERROR is: {error}".format(
                error=str(exception) + '\n' + str(traceback.format_exc()))
            response[CommonConstants.RESULT_KEY] = "ERROR"
            response[CommonConstants.ERROR_KEY] = error
            return response

if __name__ == '__main__':
        PROCESS_ID = sys.argv[1]
        FREQUENCY = sys.argv[2]
        MASTER_PROCESS_NAME = sys.argv[3]
        DQM = ExecuteDqmCheck(PROCESS_ID,FREQUENCY,MASTER_PROCESS_NAME)
        final_list = DQM.get_qc_rows()
        STATUS_MSG = "Completed fetching DQMs configured for DW datasets"
        sys.stdout.write(STATUS_MSG)
        if final_list:
            try:
                executor = ThreadPoolExecutor()
                results = executor.map(DQM.execute_dqm_check, final_list)
                detailed_status = ""
                critical_qc_count = 0
                error_qc_list = []
                status = ""
                critical_status = ""
                curr_cycle_id = ""
                data_date = ""
                for result in results:
                    if result[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED and result[CommonConstants.RESULT_KEY] == "ERROR":
                        raise Exception(result[CommonConstants.ERROR_KEY])
                    elif result[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_SUCCESS:
                        notification_details = result[CommonConstants.RESULT_KEY]
                        detailed_status =  detailed_status + notification_details["detailed_status"] + "<br>"
                        status = notification_details["status"]
                        curr_cycle_id = notification_details["curr_cycle_id"]
                        data_date = notification_details["data_date"]
                    elif result[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED and \
                         result[CommonConstants.RESULT_KEY] != "ERROR":
                        critical_qc_count += 1
                        notification_details = result[CommonConstants.RESULT_KEY]
                        detailed_status = detailed_status + notification_details["detailed_status"] + "<br>"
                        critical_status = notification_details["status"]
                        curr_cycle_id = notification_details["curr_cycle_id"]
                        data_date = notification_details["data_date"]
                        error_qc_list.append(result[CommonConstants.ERROR_KEY])
                if critical_qc_count != 0:
                    DQM.send_dqm_notification(status=critical_status, detailed_status=detailed_status,
                                              curr_cycle_id=curr_cycle_id,
                                                  data_date=data_date)
                    raise Exception(error_qc_list)
                else:
                    DQM.send_dqm_notification(status=status , detailed_status=detailed_status,
                                              curr_cycle_id=curr_cycle_id,
                                                  data_date=data_date)
            except Exception as e:
                raise e

