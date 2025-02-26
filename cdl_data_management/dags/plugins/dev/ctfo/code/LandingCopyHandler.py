#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   LandingCopyHandler
#  Purpose             :   This module will perform the pre-configured steps before invoking FileCheckUtility.py.
#  Input Parameters    :   data_source, subject_area
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   19th November 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   Landing Copy Development
# ######################################################################################################################

# Library and external modules declaration
import os
import traceback
import sys
import time
import datetime
import json
import re
import copy
import datetime as dt

service_directory_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, service_directory_path)
code_dir_path = os.path.abspath(os.path.join(service_directory_path, "../"))
sys.path.insert(1, code_dir_path)

from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
from PySparkUtility import PySparkUtility
from NotificationUtility import  NotificationUtility
# all module level constants are defined here
MODULE_NAME = "LandingCopy"
PROCESS_NAME = "Copy To Landing"

USAGE_STRING = """
SYNOPSIS
    python LandingCopyHandler.py <data_source> <subject_area>

    Where
        input parameters : data_source , subject_area, batch_id

"""


class LandingCopyHandler:
    # Default constructor
    def __init__(self, file_master_id, process_id, cluster_id, workflow_id, parent_execution_context=None):
        self.exc_info = None
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": file_master_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.cluster_mode = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "cluster_mode"])
        self.keytab = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "keytab_file"])
        self.kerberos_principal = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "kerberos_principal"])
        self.pre_landing_column_list = ""
        self.process_id = process_id
        self.dag_name = ""
        self.original_column_metadata_list = ""
        self.cleaned_column_list = ""
        self.pre_landing_column_list = ""
        self.cleaned_column_list = ""
        self.delta_column_list = ""
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def trigger_notification_utility(self, email_type, process_id, dag_name, cluster_status=None, cluster_name=None,
                                     batch_ids=None, **kwargs):
        """This method triggers notification utility"""
        try:
            replace_variables = {}
            logger.info("Staring function to trigger Notification Utility")

            logger.info("Preparing the common information to be sent in all emails")
            email_types = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                "email_type_configurations"])

            notification_flag = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
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
                email_client_region = self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "ses_region"])
                logger.info("Email client Region retrieved from Configurations is: " + str(email_client_region))
                email_template_path = self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_template_path"])
                logger.info("Email template path retrieved from Configurations is: " + str(
                    email_template_path))

                # Email type specific configurations
                email_sender = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                     "email_type_configurations",
                                                                     str(email_type),
                                                                     "sender"])
                logger.info("Email Sender retrieved from Configurations is: " + str(email_sender))

                email_recipient_list = self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                     str(email_type), "recipient_list"])
                logger.info("Email Recipient retrieved from Configurations is: " + str(
                    email_recipient_list))
                email_subject = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                      "email_type_configurations",
                                                                      str(email_type),
                                                                      "subject"])
                logger.info("Email Subject retrieved from Configurations is: " + str(email_subject))
                email_template_name = self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                     str(email_type), "template_name"])
                logger.info("Email Template Name retrieved from Configurations is: "
                            + str(email_template_name))

                email_template_path = (email_template_path + "/" + email_template_name
                                       ).replace("//", "/")
                logger.info("Final Email template path is: " + str(email_template_path))

                replace_variables['batch_id'] = self.batch_ids
                replace_variables['process_id'] = self.process_id
                replace_variables['dataset_name'] = self.dataset_id
                replace_variables['original_column_metadata_list'] = self.original_column_metadata_list
                replace_variables['original_pre_landing_column_list'] = self.pre_landing_column_list.replace(
                    "," + CommonConstants.PARTITION_FILE_ID.upper(), "").replace(
                    CommonConstants.PARTITION_FILE_ID.upper(), "")
                replace_variables['cleaned_pre_landing_column_list'] = self.cleaned_column_list.replace(
                    "," + CommonConstants.PARTITION_FILE_ID.upper(), "").replace(
                    CommonConstants.PARTITION_FILE_ID.upper(), "")
                replace_variables['delta_column_list'] = self.delta_column_list.replace(
                    "," + CommonConstants.PARTITION_FILE_ID.upper(), "").replace(
                    CommonConstants.PARTITION_FILE_ID.upper(), "")
                replace_variables['updated_column_metadata_list'] = self.delta_column_list.replace(
                    "," + CommonConstants.PARTITION_FILE_ID.upper(), "").replace(
                    CommonConstants.PARTITION_FILE_ID.upper(), "") + self.cleaned_column_list.replace(
                    "," + CommonConstants.PARTITION_FILE_ID.upper(), "").replace(
                    CommonConstants.PARTITION_FILE_ID.upper(), "")
                environment = self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, 'environment'])
                replace_variables['env'] = environment
                replace_variables['file_id'] = self.file_ids
                logger.info("Variables prepared: " + str(replace_variables))
                logger.info("Final list of replacement variables for Email: " + str(replace_variables))
                # Preparing email details
                notif_obj = NotificationUtility()
                output = notif_obj.send_notification(email_client_region, email_subject, email_sender,
                                                     email_recipient_list,
                                                     email_template=email_template_path,
                                                     replace_param_dict=replace_variables)
                logger.info("Output of Email Notification Trigger: " + str(output))

        except Exception as e:
            logger.error(str(traceback.format_exc()))
            logger.error("Failed while triggering Notification Utility. Error: " + e.message)

    def variable_file_format_ingestion(self, pre_landing_location, dataset_id, delimiter):
        """
            Purpose   :   This method is used in case the header flag is set to "V" indicating a variable
                          file format in the input
            Input     :   Pre- landing location
            Output    :   Status of Execution (True/False)
        """
        try:
            status_message = "Starting function to check file schema in case of Variable File Ingestion case"
            module_name_for_spark = MODULE_NAME + dataset_id
            logger.info(status_message,extra=self.execution_context.get_context())
            spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,
                                                                              CommonConstants.HADOOP_CONF_PROPERTY_DICT)
            # Here header is set to true as as the assumption is that header would always be
            # present in case of variable file format
            pre_landing_df = spark_context.read.format("csv").\
                option("header", "true").\
                option("delimiter", delimiter)\
                .load(pre_landing_location)
            pre_landing_df_columns_list = pre_landing_df.columns
            pre_landing_df_columns_list_upper_case = [x.upper() for x in pre_landing_df_columns_list]
            self.pre_landing_column_list = ", ".join(str(x) for x in pre_landing_df_columns_list_upper_case)
            pre_landing_df_replace_spaces = [x.replace(" " , "_") for x in pre_landing_df_columns_list_upper_case ]
            pre_landing_df_special_characters_removed = [re.sub(CommonConstants.COLUMN_NAME_SPECIAL_CHARACTER_STRING,
                                                                '', x) for x in pre_landing_df_replace_spaces]
            status_message = " The list of pre_landing columns is " + str(pre_landing_df_special_characters_removed)
            self.cleaned_column_list = ", ".join(str(x) for x in pre_landing_df_special_characters_removed)
            logger.info(status_message,extra = self.execution_context.get_context())
            configured_column_list = CommonUtils().get_header(dataset_id=dataset_id)
            configured_column_list_upper_case = [x.upper() for x in configured_column_list]
            status_message = " The list of configured columns in ctl_dataset_master is: " \
                             + str(configured_column_list_upper_case)
            self.original_column_metadata_list = ", ".join(str(x) for x in configured_column_list_upper_case)
            logger.info(status_message,extra = self.execution_context.get_context())
            changed_column_list = list(set(pre_landing_df_special_characters_removed) - set(configured_column_list_upper_case))
            status_message = " The list with the difference between the two is : " \
                             + str(changed_column_list)
            logger.info(status_message,extra = self.execution_context.get_context())
            self.delta_column_list = ", ".join(str(x) for x in changed_column_list)

            if len(changed_column_list) > 0:
                status_message = "The updated column list has an additional column. Inserting this column in " \
                                 "ctl_dataset_master table for the dataset_id " + str(dataset_id)
                logger.info(status_message,extra = self.execution_context.get_context())
                column_id ,column_sequence_number = CommonUtils().get_column_information(dataset_id=dataset_id,)
                # Calling MySQL Database utility to insert the additional column
                for column in changed_column_list:
                    if column.upper() != CommonConstants.PARTITION_BATCH_ID and column.upper() !=CommonConstants.PARTITION_FILE_ID:
                        source_column_name = copy.deepcopy(column)
                        # Replace the Spaces with underscores
                        column = column.replace(" ", "_")
                        # Incrementing column_id and column_sequence_number

                        column_id = column_id + 1
                        if column_sequence_number is None:
                            column_sequence_number=0
                        column_sequence_number = column_sequence_number + 1

                        # Remove any other special charachters except alphanumeric characters
                        # The string sorted in common constants is the list of values that we want to
                        # retain in the incoming column name
                        column = re.sub(CommonConstants.COLUMN_NAME_SPECIAL_CHARACTER_STRING, '', column)
                        query = "insert into {audit_db}.{ctl_column_metadata} (column_id,  table_name,  dataset_id,  " \
                                "column_name, column_data_type, column_date_format, column_description, " \
                                "source_column_name, column_sequence_number,column_tag) " \
                                "values({column_id},{table_name},{dataset_id},'{column_name}','{column_data_type}','" \
                                "{column_date_format}','{column_description}','{source_column_name}','" \
                                "{column_sequence_number}','{column_tag}')" \
                                .format(audit_db=self.audit_db,
                                    ctl_column_metadata=CommonConstants.COLUMN_METADATA_TABLE,
                                    column_id=column_id, table_name="NULL", dataset_id = str(dataset_id),
                                    column_name=column.upper(), column_data_type=CommonConstants.STRING_DATATYPE,
                                    column_date_format = "NULL", column_description = "NULL",
                                    source_column_name=source_column_name.upper(),
                                    column_sequence_number = column_sequence_number, column_tag="NULL")
                        status_message = "The Query is:" + str(query)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        result = MySQLConnectionManager().execute_query_mysql(query)
                        status_message = "A row was inserted in ctl_column_metadata for the column {column_name} with the " \
                                         "column_sequence_number {column_sequence_number}" \
                                         .format(column_name=column ,column_sequence_number=str(column_sequence_number))
                        logger.info(status_message, extra=self.execution_context.get_context())
            else:
                status_message = "There was no update in the schema. Thus no change would be made in the ctl_column_metadadta table"
                logger.info(status_message,extra = self.execution_context.get_context())
            replace_variables = {}
            replace_variables['batch_id'] = self.batch_ids
            replace_variables['process_id'] = self.process_id
            replace_variables['dataset_name'] = self.dataset_id
            replace_variables['original_column_metadata_list'] = self.original_column_metadata_list
            replace_variables['original_pre_landing_column_list'] = self.pre_landing_column_list
            replace_variables['cleaned_pre_landing_column_list'] = self.cleaned_column_list
            replace_variables['delta_column_list'] = self.delta_column_list
            replace_variables['updated_column_metadata_list'] = self.delta_column_list + self.cleaned_column_list
            environment = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, 'environment'])
            replace_variables['env'] = environment
            replace_variables['file_id'] = self.file_ids
            logger.info("Variables prepared: " + str(replace_variables))
            logger.info("Final list of replacement variables for Email: " + str(replace_variables))
            temp_file_location = str('/tmp/').__add__(dt.datetime.fromtimestamp(time.time())
                                                      .strftime('%Y%m%d%H%M%S')).__add__('_').__add__(str(file_master_id))\
                .__add__("_variable_file_ingestion." + "txt")
            with open (temp_file_location , 'w') as file_pointer:
                file_pointer.write(json.dumps(replace_variables))

        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            status_message = "Error occured during the execution of Variable File Format Ingestion:"
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the file check
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def execute_copy_to_landing(self, file_master_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):
        status_message = ""
        curr_batch_id = batch_id
        process_info = {}
        landing_flag = False
        try:
            status_message = "Executing File Copy function of module LandingCopyHandler"
            logger.info(status_message, extra=self.execution_context.get_context())
            # Input Validations
            process_info = {CommonConstants.BATCH_ID: batch_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.DATASET_ID: file_master_id,
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id
                            }
            if file_master_id is None:
                raise Exception('Dataset ID is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"file_master_id": file_master_id,
                                                "batch_id": batch_id})
            logger.info(status_message, extra=self.execution_context.get_context())
            # Get pre-landing location, landing and data_set_id from MySQL table for given data source and subject area
            data_set_info = CommonUtils().get_dataset_information_from_dataset_id(file_master_id)
            file_format = data_set_info['file_format']
            pre_landing_location = data_set_info['pre_landing_location']
            pre_landing_location = str(pre_landing_location).rstrip('/')
            s3_landing_location = data_set_info['s3_landing_location']
            s3_landing_location = str(s3_landing_location).rstrip('/')

            dataset_id = file_master_id
            data_delimiter = data_set_info['file_field_delimiter']
            header_flag = str(data_set_info['header_available_flag'])
            self.header_flag = header_flag
            self.dataset_id = dataset_id
            self.batch_ids = batch_id_list
            row_id_exclusion_flag = str(data_set_info['row_id_exclusion_flag'])
            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)
            process_info = {}
            landing_flag = False
            file_not_processed_flag = False
            file_failed_flag = False
            self.file_ids = ", ".join(str(file_id['file_id']) for file_id in file_details)
            for file_detail in file_details:
                file_id = file_detail['file_id']
                file_name = file_detail['file_name']
                process_info = {CommonConstants.BATCH_ID: batch_id, CommonConstants.FILE_ID: file_id,
                                CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                                CommonConstants.FILE_NAME: file_name,
                                "start_time": "NOW()",
                                "end_time": "NULL",
                                CommonConstants.DATASET_ID: dataset_id,
                                CommonConstants.CLUSTER_ID: cluster_id,
                                CommonConstants.WORKFLOW_ID: workflow_id,
                                'process_id': process_id
                                }
                audit_db = self.audit_db
                # Logic added for restartability
                query_str = "Select file_process_status from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and batch_id={batch_id} and file_id = {file_id} and file_process_name='{landing}'"
                status_message = "The executed query is " + str(query_str)
                logger.info(status_message, extra=self.execution_context.get_context())
                query = query_str.format(audit_db=self.audit_db,
                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                         dataset_id=file_master_id,
                                         cluster_id=cluster_id,
                                         workflow_id=workflow_id,
                                         process_id=process_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         landing=CommonConstants.FILE_PROCESS_NAME_LANDING_COPY)
                result = MySQLConnectionManager().execute_query_mysql(query)
                status_message = "The result from query is " + str(result)
                logger.info(status_message,extra =self.execution_context.get_context())
                file_process_status = ""
                if result:
                    file_process_status = str(result[0]['file_process_status'])
                if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:
                    if file_process_status == "":
                        file_not_processed_flag = True
                        # Creating process log entry
                        CommonUtils().create_process_log_entry(self.audit_db, process_info)
                        status_message = "Created process log entry for file id " + str(file_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                    if file_process_status == CommonConstants.STATUS_FAILED:
                        file_failed_flag = True

            try:
                if file_failed_flag or file_not_processed_flag:
                    if s3_landing_location == 'None':
                        landing_flag = True
                        status_message = "Landing Location is not configured"
                        raise Exception(status_message)
                    else:
                        file_source_url = pre_landing_location.__add__('/').__add__(
                            CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id))
                        file_target_url = s3_landing_location.__add__('/').__add__(
                            CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id))
                        query = "Select spark_config from {audit_db}.{emr_process_workflow_map_table} where " \
                                "dataset_id='{dataset_id}' and process_id='{process_id}'".format(
                            audit_db=self.audit_db,
                            emr_process_workflow_map_table=CommonConstants.EMR_PROCESS_WORKFLOW_MAP_TABLE,
                            dataset_id=dataset_id, process_id=process_id)
                        status_message = "Query to retrieve the Spark Configurations: " + str(query)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        spark_config = MySQLConnectionManager().execute_query_mysql(query, False)
                        status_message = "Query Result: " + str(spark_config)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        conf_value = ""
                        if len(spark_config) != 0:
                            conf_value = spark_config[0]['spark_config']
                            conf_value = conf_value.replace(
                                '--master yarn --deploy-mode cluster --files $(cat /home/hadoop/temp_file) --py-files $(cat /home/hadoop/temp_py)',
                                '')
                            conf_value = conf_value.strip()
                    if (header_flag).upper() == CommonConstants.VARIABLE_FILE_FORMAT_TYPE_KEY:
                        variable_file_format_result = self. \
                            variable_file_format_ingestion(pre_landing_location=file_source_url,
                                                           dataset_id= dataset_id ,delimiter=data_delimiter)

                    # Invoke Parquet Utility
                    if self.cluster_mode != 'EMR':
                        if (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                            spark_submit_command = '/usr/lib/spark/bin/spark-submit ' \
                                                   '--packages com.crealytics:spark-excel_2.11:0.13.5 ' + \
                                                   str(conf_value) + ' --keytab ' \
                                                   + str(self.keytab) + ' --principal ' \
                                                   + str(self.kerberos_principal) +' ' +CommonConstants.AIRFLOW_CODE_PATH+ '/ParquetUtility.py '
                        else:
                            spark_submit_command = '/usr/lib/spark/bin/spark-submit ' + str(conf_value) + ' --keytab ' \
                                                   + str(self.keytab) + ' --principal ' \
                                                   + str(self.kerberos_principal)+' ' +CommonConstants.AIRFLOW_CODE_PATH+ '/ParquetUtility.py '

                    else:
                        if (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                            spark_submit_command = '/usr/lib/spark/bin/spark-submit ' \
                                                   '--packages com.crealytics:spark-excel_2.11:0.13.5 ' + str(
                                                    conf_value) +' ' +CommonConstants.AIRFLOW_CODE_PATH+ '/ParquetUtility.py '

                        else:
                            spark_submit_command = '/usr/lib/spark/bin/spark-submit ' + str(
                                conf_value) +' ' +CommonConstants.AIRFLOW_CODE_PATH+ '/ParquetUtility.py '

                    landing_copy_command = str(spark_submit_command) + str(file_source_url) + ' ' + str(
                        file_target_url) + ' ' + str(row_id_exclusion_flag) + ' ' + str(
                        dataset_id) + ' \\' + str(
                        data_delimiter) + ' ' + str(header_flag) + ' ' + str(file_format)
                    CommonUtils().execute_shell_command(str(landing_copy_command))
                    process_info[CommonConstants.BATCH_ID] = curr_batch_id
                    process_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                    process_info['status_id'] = CommonConstants.SUCCESS_ID
                    record_count = PySparkUtility(self.execution_context).get_parquet_record_count(file_target_url)
                    process_info['record_count'] = record_count
                    process_info['message'] = "Landing Copy completed successfully"
                    CommonUtils().update_process_log_by_batch(self.audit_db, process_info)
                    status_message = "Updated process log entry for all files in batch id" + str(batch_id)
                    logger.info(status_message, extra=self.execution_context.get_context())

                    status_message = "Completed Landing copy for batch " + str(batch_id)
                    logger.info(status_message, extra=self.execution_context.get_context())
                else:
                    status_message = ' Batch ID : ' + str(
                        batch_id) + ' is already processed'
                    logger.info(status_message, extra=self.execution_context.get_context())

            except Exception as e:
                if landing_flag == True:
                    process_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SKIPPED
                    process_info[CommonConstants.BATCH_ID] = batch_id
                    process_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime(
                        '%Y-%m-%d %H:%M:%S')
                    process_info['status_id'] = CommonConstants.FAILED_ID

                    process_info['message'] = "Skipped task since Landing HDFS location is not configured"
                    CommonUtils().update_process_log_by_batch(self.audit_db, process_info)
                else:
                    process_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    process_info[CommonConstants.BATCH_ID] = batch_id
                    process_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime(
                        '%Y-%m-%d %H:%M:%S')
                    process_info['status_id'] = CommonConstants.FAILED_ID
                    process_info['record_count'] = str(0)
                    process_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace(
                        '"', '')
                    CommonUtils().update_process_log_by_batch(self.audit_db, process_info)
                    batches = batch_id_list.split(",")
                    for batch in batches:
                        process_info[CommonConstants.BATCH_ID] = batch
                        process_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        process_info['status_id'] = CommonConstants.FAILED_ID
                        process_info['record_count'] = str(0)
                        process_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                          "").replace(
                            '"', '')

                        CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, process_info)
                        CommonUtils().update_batch_status(self.audit_db, process_info)
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(e)
                    self.execution_context.set_context({"traceback": error})
                    logger.error(error, extra=self.execution_context.get_context())
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    self.execution_context.set_context({"traceback": ""})
                    self.exc_info = sys.exc_info()
                    raise e

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            process_info[CommonConstants.BATCH_ID] = curr_batch_id
            process_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            process_info['status_id'] = CommonConstants.FAILED_ID
            process_info['record_count'] = str(0)
            process_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"', '')
            CommonUtils().update_process_log_by_batch(self.audit_db, process_info)
            batches = batch_id_list.split(",")
            for batch in batches:
                process_info[CommonConstants.BATCH_ID] = batch
                process_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                process_info['status_id'] = CommonConstants.FAILED_ID
                process_info['record_count'] = str(0)
                process_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"',
                                                                                                                   '')
                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, process_info)
                CommonUtils().update_batch_status(self.audit_db, process_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())

            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

        finally:
            status_message = " Checking header flag and sending notification if header is set for variable " \
                             "file ingestion"
            logger.info(status_message, extra=self.execution_context.get_context())

            if self.header_flag == CommonConstants.VARIABLE_FILE_FORMAT_TYPE_KEY:
                notification_utility_result = self.trigger_notification_utility(email_type='variable_file_ingestion',
                                                                                process_id=self.process_id,
                                                                                dag_name=self.dag_name,
                                                                                batch_ids=self.batch_ids)

                status_message = 'Notification mail sent out successfully'
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                status_message = "No Email sent as header flag was set to " + str(self.header_flag)
                logger.info(status_message, extra=self.execution_context.get_context())

if __name__ == '__main__':
    file_master_id = sys.argv[1]
    cluster_id = sys.argv[2]
    workflow_id = sys.argv[3]
    process_id = sys.argv[4]
    batch_id = sys.argv[5]
    batch_id_list = sys.argv[6]
    if file_master_id is None or cluster_id is None or workflow_id is None or process_id is None or batch_id is None \
            or batch_id_list is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    batch_id = str(batch_id).rstrip("\n\r")
    landing_copy_handler = LandingCopyHandler(file_master_id, cluster_id, workflow_id, process_id)
    landing_copy_handler.execute_copy_to_landing(file_master_id, cluster_id, workflow_id, process_id, batch_id,
                                                 batch_id_list)
    status_msg = "Completed execution for Parquet Conversion and copy to landing zone for batch : " + str(batch_id)
    sys.stdout.write("batch_id=" + str(batch_id))

