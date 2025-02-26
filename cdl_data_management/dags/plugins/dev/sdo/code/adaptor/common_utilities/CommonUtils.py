#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

######################################################Module Information################################################
#   Module Name         :   CommonUtils
#   Purpose             :   Common util functions used by all utility modules
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate and object of this class and call the class functions
#   Predecessor module  :   This module is  a generic module
#   Successor module    :   NA
#   Last changed on     :   11 September 2020
#   Last changed by     :   Himanshu Khatri
#   Reason for change   :   Adding new common utility module for Common Components Project
########################################################################################################################

from datetime import datetime
import re
import traceback
import random
import ntpath
from urllib.parse import urlparse
import json
import getpass
import boto3
import boto
import subprocess
import os
import time
import sys
#os.environ["HADOOP_HOME"] = "/usr/lib/hadoop"
#import pydoop.hdfs
#import pydoop.hdfs.path as hpath
from pyhive import hive
#from awscli.clidriver import create_clidriver

sys.path.insert(0, os.getcwd())
from Pattern_Validator import PatternValidator
from ExecutionContext import ExecutionContext
from LogSetup import logger
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants

import math
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCESS"
ERROR_KEY = "error"
NONE_KEY = "none"
output_status_dict = dict()

# all module level constants are defined here
MODULE_NAME = "CommonUtils"


class CommonUtils(object):
    # Initializer method of the Common Functions
    def __init__(self, execution_context=None):
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        if execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.hive_port = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "hive_port"])
        self.private_key_location = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
        self.cluster_mode = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "cluster_mode"])

    ########################################## get_hdfs_files_list #################################################
    # Purpose   :   This method is used to get the list of files present in the HDFS source folder
    # Input     :   The base path of the folder, current path for traversal, list of files with relative paths and the
    #                 other with absolute paths
    # Output    :   Return True if the traversal was successful else return False
    # ##################################################################################################################
    #def get_hdfs_files_list(self, hdfs_path):
        #status_message = ""
        #absolute_file_list = []
        #try:
            #status_message = "Starting function to fetch HDFS files list."
            #logger.debug(status_message, extra=self.execution_context.get_context())
            #if hpath.isdir(hdfs_path):
                #curr_files_list = pydoop.hdfs.ls(hdfs_path)
                #for file_name in curr_files_list:
                   # if hpath.isdir(file_name):
                        #self.get_hdfs_files_list(file_name)
                    #else:
                       #absolute_file_list.append(file_name)
            #else:
                #absolute_file_list.append(hdfs_path)
            #return absolute_file_list

        #except KeyboardInterrupt:
            #raise KeyboardInterrupt

        #except Exception as e:
            #error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    #" ERROR MESSAGE: " + str(traceback.format_exc())
            #self.execution_context.set_context({"traceback": error})
            #logger.error(status_message, extra=self.execution_context.get_context())
            #self.execution_context.set_context({"traceback": ""})
            #raise e

    def get_user_name(self):
        """
            Purpose   :   This method is used to get logged in unix user name
            Input     :   NA
            Output    :   user name
        """
        command = 'who am i | cut -d " " -f 1'
        user_output_command = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        command_output, command_error = user_output_command.communicate()
        user_name = command_output.strip().decode("utf-8")
        logger.debug(user_name, extra=self.execution_context.get_context())
        return user_name

    ########################################## clean hdfs directory #################################################
    # Purpose   :   This method is used to get the list of files present in the HDFS source folder
    # Input     :   hdfs directory path
    # Output    :   NA
    # ##################################################################################################################
    def clean_hdfs_directory(self, hdfs_dir=None):
        status_message = ""
        absolute_file_list = []
        try:
            status_message = "Starting function to clean HDFS directory."
            self.execution_context.set_context({"HDFS_DIR": hdfs_dir})
            logger.debug(status_message, extra=self.execution_context.get_context())
            #if hpath.exists(hdfs_dir) and pydoop.hdfs.ls(hdfs_dir):
                #pydoop.hdfs.rmr(hdfs_dir + '/*')
            status_message = "Completed function to clean HDFS directory."
            logger.debug(status_message, extra=self.execution_context.get_context())
            return True

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    # ############################################### get_sequence #################################################
    # Purpose   :   This method is used to get the next value from an already created sequence in the automation log
    #               database
    # Input     :   Sequence name, Encryption enabled flag (Optional), Automation logging configuration(Optional) &
    #               Oracle connection object (Optional)
    # Output    :   The next value of the sequence
    # ##############################################################################################################
    def get_sequence(self, sequence_name):
        status_message = ""
        my_sql_utility_conn = MySQLConnectionManager()
        try:
            status_message = "Starting function to get sequence"
            logger.info(status_message, extra=self.execution_context.get_context())
            # Prepare the my-sql query to get the sequence
            query_string = "SELECT " + sequence_name + ".NEXTVAL FROM dual"
            sequence_id = my_sql_utility_conn.execute_query_mysql(query_string)
            status_message = "Sequence generation result: " + sequence_id
            logger.debug(status_message, extra=self.execution_context.get_context())
            return sequence_id
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    ################################## Get s3 details for current user #############################################
    # Purpose   :   This method is used to create a log entry for a newly started load in automation log database
    # Input     :
    # Output    :   S3 connection dictionary
    # ##############################################################################################################
    def fetch_s3_conn_details_for_user(self):
        status_message = ""
        try:
            status_message = 'Started fetch of s3 connection details for current user'
            current_user = getpass.getuser()
            self.execution_context.set_context({"current_user": current_user})
            s3_cred_file_name = self.replace_variables(CommonConstants.S3_CREDENTIAL_FILE_NAME,
                                                       {"current_user": current_user})
            logger.info(s3_cred_file_name, extra=self.execution_context.get_context())
            s3_cred_params = JsonConfigUtility(conf_file=s3_cred_file_name)
            status_message = "Fetched S3 details for user " + current_user
            logger.info(status_message, extra=self.execution_context.get_context())
            return s3_cred_params
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    ################################## create process log_entry ########################################################
    # Purpose   :   This method is used to create a log entry for a newly started load in automation log database
    # Input     :   Automation logging database name , Status of the load, Log info containing data source name,
    #               load step, Run Id for the current load and environment
    # Output    :   True for success, False for failure
    # ##################################################################################################################
    def create_process_log_entry(self, automation_log_database, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log for input : " + str(log_info)
            logger.info(status_message, extra=self.execution_context.get_context())
            batch_id = log_info[CommonConstants.BATCH_ID]
            file_id = log_info[CommonConstants.FILE_ID]
            file_name = str("'") + log_info[CommonConstants.FILE_NAME] + str("'")
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            process_status = str("'") + CommonConstants.IN_PROGRESS_DESC + str("'")
            proc_table = CommonConstants.PROCESS_LOG_TABLE_NAME
            audit_db = automation_log_database
            dataset_id = log_info[CommonConstants.DATASET_ID]
            cluster_id = str("'") + log_info[CommonConstants.CLUSTER_ID] + str("'")
            workflow_id = str("'") + log_info[CommonConstants.WORKFLOW_ID] + str("'")
            process_id = log_info['process_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "INSERT INTO " + audit_db + "." + proc_table + " (batch_id, file_id, file_name, " \
                                                                          "file_process_name, file_process_status, file_process_start_time, file_process_end_time, cluster_id, workflow_id, dataset_id, process_id) VALUES ({batch_id}, " \
                                                                          "{file_id}, {file_name}, {process_name}, {process_status}, {start_time}, {end_time}, {cluster_id}, {workflow_id}, {dataset_id}, {process_id}) ".format(
                batch_id=batch_id, file_id=file_id, file_name=file_name, process_name=process_name,
                process_status=process_status, start_time="NOW() ", end_time="NULL", cluster_id=cluster_id,
                workflow_id=workflow_id, dataset_id=dataset_id, process_id=process_id)

            status_message = "Input query for creating load automation log : " + query_string
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query_string)
            status_message = "Load automation log creation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def create_file_audit_entry(self, automation_log_database, file_audit_info):
        status_message = ""
        try:
            status_message = "Starting function to create file log for input : " + str(file_audit_info)
            logger.info(status_message, extra=self.execution_context.get_context())

            batch_id = file_audit_info[CommonConstants.BATCH_ID]
            file_name = str("'") + file_audit_info[CommonConstants.FILE_NAME] + str("'")
            file_status = str("'") + file_audit_info[CommonConstants.FILE_STATUS] + str("'")
            dataset_id = file_audit_info[CommonConstants.DATASET_ID]
            cluster_id = file_audit_info[CommonConstants.CLUSTER_ID]
            workflow_id = file_audit_info[CommonConstants.WORKFLOW_ID]
            process_id = file_audit_info['process_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "INSERT INTO {audit_db}.{file_audit_table} (file_name, dataset_id, " \
                           "file_status, batch_id, " \
                           "file_process_start_time, cluster_id, workflow_id, process_id) VALUES ({file_name}, " \
                           "{dataset_id}, " \
                           "{status_desc}, {batch_id}, {update_time}, '{cluster_id}', " \
                           "'{workflow_id}', {process_id})".format(
                audit_db=automation_log_database,
                file_audit_table=CommonConstants.FILE_AUDIT_TABLE,
                batch_id=batch_id, file_name=file_name, dataset_id=dataset_id,
                status_desc="'" + CommonConstants.IN_PROGRESS_DESC + "'",
                update_time="NOW()", cluster_id=cluster_id,
                workflow_id=workflow_id, process_id=process_id)
            status_message = "Input query for creating file audit log : " + query_string
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query_string)
            status_message = "File automation log creation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completing function to create file automation log"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def create_batch_entry(self, automation_log_database, dataset_id=None, cluster_id=None, workflow_id=None,
                           process_id=None):
        status_message = ""
        try:
            status_message = "Starting function to create batch entry for dataset_id : " + str(
                dataset_id) + " ,process id : " + str(process_id) + " ,cluster_id : " + str(cluster_id)

            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_status_id = CommonConstants.IN_PROGRESS_ID
            batch_status_desc = str("'") + CommonConstants.IN_PROGRESS_DESC + str("'")

            cluster_id = str("'") + str(cluster_id) + str("'")
            workflow_id = str("'") + str(workflow_id) + str("'")

            retry_count = 0
            # Check Flag is 0 when retries have to carry on. When batch ID is found unique check flag is mad 1
            check_flag = 0
            max_retry_limit = CommonConstants.MAX_RETRY_LIMIT
            while retry_count < max_retry_limit and check_flag == 0:
                batch_id = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
                batch_id = batch_id[:-1]
                status_message = "Batch ID generated and yet to check if there exists duplicate entry"
                logger.info(status_message, extra=self.execution_context.get_context())
                query_string = "INSERT INTO {audit_db}.{batch_details_table} (batch_id, dataset_id," \
                               "batch_status, batch_start_time, cluster_id, workflow_id, process_id)" \
                               "VALUES ({batch_id},{dataset_id}, {batch_status_desc}, {start_dt}," \
                               "{cluster_id}, {workflow_id}, {process_id})"
                check_flag = 1
                try:
                    query_format = query_string.format(
                        audit_db=automation_log_database,
                        batch_details_table=CommonConstants.BATCH_TABLE,
                        batch_status_id=batch_status_id, batch_status_desc=batch_status_desc,
                        start_dt="NOW()",
                        dataset_id=dataset_id, cluster_id=cluster_id, workflow_id=workflow_id, process_id=process_id,
                        batch_id=batch_id)
                    MySQLConnectionManager().execute_query_mysql(query_format)
                except:
                    logger.warn("Duplicate batch ID detected, regenerating new batch ID")
                    random_int = random.randint(1, 10)
                    time.sleep(random_int)
                    retry_count += 1
                    check_flag = 0
                    pass

            if check_flag == 0 and retry_count == max_retry_limit:
                raise Exception("Duplicate Batch IDs found and Maximum retry limit reached")

            return batch_id
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    ################################## Inser logging details in predqm log table #############################################
    # Purpose   :   This method is used to insert a log entry for a newly started load in pre dqm logging table
    # Input     :
    # Output    :
    # ##############################################################################################################

    def insert_pre_dqm_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log"
            logger.debug(status_message, extra=self.execution_context.get_context())
            audit_db = self.audit_db
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            dataset_id = log_info[CommonConstants.DATASET_ID]
            workflow_id = str("'") + log_info[CommonConstants.WORKFLOW_ID] + str("'")
            batch_id = log_info[CommonConstants.BATCH_ID]
            function_name = log_info['function_name']
            function_params = log_info['function_params']
            process_id = log_info['process_id']
            workflow_name = log_info['workflow_name']
            c_name = log_info['c_name']

            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Inserting the status for the pre-dqm checks for file id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query = "insert into {audit_db}.{log_pre_dqm_dtl} (process_id, process_name, workflow_id, workflow_name, dataset_id, file_id, batch_id, column_name,function_name, function_params, status, updated_timestamp) " \
                        "values({process_id},{process_name},{workflow_id},'{workflow_name}','{dataset_id}','{file_id}','{batch_id}','{c_name}','{function_name}','{function_params}','{inprogress}',now())" \
                    .format(audit_db=audit_db, log_pre_dqm_dtl=CommonConstants.LOG_PRE_DQM_DTL,
                            process_name=process_name, process_id=process_id, workflow_name=workflow_name,
                            c_name=c_name,
                            function_name=function_name, function_params=function_params,
                            inprogress=CommonConstants.STATUS_RUNNING,
                            workflow_id=workflow_id, dataset_id=dataset_id, batch_id=batch_id, file_id=file_id)
                result = MySQLConnectionManager().execute_query_mysql(query)
                status_message = "Load Pre DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_pre_dqm_success_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log"
            logger.debug(status_message, extra=self.execution_context.get_context())
            dataset_id = log_info[CommonConstants.DATASET_ID]
            batch_id = log_info[CommonConstants.BATCH_ID]
            c_name = log_info['c_name']
            audit_db = self.audit_db
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Updating Success status for the pre-dqm checks for file id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query_update = (
                    "update {audit_db}.{log_pre_dqm_dtl} set status = '{success}' where column_name = '{c_name}' and dataset_id = '{dataset_id}' and file_id = {file_id} and batch_id = {batch_id}").format(
                    audit_db=audit_db, success=CommonConstants.STATUS_SUCCEEDED,
                    log_pre_dqm_dtl=CommonConstants.LOG_PRE_DQM_DTL,
                    c_name=c_name, dataset_id=dataset_id, batch_id=batch_id, file_id=file_id)

                result = MySQLConnectionManager().execute_query_mysql(query_update)
                status_message = "Load Pre DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_pre_dqm_failure_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log"
            logger.debug(status_message, extra=self.execution_context.get_context())
            dataset_id = log_info[CommonConstants.DATASET_ID]
            batch_id = log_info[CommonConstants.BATCH_ID]
            c_name = log_info['c_name']

            audit_db = self.audit_db
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Updating Failure status for the pre-dqm checks for filr id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query_update_failure = (
                    "update {audit_db}.{log_pre_dqm_dtl} set status = '{failed}' where column_name = '{c_name}' and dataset_id = '{dataset_id}' and file_id = {file_id} and batch_id = {batch_id}").format(
                    audit_db=audit_db, failed=CommonConstants.STATUS_FAILED,
                    log_pre_dqm_dtl=CommonConstants.LOG_PRE_DQM_DTL,
                    c_name=c_name, dataset_id=dataset_id, batch_id=batch_id, file_id=file_id)
                result = MySQLConnectionManager().execute_query_mysql(query_update_failure)
                status_message = "Load Pre DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_batch_status(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update batch status for input : " + str(log_info)
            logger.info(status_message, extra=self.execution_context.get_context())

            batch_id = log_info[CommonConstants.BATCH_ID]
            status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            status_id = log_info['status_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{batch_table} set  " \
                           "batch_status = {status_desc}, batch_end_time = {end_time} " \
                           "where batch_id = {batch_id} "
            query = query_string.format(automation_db_name=automation_log_database,
                                        batch_table=CommonConstants.BATCH_TABLE,
                                        batch_status_id=status_id, batch_id=batch_id, status_desc=status,
                                        end_time="NOW() ")
            status_message = "Input query for updating batch table entry : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update batch entry"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_process_status(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update process log status with input : " + str(log_info)
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info[CommonConstants.BATCH_ID]
            file_id = log_info[CommonConstants.FILE_ID]
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")

            record_count = str(0)
            message = str("'") + str("NA") + str("'")
            if 'record_count' in log_info:
                record_count = str(log_info['record_count'])

            if 'message' in log_info:
                message = str("'") + str(log_info['message']) + str("'")

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{process_log_table} set file_process_status = {process_status}, " \
                           " file_process_end_time = {end_time},record_count = {record_count}, " \
                           " process_message = {message}  where batch_id = {batch_id} and file_id = {file_id} " \
                           "and file_process_name = {" \
                           "process_name} "
            query = query_string.format(automation_db_name=automation_log_database,
                                        process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        process_status=process_status, batch_id=batch_id, file_id=file_id,
                                        process_name=process_name, end_time="NOW() ",
                                        message=message, record_count=record_count)

            status_message = "Input query for creating load automation log : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_file_audit_status(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to file audit status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info[CommonConstants.BATCH_ID]
            file_id = log_info[CommonConstants.FILE_ID]
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            process_status_id = log_info['status_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{file_audit_table} " \
                           "set " \
                           "file_status = {status_desc}" \
                           ", file_process_end_time = {upd_time} " \
                           "where batch_id = {batch_id} and file_id = {file_id} "
            query = query_string.format(automation_db_name=automation_log_database,
                                        file_audit_table=CommonConstants.FILE_AUDIT_TABLE,
                                        status_id=process_status_id, status_desc=process_status,
                                        batch_id=batch_id, file_id=file_id,
                                        upd_time="NOW() ")

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_file_audit_status_by_batch_id(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update file audit status for input " + str(log_info)
            logger.info(status_message, extra=self.execution_context.get_context())

            batch_id = log_info['batch_id']
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            process_status_id = log_info['status_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{file_audit_table} " \
                           "set " \
                           "file_status = {status_desc}" \
                           ", file_process_end_time = {upd_time} " \
                           "where batch_id= {batch_id}  "
            query = query_string.format(automation_db_name=automation_log_database,
                                        file_audit_table=CommonConstants.FILE_AUDIT_TABLE,
                                        status_id=process_status_id, status_desc=process_status,
                                        batch_id=str(batch_id), upd_time="NOW() ")

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query)
            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_proces_log_status_by_batch_id(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update file process status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info['batch_id']
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            process_status_id = log_info['status_id']
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            query_string = "UPDATE {automation_db_name}.{process_log} " \
                           "set " \
                           "file_process_status = {status_desc}" \
                           ", file_process_end_time = {upd_time} " \
                           "where batch_id= {batch_id}  and file_process_name = {process_name}"
            query = query_string.format(automation_db_name=automation_log_database,
                                        process_log=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        status_desc=process_status,
                                        batch_id=str(batch_id), process_name=process_name, upd_time="NOW() ")

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_process_log_by_batch(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update file process status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info['batch_id']
            process_status = log_info[CommonConstants.PROCESS_STATUS]
            # process_status_id = log_info['status_id']
            process_name = log_info[CommonConstants.PROCESS_NAME]
            process_message = log_info['message']
            record_count = None
            if process_name != CommonConstants.FILE_PROCESS_NAME_DQM and process_status == CommonConstants.STATUS_SUCCEEDED:
                record_count = log_info['record_count']

            query_string = "UPDATE {automation_db_name}.{process_log} " \
                           "set " \
                           "file_process_status = '{status_desc}'" \
                           ", file_process_end_time = {upd_time} " \
                           ", process_message = '{message}'" \
                           " where batch_id= {batch_id}  and file_process_name = '{process_name}'"
            query = query_string.format(automation_db_name=automation_log_database,
                                        process_log=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        status_desc=process_status,
                                        batch_id=str(batch_id), process_name=process_name, upd_time="NOW() ",
                                        message=process_message)

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)

            # Update record count for each file id if the file process status is SUCCEEDED
            if process_status == CommonConstants.STATUS_SUCCEEDED:
                file_id_result = self.get_file_ids_by_batch_id(automation_log_database, batch_id)
                for i in range(0, len(file_id_result)):
                    file_id = file_id_result[i]['file_id']
                    if process_name != CommonConstants.FILE_PROCESS_NAME_DQM:
                        count = \
                        record_count.filter(record_count[CommonConstants.PARTITION_FILE_ID] == file_id).collect()[0][
                            'count']
                    else:
                        count = 0
                    CommonUtils().update_record_count_for_file(automation_log_database, file_id, count, process_name)

            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_record_count_for_file(self, automation_log_database, file_id, count, process_name):
        status_message = ""
        try:
            status_message = "Starting function to update record count for file_id : " + str(
                file_id) + " and process_name : " + str(process_name)
            logger.debug(status_message, extra=self.execution_context.get_context())

            query_string = "update {automation_log_database}.{log_file_dtl} set record_count={count} where file_id={file_id} and file_process_name='{process_name}'"
            query = query_string.format(automation_log_database=automation_log_database,
                                        log_file_dtl=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        count=count,
                                        file_id=file_id,
                                        process_name=process_name)
            result = MySQLConnectionManager().execute_query_mysql(query)

            status_message = "Completed function to update record count for file_id : " + str(
                file_id) + " process_name : " + str(process_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_batch_ids_by_process_info(self, automation_log_database, dataset_id, cluster_id, workflow_id, process_id):
        status_message = ""
        try:
            status_message = "Starting function to get batches from log_batch_dtl"
            logger.debug(status_message, extra=self.execution_context.get_context())

            query_string = "select batch_id from {automation_log_database}.{log_batch_dtl}" \
                           " where dataset_id={dataset_id} and cluster_id='{cluster_id}' and workflow_id='{workflow_id}'" \
                           " and process_id={process_id}"
            query = query_string.format(automation_log_database=automation_log_database,
                                        log_batch_dtl=CommonConstants.BATCH_TABLE,
                                        dataset_id=dataset_id,
                                        cluster_id=cluster_id,
                                        workflow_id=workflow_id,
                                        process_id=process_id)
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Completed function to get batch ids from log_batch_dtl table"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_file_ids_by_batch_id(self, automation_log_database, batch_id):
        status_message = ""
        try:
            status_message = "Starting function to get file ids for batch id : ", str(batch_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            query_to_get_files_for_batch_str = "Select file_id from {automation_db_name}.{log_file_smry} where batch_id={batch_id}"
            query_to_get_files_for_batch = query_to_get_files_for_batch_str.format(
                automation_db_name=automation_log_database,
                log_file_smry=CommonConstants.FILE_AUDIT_TABLE,
                batch_id=batch_id)
            file_id_result = MySQLConnectionManager().execute_query_mysql(query_to_get_files_for_batch)
            status_message = "Completed function to get file ids for batch id : ", str(batch_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "File ids retrieved : ", str(file_id_result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return file_id_result
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def check_dqm_error_location(self, s3_dir_path, cluster_mode=None):
        status_message = ""
        check_flag = False
        try:
            status_message = "Started function to list all the files in DQM error location : "
            logger.info(status_message, extra=self.execution_context.get_context())
            cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                 "cluster_mode"])
            # Fetch s3 connection
            if cluster_mode == 'EMR':
                # conn = boto.s3.connect_to_region(str(s3_aws_region), aws_access_key_id=s3_access_key,
                #                                  aws_secret_access_key=s3_secret_key, is_secure=True,
                #                                  calling_format=boto.s3.connection.OrdinaryCallingFormat())
                conn = boto.connect_s3(host='s3.amazonaws.com')
            else:
                s3_cred_params = self.fetch_s3_conn_details_for_user()
                s3_access_key = s3_cred_params.get_configuration([CommonConstants.S3_ACCESS_KEY])
                s3_secret_key = s3_cred_params.get_configuration([CommonConstants.S3_SECRET_KEY])
                s3_aws_region = s3_cred_params.get_configuration([CommonConstants.S3_AWS_REGION])
                logger.debug(s3_access_key, extra=self.execution_context.get_context())
                logger.debug(s3_secret_key, extra=self.execution_context.get_context())
                logger.debug(s3_aws_region, extra=self.execution_context.get_context())
                conn = boto.connect_s3(aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key,host='s3.amazonaws.com')
            status_message = "Created s3 connection object with provided connecton details"
            logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Started process to get all file paths (with complete url) in s3 folder " + s3_dir_path
            logger.debug(status_message, extra=self.execution_context.get_context())
            bucket_name = self.get_s3_bucket_name(s3_dir_path)
            bucket = conn.get_bucket(bucket_name)
            s3_folder_path = self.get_s3_folder_path(s3_dir_path)
            s3_folder_list = bucket.list(s3_folder_path)
            s3_file_list = []
            file_count = 0
            if not s3_folder_list:
                status_message = "Provided S3 directory is empty " + s3_dir_path
                return True
            else:
                for file in s3_folder_list:
                    file_name = self.get_file_name(file.name)
                    s3_file_list.append(file_name)

                    if str(file.name).endswith('.parquet'):
                        file_count = file_count + 1

                if file_count <= 0:
                    check_flag = True

            status_message = "Completed function to list all the files in S3 directory"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return check_flag
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_file_name(self, file_path):
        name_list = file_path.split("/")
        return name_list[len(name_list) - 1]

    def insert_dqm_status(self, log_info):
        try:
            status_message = "Starting function to create logs for dqm"
            logger.debug(status_message, extra=self.execution_context.get_context())
            audit_db = self.audit_db
            application_id = log_info['application_id']
            dataset_id = log_info['dataset_id']
            batch_id = log_info['batch_id']
            qc_id = log_info['qc_id']
            column_name = log_info['column_name']
            qc_type = log_info['qc_type']
            qc_param = log_info['qc_param']
            criticality = log_info['criticality']
            error_count = log_info['error_count']
            qc_message = log_info['qc_message']
            qc_failure_percentage = log_info['qc_failure_percentage']
            qc_status = log_info['qc_status']
            qc_start_time = log_info['qc_start_time']
            qc_create_by = log_info['qc_create_by']
            qc_create_date = log_info['qc_create_date']
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Inserting the status for the pre-dqm checks for file id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query_str = "insert into {audit_db}.{log_dqm_smry} (application_id,dataset_id,batch_id,file_id,qc_id," \
                            "column_name,qc_type,qc_param,criticality,error_count,qc_message,qc_failure_percentage," \
                            "qc_status,qc_start_time,qc_create_by,qc_create_date)" \
                            "values('{app_id}',{dataset_id},{batch_id},{file_id},{qc_id},'{column_name}','{qc_type}'," \
                            "'{qc_param}','{criticality}',{error_count},'{qc_message}','{qc_failure_percentage}'," \
                            "'{qc_status}','{qc_start_time}','{create_by}','{create_ts}')"
                query = query_str.format(audit_db=audit_db,
                                         log_dqm_smry=CommonConstants.LOG_DQM_SMRY,
                                         app_id=application_id,
                                         dataset_id=dataset_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         qc_id=qc_id,
                                         column_name=column_name,
                                         qc_type=qc_type,
                                         qc_param=qc_param,
                                         criticality=criticality,
                                         error_count=error_count,
                                         qc_message=qc_message,
                                         qc_failure_percentage=qc_failure_percentage,
                                         qc_status=qc_status,
                                         qc_start_time=qc_start_time,
                                         create_by=qc_create_by,
                                         create_ts=qc_create_date)
                status_message = "Query DQM log creation result : ", query
                logger.debug(status_message, extra=self.execution_context.get_context())
                result = MySQLConnectionManager().execute_query_mysql(query)
                status_message = "Load DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create DQM log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_dqm_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create logs for dqm"
            logger.debug(status_message, extra=self.execution_context.get_context())
            audit_db = self.audit_db
            application_id = log_info['application_id']
            dataset_id = log_info['dataset_id']
            batch_id = log_info['batch_id']
            qc_id = log_info['qc_id']
            qc_status = log_info['qc_status']
            qc_end_time = log_info['qc_end_time']
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            if qc_status == CommonConstants.STATUS_FAILED:
                for i in range(0, len(file_id_result)):
                    file_id = file_id_result[i]['file_id']
                    status_message = "Updating status for the dqm checks for file id : ", file_id
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    query_update_str = "update {audit_db}.{log_dqm_smry} set qc_status ='{qc_status}'," \
                                       "qc_end_time ='{qc_end_time}' where application_id ='{application_id}'" \
                                       " and dataset_id={dataset_id} and batch_id= {batch_id} and file_id={file_id} and qc_id={qc_id}"
                    query_update = query_update_str.format(audit_db=audit_db,
                                                           log_dqm_smry=CommonConstants.LOG_DQM_SMRY,
                                                           qc_status=qc_status,
                                                           qc_end_time=qc_end_time,
                                                           application_id=application_id,
                                                           dataset_id=dataset_id,
                                                           batch_id=batch_id,
                                                           file_id=file_id,
                                                           qc_id=qc_id)
                    status_message = "Query to update DQM log : " + json.dumps(query_update)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    result = MySQLConnectionManager().execute_query_mysql(query_update)
                    status_message = "Load DQM log creation result : " + json.dumps(result)
                    logger.debug(status_message, extra=self.execution_context.get_context())
            if qc_status == CommonConstants.STATUS_SUCCEEDED:
                file_id = log_info['file_id']
                error_count = log_info['error_count']
                qc_message = log_info['qc_message']
                qc_failure_percentage = log_info['qc_failure_percentage']
                query_update_str = "update {audit_db}.{log_dqm_smry} set qc_status ='{qc_status}'," \
                                   "qc_end_time ='{qc_end_time}',error_count={error_count},qc_message='{qc_message}'," \
                                   "qc_failure_percentage='{qc_failure_percentage}' where application_id ='{application_id}' " \
                                   "and dataset_id={dataset_id} and batch_id= {batch_id} and file_id={file_id} and qc_id={qc_id}"
                query_update = query_update_str.format(audit_db=audit_db,
                                                       log_dqm_smry=CommonConstants.LOG_DQM_SMRY,
                                                       qc_status=qc_status,
                                                       qc_end_time=qc_end_time,
                                                       error_count=error_count,
                                                       qc_message=qc_message,
                                                       qc_failure_percentage=qc_failure_percentage,
                                                       application_id=application_id,
                                                       dataset_id=dataset_id,
                                                       batch_id=batch_id,
                                                       file_id=file_id,
                                                       qc_id=qc_id)
                result = MySQLConnectionManager().execute_query_mysql(query_update)

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def _get_complete_s3_url(self, s3_location, s3_access_key, s3_secret_key):
        """
        Purpose   :   This method will form the fully qualified s3 url with access, secret key and bucket
        Input     :   s3 location without access and secret key, access key and secret key
        Output    :   Returns the s3 url in the form - s3://access_key:secret_key@bucket/path
        """
        status_message = "Starting function to get complete s3 url"
        logger.debug(status_message, extra=self.execution_context.get_context())
        if s3_access_key is None or s3_secret_key is None or s3_access_key == "" or s3_secret_key == "":
            return s3_location
        # This will extract url is s3, s3a, s3n, etc.
        s3_uri = s3_location[0:s3_location.index("://") + 3]
        # Replacing forward slash with "%2f" for encoding http url
        s3_access_key = s3_access_key.replace("/", "%2F")
        s3_secret_key = s3_secret_key.replace("/", "%2F")
        # This will form the url prefix with access key and secret key
        s3_prefix_path = s3_uri + s3_access_key + ":" + s3_secret_key + "@"
        # Forming the complete s3 url by replacing the s3X://
        s3_complete_location = s3_location.replace(s3_uri, s3_prefix_path)
        status_message = "S3 url with access key and secret key is created"
        logger.debug(status_message, extra=self.execution_context.get_context())
        return s3_complete_location

    def replace_variables(self, raw_string, dict_replace_params):
        status_message = ""
        try:
            # Sort the dictionary keys in ascending order
            dict_keys = list(dict_replace_params.keys())
            dict_keys.sort(reverse=True)
            # Iterate through the dictionary containing parameters for replacements and replace all the variables in the
            # raw string with their values
            for key in dict_keys:
                raw_string = raw_string.replace(
                    str(CommonConstants.VARIABLE_PREFIX + key + CommonConstants.VARIABLE_PREFIX),
                    dict_replace_params[key])
            if str(raw_string).__contains__(CommonConstants.VARIABLE_PREFIX):
                status_message = "Variable value(s) not found in query parameter config files" + str(raw_string)
                raise Exception(status_message)

            return raw_string
        except Exception as e:
            raise e

    def replace_values(self, original_value, replaced_value):
        """ Replaces set of values with new values """
        for key, val in list(replaced_value.items()):
            original_value = original_value.replace(key, val)
        return original_value

    def list_files_in_s3_directory(self, s3_dir_path, cluster_mode=None, file_pattern=None):
        status_message = ""
        try:
            status_message = "Started function to list all the files in S3 directory for s3_dir_path : " + str(
                s3_dir_path) + " and file pattern : " + str(file_pattern)
            logger.info(status_message, extra=self.execution_context.get_context())
            cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                 "cluster_mode"])
            # Fetch s3 connection
            if cluster_mode == 'EMR':
                # conn = boto.s3.connect_to_region(str(s3_aws_region), aws_access_key_id=s3_access_key,
                #                                  aws_secret_access_key=s3_secret_key, is_secure=True,
                #                                  calling_format=boto.s3.connection.OrdinaryCallingFormat())
                conn = boto.connect_s3(host='s3.amazonaws.com')
            else:
                s3_cred_params = self.fetch_s3_conn_details_for_user()
                s3_access_key = s3_cred_params.get_configuration([CommonConstants.S3_ACCESS_KEY])
                s3_secret_key = s3_cred_params.get_configuration([CommonConstants.S3_SECRET_KEY])
                s3_aws_region = s3_cred_params.get_configuration([CommonConstants.S3_AWS_REGION])
                logger.debug(s3_access_key, extra=self.execution_context.get_context())
                logger.debug(s3_secret_key, extra=self.execution_context.get_context())
                logger.debug(s3_aws_region, extra=self.execution_context.get_context())
                conn = boto.connect_s3(aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key,host='s3.amazonaws.com')
            status_message = "Created s3 connection object with provided connecton details"
            logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Started process to get all file paths (with complete url) in s3 folder " + s3_dir_path
            logger.debug(status_message, extra=self.execution_context.get_context())
            bucket_name = self.get_s3_bucket_name(s3_dir_path)
            bucket = conn.get_bucket(bucket_name)
            s3_folder_path = self.get_s3_folder_path(s3_dir_path)
            s3_folder_list = bucket.list(s3_folder_path)
            s3_file_list = []
            if not s3_folder_list:
                status_message = "Provided S3 directory is empty " + s3_dir_path
                raise Exception(status_message)
            for file in s3_folder_list:
                file_name = self.get_file_name(file.name)
                if not str(file.name).endswith('/'):
                    s3_file_full_url = None
                    if file_pattern is None:
                        s3_file_full_url = str(CommonConstants.S3A_PREFIX). \
                            __add__(bucket_name).__add__('/').__add__(str(file.name))
                        s3_file_list.append(s3_file_full_url)
                    else:
                        is_pattern_match = PatternValidator().patter_validator(file_name, file_pattern)
                        if is_pattern_match:
                            s3_file_full_url = str(CommonConstants.S3A_PREFIX). \
                                __add__(bucket_name).__add__('/').__add__(str(file.name))
                            s3_file_list.append(s3_file_full_url)
            status_message = "Completed function to list all the files in S3 directory"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return s3_file_list
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_s3_bucket_name(self, s3_full_dir_path):
        status_message = ""
        s3_tmp_path = ""
        try:
            status_message = "Getting s3 bucket name from s3 complete url"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if s3_full_dir_path is not None:
                s3_tmp_path = str(s3_full_dir_path).replace("s3://", "").replace("s3n://", "").replace("s3a://", "")
            else:
                status_message = "S3 full directory path is not provided"
                raise Exception(status_message)
            dirs = s3_tmp_path.split('/')
            s3_bucket_name = dirs[0]
            status_message = "Completed fetch of s3 bucket name from s3 complete url(with s3n or s3a prefixes) with bucket name = " + str(
                s3_bucket_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return s3_bucket_name
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_s3_folder_path(self, s3_full_dir_path):
        status_message = ""
        s3_tmp_path = ""
        try:
            status_message = "Getting s3 folder path from s3 complete url(with s3n or s3a prefixes)"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if s3_full_dir_path is not None:
                s3_tmp_path = str(s3_full_dir_path).replace("s3://", "").replace("s3n://", "").replace("s3a://", "")

            dirs = s3_tmp_path.split('/')
            s3_bucket_name = dirs[0]
            s3_folder_path = s3_tmp_path.lstrip(s3_bucket_name).lstrip('/')
            status_message = "Completed fetch of s3 folder path from s3 complete url with s3n or s3a prefixes"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return s3_folder_path
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def get_dataset_information(self, dataset_id=None):
        status_message = ""
        source_file_location = None
        try:
            status_message = "Executing get_source_file_location function of module FileCheckHandler for dataset_id : " + str(
                dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager(self.execution_context)

            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME + " where dataset_id = " + \
                               str(dataset_id)
                result = mysql_manager.execute_query_mysql(query_string, True)
                status_message = "Dataset details for dataset_id : " + str(dataset_id) + " - " + str(result)
                logger.debug(status_message, extra=self.execution_context.get_context())
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_dataset_information_from_dataset_id(self, dataset_id=None):
        status_message = ""
        source_file_location = None
        try:
            status_message = "Executing get_dataset_information_from_dataset_id function with dataset id : " + str(
                dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager(self.execution_context)

            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME + " where dataset_id = " + \
                               str(dataset_id)
                result = mysql_manager.execute_query_mysql(query_string, True)
                status_message = "Dataset details for dataset_id : " + str(dataset_id) + " - " + str(result)
                logger.debug(status_message, extra=self.execution_context.get_context())
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def get_file_details_for_current_batch(self, curr_batch_id=None, dataset_id=None):
        status_message = ""
        file_details = dict()
        pre_landing_location = None
        try:
            status_message = "Get file details from previous load step"
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager()
            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.FILE_AUDIT_TABLE + " where batch_id = " + \
                               str(curr_batch_id) + " and dataset_id = " + str(
                    dataset_id)
                result = mysql_manager.execute_query_mysql(query_string)
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def execute_shell_command(self, command):
        status_message = ""
        try:
            status_message = "Started executing shell command " + command
            logger.debug(status_message, extra=self.execution_context.get_context())
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            if standard_output:
                standard_output_lines = standard_output.splitlines()
                for line in standard_output_lines:
                    logger.debug(line, extra=self.execution_context.get_context())
            if standard_error:
                standard_error_lines = standard_error.splitlines()
                for line in standard_error_lines:
                    logger.debug(line, extra=self.execution_context.get_context())
            if command_output.returncode == 0:
                return True
            else:
                status_message = "Error occurred while executing command on shell :" + command
                raise Exception(status_message)
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def execute_restricted_shell_command(self, command):
        status_message = ""
        try:
            status_message = "Started executing restricted shell command "
            logger.debug(status_message, extra=self.execution_context.get_context())
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            logger.debug(standard_output, extra=self.execution_context.get_context())
            logger.debug(standard_error, extra=self.execution_context.get_context())
            if command_output.returncode == 0:
                return True
            else:
                status_message = "Error occurred while executing restricted command on shell :"
                raise Exception(status_message)
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_dataset_info(self, file_master_id=None):
        status_message = ""
        source_file_location = None
        try:
            status_message = "Executing get_source_file_location function of module FileCheckHandler"
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager()

            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME + " where dataset_id = " + \
                               str(file_master_id)
                result = mysql_manager.execute_query_mysql(query_string, True)
                status_message = "Dataset details for dataset_id : " + str(file_master_id) + " - " + str(result)
                logger.debug(status_message, extra=self.execution_context.get_context())
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_loaded_file_details(self, file_master_id=None, file_name=None, process_name=None):
        status_message = ""
        file_details = dict()
        pre_landing_location = None
        try:
            status_message = "Starting function get_loaded_file_details for dataset id: " + str(
                file_master_id) + " and file name : " + str(file_name)
            logger.info(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager(self.execution_context)
            if mysql_manager is not None:
                query = "select * from {audit_information}.{file_audit_information} " \
                        "where dataset_id = {file_master_id} and file_name = '{file_name}' " \
                        "and file_status IN ('{process_success}', '{process_inprogress}','{process_skipped}') ". \
                    format(audit_information=self.audit_db,
                           file_audit_information=CommonConstants.FILE_AUDIT_TABLE,
                           file_master_id=str(file_master_id),
                           file_name=file_name,
                           process_success=CommonConstants.STATUS_SUCCEEDED,
                           process_skipped=CommonConstants.STATUS_SKIPPED,
                           process_inprogress=CommonConstants.IN_PROGRESS_DESC)
                result = mysql_manager.execute_query_mysql(query)
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def create_load_summary_entry(self, file_master_id=None, batch_id=None, file_id=None, prelanding_count=None):
        status_message = ""
        try:
            status_message = "Starting function to create load summary entry"
            logger.debug(status_message, extra=self.execution_context.get_context())
            # Prepare the query string for creating an entry for the newly started batch process
            query_string = "INSERT INTO {audit_db}.{load_summary_table} (dataset_id,batch_id, file_id" \
                           "pre_landing_record_count) VALUES ({file_master_id}, {batch_id}, {file_id}, " \
                           "{prelanding_count)".format(
                audit_db=self.audit_db,
                load_summary_table=CommonConstants.LOAD_SUMMARY_TABLE,
                file_master_id=file_master_id, batch_id=batch_id, file_id=file_id,
                prelanding_count=prelanding_count)

            status_message = "Input query for creating load summary entry : " + query_string
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            MySQLConnectionManager().execute_query_mysql(query_string)
            status_message = 'Completed the creation of load summary entry'
            logger.debug(status_message, extra=self.execution_context.get_context())
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_load_summary_count(self, file_master_id=None, batch_id=None, file_id=None, location_type=None,
                                  record_count=None):
        status_message = ""
        try:
            status_message = "Starting function to update process log status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration

            query_string = "UPDATE {automation_db_name}.{load_summary_table} set {file_location_type} = " \
                           "{record_count} " \
                           "where dataset_id = {file_master_id} and batch_id = {batch_id} and file_id = {" \
                           "file_id} "

            query = query_string.format(automation_db_name=self.audit_db,
                                        file_location_type=location_type,
                                        record_count=record_count, batch_id=batch_id, file_id=file_id)

            status_message = "Input query for updating the load summary table : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Update Load summary result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update load summary count"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_emr_termination_status(self, cluster_id=None, db_name=None):
        status_message = ""
        if db_name is None:
            db_name = self.audit_db
        try:
            status_message = "Starting function to update emr termination status and time"
            print(status_message)
            # .debug(status_message, extra=self.execution_context.get_context())
            cluster_terminate_user = CommonUtils().get_user_name()
            query_string = "UPDATE {db_name}.{emr_cluster_details} set cluster_status ='{cluster_emr_terminate}', cluster_stop_time = NOW(),cluster_terminate_user='{cluster_terminate_user}' " \
                           " where cluster_id = '{cluster_id}'"
            query = query_string.format(db_name=db_name,
                                        emr_cluster_details=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                                        cluster_emr_terminate=CommonConstants.CLUSTER_EMR_TERMINATE_STATUS,
                                        cluster_id=cluster_id, cluster_terminate_user=cluster_terminate_user)
            print(query)
            status_message = "Input query for creating emr termianting status : " + query
            # .debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            # .debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update load automation log"
            # .info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            # .error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def correct_file_extension(self, file_complete_uri=None, location_type=None):
        status_message = "Starting correct_file_extension for  file_complete_uri : " + str(
            file_complete_uri) + " and location type : " + str(location_type)
        logger.info(status_message, extra=self.execution_context.get_context())

        try:
            if file_complete_uri and not str(file_complete_uri).endswith('/'):
                location_extension = os.path.splitext(file_complete_uri)
                if self.is_file_with_invalid_extension:
                    path_with_corrected_extension = location_extension[0] + location_extension[1].lower()
                    if location_type == 'S3':
                        #driver = create_clidriver()
                        s3_move_cmd = 'aws s3 mv ' + file_complete_uri + ' ' + path_with_corrected_extension
                        #driver.main(s3_move_cmd.split())
                        CommonUtils(self.execution_context).execute_shell_command(s3_move_cmd)
                        return path_with_corrected_extension
                    elif location_type == 'HDFS':
                        hdfs.rename(file_complete_uri, path_with_corrected_extension)
                        return path_with_corrected_extension
                    else:
                        status_message = 'Invalid location type is not provided or invalid'
                        raise Exception(status_message)
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    # will go in CommonUtils
    def is_file_with_invalid_extension(self, file_complete_uri=None):

        status_message = "Starting is_file_with_invalid_extension for  file_complete_uri : " + str(file_complete_uri)
        logger.info(status_message, extra=self.execution_context.get_context())

        if file_complete_uri and not str(file_complete_uri).endswith('/'):
            file_extension = os.path.splitext(file_complete_uri)[1]
            return not file_extension.islower()

    # will go in CommonUtils
    def get_s3_file_name(self, s3_complete_uri=None):
        status_message = "Starting get_s3_file_name for  s3_complete_uri : " + str(s3_complete_uri)
        logger.info(status_message, extra=self.execution_context.get_context())

        s3_file_name = ""
        if s3_complete_uri:
            splits = str(s3_complete_uri).split('/')
            if splits.__len__() > 0:
                s3_file_name = splits[splits.__len__() - 1]
        return str(s3_file_name)

    # will go in CommonUtils
    def perform_hadoop_distcp(self, source_location=None, target_location=None, multipart_size_mb=None):
        try:
            cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                 "cluster_mode"])
            status_message = "Starting perform_hadoop_distcp for source location : " + source_location + " and target_location : " + str(
                target_location)
            logger.info(status_message, extra=self.execution_context.get_context())

            if cluster_mode != 'EMR':
                # Fetch s3 connection details for current user
                aws_s3_cred_params = self.fetch_s3_conn_details_for_user()
                s3_access_key = aws_s3_cred_params.get_configuration([CommonConstants.S3_ACCESS_KEY])
                s3_secret_key = aws_s3_cred_params.get_configuration([CommonConstants.S3_SECRET_KEY])
                distcp_cmd = str('hadoop distcp ').__add__('-Dfs.s3a.awsAccessKeyId=').__add__(s3_access_key).__add__(
                    ' -Dfs.s3a.awsSecretAccessKey=').__add__(s3_secret_key).__add__(' -overwrite ').__add__(
                    source_location). \
                    __add__(' ').__add__(target_location)
            else:
                distcp_cmd = str('hadoop distcp ').__add__(' -overwrite ').__add__(
                    source_location). \
                    __add__(' ').__add__(target_location)

            status_message = "Performing hadoop distcp command  : " + str(distcp_cmd)
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Execute s3 cmd shell command for file copy
            self.execute_shell_command(distcp_cmd)
            status_message = 'Executed distcp command successfully '
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def delete_file(self, file_complete_uri=None, location_type=None):
        if location_type == 'S3':
            #driver = create_clidriver()
            s3_rm_cmd = 'aws s3 rm ' + file_complete_uri
            #driver.main(s3_rm_cmd.split())
            CommonUtils(self.execution_context).execute_shell_command(s3_rm_cmd)
        #elif location_type == 'HDFS':
            #pydoop.hdfs.rmr(file_complete_uri)

    def create_step_audit_entry(self, process_id, frequency, step_name, data_date, cycle_id):
        """
            Purpose :   This method inserts step entry in step log table
            Input   :   Process Id, Frequency, Step Name, Data Date , Cycle Id
            Output  :   NA
        """
        try:
            status = CommonConstants.STATUS_RUNNING
            status_message = "Started preparing query for doing step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            step_audit_entry_query = "Insert into {audit_db}.{step_audit_table} (process_id, frequency, data_date, " \
                                     "cycle_id,step_name, step_status, step_start_time) " \
                                     "values({process_id}, '{frequency}', '{data_date}', {cycle_id}, '{step_name}'," \
                                     " '{step_status}', {step_start_time})". \
                format(audit_db=self.audit_db, step_audit_table=CommonConstants.LOG_STEP_DTL,
                       process_id=process_id, frequency=frequency, data_date=data_date,
                       cycle_id=cycle_id, step_name=step_name,
                       step_start_time="NOW()", step_status=status)
            logger.debug(step_audit_entry_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(step_audit_entry_query)
            status_message = "Completed executing query for doing step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in doing step audit entry for process_id:" + str(process_id) + " " \
                             + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def update_step_audit_entry(self, process_id, frequency, step_name, data_date, cycle_id, status):
        """
            Purpose :   This method updates step execution status in step log table
            Input   :   Process Id, Frequency, Step Name, Data Date , Cycle Id, Status
            Output  :   NA
        """
        try:
            status_message = "Started preparing query to update step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            step_update_audit_status_query = "Update {audit_db}.{step_audit_table} set step_status='{status}'," \
                                             "step_end_time={step_end_time} where " \
                                             "process_id={process_id} and frequency='{frequency}' and " \
                                             "step_name='{step_name}' and data_date='{data_date}' and " \
                                             "cycle_id = '{cycle_id}'".format(audit_db=self.audit_db,
                                                                              step_audit_table=CommonConstants.LOG_STEP_DTL,
                                                                              process_id=process_id, status=status,
                                                                              step_end_time="NOW()",
                                                                              frequency=frequency,
                                                                              step_name=step_name,
                                                                              data_date=data_date, cycle_id=cycle_id)
            logger.debug(step_update_audit_status_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(step_update_audit_status_query)
            status_message = "Completed executing query to update step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "and cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in  updating step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "and cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def replace_key(self, path=None):
        try:
            status_message = "In Replace Key Function For Path : " + str(path)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if path is None:
                raise Exception("Input Path Can Not Be None")

            if path == "Null" or path == "NONE" or path == "":
                raise Exception("Input Path Is Null/None/Empty")

            regex_for_key = "((\\$\\$(\\w+)\\$\\$))"
            compiled_regex = re.compile(regex_for_key)

            keys_arr = compiled_regex.findall(path)

            i = 0
            for key in keys_arr:
                search_key = str(key[2])
                i = i + 1
                if search_key is not None:
                    value = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, search_key])
                    if value is None:
                        status_message = "Key Not Found For Path : " + str(path)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception("Key Not Found In Env Json For Path" + str(path))
                    else:
                        path = path.replace('$$' + search_key + '$$', str(value))

            return path
        except Exception as exception:
            status_message = "Exception In Replace Key Function Due To : " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def execute_hive_query(self, query=None):
        """
        Purpose   :   This method is used to execute hive query
        Input     :   query  (Mandatory)
        Output    :   Returns the result
        """
        conn = None
        try:
            status_message = "Executing execute_hive_query function for query : " + str(query)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if query is None or query == "":
                raise Exception("Query Can Not be None/Empty")

            job_flow = json.load(open(CommonConstants.EMR_JOB_FLOW_FILE_PATH, "r"))
            master_host = job_flow[CommonConstants.MASTER_PRIVATE_DNS_KEY]

            if self.hive_port is None:
                h_port = 10000
            elif self.hive_port == "":
                h_port = 10000
            elif not self.hive_port:
                h_port = 10000
            else:
                h_port = self.hive_port

            conn = hive.Connection(host=str(master_host), port=int(h_port))
            cursor = conn.cursor()
            cursor.execute(query)

            if query.lower().startswith("describe"):
                return cursor.fetchall()

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
        finally:
            if conn is not None:
                conn.close()

    def update_cycle_audit_entry(self, process_id, frequency, data_date, cycle_id, status):
        """
            Purpose :   This method updates cycle execution status in cycle log table
            Input   :   Process Id, Frequency, Data Date , Cycle Id, Status
            Output  :   NA
        """
        try:
            status_message = "Started preparing query to update " + status + " cycle status for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if status != CommonConstants.STATUS_RUNNING:
                cycle_update_audit_status_query = "Update {audit_db}.{cycle_details} set cycle_status='{status}'," \
                                                  "cycle_end_time={cycle_end_time} where process_id={process_id} and " \
                                                  "frequency='{frequency}' and data_date='{data_date}' and " \
                                                  "cycle_id = '{cycle_id}'".format(audit_db=self.audit_db,
                                                                                   cycle_details=CommonConstants.LOG_CYCLE_DTL,
                                                                                   process_id=process_id, status=status,
                                                                                   cycle_end_time="NOW()",
                                                                                   frequency=frequency,
                                                                                   data_date=data_date,
                                                                                   cycle_id=cycle_id)
            else:
                cycle_update_audit_status_query = "Update {audit_db}.{cycle_details} set cycle_status='{status}' " \
                                                  "where process_id={process_id} and " \
                                                  "frequency='{frequency}' and data_date='{data_date}' and " \
                                                  "cycle_id = '{cycle_id}'".format(audit_db=self.audit_db,
                                                                                   cycle_details=CommonConstants.LOG_CYCLE_DTL,
                                                                                   process_id=process_id, status=status,
                                                                                   frequency=frequency,
                                                                                   data_date=data_date,
                                                                                   cycle_id=cycle_id)
            logger.debug(cycle_update_audit_status_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(cycle_update_audit_status_query)
            status_message = "Completed executing query to update " + status + " cycle status for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in to updating " + status + " cycle status for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_cycle_id_for_data_date(self, process_id, frequency, data_date):
        """
            Purpose :   This method fetches in progress cycle id for input process , frequency and data date
            Input   :   Process Id, Frequency,  Data Date
            Output  :   Cycle Id
        """
        try:
            status_message = "Started preparing query to fetch in progress cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_in_progress_cycle_query = "Select cycle_id from {audit_db}." \
                                            "{cycle_details} where process_id={process_id} and data_date='{data_date}' and " \
                                            " frequency='{frequency}' and cycle_status='{cycle_status}'". \
                format(audit_db=self.audit_db,
                       cycle_details=CommonConstants.LOG_CYCLE_DTL, process_id=process_id,
                       frequency=frequency, data_date=data_date,
                       cycle_status=CommonConstants.STATUS_RUNNING)
            logger.debug(fetch_in_progress_cycle_query, extra=self.execution_context.get_context())
            cycle_id_result = MySQLConnectionManager().execute_query_mysql(fetch_in_progress_cycle_query, True)
            status_message = "Completed executing query to fetch in progress cycle id for process_id:" + str(
                process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.info(status_message, extra=self.execution_context.get_context())
            if not cycle_id_result:
                status_message = " No cycle id is in " + CommonConstants.STATUS_RUNNING + " for process_id:" \
                                 + str(process_id) + " " + "and frequency:" + str(frequency) + " " + \
                                 "and data date:" + str(data_date)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            logger.debug(cycle_id_result, extra=self.execution_context.get_context())
            cycle_id = cycle_id_result['cycle_id']
            return cycle_id
        except Exception as exception:
            status_message = "Error occured to fetch in progress cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_data_date_process(self, process_id, frequency):
        """
            Purpose :   This method fetches data date for input process id and frequency
            Input   :   Process Id, Frequency
            Output  :   Data Date
        """
        try:
            status_message = "Started preparing query to fetch data date for process_id:" + str(process_id) + " " + \
                             "and frequency:" + str(frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_data_date_query = "Select data_date from {audit_db}.{process_date_table} where " \
                                    "process_id={process_id} and frequency='{frequency}'". \
                format(audit_db=self.audit_db,
                       process_date_table=CommonConstants.PROCESS_DATE_TABLE,
                       process_id=process_id, frequency=frequency
                       )
            logger.debug(fetch_data_date_query, extra=self.execution_context.get_context())
            data_date_result = MySQLConnectionManager().execute_query_mysql(fetch_data_date_query, True)
            status_message = "Completed executing query to fetch data date for process_id:" + str(process_id) + " " + \
                             "and frequency:" + str(frequency)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(data_date_result, extra=self.execution_context.get_context())
            if not data_date_result:
                status_message = "No entry in " + CommonConstants.PROCESS_DATE_TABLE + " for process_id:" + \
                                 str(process_id) + " " + "and frequency:" + str(frequency)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            data_date = data_date_result['data_date']
            return data_date
        except Exception as exception:
            status_message = "Exception in fetching data date for process id:" + str(process_id) + " " + \
                             "and frequency:" + str(frequency)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def copy_hdfs_to_s3(self, table_name):
        """
            Purpose :   This method is a wrapper to copy data from hdfs to s3
            Input   :   Hive Table Name
            Output  :   NA
        """
        try:
            status_message = "Starting handler function to invoke Copy HDFS to S3 for table name:" + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Starting to prepare command to copy " + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            copy_command = "python3 ClusterToS3LoadHandler.py -ht " + str(table_name) + " "
            logger.debug(copy_command, extra=self.execution_context.get_context())
            copy_command_response = subprocess.Popen(copy_command, stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE, shell=True)
            copy_command_output, copy_command_error = copy_command_response.communicate()
            if copy_command_response.returncode != 0:
                logger.error(copy_command_error, extra=self.execution_context.get_context())
                raise Exception(copy_command_error)
            logger.debug(copy_command_output, extra=self.execution_context.get_context())
            status_message = "Completed executing handler function to Copy data from HDFS to S3 for " \
                             "table name:" + str(table_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in copy hdfs to s3 for table name" + str(table_name)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_emr_ip_list(self, cluster_id, region, instance_group_types_list=["MASTER"]):
        """
            Purpose             :   This module performs below operation:
                                      a. Gets IP from cluster_id
            Input               :   cluster_id,region name,instance_group_types_list
            Output              :   Returns list of ips from a cluster id
        """
        ip_list = []
        retries = 1
        retry = True


        while retry is True and retries <= 4:
            try:
                logger.info("starting function get_emr_ip_list", extra=self.execution_context.get_context())
                logger.debug("Preparing boto client", extra=self.execution_context.get_context())
                client = boto3.client(
                    "emr",
                    region_name=region
                )
                logger.debug("Triggering API call to list EMR instances", extra=self.execution_context.get_context())

                response = client.list_instances(
                    ClusterId=cluster_id,
                    InstanceGroupTypes=instance_group_types_list,
                    InstanceStates=['RUNNING'],
                )
                logger.debug(str(response), extra=self.execution_context.get_context())
                retry = False
                length = len(response['Instances'])


                logger.debug("length of instances"+str(length), extra=self.execution_context.get_context() )
                logger.debug("Response recieved: %s "+str(response), extra=self.execution_context.get_context())
                for i in range(length):
                    ip_list.append(response['Instances'][i]['PrivateIpAddress'])

                logger.debug("Final IP list returned: %s "+str(ip_list), extra=self.execution_context.get_context())
                logger.info("Completing function get_emr_ip_list", extra=self.execution_context.get_context())
                return {STATUS_KEY:SUCCESS_KEY,RESULT_KEY:ip_list}

            except:
                error_message = str(traceback.format_exc())
                if ("ThrottlingException" in error_message):
                    if retries >= 4:
                        logger.debug("Number of attempts: %s has reached the configured limit"+
                                     str(retries), extra=self.execution_context.get_context())
                        #return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}
                        raise Exception(error_message)

                    else:
                        retry = True
                        logger.debug("Retrying API call. No of attempts so far: %s"+str(retries), extra=self.execution_context.get_context())
                        time.sleep(math.pow(2,retries) / 100)
                        retries += 1

                else:
                    # return {STATUS_KEY:FAILED_KEY,ERROR_KEY:error_message}
                    raise Exception(error_message)



    def get_master_ip_from_cluster_id(self, cluster_id=None):
        try:
            status_message = "Fetching EMR master ip for cluster id: " + str(cluster_id)
            logger.info(status_message, extra=self.execution_context.get_context())

            query = "select master_node_dns from {audit_db}.{log_cluster_dtl} where cluster_id='{cluster_id}'".format(
                audit_db=self.audit_db, log_cluster_dtl=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                cluster_id=str(cluster_id))

            status_message = "Query to fetch master node DNS " + str(query)
            logger.debug(status_message, extra=self.execution_context.get_context())

            result_set = MySQLConnectionManager().execute_query_mysql(query)

            if len(result_set) == 0:
                raise Exception("No Cluster Found with cluster id: " + str(cluster_id))

            if result_set:
                master_node_dns = result_set[0]['master_node_dns']

            extracted_ip = re.search('ip.(.+?).ec2', master_node_dns).group(1)
            host_ip_address = extracted_ip.replace("-", ".")

            status_message = " master node DNS " + str(host_ip_address)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return host_ip_address
        except Exception as exception:
            status_message = "Exception in get_master_ip_from_cluste_id due to  " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_column_details(self, dataset_id, column_identifier_name):
        """
            Purpose :   This method fetch column information for input column name
            Input   :   Dataset Id , Column Name
            Output  :   Column Identifier Name Result List
        """
        try:
            status_message = "Starting function to fetch column details for dataset id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Column name is:" + str(column_identifier_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_column_metadata_query = "Select {column_identifier},column_sequence_number from {audit_db}." \
                                          "{column_metadata_table} where dataset_id={dataset_id}". \
                format(audit_db=self.audit_db, column_metadata_table=CommonConstants.COLUMN_METADATA_TABLE,
                       dataset_id=dataset_id, column_identifier=column_identifier_name)
            logger.debug(fetch_column_metadata_query, extra=self.execution_context.get_context())
            column_details = MySQLConnectionManager().execute_query_mysql(fetch_column_metadata_query)
            column_id_seq_dict = dict()
            column_identifier_list = []
            status_message = "Starting to prepare list for " + str(column_identifier_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            for column_detail in column_details:
                column_id_seq_dict.__setitem__(column_detail['column_sequence_number'],
                                               column_detail[column_identifier_name])
            for column_seq in sorted(column_id_seq_dict.keys()):
                column_identifier_list.append(column_id_seq_dict.get(column_seq))
            status_message = "Completed preparing list for " + str(column_identifier_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return column_identifier_list
        except Exception as exception:
            status_message = "Exception occured while fetching column details for dataset_id:" + str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_partition(self, max_size=None):

        status_message = "Starting get_partition value for Total size : " + str(max_size)
        logger.debug(status_message, extra=self.execution_context.get_context())

        if max_size is None:
            raise Exception("total_size can not be None")

        result = int(max_size) / (int(CommonConstants.BLOCK_SIZE) * int(CommonConstants.BYTES_TO_MB))
        rounding_result = math.ceil(result)

        if int(rounding_result) < 2:
            return 2
        else:
            return int(rounding_result)

    def make_s3_dir(self, s3path):

        """
                Purpose     :   To  make a new directory/file on s3
                Input       :   s3 path(ie. s3://<bucket name>/<file path>)
                Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
        """

        try:

            logging.info("starting creation of dictionary", extra=self.execution_context.get_context())
            output = urlparse(s3path)
            bucket_name = output.netloc
            src_key_path = output.path.lstrip("/")
            logger.debug("s3 path where you want to make a directory" + src_key_path)
            response = self.client.put_object(Bucket=bucket_name, Key=src_key_path)
            status_code = 0
            logger.debug("response" + response)
            if "ResponseMetadata" in response:
                status_code = response['ResponseMetadata']['HTTPStatusCode']

            if status_code == 200:
                output_status_dict[STATUS_KEY] = SUCCESS_KEY
                output_status_dict[RESULT_KEY] = SUCCESS_KEY
                output_status_dict[ERROR_KEY] = NONE_KEY

            logger.info("completing creation of directory", extra=self.execution_context.get_context())

        except:
            output_status_dict[STATUS_KEY] = FAILED_KEY
            output_status_dict[RESULT_KEY] = FAILED_KEY
            logger.error("error in make s3 directory" + str(traceback.format_exc()), extra=self.execution_context.get_context())
            output_status_dict[ERROR_KEY] = str(traceback.format_exc())

        return output_status_dict

        # path can be file path or directory path deletes both

    def delete_s3_object(self, s3_path):

        """
                Purpose     :   To  delete an s3 object present on s3.This s3 object can be file or directory as given below:
                                    1. if path of a file is given (ie. /file.txt) ,deletes the respective file
                                    2. if path of directory is given(i.e /dir1/ ),deletes all the files and directory inside dir1
                                    3. if directory name is given(i.e /dir1 ),returns error
                Input       :   s3 path
                Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
        """

        status_message = ""
        try:
            status_message = "starting delete_s3_object object"
            logger.info(status_message, extra=self.execution_context.get_context())

            output = urlparse(s3_path)
            bucket_name = output.netloc
            key_path = output.path.lstrip("/")
            logger.debug("path of s3 object to be deleted - " + str(s3_path), extra=self.execution_context.get_context())
            next_token = None

            # list_objects can't return more than 1000 keys so you need to run this code multiple times
            while True:
                if next_token:
                    output = self.client.list_objects_v2(Bucket=bucket_name, Prefix=key_path,
                                                         ContinuationToken=next_token)
                else:
                    output = self.client.list_objects_v2(Bucket=bucket_name, Prefix=key_path)


                if "Contents" in output:
                    for content in output['Contents']:
                        object_path = content['Key']

                        status_message = "deleting object= " + object_path + " from bucket " + bucket_name
                        logger.debug(str(status_message), extra=self.execution_context.get_context())
                        self.client.delete_object(Bucket=bucket_name, Key=object_path)
                else:
                    raise Exception("s3 path does not exist")

                if "NextContinuationToken" in output:
                    next_token = output["NextContinuationToken"]
                else:
                    status_message = "Next token is not found hence breaking out of while loop"
                    logger.debug("status message" + status_message, extra=self.execution_context.get_context())
                    break

            status_message = "completing delete_s3_object object"
            logger.info(status_message, extra=self.execution_context.get_context())

            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logger.error(error_message, extra=self.execution_context.get_context())
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def download_s3_object(self, s3_path, local_path_dir):
        """
                Purpose     :   To  download a file from s3 to local machine(i.e. ec2 machine).
                Input       :   s3 path(i.e s3://<bucket-name>/<filepath>),local path(eg. di_internal/tmp/test/solution.txt )
                Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
        """

        status_message = ""
        try:
            status_message = "starting download_s3_object object"
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug("s3 path - " + str(s3_path), extra=self.execution_context.get_context())

            if os.path.isfile(local_path_dir):
                logger.debug("Local path dir is not a directory: " + local_path_dir, extra=self.execution_context.get_context())
                raise Exception("Local path dir is not a directory: " + local_path_dir)

            if not os.path.isdir(local_path_dir):
                logger.debug("Creating a new directory ", extra=self.execution_context.get_context())
                os.makedirs(local_path_dir)

            output = urlparse(s3_path)
            bucket_name = output.netloc
            key_path = output.path.lstrip("/")
            logger.debug("s3 path : " + key_path, extra=self.execution_context.get_context())
            logger.debug("bucket name : " + bucket_name, extra=self.execution_context.get_context())

            next_token = None

            while True:
                if next_token:
                    output = self.client.list_objects_v2(Bucket=bucket_name, Prefix=key_path,
                                                         ContinuationToken=next_token)
                else:
                    output = self.client.list_objects_v2(Bucket=bucket_name, Prefix=key_path)

                logger.debug("listing objects in s3 : " + output, extra=self.execution_context.get_context())
                if "Contents" in output:
                    for content in output['Contents']:
                        object_path = content['Key']
                        status_message = "Copying object= " + object_path + " from bucket " + bucket_name
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        local_file_path = os.path.join(local_path_dir, ntpath.basename(object_path))
                        logger.debug("local file path : " + local_file_path, extra=self.execution_context.get_context())

                        self.client.download_file(Bucket=bucket_name, Key=object_path, Filename=local_file_path)

                else:
                    raise Exception("s3 path does not exist")
                if "NextContinuationToken" in output:
                    next_token = output["NextContinuationToken"]
                else:
                    status_message = "Next token is not found hence breaking out of while loop"
                    logger.debug(status_message, extra=self.execution_context.get_context())

                    break

            status_message = "completing download_s3_object object"
            logger.info(status_message, extra=self.execution_context.get_context())
            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logger.error(error_message, extra=self.execution_context.get_context())
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def head(self, s3path):

        """
            Purpose     :   reading the contents of a file mentioned in s3 path
            Input       :   s3 path(i.e s3://<bucket-name>/<filepath>  )
            Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
                            if s3 path does not exists raises exception "No Such Key"
        """

        output = urlparse(s3path)
        src_key_path = output.path.lstrip("/")
        bucket_name = output.netloc
        logger.debug("s3 path - " + str(s3path), extra=self.execution_context.get_context())
        try:

            logger.info("starting reading of a file", extra=self.execution_context.get_context())

            obj = self.s3.Object(bucket_name, src_key_path)
            response = obj.get()['Body'].read().decode('utf-8')
            logger.debug("contents in the file" + response, extra=self.execution_context.get_context())
            output_status_dict[STATUS_KEY] = SUCCESS_KEY
            output_status_dict[RESULT_KEY] = response
            output_status_dict[ERROR_KEY] = NONE_KEY

            logger.info("Completing reading of a file", extra=self.execution_context.get_context())
        except:
            output_status_dict[STATUS_KEY] = FAILED_KEY
            output_status_dict[RESULT_KEY] = FAILED_KEY
            output_status_dict[ERROR_KEY] = str(traceback.format_exc())
        return output_status_dict

    def copy_file_s3(self, src_location, target_location, copy_with_suffix=None):
        """
            Purpose     :   copy file from one s3 location to another in the manner mentioned below
                            1. While copying a directory(i.e /dir1 ) from one place to another on s3
                                a. copies the files of the directory (i.e. /dir1 )to another location mentioned,does not make a directory with same name at other loaction
                            2. when copying a directory (i.e /dir1/) from one place to another
                                a. copies the files of the directory to another location
                            3. when copying a file(ie. dir1/file.txt) form one location to another
                                a. copies the file from one location to another

            Input       :   s3 path(i.e s3://<bucket-name>/<filepath>  )
            Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
        """

        try:
            print("src_location = "+str(src_location))
            print("target_location"+str(target_location))
            logger.info("Starting copying file from one s3 location to another", extra=self.execution_context.get_context())
            logger.debug("Source location - " + str(src_location), extra=self.execution_context.get_context())
            logger.debug("Target location - " + str(target_location), extra=self.execution_context.get_context())
            output = urlparse(src_location)
            src_bucket_name = output.netloc
            src_key_path = output.path.lstrip("/")
            logger.debug("source location of file: " + src_key_path, extra=self.execution_context.get_context())

            output = urlparse(target_location)
            target_bucket_name = output.netloc
            target_key_path = output.path.lstrip("/")
            next_token = None

            status_message = ""
            while True:
                if next_token:
                    output = self.client.list_objects_v2(Bucket=src_bucket_name, Prefix=src_key_path,
                                                         ContinuationToken=next_token)
                else:
                    output = self.client.list_objects_v2(Bucket=src_bucket_name, Prefix=src_key_path)
                logger.debug(output, extra=self.execution_context.get_context())
                if "Contents" in output:
                    for content in output['Contents']:
                        object_path = content['Key']
                        status_message = "Copying object= " + object_path + " from bucket " + src_bucket_name
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        copy_source = {'Bucket': src_bucket_name, 'Key': object_path}
                        target_file_path = os.path.join(target_key_path, ntpath.basename(object_path))
                        if copy_with_suffix:
                            if not target_file_path.endswith(copy_with_suffix):
                                status_message = "not copying file", target_file_path
                                logger.debug(status_message, extra=self.execution_context.get_context())
                                continue
                        self.client.copy_object(Bucket=target_bucket_name, CopySource=copy_source, Key=target_file_path)
                else:
                    raise Exception("source s3 path does not exist")
                if "NextContinuationToken" in output:
                    next_token = output["NextContinuationToken"]
                else:
                    status_message = "Next token is not found hence breaking out of while loop"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    break

            logger.info("Completing copying file from one s3 location to another", extra=self.execution_context.get_context())

            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logger.error(error_message, extra=self.execution_context.get_context())
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


            # path if file or directory uploads both-works fine with both

    def upload_s3_object(self, local_path, s3_path):

        """
            Purpose     :   uploading a file from ec2 machine to s3
                                a. if path of directory(ie. /dir1) mentioned,lists all the files in the directory and uploads to s3
                                b. if path inside the directory (ie. /dir1/ )mentioned ,lists all the files and uploads to s3
                                c.if patn of file mentioned,uploads it to s3
            Input       :   local path(ie. /<filepath>),s3 path(i.e s3://<bucket-name>/<filepath>  )
            Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
        """

        status_message = ""
        try:
            status_message = "starting upload_s3_object object"
            logger.info(status_message, extra=self.execution_context.get_context())
            output = urlparse(s3_path)
            bucket_name = output.netloc
            key_path = output.path.lstrip("/")
            logger.debug("local path - " + str(local_path), extra=self.execution_context.get_context())
            logger.debug("s3_path - " + str(s3_path), extra=self.execution_context.get_context())
            #logger.debug(str(os.getcwd())+"\t"+str(os.listdir()))
            print(str(os.path.exists(local_path.strip())))
            local_path = local_path.strip()
            if not (os.path.exists(local_path)):
                return {STATUS_KEY: FAILED_KEY, ERROR_KEY: "source path does not exist"}

            if os.path.isdir(local_path):
                for file_name in os.listdir(local_path):
                    src_file_path = os.path.join(local_path, file_name)
                    target_file_path = os.path.join(key_path, file_name)

                    self.client.upload_file(src_file_path, bucket_name, target_file_path)

                    status_message = "uploading file " + src_file_path + " to s3://" + target_file_path
                    logger.debug(status_message, extra=self.execution_context.get_context())

                status_message = "completing upload_s3_object object"
                logger.debug(status_message, extra=self.execution_context.get_context())

            elif os.path.isfile(local_path):

                logger.debug("It is a normal file", extra=self.execution_context.get_context())
                src_file_path = local_path
                logger.debug("file name" + ntpath.basename(local_path), extra=self.execution_context.get_context())
                target_file_path = os.path.join(key_path, ntpath.basename(local_path))
                logger.debug("target file path " + target_file_path, extra=self.execution_context.get_context())
                self.client.upload_file(src_file_path, bucket_name, target_file_path)

            logger.info("Completing upload of s3 object", extra=self.execution_context.get_context())
            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logger.error(error_message, extra=self.execution_context.get_context())
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def put_s3_object(self, s3_path, input_body):
        """
            Purpose     :   To  put string content in a file
            Input       :   s3 path(i.e s3://<bucket-name>/<filepath>),string content to be put in file
            Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
        """
        status_message = ""
        try:
            status_message = "starting put_s3_object object"
            logger.info(status_message)
            logger.debug("s3 path - " + str(s3_path))
            output = urlparse(s3_path)
            bucket_name = output.netloc
            key_path = output.path.lstrip("/")
            response = self.client.put_object(Body=input_body, Bucket=bucket_name, Key=key_path)
            logger.debug(response)

            logger.info("Completing putting a string in a file")

            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logger.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def get_extension(self, file_path):

        status_message = "Starting to get extension for file for input path : " + str(file_path)
        logger.debug(status_message, extra=self.execution_context.get_context())

        if file_path is None:
            raise Exception("File Path Can Not be None")

        if len(file_path) > 0:
            return file_path.split(".")[::-1][0]
        else:
            raise Exception("Error Occured In Getting File Extension For Input Path: " + str(file_path))

    def get_dynamic_repartition_value(self, s3_dir_path=None):

        try:

            status_message = "Starting get dynamic repartion value for s3 path : " + str(s3_dir_path)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if s3_dir_path is None:

                raise Exception("Input Path Can Not Be None")

            s3_loc = s3_dir_path.replace("*", "")
            conn = boto.connect_s3(host="s3.amazonaws.com")
            bucket_name = self.get_s3_bucket_name(s3_loc)
            bucket = conn.get_bucket(bucket_name)
            s3_folder_path = self.get_s3_folder_path(s3_loc)
            s3_folder_list = bucket.list(s3_folder_path)
            # total_size = 0
            max_size = 0
            ext = ""

            #            for file in s3_folder_list:
            #                total_size = total_size + file.size
            #                ext = self.get_extension(file.key)

            for file in s3_folder_list:
                if file.size > max_size:
                    max_size = file.size
                ext = self.get_extension(file.key)

            status_message = "Extension For File Is  : " + str(ext)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if ext.upper() == 'GZ':
                max_size = max_size * CommonConstants.GZ_COMPRESSION_RATIO
            if ext.upper() == 'ZIP':
                max_size = max_size * CommonConstants.ZIP_COMPRESSION_RATIO
            if ext.upper() == 'BZ2':
                max_size = max_size * CommonConstants.BZ2_COMPRESSION_RATIO

            status_message = "Total size for location : " + str(s3_dir_path) + " is : " + str(max_size)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if max_size > CommonConstants.BYTES_TO_GB:
                status_message = "File Greater then 1 GB"
                logger.debug(status_message, extra=self.execution_context.get_context())

            elif max_size <= CommonConstants.BYTES_TO_GB and max_size >= CommonConstants.BYTES_TO_MB:
                status_message = "File Size in MB"
                logger.debug(status_message, extra=self.execution_context.get_context())

            else:
                status_message = "File size in KB"
                logger.debug(status_message, extra=self.execution_context.get_context())

            partition = self.get_partition(max_size)
            status_message = "Number Of Partitions Assigned For The File Path  : " + str(s3_dir_path) + " Is : " \
                             + str(partition)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return partition
        except Exception as exception:
            status_message = "Exception In Get Dynamic repartiton Due To : " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)

    def fetch_new_cycle_id(self, process_id, frequency, data_date):
        """
            Purpose :   This method returns cycle id in "NEW" state for input process id, frequency and data date
            Input   :   Process Id, Frequency, Data Date
            Output  :   Cycle Id
        """
        try:
            status_message = "Started preparing query to fetch new cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.debug(status_message, extra=self.execution_context.get_context())
            check_new_cycle_status_query = "Select cycle_id from {audit_db}.{cycle_details} where " \
                                           "process_id={process_id} and frequency='{frequency}' and " \
                                           "data_date='{data_date}' and cycle_status='{cycle_status}'" \
                .format(audit_db=self.audit_db,
                        cycle_details=CommonConstants.LOG_CYCLE_DTL,
                        process_id=process_id,
                        frequency=frequency,
                        data_date=data_date,
                        cycle_status=CommonConstants.STATUS_NEW)
            logger.debug(check_new_cycle_status_query, extra=self.execution_context.get_context())
            cycle_id_result = MySQLConnectionManager().execute_query_mysql(check_new_cycle_status_query, True)
            status_message = "Completed executing query to fetch new cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(cycle_id_result, extra=self.execution_context.get_context())
            cycle_id = cycle_id_result['cycle_id']
            if cycle_id is None:
                status_message = "No cycle id is in " + CommonConstants.STATUS_NEW + " for process id:" + str(
                    process_id) + \
                                 " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            return cycle_id
        except Exception as exception:
            status_message = "Error occured in fetching new cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def update_cycle_cluster_id_entry(self, process_id, frequency, data_date, cycle_id, cluster_id):
        """
            Purpose :   This method updates cluster id in cycle log table
            Input   :   Process Id, Frequency, Data Date , Cycle Id, Cluster Id
            Output  :   NA
        """
        try:
            status_message = "Started preparing query to update cluster id '" + cluster_id + "' for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            cycle_update_audit_cluster_id_query = "Update {audit_db}.{cycle_details} set cluster_id='{cluster_id}' " \
                                                  "where process_id={process_id} and frequency='{frequency}' " \
                                                  "and data_date='{data_date}' and cycle_id = '{cycle_id}' and " \
                                                  "cycle_status='{cycle_status}'".format(audit_db=self.audit_db,
                                                                                         cycle_details=CommonConstants.LOG_CYCLE_DTL,
                                                                                         process_id=process_id,
                                                                                         frequency=frequency,
                                                                                         data_date=data_date,
                                                                                         cycle_id=cycle_id,
                                                                                         cluster_id=cluster_id,
                                                                                         cycle_status=CommonConstants.STATUS_RUNNING)
            logger.debug(cycle_update_audit_cluster_id_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(cycle_update_audit_cluster_id_query)
            status_message = "Completed executing query to update cluster id '" + cluster_id + "' for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in updating cluster id '" + cluster_id + "' for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def execute_threading(self, threads, thread_limit, queue_instance, failed_flag):
        """
            Purpose :   This method invokes thread execution for each thread
            Input   :   Thread List , Thread Limit
            Output  :   NA
        """
        thread_chunks = [threads[x:x + thread_limit] for x in range(0, len(threads), thread_limit)]
        for thread in thread_chunks:
            for thread_instance in thread:
                try:
                    thread_instance.start()
                except Exception as exception:
                    status_message = "Error in Thread Chunks start method"
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise exception
            for thread_instance in thread:
                try:
                    thread_instance.join()
                    while not queue_instance.empty():
                        result = queue_instance.get()
                        if result[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                            failed_flag = True
                except Exception as exception:
                    status_message = "Error in Thread Chunks join method"
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise exception
        return failed_flag


    def fetch_successful_parquet_path_for_adapter(self, source_name, step_name):
        """
            Purpose :   This method will fetch latest successful batch Id
            Input   :   Dataset Id
            Output  :   Batch Id
        """

        try:
            status_message = "Started preparing query to fetch latest successful parquet path for adapter:" \
                             "" + str(source_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_successful_parquet_path_query = """
                select structured_file_path from {audit_db}.{log_data_acquisition_dtl} dtl
                inner join {audit_db}.{log_data_acquisition_smry} smry
                on smry.status = dtl.status and smry.adapter_id = dtl.adapter_id and smry.{cycle_id} = dtl.{cycle_id} and smry.source_name = dtl.source_name
                where smry.status = 'SUCCEEDED' and dtl.step_name = '{step_name}' and smry.source_name = '{source_name}'
                and dtl.{cycle_id} = (select max({cycle_id}) from {audit_db}.{log_data_acquisition_smry} where status = 'SUCCEEDED'  and source_name = '{source_name}')
            """.format(log_data_acquisition_dtl=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                       log_data_acquisition_smry=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                       cycle_id=CommonConstants.CYCLE_ID_LOG_KEY, source_name=source_name, audit_db=self.audit_db,
                       step_name=step_name)

            logger.debug(fetch_successful_parquet_path_query, extra=self.execution_context.get_context())

            structured_file_path_result = MySQLConnectionManager(self.execution_context).execute_query_mysql \
                (fetch_successful_parquet_path_query, True)
            status_message = "Completed executing query to fetch latest successful parquet path for adapter:" \
                             "" + str(source_name)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(structured_file_path_result, extra=self.execution_context.get_context())
            structured_file_path = structured_file_path_result['structured_file_path']
            if structured_file_path is None:
                status_message = "There is no successful parquet path for adapter:" + str(source_name)
                # logger.error(status_message, extra=self.execution_context.get_context())
                # raise Exception(status_message)
            return structured_file_path
        except Exception as exception:
            status_message = "Exception occured in fetching successful parquet path for adapter:" + str(source_name)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def perform_data_stitching_type_1(self, unique_column_names, source_location, current_df):
        """
            Purpose :   This method performs data stitching of Type 1
            Input   :   Stitch Key Columns, Source Location, Current Df , Batch Id
            Output  :   Stitched DF
        """
        status_message = ""
        try:
            status_message = "Starting to execute perfor data stitching based on column name list:" + str(
                unique_column_names)
            logger.debug(status_message, extra=self.execution_context.get_context())
            from pyspark.sql.functions import col,lit,when
            from pyspark.sql import SparkSession

            module_name_for_spark = str(MODULE_NAME + "stitching")
            spark = SparkSession.builder.appName(module_name_for_spark).enableHiveSupport().getOrCreate()
            # spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,
            # CommonConstants.HADOOP_CONF_PROPERTY_DICT)
            status_message = "Source location for data stitching is:" + source_location
            logger.info(status_message, extra=self.execution_context.get_context())
            history_df = spark.read.format("parquet").load(source_location)
            unique_column_list = unique_column_names.split(",")
            pk_unique_column_list = ["pk_" + s for s in unique_column_list]
            unique_column_dict = dict(zip(unique_column_list, pk_unique_column_list))
            for column, pk_column in unique_column_dict.items():
                current_df = current_df.withColumn(pk_column, when(col(column).isNull(), lit("0")).otherwise(col(column)))
                current_df = current_df.fillna({pk_column: 0})
                history_df = history_df.withColumn(pk_column, when(col(column).isNull(), lit("0")).otherwise(col(column)))
                history_df = history_df.fillna({pk_column: 0})
            # Prepare dataframe of new/updated records
            updated_record_df = current_df.join(history_df, [history_df[f] == current_df[s] for (f, s) in
                                                             zip(pk_unique_column_list, pk_unique_column_list)],
                                                "left_outer").select(current_df["*"])
            final_where_condition = ""
            new_unique_column_list = [None] * len(pk_unique_column_list)
            for index in range(0, len(pk_unique_column_list)):
                # Renaming unique columns for join condition
                new_unique_column_list[index] = pk_unique_column_list[index] + "_tmp"
                # Renaming columns associated with current dataframe
                current_df = current_df.withColumnRenamed(pk_unique_column_list[index],
                                                          pk_unique_column_list[index] + "_tmp")
                # creating where condition with renamed columns
                initial_where_condition = pk_unique_column_list[index] + "_tmp" + " is null"
                if index != (len(pk_unique_column_list) - 1):
                    initial_where_condition = initial_where_condition + " and "
                final_where_condition = final_where_condition + initial_where_condition

            status_message = "Final where condition:" + str(final_where_condition)
            logger.info(status_message, extra=self.execution_context.get_context())
            # Prepare dataframe of history record not present in current(delta)
            new_record_df = history_df.alias('a').join(current_df.alias('b'),
                                                       [history_df[f] == current_df[s] for (f, s) in
                                                        zip(pk_unique_column_list, new_unique_column_list)],
                                                       how='left_outer').where(final_where_condition).select(
                history_df["*"])
            print(updated_record_df.printSchema())
            print(new_record_df.printSchema())
            stitched_df = updated_record_df.union(new_record_df)
            stitched_df = stitched_df.distinct()

            for pk_column in pk_unique_column_list:
                stitched_df = stitched_df.drop(pk_column)

            status_message = "Completed executing data stitching based on column name list:" + str(
                unique_column_names)
            logger.debug(status_message, extra=self.execution_context.get_context())

            return stitched_df
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"function_status": 'FAILED', "traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e
