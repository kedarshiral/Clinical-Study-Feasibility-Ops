#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# Library and external modules declaration
import json
import traceback
import sys
import datetime
import time
import os
sys.path.insert(0, os.getcwd())
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
from PreDQMStandardizationUtility import PreDQMStandardizationUtility
from PySparkUtility import PySparkUtility
# all module level constants are defined here
MODULE_NAME = "Pre DQM Standardization"
PROCESS_NAME = "Pre DQM Standardization"

USAGE_STRING = """
SYNOPSIS
    python PreDQMStandardizationHandler.py <dataset_id> <batch_id> <cluster_id> <workflow_id>

    Where
        input parameters : dataset_id , batch_id, cluster_id, workflow_id

"""


class PreDQMStandardizationHandler(object):
    def __init__(self,DATASET_ID=None, CLUSTER_ID=None, WORKFLOW_ID=None):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": DATASET_ID})
        self.execution_context.set_context({"cluster_id": CLUSTER_ID})
        self.execution_context.set_context({"workflow_id": WORKFLOW_ID})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

        # ########################################### execute_pre_dqm_standization ###########################
        # Purpose            :   Executing the Pre DQM standization
        # Input              :   NA
        # Output             :   NA
        # #########################################################################################
    def execute_pre_dqm_standization(self, dataset_id=None, batch_id=None):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        status_message = ""
        no_conversion_flag = False
        try:
            status_message = "Starting Pre DQM function of module Pre DQM Standization"
            print(status_message)
            logger.info(status_message, extra=self.execution_context.get_context())
            # Input Validations
            if dataset_id is None:
                raise Exception('Dataset Id is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"dataset_id": dataset_id, "batch_id": batch_id})
            logger.info(status_message, extra=self.execution_context.get_context())

            # Get landing location and data_set_id for given    data source and subject area
            data_set_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
            landing_location = data_set_info[CommonConstants.DATASET_LANDING_LOCATION]
            pre_dqm_location = data_set_info['dataset_hdfs_pre_dqm_location']

            #Get Column Meta Data For PRE DQM Checksnull_conversion,type_conversion
            query_string = "select column_name_pk,null_conversion,type_conversion from {automation_db_name}.{column_metadata_table} where table_id_fk={dataset_id}"
            execute_query = query_string.format(automation_db_name=CommonConstants.AUDIT_DB_NAME,column_metadata_table=CommonConstants.COLUMN_METADATA_TABLE,dataset_id=dataset_id)

            column_standard_checks = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            if len(column_standard_checks) == 0 or column_standard_checks is None:
                no_conversion_flag = True
            else:
                null_conversion_list = []
                upper_conversion_list = []
                lower_conversion_list = []
                for check in column_standard_checks:
                    if check['null_conversion'] != None:
                        null_conversion_list.append(check['column_name_pk'])
                    if check['type_conversion'] != None:
                        if check['type_conversion'] == 'Upper':
                            upper_conversion_list.append(check['column_name_pk'])
                        elif check['type_conversion'] == 'Lower':
                            lower_conversion_list.append(check['column_name_pk'])
                print(null_conversion_list)
                print(upper_conversion_list)
                print(lower_conversion_list)


            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)

            spark_context = PySparkUtility().get_spark_context()

            for file_detail in file_details:
                try:
                    file_id = file_detail[CommonConstants.MYSQL_FILE_ID]
                    file_name = file_detail[CommonConstants.MYSQL_FILE_NAME]
                    # Creating process log entry
                    log_info = {CommonConstants.BATCH_ID: batch_id, CommonConstants.FILE_ID: file_id, \
                                CommonConstants.PROCESS_NAME: PROCESS_NAME, \
                                CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING, \
                                CommonConstants.FILE_NAME: file_name}
                    CommonUtils().create_process_log_entry(CommonConstants.AUDIT_DB_NAME, log_info)
                    status_message = "Created process log entry "
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    if no_conversion_flag == False:

                        # Get the complete file url
                        file_complete_url = landing_location + "/batch_id=" + str(batch_id) + "/file_id=" + str( \
                            file_id)
                        self.execution_context.set_context({'file_complete_path': file_complete_url})
                        status_message = 'Fetched complete file url:' + file_complete_url
                        logger.info(status_message, extra=self.execution_context.get_context())
                        # Invoke DQMChecker Utility
                        file_df = spark_context.read.parquet(file_complete_url)
                        PreDQMStandardizationUtility().perform_pre_dqm_standardization(file_df, null_conversion_list, upper_conversion_list, lower_conversion_list, pre_dqm_location, file_id,batch_id, spark_context)
                        status_message = "PRE-DQM Standard Utility Completed "
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        # Update process log and file audit entry to SUCCESS
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                    else:
                        status_message = "No Pre - DQM checks configured for dataset id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        raise Exception
                except Exception as exception:
                    if no_conversion_flag == True:
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.NO_PRE_DQM_CONFIGURED
                    else:
                        log_info[CommonConstants.BATCH_ID] = batch_id
                        log_info['status_id'] = CommonConstants.FAILED_ID
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        CommonUtils().update_batch_status(CommonConstants.AUDIT_DB_NAME, log_info)

                        log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime(
                            '%Y-%m-%d %H:%M:%S')
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        log_info['status_id'] = CommonConstants.FAILED_ID
                        log_info[CommonConstants.BATCH_ID] = batch_id
                        CommonUtils().update_file_audit_status_by_batch_id(CommonConstants.AUDIT_DB_NAME,
                                                                                    log_info)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        CommonUtils().clean_hdfs_directory(
                            CommonConstants.DQM_ERROR_LOCATION + "/batch_id=" + str(
                                batch_id)+"/file_id="+str(file_id))
                        raise exception
                finally:
                    CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)
                    status_message = "Updated process log for file:" + str(file_id)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    if spark_context:
                        spark_context.stop()
            status_message = "Pre-DQM Completed for dataset id:" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            print(str(exception))
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            log_info[CommonConstants.BATCH_ID]= batch_id
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            CommonUtils().update_batch_status(CommonConstants.AUDIT_DB_NAME, log_info)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: str(exception)}
            raise exception


    # ############################################# Main ######################################
    # Purpose   : Handles the process of executing Pre DQM Standization returning the status
    #             and records (if any)
    # Input     : Requires dataset_id,batch id
    # Output    : Returns execution status and records (if any)
    # ##########################################################################################

    def main(self, dataset_id=None, batch_id=None):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for Pre Dqm Standization"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.execute_pre_dqm_standization(dataset_id, batch_id)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for Pre Dqm Standization"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            print(str(exception))
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception


if __name__ == '__main__':
    DATASET_ID = sys.argv[1]
    BATCH_ID = sys.argv[2]
    CLUSTER_ID = sys.argv[3]
    WORKFLOW_ID = sys.argv[4]
    pre_dqm_stand = PreDQMStandardizationHandler(DATASET_ID, CLUSTER_ID, WORKFLOW_ID)
    RESULT_DICT = pre_dqm_stand.main(DATASET_ID, BATCH_ID)
    STATUS_MSG = "\nCompleted execution for Pre-DQm Standization Utility with status " + json.dumps(RESULT_DICT) + "\n"
    sys.stdout.write("batch_id=" + str(BATCH_ID))