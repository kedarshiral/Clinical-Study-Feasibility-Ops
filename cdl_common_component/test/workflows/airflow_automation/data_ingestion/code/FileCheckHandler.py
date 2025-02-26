#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   FileCheckHandler
#  Purpose             :   This module will perform the pre-configured steps before invoking FileCheckUtility.py.
#  Input Parameters    :   data_source, subject_area
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   19th November 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   File Check Module development
# ######################################################################################################################

# Library and external modules declaration
import os
import traceback
import sys

sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from FileCheckUtility import FileCheckUtility
from MySQLConnectionManager import MySQLConnectionManager
from PySparkUtility import PySparkUtility

# all module level constants are defined here
MODULE_NAME = "FileCheckHandler"
PROCESS_NAME = "File Schema Check"

USAGE_STRING = """
SYNOPSIS
    python FileCheckHandler.py <data_source> <subject_area>

    Where
        input parameters : data_source , subject_area

"""


class FileCheckHandler:
    # Default constructor
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the file check
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def execute_file_check(self, file_master_id=None, batch_id=None):
        status_message = ""
        curr_batch_id = batch_id
        log_info = {}
        try:
            status_message = "Executing File Check function of module FileCheckHandler"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Input Validations
            if file_master_id is None:
                raise Exception('File Master ID is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"file_master_id": file_master_id,
                                                "batch_id": batch_id})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get pre-landing location and data_set_id from MySQL table for given data source and subject area
            data_set_info = CommonUtils().get_dataset_info(file_master_id)
            pre_landing_location = data_set_info['dataset_hdfs_pre_landing_location']
            # dataset_id = data_set_info['dataset_id_pk']

            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, file_master_id)

            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)

            # Fetch column metadata from mysql table for provided dataset
            fetch_column_metadata_query = "select * from {audit_db}.{column_metadata_table} where " \
                                          "table_id_fk in (select table_id_pk from " \
                                          "{audit_db}.{table_metadata_table} where datasource_id_fk = ".__add__ \
                (str(file_master_id)).__add__(")"). \
                format(audit_db=CommonConstants.AUDIT_DB_NAME,
                       column_metadata_table=CommonConstants.COLUMN_METADATA_TABLE,
                       table_metadata_table=CommonConstants.TABLE_METADATA_TABLE)
            column_details = MySQLConnectionManager().execute_query_mysql(fetch_column_metadata_query)
            column_id_seq_dict = dict()
            column_name_list = []
            for column_detail in column_details:
                column_id_seq_dict.__setitem__(column_detail['column_sequence'], column_detail['column_name'])
            for column_seq in sorted(column_id_seq_dict.keys()):
                column_name_list.append(column_id_seq_dict.get(column_seq))

            # Loop through each file and invoke File Check Utility
            for file_detail in file_details:
                # Get detail for each file
                file_id = file_detail['file_id_pk']
                file_name = file_detail['file_name']

                # Creating process log entry
                log_info = {CommonConstants.BATCH_ID: curr_batch_id, CommonConstants.FILE_ID: file_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                            CommonConstants.FILE_NAME: file_name}
                CommonUtils().create_process_log_entry(CommonConstants.AUDIT_DB_NAME, log_info)
                status_message = "Created process log entry "
                logger.debug(status_message, extra=self.execution_context.get_context())

                # Get the complete file url
                file_complete_url = pre_landing_location.__add__('/').__add__("batch_id=" + str(batch_id)).__add__(
                    '/').__add__(
                    "file_id=" + str(file_id)).__add__('/*')
                self.execution_context.set_context({'file_complete_path': file_complete_url})
                status_message = 'Fetched complete file url '
                logger.debug(status_message, extra=self.execution_context.get_context())

                # Invoke FileChecker Utility
                header_check = FileCheckUtility().perform_header_check(file_complete_url, column_name_list)

                # Raise exception, if header sequence is not correct
                if not header_check:
                    status_message = 'Header Check failed for file id :' + str(file_id)
                    raise Exception(status_message)

                # Update process log and file audit entry to SUCCESS
                record_count = PySparkUtility().get_file_record_count(file_complete_url)
                log_info['record_count'] = str(record_count)
                log_info['message'] = "Process Completed Successfully"
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)

            # status_message = "Completed check of pre-landing files for " + data_source + " and " + subject_area
            self.execution_context.set_context({"pre_landing_location": pre_landing_location})
            logger.debug(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info['record_count'] = str(0)
            log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)]
            CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)
            CommonUtils().update_file_audit_status_by_batch_id(CommonConstants.AUDIT_DB_NAME, log_info)
            CommonUtils().update_batch_status(CommonConstants.AUDIT_DB_NAME, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e


if __name__ == '__main__':
    file_master_id = sys.argv[1]
    batch_id = sys.argv[2]

    file_check_handler = FileCheckHandler()
    return_value = file_check_handler.execute_file_check(file_master_id, batch_id)
    status_msg = "Completed execution for File Check Handler with status "
    sys.stdout.write("batch_id=" + batch_id)
