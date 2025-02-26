# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Staging Handler
#  Purpose             :   This module will perform the pre-configured steps before invoking StagingUtility.py.
#  Input Parameters    :   data_source, subject_area
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   19th November 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   Staging Module development
# ######################################################################################################################

# Library and external modules declaration
import os
import traceback
import sys
import hadoopy

sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from FileCheckUtility import FileCheckUtility
from MySQLConnectionManager import MySQLConnectionManager
from ParquetUtility import ParquetUtility
from PreDQMStandardization import PreDQMStandardizationUtility
from StagingUtility import StagingUtility

# all module level constants are defined here
MODULE_NAME = "StagingHandler"
PROCESS_NAME = "Staging"

USAGE_STRING = """
SYNOPSIS
    python StagingHandler.py <data_source> <subject_area>

    Where
        input parameters : data_source , subject_area, batch_id

"""


class StagingHandler:
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
    def execute_staging(self, file_master_id=None, batch_id=None):
        status_message = ""
        curr_batch_id = batch_id
        log_info={}
        try:
            status_message = "Executing File Copy function of module StagingHandler"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Input Validations
            if file_master_id is None:
                raise Exception('File Master id is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is not provided")
            self.execution_context.set_context({"file_master_id": file_master_id})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get pre-landing location, landing and data_set_id from MySQL table for given data source and subject area
            data_set_info = CommonUtils().get_dataset_info(file_master_id)
            print data_set_info
            landing_location = data_set_info['dataset_hdfs_pre_dqm_location']
            post_dqm_location = data_set_info['dataset_hdfs_post_dqm_location']
            dqm_error_location = CommonConstants.DQM_ERROR_LOCATION
            print post_dqm_location
            print landing_location
            print dqm_error_location
            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, file_master_id)

            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)

            # Loop through each file and invoke Staging Utility
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
                landing_url = landing_location.__add__('/').__add__("batch_id=" + str(batch_id)).__add__(
                    '/').__add__(
                    "file_id=" + str(file_id))
                post_dqm_url = post_dqm_location.__add__('/').__add__("batch_id=" + str(batch_id)).__add__(
                    '/').__add__(
                    "file_id=" + str(file_id))
                file_error_url = dqm_error_location.__add__('/').__add__("batch_id=" + str(batch_id))
                logger.debug(status_message, extra=self.execution_context.get_context())

                # Invoke Staging Utility
                StagingUtility().perform_dqm_filter(landing_url, file_error_url, post_dqm_url, file_id)

                # Update process log and file audit entry to SUCCESS
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)

            status_message = "Completed Staging filtering for file_master id: " + str(file_master_id)
            logger.debug(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)
            log_info['status_id'] = CommonConstants.FAILED_ID
            CommonUtils().update_file_audit_status(CommonConstants.AUDIT_DB_NAME, log_info)
            log_info[CommonConstants.BATCH_ID] = curr_batch_id
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

    staging_handler = StagingHandler()
    staging_handler.execute_staging(file_master_id, batch_id)
    sys.stdout.write("batch_id="+batch_id)