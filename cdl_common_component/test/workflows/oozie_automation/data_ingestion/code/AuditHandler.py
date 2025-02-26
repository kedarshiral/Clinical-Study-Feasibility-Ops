#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   AuditHandler
#  Purpose             :
#  Input Parameters    :
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :
#  Last changed by     :
#  Reason for change   :   Audit Handler Module development
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
from ParquetUtility import ParquetUtility

# all module level constants are defined here
MODULE_NAME = "Audit Handler"
PROCESS_NAME = "Update audit status after completion of all steps"

USAGE_STRING = """
SYNOPSIS
    python AuditHandler.py <file_master_id> <batch_id>

    Where
        input parameters : file_master_id, batch_id

"""


class AuditHandler:
    # Default constructor
    def __init__(self, file_master_id, cluster_id, workflow_id, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
	self.execution_context.set_context({"dataset_id": file_master_id})
	self.execution_context.set_context({"cluster_id": cluster_id})
	self.execution_context.set_context({"workflow_id": workflow_id})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the file audit update status
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def update_audit_status_to_success(self, batch_id=None):
        status_message = ""
        curr_batch_id = batch_id
        log_info = {}
        try:
            status_message = "Updating the audit status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Input Validations
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"batch_id": batch_id})
            logger.debug(status_message, extra=self.execution_context.get_context())
            log_info['batch_id'] = curr_batch_id
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
            log_info['status_id'] = CommonConstants.SUCCESS_ID
            CommonUtils().update_file_audit_status_by_batch_id(CommonConstants.AUDIT_DB_NAME, log_info)
            CommonUtils().update_batch_status(CommonConstants.AUDIT_DB_NAME, log_info)
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info['status_id'] = CommonConstants.FAILED_ID
            CommonUtils().update_file_audit_status_by_batch_id(CommonConstants.AUDIT_DB_NAME, log_info)
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
    cluster_id = sys.argv[3]
    workflow_id = sys.argv[4]
    audit_handler = AuditHandler(file_master_id, cluster_id, workflow_id)
    audit_handler.update_audit_status_to_success(batch_id)
    status_msg = "Updated the audit status for batch : " + str(batch_id)
