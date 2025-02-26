#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ############################################## Module Information ####################################################
#   Module Name         : UpdateRecordCountUtility
#   Purpose             : This class is used for updating record count in log_file_dtl table
#   Input Parameters    : log info details in dictionary format
#   Output Value        :
#  Dependencies         :
#  Predecessor Module   :
#  Successor Module     :
#  Pre-requisites       : All the dependent libraries should be present in same environment from where the module would
#                         execute.
#   Last changed on     :
#   Last changed by     :
#   Reason for change   :
# ######################################################################################################################


# Library and external modules declaration
import os
import sys
import ast

sys.path.insert(0, os.getcwd())
import traceback
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from ConfigUtility import JsonConfigUtility
from PySparkUtility import PySparkUtility

MODULE_NAME = "UpdateRecordCountUtility"


class UpdateRecordCountUtility(object):
    # Initialization instance of class
    def __init__(self, parent_execution_context=None):
        # Parent execution context can be passed to the utility, in case its not; then instantiate the execution context
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)

    def update_log_file_dtl(self, db_name=None, log_info=None, file_url=None, file_type=None, header=None):
        try:
            status_message = "Starting to execute record update process in log file dtl table"
            logger.debug(status_message, extra=self.execution_context.get_context())
            record_count = 0
            if file_type == "parquet":
                record_count = PySparkUtility().get_parquet_record_count(file_url)
            else:
                record_count = PySparkUtility().get_file_record_count(file_url, header)

            log_info['record_count'] = str(record_count)
            logger.debug("Record Count of file is " + str(record_count), extra=self.execution_context.get_context())
            CommonUtils().update_process_status(db_name, log_info)
            logger.debug("Updated record count in log_file_dtl table", extra=self.execution_context.get_context())
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e


if __name__ == '__main__':
    db_name = str(sys.argv[1])
    log_info = str(sys.argv[2])
    file_url = str(sys.argv[3])
    file_type = str(sys.argv[4])
    header = None
    if file_type != 'parquet':
        header = str(sys.argv[5])
    log_info = ast.literal_eval(log_info)
    UpdateRecordCountUtility().update_log_file_dtl(db_name, log_info, file_url, file_type, header)


