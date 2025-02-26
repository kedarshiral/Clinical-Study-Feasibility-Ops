#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Archive Source File Wrapper
#  Purpose             :   This Wrapper Will Archive Source File
#  Input Parameters    :   Dataset Id
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   5rd Jan 2018
#  Last changed by     :   Pushpendra Singh
#  Reason for change   :   Archive Source File Handler Development
# ######################################################################################################################

# Library and external modules declaration
import json
import datetime
import sys
import traceback
import time
import os
sys.path.insert(0, os.getcwd())
from S3MoveCopyUtility import S3MoveCopyUtility
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
from LogSetup import logger

MODULE_NAME = "ArchiveSourceFileWrapper"
PROCESS_NAME = 'Archive File From Source'

class ArchiveFileHandler:
    def __init__(self,dataset_id=None, cluster_id=None, workflow_id=None, execution_context=None):
        self.execution_context = ExecutionContext()
        if execution_context is None:
            self.execution_context.set_context({"module_name": MODULE_NAME})
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": dataset_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### archive_source_file_handler ###############################################
    # Purpose            :   Archiving Source File Handler
    # Input              :   Dataset Id
    # Output             :   returns the status SUCCESS or FAILURE
    #
    #
    #
    # ##################################################################################################################
    def archive_source_file(self, dataset_id=None, batch_id=None):
        curr_batch_id=batch_id
        try:
            status_message = "Executing File Archiving For Dataset Id: "+dataset_id
            if dataset_id is None or batch_id is None:
                raise Exception('Dataset Id or Batch Id is not provided')
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get source location from MySQL table for given dataset id
            dataset_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
            dataset_archive_location = dataset_info['dataset_archive_location']
            dataset_archive_location = dataset_archive_location.rstrip('/')
            dataset_file_source_location = dataset_info['dataset_file_source_location']
            dataset_file_source_location = dataset_file_source_location.rstrip('/')
            status_message = "Completed fetch of source_location for " + str(dataset_id)
            status_message = status_message + " - " + dataset_archive_location
            logger.info(status_message, extra=self.execution_context.get_context())
            #print(str(dataset_archive_location))
            #print(str(dataset_file_source_location))
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)
            now = datetime.datetime.now().strftime("%Y-%m-%d")

            for file_detail in file_details:
                # Get detail for each file
                file_id = file_detail[CommonConstants.MYSQL_FILE_ID]
                file_name = file_detail[CommonConstants.MYSQL_FILE_NAME]
                try:
                    # Creating process log entry
                    log_info = {CommonConstants.BATCH_ID: batch_id, CommonConstants.FILE_ID: file_id, CommonConstants.PROCESS_NAME: PROCESS_NAME, CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING, CommonConstants.FILE_NAME: file_name,"start_time":datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),"end_time":"NULL"}
                    CommonUtils().create_process_log_entry(CommonConstants.AUDIT_DB_NAME, log_info)
                    status_message = "Created process log entry "
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    # Create Source Location
                    source_location = dataset_file_source_location + "/" + str(file_name)
                    #print(str(source_location))
                    target_location = str(dataset_archive_location) + "/" + str(now)+ "/batch_id=" + str(batch_id) + "/file_id=" + str(file_id)+ "/" + str(file_name)

                    S3MoveCopyUtility().copy_s3_to_s3_with_s3cmd(source_location, target_location, None)
                    # Update process log to success
                    log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED

                except Exception as exception:
                    print(str(exception))
                    status_message = "Archival Failed For File Id:" + str(file_id) + "due to" + str(exception)
                    log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    log_info['status_id'] = CommonConstants.FAILED_ID
                    log_info[CommonConstants.BATCH_ID]= curr_batch_id
                    CommonUtils().update_file_audit_status_by_batch_id(CommonConstants.AUDIT_DB_NAME, log_info)
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise exception
                finally:
                    CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)
                    status_message = "Updated process log for file:" + str(file_id)
                    logger.info(status_message, extra=self.execution_context.get_context())

            status_message = "Archival Completed for dataset id:" + str(dataset_id)
            self.execution_context.set_context({"dataset_archive_location(Source Location)": dataset_archive_location})
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            print(str(exception))
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            log_info[CommonConstants.BATCH_ID]= curr_batch_id
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            CommonUtils().update_batch_status(CommonConstants.AUDIT_DB_NAME, log_info)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: str(exception)}
            raise exception

    def main(self, dataset_id, batch_id):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for Source File Archival"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.archive_source_file(dataset_id, batch_id)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
               raise Exception
            status_message = "Completing the main function for Source File Archival"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            print str(exception)
            return result_dictionary

if __name__ == '__main__':
    dataset_id = sys.argv[1]
    batch_id = sys.argv[2]
    cluster_id = sys.argv[3]
    workflow_id = sys.argv[4]
    archive_source_file = ArchiveFileHandler(dataset_id, cluster_id, workflow_id)
    result_dict = archive_source_file.main(dataset_id, batch_id)
    STATUS_MSG = "\nCompleted execution for Archival Utility with status " + json.dumps(result_dict) + "\n"
    sys.stdout.write(STATUS_MSG)