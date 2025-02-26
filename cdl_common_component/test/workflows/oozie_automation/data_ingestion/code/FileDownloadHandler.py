#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   FileDownloadHandler
#  Purpose             :   This module will perform the pre-configured steps before invoking FileDownloadUtility.py.
#  Input Parameters    :   dataset_id
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   22nd November 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   File Download Module development
# ######################################################################################################################

# Library and external modules declaration

import os
import traceback
import json
import sys
import hadoopy
import os.path

sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from PySparkUtility import PySparkUtility
from ConfigUtility import JsonConfigUtility
from S3MoveCopyUtility import S3MoveCopyUtility

# all module level constants are defined here
MODULE_NAME = "FileDownloadHandler"
PROCESS_NAME = "Download Files from source location"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"

USAGE_STRING = """
SYNOPSIS
    python FileDownloadHandler.py <dataset_id>

    Where
        input parameters : dataset_id

"""


class FileDownloadHandler:
    # Default constructor
    def __init__(self, dataset_id, cluster_id=None, workflow_id=None, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": dataset_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the file download command (main function)
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def execute_file_download(self, dataset_id=None, cluster_id=None, workflow_id=None):
        status_message = ""
        curr_batch_id = None
        log_info = {}
        audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "cluster_mode"])
        try:
            status_message = "Executing File Download function of module FileDownloadHandler"
            if dataset_id is None:
                raise Exception('Dataset ID is not provided')
            self.execution_context.set_context({"dataset_id": str(dataset_id)})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Create new batch for current run
            curr_batch_id = CommonUtils().create_batch_entry(audit_db,dataset_id)
            log_info[CommonConstants.BATCH_ID]=curr_batch_id
            status_message = "Created new batch with id :" + str(curr_batch_id)
            self.execution_context.set_context({"curr_batch_id": curr_batch_id})

            # Get source file location from MySQL table for given data source and subject area
            dataset_info = CommonUtils().get_dataset_info(dataset_id)
            source_file_location = dataset_info['dataset_file_source_location']
            source_file_pattern = dataset_info['dataset_file_name_pattern']
            source_file_type = dataset_info['dataset_file_source_type']
            #dataset_id = dataset_info['dataset_id_pk']
            status_message = "Completed fetch of source file location for " + str(dataset_id)
            status_message = status_message + " - " + source_file_location
            self.execution_context.set_context({"source_file_location": source_file_location})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get target file location from MySQL table for given data source and subject area
            target_file_location = dataset_info['dataset_hdfs_pre_landing_location']
            status_message = "Completed fetch of target file location for " + str(dataset_id)
            status_message = status_message + " - " + target_file_location
            self.execution_context.set_context({"target_file_location": target_file_location})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # To fetch s3 file list located at source location
            s3_file_list = CommonUtils().list_files_in_s3_directory(source_file_location)
            if not s3_file_list:
                raise Exception("No files available for Processing")
            load_files = []
            for file in s3_file_list:
                file_name = CommonUtils().get_s3_file_name(file)
                existing_file_details = CommonUtils().get_loaded_file_details(dataset_id, file_name,
                                                                                       PROCESS_NAME)
                if not existing_file_details:
                    load_files.append(file)

            if not load_files:
                status_message = "File is already processed"
                raise Exception(status_message)

            # To copy each file from source to target location
            status_message = "Started copy of files from source to target location"
            logger.debug(status_message, extra=self.execution_context.get_context())
            #cleanup_success = CommonUtils().clean_hdfs_directory(target_file_location)

            if True:
                for s3_file_path in load_files:
                    # Accumulation of process log entry and file audit details
                    try:
                        file_audit_info = {CommonConstants.FILE_NAME: s3_file_path,
                                           CommonConstants.FILE_STATUS: CommonConstants.STATUS_RUNNING,
                                           CommonConstants.DATASET_ID: dataset_id,
                                           CommonConstants.BATCH_ID: curr_batch_id,
                                           CommonConstants.CLUSTER_ID: cluster_id,
                                           CommonConstants.WORKFLOW_ID: workflow_id}
                        file_id = CommonUtils().create_file_audit_entry(audit_db,
                                                                                 file_audit_info)
                        # file_id = CommonUtils().
                        #                   get_sequence(CommonConstants.FILE_ID_GENERATOR_SEQUENCE_NAME)
                        status_message = "Creating process log entry "
                        self.execution_context.set_context({CommonConstants.BATCH_ID: curr_batch_id})
                        self.execution_context.set_context({CommonConstants.FILE_ID: file_id})
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        log_info = {CommonConstants.BATCH_ID: curr_batch_id, CommonConstants.FILE_ID: file_id,
                                    CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                    CommonConstants.FILE_NAME: s3_file_path,
                                    CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING}

                        # Create process log entry
                        CommonUtils().create_process_log_entry(audit_db, log_info)
                        status_message = "Created process log entry "
                        logger.debug(status_message, extra=self.execution_context.get_context())

                        # Invoke hadoop distcp command
                        file_name = str(CommonUtils().get_s3_file_name(s3_file_path))
                        print file_name
                        target_dir = target_file_location.__add__('/batch_id=' + str(curr_batch_id)). \
                            __add__('/file_id=' + str(file_id))
                        print target_dir
                        target_file_name = str(target_dir).__add__('/' + file_name)
                        print target_file_name

                        if cluster_mode == 'EMR':
                            S3MoveCopyUtility().copy_s3_to_s3_with_aws_s3_in_recursive(s3_file_path,
                                                                                       str(target_file_name))
                        else:
                            CommonUtils().perform_hadoop_distcp(s3_file_path, str(target_file_name))

                        status_message = "Completed copy of file from " + s3_file_path + " to " + target_file_location
                        logger.debug(status_message, extra=self.execution_context.get_context())

                        if CommonUtils().is_file_with_invalid_extension(target_file_name):
                            corrected_filename = CommonUtils().correct_file_extension(target_file_name, CommonConstants.PRE_LANDING_LOCATION_TYPE)
                        else:
                            corrected_filename = target_file_name
                        #ParquetUtility().convertToParquet(corrected_filename, str(target_dir))
                        #CommonUtils().delete_file(corrected_filename, CommonConstants.PRE_LANDING_LOCATION_TYPE)
                        status_message = "Completed copy of file from " + s3_file_path + " to " + target_file_location
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        record_count = PySparkUtility().get_file_record_count(corrected_filename)
                        # Update process log to SUCCESS
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                        log_info['record_count'] = str(record_count)
                        log_info['message'] = "Process Completed Successfully"
                        CommonUtils().update_process_status(audit_db, log_info)
                    except Exception as e:
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        log_info['status_id'] = CommonConstants.FAILED_ID
                        log_info['record_count'] = str(0)
                        log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)]
                        CommonUtils().update_process_status(audit_db, log_info)
                        CommonUtils().update_file_audit_status_by_batch_id(audit_db, log_info)
                        error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                                " ERROR MESSAGE: " + str(e)
                        self.execution_context.set_context({"traceback": error})
                        logger.error(error, extra=self.execution_context.get_context())
                        raise e
            status_message = "Completed copy of all files from source to target location"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return str(curr_batch_id)

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            log_info[CommonConstants.BATCH_ID]= curr_batch_id
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            if curr_batch_id:
                CommonUtils().update_batch_status(audit_db, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(e)
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            raise e


if __name__ == '__main__':
    dataset_id = sys.argv[1]
    cluster_id = sys.argv[2]
    workflow_id = sys.argv[3]
    write_batch_id_to_file_flag = sys.argv[4]

    file_download_handler = FileDownloadHandler(dataset_id, cluster_id, workflow_id)
    batch_id = file_download_handler.execute_file_download(dataset_id, cluster_id, workflow_id)

    if write_batch_id_to_file_flag == 'Y':
        batch_file_path = sys.argv[5]
        batch_dir_path = os.path.dirname(batch_file_path)
        if not os.path.exists(batch_dir_path):
            CommonUtils().execute_shell_command("mkdir -p " + str(batch_dir_path))

        file_content = open(str(batch_file_path), "w+")
        file_content.write(str(batch_id))

    status_msg = "Completed execution for File Download Handler"
    sys.stdout.write("batch_id=" + str(batch_id))

