#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
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

sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager
from ParquetUtility import ParquetUtility
import threading
from PySparkUtility import PySparkUtility

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
    def __init__(self, file_master_id, cluster_id, workflow_id, parent_execution_context=None):
        self.exc_info = None
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
    # Purpose            :   Executing the file check
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def execute_copy_to_landing(self, file_master_id=None, batch_id=None):
        status_message = ""
        curr_batch_id = batch_id
        log_info = {}
        try:
            status_message = "Executing File Copy function of module LandingCopyHandler"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Input Validations
            if file_master_id is None:
                raise Exception('Dataset ID is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"file_master_id": file_master_id,
                                                "batch_id": batch_id})
            logger.debug(status_message, extra=self.execution_context.get_context())
            # Get pre-landing location, landing and data_set_id from MySQL table for given data source and subject area
            data_set_info = CommonUtils().get_dataset_information_from_dataset_id(file_master_id)
            pre_landing_location = data_set_info['dataset_hdfs_pre_landing_location']
            landing_location = data_set_info['dataset_hdfs_landing_location']
            dataset_id = file_master_id
            data_delimiter = data_set_info['dataset_file_filed_delimiter']
            header_flag = str(data_set_info['dataset_header_available_flag'])
            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)
            log_info = {CommonConstants.BATCH_ID: curr_batch_id,
                        CommonConstants.PROCESS_NAME: PROCESS_NAME,
                        CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                        "start_time": datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                        "end_time": "NULL"
                        }

            module_name = 'LandingCopy - ' + str(batch_id)
            i = 0
            threads = []
            thread_limit = int(CommonConstants.MAX_THREAD_LIMIT)
            for file_detail in file_details:
                t = threading.Thread(target=self.landing_copy_task, args=(file_detail, log_info, pre_landing_location,
                                                                          landing_location, dataset_id, batch_id,
                                                                          header_flag,
                                                                          data_delimiter))
                print 'Spark task for File ', i + 1, 'has started'
                i = i + 1
                threads.append(t)

            thread_chunks = [threads[x:x + thread_limit] for x in xrange(0, len(threads), thread_limit)]
            for x in thread_chunks:
                for y in x:
                    try:
                        y.start()
                    except Exception as e:
                        print "Error in thread chunk start()"
                        print str(e)
                        logger.error("Error in Thread Chunks")
                        logger.error(str(e))
                        raise e
                for y in x:
                    try:
                        y.join()
                    except Exception as e:
                        print "Error in thread chunk join()"
                        print str(e)
                        logger.error("Error in Thread Chunks join()")
                        logger.error(str(e))
                        raise e
            if self.exc_info:
                raise self.exc_info[1], None, self.exc_info[2]

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            print str(e)
            log_info[CommonConstants.BATCH_ID] = curr_batch_id
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            CommonUtils().update_batch_status(CommonConstants.AUDIT_DB_NAME, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    # def landing_copy_task(self, file_detail, log_info, spark, pre_landing_location, landing_location, dataset_id, batch_id,
    #                      header_flag, data_delimiter):
    def landing_copy_task(self, file_detail, log_info, pre_landing_location, landing_location, dataset_id, batch_id,
                          header_flag, data_delimiter):
        try:
            file_id = file_detail['file_id_pk']
            file_name = file_detail['file_name']
            log_info = {CommonConstants.BATCH_ID: batch_id, CommonConstants.FILE_ID: file_id,
                        CommonConstants.PROCESS_NAME: PROCESS_NAME,
                        CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                        CommonConstants.FILE_NAME: file_name,
                        "start_time": datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                        "end_time": "NULL"
                        }

            CommonUtils().create_process_log_entry(CommonConstants.AUDIT_DB_NAME, log_info)
            status_message = "Created process log entry "
            # logger.debug(status_message, extra=self.execution_context.get_context())
            # Get the complete file url
            file_source_url = pre_landing_location.__add__('/').__add__("batch_id=" + str(batch_id)).__add__(
                '/').__add__(
                "file_id=" + str(file_id)).__add__('/*')
            file_target_url = landing_location.__add__('/').__add__("batch_id=" + str(batch_id)).__add__(
                '/').__add__(
                "file_id=" + str(file_id))
            self.execution_context.set_context({'file_source_path': file_source_url})
            self.execution_context.set_context({'file_target_path': file_target_url})
            status_message = 'Fetched complete file urls for source and target '
            # logger.debug(status_message, extra=self.execution_context.get_context())

            # Invoke Parquet Utility
            spark_submit_command = 'spark2-submit --jars spl.jar --packages com.databricks:spark-csv_2.10:1.4.0 --keytab ' \
                                   + str(CommonConstants.KEYTAB) + ' --principal ' \
                                   + str(CommonConstants.PRINCIPAL) + ' ParquetUtility.py  '
            landing_copy_command = str(spark_submit_command) + str(file_source_url) + ' ' + str(
                    file_target_url) + ' ' + str(True) + ' ' + str(dataset_id) + ' \\' + str(
                    data_delimiter) + ' ' + str(header_flag)
            CommonUtils().execute_shell_command(str(landing_copy_command))

            # Update process log and file audit entry to SUCCESS
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
            log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            log_info['status_id'] = CommonConstants.SUCCESS_ID
            record_count = PySparkUtility().get_file_record_count(file_target_url)
            log_info['record_count'] = str(record_count)
            log_info['message'] = "Process Completed Successfully"
            CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)

            status_message = "Completed check of pre-landing files for " + str(dataset_id)
            self.execution_context.set_context({"pre_landing_location": pre_landing_location})
            # logger.debug(status_message, extra=self.execution_context.get_context())
            # Get detail for each file
        except Exception as e:
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info[CommonConstants.BATCH_ID] = batch_id
            log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info['record_count'] = str(0)
            log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)]
            CommonUtils().update_file_audit_status_by_batch_id(CommonConstants.AUDIT_DB_NAME, log_info)
            CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            # logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            self.exc_info = sys.exc_info()
            raise e


if __name__ == '__main__':
    file_master_id = sys.argv[1]
    batch_id = sys.argv[2]
    cluster_id = sys.argv[3]
    workflow_id = sys.argv[4]
    landing_copy_handler = LandingCopyHandler(file_master_id, cluster_id, workflow_id)
    landing_copy_handler.execute_copy_to_landing(file_master_id, batch_id)
    status_msg = "Completed execution for Parquet Conversion and copy to landing zone for batch : " + str(batch_id)
    sys.stdout.write("batch_id=" + str(batch_id))
