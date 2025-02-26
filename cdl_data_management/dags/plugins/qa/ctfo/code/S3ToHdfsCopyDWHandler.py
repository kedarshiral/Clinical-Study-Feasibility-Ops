#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import os
import sys
import traceback
import subprocess
from threading import Thread
import queue
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from LogSetup import logger

sys.path.insert(0, os.getcwd())

MODULE_NAME = "S3ToHdfsCopyDWHandler"

"""
Module Name         :   S3ToHdfsCopyDWHandler
Purpose             :   This module will copy data from S3 to HDFS as part of DW Step
Input Parameters    :   process id, frequency, data date, cycle id
Output Value        :   None
Pre-requisites      :   None
Last changed on     :   25-06-2018
Last changed by     :   Sushant Choudhary
Reason for change   :   Major Code Issue Fix
"""

class S3ToHdfsCopyDWHandler(object):
    def __init__(self, parent_execution_context=None):
        self.process_id = sys.argv[1]
        self.frequency = sys.argv[2]
        self.data_date = sys.argv[3]
        self.cycle_id = sys.argv[4]
        self.step_name = None
        self.configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_id": self.process_id})
        self.execution_context.set_context({"frequency": self.frequency})
        self.execution_context.set_context({"data_date": self.data_date})
        self.execution_context.set_context({"cycle_id": self.cycle_id})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def copy_data_s3_hdfs_handler(self):
        """
            Purpose :   Driver handler method for calling helper functions required for copying data from HDFS to S3
            Input   :   NA
            Output  :   NA
        """
        try:
            status_message = "Starting s3 data copy to hdfs for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            self.step_name = CommonConstants.DATA_COPY_S3_HDFS_STEP
            self.execution_context.set_context({"step_name": self.step_name})
            CommonUtils(self.execution_context).create_step_audit_entry(self.process_id, self.frequency, self.step_name,
                                                           self.data_date,
                                                           self.cycle_id)
            status_message = "Starting preparing query to fetch dataset dependency details for process_id:" \
                             + str(self.process_id) + " " + "and frequency:" + str(self.frequency) + " " + "and " \
                                                                                                           "data_date:" + str(
                self.data_date) + " " + "and cycle id:" + str(self.cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_table_details_query = " Select {process_dependency_table}.dataset_id, hdfs_post_dqm_location," \
                                        " dataset_type, source_path, table_hdfs_path, dependency_pattern from " \
                                        "{audit_db}.{process_dependency_table} INNER JOIN {audit_db}.{dataset_table} " \
                                        "ON {process_dependency_table}.dataset_id" \
                                        "={dataset_table}.dataset_id where process_id={process_id} and data_date=" \
                                        "'{data_date}' and frequency='{frequency}' and " \
                                        "write_dependency_value={cycle_id_value} and copy_data_s3_to_hdfs_status is " \
                                        "NULL and lower({process_dependency_table}.table_type)=" \
                                        "'{table_type}'".format(
                audit_db=self.audit_db, process_dependency_table=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                dataset_table=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME, process_id=self.process_id,
                data_date=self.data_date, cycle_id_value=self.cycle_id, frequency=self.frequency,
                table_type=CommonConstants.SOURCE_TABLE_TYPE
            )
            logger.debug(fetch_table_details_query, extra=self.execution_context.get_context())
            table_resultset = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_table_details_query)
            status_message = "Completed executing query to fetch dataset dependency details for process_id:" \
                             + str(self.process_id) + " " + "and frequency:" + str(self.frequency) + " " + \
                             "and data_date:" + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(table_resultset, extra=self.execution_context.get_context())
            if table_resultset:
                thread_limit = int(len(table_resultset))
                status_message = "Thread limit is set to " +str(thread_limit)
                logger.info(status_message, extra=self.execution_context.get_context())
                queue_instance = queue.Queue()
                failed_flag = False
                threads = []
                for table_details in table_resultset:
                    status_message = "Dataset result dict:" + str(table_details)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    dataset_id = table_details['dataset_id']
                    self.execution_context.set_context({"dataset_id": dataset_id})
                    dataset_type = table_details['dataset_type']
                    dependency_pattern = table_details['dependency_pattern']
                    source_path = table_details['source_path']
                    self.validate_dataset_details(dataset_id, dataset_type, dependency_pattern, source_path)
                    status_message = "Dataset id is:" + str(dataset_id) + " and dataset type is:" + str(dataset_type)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    source_path_array = source_path.split("/")
                    if dependency_pattern == CommonConstants.LATEST_LOAD_TYPE:
                        if dataset_type.lower() == CommonConstants.RAW_DATASET_TYPE:
                            target_path = table_details['hdfs_post_dqm_location']
                            if target_path is None:
                                status_message = "Target HDFS path is not provided for dataset_id:" + str(dataset_id)
                                logger.error(status_message, extra=self.execution_context.get_context())
                                raise Exception(status_message)
                            target_path_value = source_path_array[len(source_path_array) - 1]
                        elif dataset_type.lower() == CommonConstants.PROCESSED_DATASET_TYPE:
                            target_path = table_details['table_hdfs_path']
                            if target_path is None:
                                status_message = "Target HDFS path is not provided for dataset_id:" + str(dataset_id)
                                logger.error(status_message, extra=self.execution_context.get_context())
                                raise Exception(status_message)
                            data_date = source_path_array[len(source_path_array) - 2]
                            cycle_id = source_path_array[len(source_path_array) - 1]
                            target_path_value = str(data_date) + "/" + str(cycle_id)
                        else:
                            status_message = "Dataset type is incorrectly configured for dataset_id:" + str(dataset_id)
                            logger.error(status_message, extra=self.execution_context.get_context())
                            raise Exception(status_message)
                        final_target_path = target_path + "/" + str(target_path_value)
                    elif dependency_pattern == CommonConstants.FULL_LOAD_TYPE:
                        if dataset_type.lower() == CommonConstants.RAW_DATASET_TYPE:
                            target_path = table_details['hdfs_post_dqm_location']
                        elif dataset_type.lower() == CommonConstants.PROCESSED_DATASET_TYPE:
                            target_path = table_details['table_hdfs_path']
                        else:
                            status_message = "Dataset type is incorrectly configured for dataset_id:" + str(dataset_id)
                            logger.error(status_message, extra=self.execution_context.get_context())
                            raise Exception(status_message)
                        final_target_path = target_path
                    else:
                        status_message = "Dependency pattern is incorrectly configured for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    status_message = "Source S3 path is:'" + str(source_path) + "' for dataset_id:" +str(dataset_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    status_message = "Target HDFS path is:'" + str(final_target_path) + "' for dataset_id:" +\
                                     str(dataset_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    thread_instance = Thread(target=lambda q, source_location, target_location, dataset_id: q.put(
                        self.call_s3_to_hdfs_transfer(source_location, target_location, dataset_id)),
                           args=(queue_instance, source_path, final_target_path, dataset_id))
                    threads.append(thread_instance)
                failed_flag = CommonUtils(self.execution_context).execute_threading(threads, thread_limit,
                                                                                    queue_instance, failed_flag)
                if failed_flag:
                    status_message = "Data copy failed"
                    raise Exception(status_message)
            else:
                status_message = "There are no datasets to copy from s3 to  hdfs for process_id:" + str(self.process_id) + " " \
                                 + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                                 + " " + "cycle id:" + str(self.cycle_id)
                logger.warn(status_message, extra=self.execution_context.get_context())
            status = CommonConstants.STATUS_SUCCEEDED
        except Exception as exception:
            status = CommonConstants.STATUS_FAILED
            status_message = "Exception in s3 data copy to hdfs for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception
        finally:
            CommonUtils(self.execution_context).update_step_audit_entry(self.process_id, self.frequency,
                                                                        self.step_name, self.data_date,
                                                  self.cycle_id, status)
            if status == CommonConstants.STATUS_FAILED:
                CommonUtils(self.execution_context).update_cycle_audit_entry(self.process_id, self.frequency,
                                                                             self.data_date,
                                                            self.cycle_id, status)
            status_message = "Completed s3 data copy to hdfs for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())

    def validate_dataset_details(self, dataset_id, dataset_type, dependency_pattern, source_path):
        """
            Purpose :   This method invokes validates dataset related attributes
            Input   :   Dataset Id, Dataset Type, Dependency Pattern, Source Path
            Output  :   NA
        """
        if dependency_pattern is None:
            status_message = "Dependency pattern is not provided for dataset_id:" + str(dataset_id)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)
        if dataset_type is None:
            status_message = "Dataset type is not provided for dataset_id:" + str(dataset_id)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)
        if source_path is None:
            status_message = "Source S3 path is not provided for dataset_id:" + str(dataset_id)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)

    def call_s3_to_hdfs_transfer(self, source_path, target_path, dataset_id):
        """
            Purpose :   This method calls S3 to Cluster Handler with source and target path
            Input   :   Source Path, Target Path, Dataset Id
            Output  :   NA
        """
        try:
            status_message = "Started s3 to hdfs copy for dataset_id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Preparing command to copy data from " + str(source_path) + " to " + str(target_path)
            logger.debug(status_message, extra=self.execution_context.get_context())
            s3_hdfs_copy_command = "python3 S3ToClusterLoadHandler.py -s " + source_path + " -t " + \
                                   target_path + " -sp " + CommonConstants.SRC_PATTERN
            logger.info(s3_hdfs_copy_command, extra=self.execution_context.get_context())
            s3_hdfs_copy_command_response = subprocess.Popen(s3_hdfs_copy_command, stdout=subprocess.PIPE,
                                                             stderr=subprocess.PIPE, shell=True)
            s3_hdfs_copy_command_output, s3_hdfs_copy_command_error = s3_hdfs_copy_command_response.communicate()
            if s3_hdfs_copy_command_response.returncode != 0:
                logger.error(s3_hdfs_copy_command_error, extra=self.execution_context.get_context())
                raise Exception(s3_hdfs_copy_command_error)
            logger.info(s3_hdfs_copy_command_output, extra=self.execution_context.get_context())
            status_message = "Completed executing command to copy data from " + str(source_path) + " to " \
                             + str(target_path)
            logger.info(status_message, extra=self.execution_context.get_context())
            self.update_copy_s3_hdfs_status(dataset_id)
            status_message = "Completed s3 to hdfs copy for dataset_id:" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary
        except Exception as exception:
            status_message = "Exception occured in copy data from s3 to hdfs for dataset_id:" + str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
            return result_dictionary

    def update_copy_s3_hdfs_status(self, dataset_id):
        """
            Purpose :   This method sets s3 to hdfs data copy flag to 'Y'
            Input   :   Dataset Id
            Output  :   NA
        """
        try:
            status_message = "Started preparing query to update data copy status for dataset_id:" +str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            update_copy_status_query = "Update {audit_db}.{process_dependency_table} set copy_data_s3_to_hdfs" \
                                   "_status='{status_flag}' where process_id={process_id} and " \
                                   "frequency='{frequency}' and data_date='{data_date}' and " \
                                   "write_dependency_value={cycle_id} and dataset_id='{dataset_id}'". \
            format(audit_db=self.audit_db, process_dependency_table=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                   status_flag=CommonConstants.ACTIVE_IND_VALUE, process_id=self.process_id, dataset_id=dataset_id,
                   frequency=self.frequency, data_date=self.data_date, cycle_id=self.cycle_id)
            logger.debug(update_copy_status_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(update_copy_status_query)
            status_message = "Completed executing query to update data copy status for dataset_id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
        except:
            status_message = "Exception occured in updating data copy status for dataset_id:" +str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


if __name__ == '__main__':
    try:
        DATA_COPY_HANDLER = S3ToHdfsCopyDWHandler()
        DATA_COPY_HANDLER.copy_data_s3_hdfs_handler()
        STATUS_MESSAGE = "Completed execution for S3ToHdfsCopyDWHandler"
        sys.stdout.write(STATUS_MESSAGE)
    except Exception as exception:
        raise exception
