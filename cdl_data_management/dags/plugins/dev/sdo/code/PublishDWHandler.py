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
from FileTransferUtility import FileTransferUtility

sys.path.insert(0, os.getcwd())

MODULE_NAME = "PublishDWHandler"

"""
Module Name         :   PublishDWHandler
Purpose             :   This module will be used for publishing data as part of DW Step
Input Parameters    :   process id, frequency, data date, cycle id
Output Value        :   None
Pre-requisites      :   None
Last changed on     :   02-07-2018
Last changed by     :   Sushant Choudhary
Reason for change   :   DW Publish Development
"""


class PublishDWHandler(object):
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

    def publish_data_handler(self):
        """
            Purpose :   Driver handler method for calling helper functions required for publish copy
            Input   :   NA
            Output  :   NA
        """
        try:
            status_message = "Starting publish step for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            self.step_name = CommonConstants.PUBLISH_STEP
            self.execution_context.set_context({"step_name": self.step_name})
            CommonUtils(self.execution_context).create_step_audit_entry(self.process_id, self.frequency, self.step_name,
                                                  self.data_date,
                                                  self.cycle_id)
            status_message = "Started preparing query to fetch target table details to be published for process_id:" \
                             + str(self.process_id) + " " + "and frequency:" + str(self.frequency) + \
                             " " + "and data_date:" + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_publish_table_details_query = "Select {dataset_table}.dataset_id,table_s3_path,publish_s3_path, " \
                                                "publish_type from {audit_db}.{process_dependency_master_table} " \
                                                "INNER JOIN {audit_db}.{dataset_table} ON {dataset_table}.dataset_id=" \
                                                "{process_dependency_master_table}.dataset_id where " \
                                                "process_id={process_id}  and frequency='{frequency}' and " \
                                                "{process_dependency_master_table}.table_type='{table_type}' and " \
                                                "dataset_publish_flag='{publish_flag_value}'"\
                .format(
                audit_db=self.audit_db, process_dependency_master_table=CommonConstants.PROCESS_DEPENDENCY_MASTER_TABLE,
                dataset_table=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME, process_id=self.process_id,
                frequency=self.frequency, table_type=CommonConstants.TARGET_TABLE_TYPE,
                publish_flag_value=CommonConstants.ACTIVE_IND_VALUE
            )
            logger.debug(fetch_publish_table_details_query, extra=self.execution_context.get_context())
            publish_table_resultset = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_publish_table_details_query)
            status_message = "Completed query to fetch target table details to be published for process_id:" \
                             + str(self.process_id) + " " + "and frequency:" + str(self.frequency) + \
                             " " + "and data_date:" + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(publish_table_resultset, extra=self.execution_context.get_context())
            if publish_table_resultset:
                thread_limit = int(len(publish_table_resultset))
                status_message = "Thread limit is set to " +str(thread_limit)
                logger.info(status_message, extra=self.execution_context.get_context())
                queue_instance = queue.Queue()
                failed_flag = False
                threads = []
                for table_details in publish_table_resultset:
                    status_message = "Dataset result dict:" + str(table_details)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    dataset_id = table_details['dataset_id']
                    self.execution_context.set_context({"dataset_id": dataset_id})
                    table_source_path = table_details['table_s3_path']
                    table_publish_path = table_details['publish_s3_path']
                    table_publish_type = table_details['publish_type']
                    self.validate_dataset_details(dataset_id, table_source_path, table_publish_path)
                    status_message = "Dataset id is:" + str(dataset_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    if table_publish_type is None or table_publish_type == CommonConstants.FULL_LOAD_TYPE:
                        table_publish_path_date = table_publish_path + "/"  + CommonConstants.DATE_PARTITION + \
                                                  "=" + self.data_date.replace("-", "")
                        delete_command = "aws s3 rm" + " " + table_publish_path_date + " " + "--recursive"
                        CommonUtils(self.execution_context).execute_shell_command(delete_command)
                    elif table_publish_type == CommonConstants.LATEST_LOAD_TYPE:
                        delete_command = "aws s3 rm" + " " + table_publish_path + " " + "--recursive"
                        CommonUtils(self.execution_context).execute_shell_command(delete_command)
                    else:
                        status_message = "Publish type is incorrectly configured for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    source_path = table_source_path + "/" + CommonConstants.DATE_PARTITION + "=" + \
                                  self.data_date.replace("-", "") \
                                  + "/" + CommonConstants.CYCLE_PARTITION + "=" + self.cycle_id
                    table_publish_path = table_publish_path + "/" + CommonConstants.DATE_PARTITION + "=" + \
                                         self.data_date.replace("-", "") + "/" + CommonConstants.CYCLE_PARTITION + \
                                         "=" + self.cycle_id
                    status_message = "Source S3 path is:'" + str(source_path) + "' for dataset_id:" +str(dataset_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    status_message = "Target Publish S3 path is:'" + str(table_publish_path) + "' for dataset_id:" +\
                                     str(dataset_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    thread_instance = Thread(target=lambda q, source_location, target_location, dataset_id: q.put(
                        self.call_s3_to_hdfs_transfer(source_location, target_location, dataset_id)),
                           args=(queue_instance, source_path, table_publish_path, dataset_id))
                    threads.append(thread_instance)
                failed_flag = CommonUtils(self.execution_context).execute_threading(threads, thread_limit,
                                                                                    queue_instance, failed_flag)
                if failed_flag:
                    status_message = "Publish Copy failed"
                    raise Exception(status_message)
            else:
                status_message = "There are no datasets to publish for process_id:" + str(self.process_id) + " " \
                                 + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                                 + " " + "cycle id:" + str(self.cycle_id)
                logger.warn(status_message, extra=self.execution_context.get_context())
            status = CommonConstants.STATUS_SUCCEEDED
        except Exception as exception:
            status = CommonConstants.STATUS_FAILED
            status_message = "Exception in publish data step for process_id:" + str(self.process_id) + " " \
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
                CommonUtils(self.execution_context).update_cycle_audit_entry(self.process_id, self.frequency, self.data_date,
                                                            self.cycle_id, status)
            status_message = "Completed publish data step for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())

    def validate_dataset_details(self, dataset_id, table_source_path, table_publish_path):
        """
            Purpose :   This method invokes validates dataset related attributes
            Input   :   Dataset Id, Table Source Path, Table Publish Path
            Output  :   NA
        """
        if table_source_path is None:
            status_message = "Table source path is not provided for dataset_id:" + str(dataset_id)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)
        if table_publish_path is None:
            status_message = "Publish path is not provided for dataset_id:" + str(dataset_id)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)

    def call_s3_to_hdfs_transfer(self, source_path, target_path, dataset_id):
        """
            Purpose :   This method calls S3 to Cluster Handler with source and target path
            Input   :   Source Path, Target Path, Dataset Id
            Output  :   NA
        """
        try:
            status_message = "Started publish copy for dataset_id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Preparing command to copy data from " + str(source_path) + " to " + str(target_path)
            logger.debug(status_message, extra=self.execution_context.get_context())
            FileTransferUtility().transfer_files(source_path, target_path)
            status_message = "Completed executing command to copy data from " + str(source_path) + " to " \
                             + str(target_path)
            logger.info(status_message, extra=self.execution_context.get_context())
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



if __name__ == '__main__':
    try:
        PUBLISH_HANDLER = PublishDWHandler()
        PUBLISH_HANDLER.publish_data_handler()
        STATUS_MESSAGE = "Completed execution for PublishDWHandler"
        sys.stdout.write(STATUS_MESSAGE)
    except Exception as exception:
        raise exception
