""" Module for HDFS to S3 load Handler"""
#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ####################################################Module Information############################
#  Module Name         : HdfsToS3LoadHandler
#  Purpose             : This module will perform the pre-configured steps before invoking
#                        HdfsToS3LoadUtility.py.
#  Input Parameters    : dataset_id, batch_id
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     : 6th June 2018
#  Last changed by     : Neelakshi Kulkarni
#  Reason for change   : Enhancement to provide restartability (this module restarts at batch level)
# ##################################################################################################

# Library and external modules declaration
import os
import traceback
import json
import sys
import time
import datetime
import pydoop.hdfs as hdfs
import pydoop.hdfs.path as hpath
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
from ConfigUtility import JsonConfigUtility
sys.path.insert(0, os.getcwd())

# all module level constants are defined here
MODULE_NAME = "HdfsToS3LoadHandler"
PROCESS_NAME = "Copy Files from HDFS to S3"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"

USAGE_STRING = """
SYNOPSIS
    python HdfsToS3LoadHandler.py <dataset_id> <batch_id> <cluster_id> <workflow_id>

    Where
        input parameters : dataset_id , batch_id, cluster_id, workflow_id

"""


class HdfsToS3LoadHandler:
    """ Class for HDFS to S3 load Handler"""
    # Default constructor
    def __init__(self, dataset_id=None, cluster_id=None, workflow_id=None,
                 parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": dataset_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.
                                                              ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.dqm_s3_error_location = str(self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "dqm_s3_error_location"]))
        self.dqm_s3_summary_location = str(self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "dqm_s3_summary_location"]))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

# ########################################### execute_command ###################################
# Purpose            :   Executing the hdfs to s3 copy (main function)
# Input              :   data source, subject area
# Output             :   NA
# ###############################################################################################
    def execute_hdfs_s3_copy(self, dataset_id, cluster_id, workflow_id, process_id, batch_id,
                             batch_id_list):
        """ Method to execute HDFS S3 copy"""
        status_message = ""
        curr_batch_id = batch_id
        #spark_context = None
        log_info = {}
        file_process_status = ""
        update_log_file_dtl_flag = True
        try:
            status_message = "Executing HDFS to S3 copy function of module " \
                             "HdfsToS3LoadHandlerHandler"
            if dataset_id is None or batch_id is None:
                raise Exception('Dataset id or batch_id is not provided')
            self.execution_context.set_context({"dataset_id": dataset_id, "batch_id": batch_id})
            logger.info(status_message, extra=self.execution_context.get_context())
            # Get post dqm location from MySQL table for given dataset id
            dataset_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
            post_dqm_location = dataset_info['hdfs_post_dqm_location']
            post_dqm_location = str(post_dqm_location).rstrip('/')
            s3_post_dqm_location = dataset_info['s3_post_dqm_location']
            s3_post_dqm_location = str(s3_post_dqm_location).rstrip('/')
            staging_location = dataset_info['staging_location']
            staging_location = str(staging_location).rstrip('/')
            #dataset_id = dataset_info['dataset_id']
            status_message = "Completed fetch of post dqm location for " + str(dataset_id)
            status_message = status_message + " - " + post_dqm_location
            self.execution_context.set_context({"post_dqm_location": post_dqm_location})
            logger.info(status_message, extra=self.execution_context.get_context())
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)

    # write a for loop here to make entry in the table for all files under the batch as In-Progress
            file_failed_flag = False
            file_not_processed_flag = False
            for file_detail in file_details:
                # Get detail for each file
                # Logic added for restartability
                file_id = file_detail[CommonConstants.MYSQL_FILE_ID]
                file_name = file_detail[CommonConstants.MYSQL_FILE_NAME]
                log_info = {CommonConstants.BATCH_ID: batch_id,
                            CommonConstants.FILE_ID: file_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                            CommonConstants.FILE_NAME: file_name,
                            "start_time":datetime.datetime.fromtimestamp(time.time()).
                                         strftime('%Y-%m-%d %H:%M:%S'),
                            "end_time":"NULL",
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id,
                            CommonConstants.DATASET_ID: dataset_id
                           }

                query_str = "Select file_process_status from {audit_db}.{process_log_table_name} " \
                            "where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and" \
                            " workflow_id = '{workflow_id}' and process_id = {process_id} and " \
                            "batch_id={batch_id} and file_id = {file_id} and file_process_name='" \
                            "{hdfstos3}'"
                query = query_str.format(audit_db=self.audit_db,
                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                         dataset_id=dataset_id,
                                         cluster_id=cluster_id,
                                         workflow_id=workflow_id,
                                         process_id=process_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         hdfstos3=CommonConstants.FILE_PROCESS_NAME_HDFS_TO_S3)

                result = MySQLConnectionManager().execute_query_mysql(query)
                if result:
                    file_process_status = str(result[0]['file_process_status'])
                if file_process_status == "" or file_process_status == CommonConstants.\
                        STATUS_FAILED:
                    if file_process_status == "":
                        file_not_processed_flag = True
                        # Creating process log entry
                        CommonUtils().create_process_log_entry(self.audit_db, log_info)
                        status_message = "Created process log entry for file id" + str(file_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                    if file_process_status == CommonConstants.STATUS_FAILED:
                        file_failed_flag = True

            # move files to s3 at batch level
            try:
                if file_failed_flag or file_not_processed_flag:
                    status_message = 'Copying files from HDFS to S3'
                    logger.info(status_message, extra=self.execution_context.get_context())

                    status_message = 'Copying DQM Error Details & Summary from HDFS to S3'
                    logger.info(status_message, extra=self.execution_context.get_context())

                    # Copy files from hdfs dqm error location to s3 dqm error location
                    dqm_hdfs_error_location = CommonConstants.DQM_ERROR_LOCATION + "/batch_id=" + \
                                              str(batch_id)
                    dqm_s3_error_location = self.dqm_s3_error_location + "/batch_id=" +\
                                            str(batch_id)
                    if hpath.exists(dqm_hdfs_error_location) and hdfs.\
                            ls(dqm_hdfs_error_location):
                        CommonUtils().perform_hadoop_distcp(dqm_hdfs_error_location,
                                                            dqm_s3_error_location)

                    # Copy files from hdfs dqm summary location to s3 dqm summary location
                    dqm_hdfs_summary_location = CommonConstants.DQM_SUMMARY_LOCATION + "/batch_id="\
                                                + str(batch_id)
                    dqm_s3_summary_location = self.dqm_s3_summary_location + "/batch_id=" +\
                                              str(batch_id)
                    if hpath.exists(dqm_hdfs_summary_location) and hdfs.\
                            ls(dqm_hdfs_summary_location):
                        CommonUtils().perform_hadoop_distcp(dqm_hdfs_summary_location,
                                                            dqm_s3_summary_location)

                    file_complete_url = post_dqm_location + "/batch_id=" + str(batch_id)
                    self.execution_context.set_context({'file_complete_path': file_complete_url})
                    status_message = 'Fetched complete file url:' + file_complete_url
                    logger.info(status_message, extra=self.execution_context.get_context())


                    final_staging_location = staging_location + "/batch_id=" + str(batch_id)
                    final_s3_post_dqm_location = s3_post_dqm_location + "/batch_id=" + str(batch_id)



                    # Copy files from post dqm location to staging location
                    # HdfsToS3LoadUtility().copy_from_hdfs_to_s3(file_complete_url,
                    # final_staging_location,"error",spark_context)
                    if hpath.exists(file_complete_url) and hdfs.ls(file_complete_url):
                        CommonUtils().perform_hadoop_distcp(file_complete_url,
                                                            final_staging_location)
                    if hpath.exists(file_complete_url) and hdfs.ls(file_complete_url):
                        CommonUtils().perform_hadoop_distcp(file_complete_url,
                                                            final_s3_post_dqm_location)

                    landing_location = str(dataset_info['hdfs_landing_location']).rstrip('/')
                    landing_s3_location = str(dataset_info['s3_landing_location']).rstrip('/')

                    pre_dqm_location = str(dataset_info['hdfs_pre_dqm_location']).rstrip('/')
                    pre_dqm_s3_location = str(dataset_info['s3_pre_dqm_location']).rstrip('/')

                    file_landing_url = landing_location + "/batch_id=" + str(batch_id)
                    file_pre_dqm_url = pre_dqm_location + "/batch_id=" + str(batch_id)

                    file_landing_s3_url = landing_s3_location + "/batch_id=" + str(batch_id)
                    file_pre_dqm_s3_url = pre_dqm_s3_location + "/batch_id=" + str(batch_id)

                    if hpath.exists(file_landing_url) and hdfs.ls(file_landing_url):
                        CommonUtils().perform_hadoop_distcp(file_landing_url, file_landing_s3_url)
                    if hpath.exists(file_pre_dqm_url) and hdfs.ls(file_pre_dqm_url):
                        CommonUtils().perform_hadoop_distcp(file_pre_dqm_url, file_pre_dqm_s3_url)

                    log_info[CommonConstants.BATCH_ID] = curr_batch_id
                    log_info['status_id'] = CommonConstants.SUCCESS_ID
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                    log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime\
                        ('%Y-%m-%d %H:%M:%S')

                else:
                    update_log_file_dtl_flag = False
                    status_message = 'Batch ID : ' + str(
                        batch_id) + ' is already processed'
                    logger.debug(status_message, extra=self.execution_context.get_context())

            except Exception as exception:
                print(str(exception))
                status_message = "Copy To S3 DQM Failed For Batch Id " + str(batch_id) + str(traceback.format_exc())
                logger.error(str(traceback.format_exc()))
                self.execution_context.set_context({"batch_id": batch_id})
                logger.error(status_message, extra=self.execution_context.get_context())

                print(str(exception))
                status_message = "Publish failed for Batch Id:" + str(batch_id) + "due to" + \
                                 str(exception)
                log_info[CommonConstants.BATCH_ID] = curr_batch_id
                log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].\
                    replace("'", "").replace('"', '')
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime\
                    ('%Y-%m-%d %H:%M:%S')
                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                failure_query_string = "Update {audit_db}.{file_audit_table} set file_status =" \
                                       " '{status_failed}' where batch_id in ({batches})"
                failure_query = failure_query_string.format(audit_db=self.audit_db,
                                                            file_audit_table=CommonConstants.
                                                            FILE_AUDIT_TABLE,
                                                            status_failed=CommonConstants.
                                                            STATUS_FAILED,
                                                            batches=batch_id_list)
                MySQLConnectionManager().execute_query_mysql(failure_query)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise exception

            finally:
                status_message = "In  Finally Block for HDFS to S3 for copy at Batch Level"
                logger.debug(status_message, extra=self.execution_context.get_context())
                if update_log_file_dtl_flag:
                    # updating process log entry
                    CommonUtils().update_proces_log_status_by_batch_id(self.audit_db, log_info)
                    status_message = "Updated process log entry for all files in batch id" + \
                                     str(batch_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                else:
                    status_message = "Files were processed in a previous run, hence process" \
                                     " log was not updated"
                    logger.info(status_message, extra=self.execution_context.get_context())
    # write a for loop here to make entry in the table for all files under the batch as Succesfull
    #If something fails then  write a for loop here to make entry in the table for all files under
    # the batch as Failed and then raise exception , nut we will not be able to clean the files

            status_message = "Completed Copy of HDFS for dataset id:" + str(dataset_id)
            self.execution_context.set_context({"dataset_id": dataset_id})
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(str(traceback.format_exc()))
            log_info[CommonConstants.BATCH_ID] = curr_batch_id
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            CommonUtils().update_batch_status(self.audit_db, log_info)
            failure_query_string = "Update {audit_db}.{batch_table} set batch_status = " \
                                   "'{status_failed}' where batch_id in ({batches})"
            failure_query = failure_query_string.format(audit_db=self.audit_db,
                                                        batch_table=CommonConstants.BATCH_TABLE,
                                                        status_failed=CommonConstants.STATUS_FAILED,
                                                        batches=batch_id_list)
            MySQLConnectionManager().execute_query_mysql(failure_query)
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: str(exception)}
            raise exception

    def main(self, dataset_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):
        """Main method"""
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for HdfsToS3LoadHandler"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.execute_hdfs_s3_copy(dataset_id, cluster_id, workflow_id,
                                                          process_id, batch_id, batch_id_list)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for HdfsToS3LoadHandler"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            logger.error(str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            #return result_dictionary
            raise exception


if __name__ == '__main__':
    DATASET_ID = sys.argv[1]
    BATCH_ID = sys.argv[5]
    CLUSTER_ID = sys.argv[2]
    WORKFLOW_ID = sys.argv[3]
    PROCESS_ID = sys.argv[4]
    BATCH_ID_LIST = sys.argv[6]

    if DATASET_ID is None or CLUSTER_ID is None or WORKFLOW_ID is None or PROCESS_ID is None or\
            BATCH_ID is None or BATCH_ID_LIST is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    BATCH_ID = str(BATCH_ID).rstrip("\n\r")
    HDFS_S3_HANDLER = HdfsToS3LoadHandler(DATASET_ID, CLUSTER_ID, WORKFLOW_ID)
    RESULT_DICT = HDFS_S3_HANDLER.main(DATASET_ID, CLUSTER_ID, WORKFLOW_ID, PROCESS_ID,
                                       BATCH_ID, BATCH_ID_LIST)
    STATUS_MSG = "\nCompleted execution for CopyHdfsToS3 Utility with status " + \
                 json.dumps(RESULT_DICT) + "\n"
    sys.stdout.write(STATUS_MSG)
