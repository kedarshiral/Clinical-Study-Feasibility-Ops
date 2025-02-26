#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   HdfsToS3LoadHandler
#  Purpose             :   This module will perform the pre-configured steps before invoking HdfsToS3LoadUtility.py.
#  Input Parameters    :   dataset_id, batch_id
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   4th January 2018
#  Last changed by     :   Sushant Choudhary
#  Reason for change   :   HDFS to S3 Copy Module development
# ######################################################################################################################

# Library and external modules declaration
import hadoopy
import os
import traceback
import json
import sys
import time
import datetime
sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
from HdfsToS3LoadUtility import HdfsToS3LoadUtility
from PySparkUtility import PySparkUtility

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
    # Default constructor
    def __init__(self, dataset_id=None, cluster_id=None, workflow_id=None, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": dataset_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the hdfs to s3 copy (main function)
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def execute_hdfs_s3_copy(self,dataset_id=None,batch_id=None):
        status_message = ""
        curr_batch_id = batch_id
        try:
            status_message = "Executing HDFS to S3 copy function of module HdfsToS3LoadHandlerHandler"
            if dataset_id is None or batch_id is None:
                raise Exception('Dataset id or batch_id is not provided')
            self.execution_context.set_context({"dataset_id": dataset_id, "batch_id": batch_id})
            logger.info(status_message, extra=self.execution_context.get_context())
            # Get post dqm location from MySQL table for given dataset id
            dataset_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
            post_dqm_location = dataset_info['dataset_hdfs_post_dqm_location']
            staging_location=dataset_info['dataset_hdfs_staging_location']
            #dataset_id = dataset_info['dataset_id_pk']
            status_message = "Completed fetch of post dqm location for " + str(dataset_id)
            status_message = status_message + " - " + post_dqm_location
            self.execution_context.set_context({"post_dqm_location": post_dqm_location})
            logger.info(status_message, extra=self.execution_context.get_context())
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)
            spark_context = PySparkUtility().get_spark_context()
            print("spark context created")

            try:
                status_message = 'Fetched spark session  context and Copying DQM Summary And Error At batch Level'
                logger.info(status_message, extra=self.execution_context.get_context())

                # Copy files from hdfs dqm error location to s3 dqm error location
                dqm_hdfs_error_location = CommonConstants.DQM_ERROR_LOCATION + "/batch_id=" + str(batch_id)
                dqm_s3_error_location = CommonConstants.DQM_S3_ERROR_LOCATION + "/batch_id=" + str(batch_id)
                if hadoopy.exists(dqm_hdfs_error_location) and hadoopy.ls(dqm_hdfs_error_location):
                    # HdfsToS3LoadUtility().copy_from_hdfs_to_s3(dqm_hdfs_error_location, dqm_s3_error_location,
                    #                                            "append", spark_context)
                    CommonUtils().perform_hadoop_distcp(dqm_hdfs_error_location, dqm_s3_error_location)

                # Copy files from hdfs dqm summary location to s3 dqm summary location
                dqm_hdfs_summary_location = CommonConstants.DQM_SUMMARY_LOCATION + "/batch_id=" + str(batch_id)
                dqm_s3_summary_location = CommonConstants.DQM_S3_SUMMARY_LOCATION + "/batch_id=" + str(batch_id)
                if hadoopy.exists(dqm_hdfs_summary_location) and hadoopy.ls(dqm_hdfs_summary_location):
                    # HdfsToS3LoadUtility().copy_from_hdfs_to_s3(dqm_hdfs_summary_location, dqm_s3_summary_location,
                    #                                            "append", spark_context)
                    CommonUtils().perform_hadoop_distcp(dqm_hdfs_summary_location, dqm_s3_summary_location)
            except Exception as exception:
                print(str(exception))
                status_message = "Copy To S3 DQM Failed For Batch Id " + str(batch_id)
                self.execution_context.set_context({"batch_id": batch_id})
                logger.error(status_message, extra=self.execution_context.get_context())
            finally:
                status_message = "HDFS to S3 for DQM at Batch Level Finally Block"
                logger.debug(status_message, extra=self.execution_context.get_context())


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
                    # Get the complete file url
                    file_complete_url = post_dqm_location+"/batch_id="+str(batch_id)+"/file_id="+str(file_id)
                    self.execution_context.set_context({'file_complete_path': file_complete_url})
                    status_message = 'Fetched complete file url:'+file_complete_url
                    logger.info(status_message, extra=self.execution_context.get_context())
                    # Invoke HdfsToS3LoadUtility
                    final_staging_location=staging_location+"/batch_id="+str(batch_id)+"/file_id="+str(file_id)

                    #Create Spark context
                    #spark_context = PySparkUtility().get_spark_context()
                    status_message = 'Fetched spark session  context'
                    logger.info(status_message, extra=self.execution_context.get_context())

                    #Copy files from post dqm location to staging location
                    #HdfsToS3LoadUtility().copy_from_hdfs_to_s3(file_complete_url,final_staging_location,"error",spark_context)
                    CommonUtils().perform_hadoop_distcp(file_complete_url,final_staging_location)

                    landing_location = dataset_info['dataset_hdfs_landing_location']
                    landing_s3_location = dataset_info['dataset_s3_landing_location']

                    pre_dqm_location = dataset_info['dataset_hdfs_pre_dqm_location']
                    pre_dqm_s3_location = dataset_info['dataset_s3_pre_dqm_location']

                    file_landing_url = landing_location+"/batch_id="+str(batch_id)+"/file_id="+str(file_id)
                    file_pre_dqm_url = pre_dqm_location+"/batch_id="+str(batch_id)+"/file_id="+str(file_id)

                    file_landing_s3_url = landing_s3_location+"/batch_id="+str(batch_id)+"/file_id="+str(file_id)
                    file_pre_dqm_s3_url = pre_dqm_s3_location+"/batch_id="+str(batch_id)+"/file_id="+str(file_id)

                    CommonUtils().perform_hadoop_distcp(file_landing_url, file_landing_s3_url)
                    CommonUtils().perform_hadoop_distcp(file_pre_dqm_url, file_pre_dqm_s3_url)

                    # Update process log and file audit entry to SUCCESS
                    log_info["end_time"]=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                except Exception as exception:
                    print(str(exception))
                    status_message = "HDFS to S3 Copy failed for File Id:"+str(file_id)+"due to"+str(exception)
                    log_info[CommonConstants.BATCH_ID]= curr_batch_id
                    log_info['status_id'] = CommonConstants.FAILED_ID
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    log_info["end_time"]=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    CommonUtils().update_file_audit_status_by_batch_id(CommonConstants.AUDIT_DB_NAME, log_info)
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise exception
                finally:
                    #status_message = "Closing spark context"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    #if spark_context:
                    #    spark_context.stop()
                    #status_message = "Successfully closed spark context"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    CommonUtils().update_process_status(CommonConstants.AUDIT_DB_NAME, log_info)
                    status_message = "Updated process log for file:"+str(file_id)
                    logger.info(status_message, extra=self.execution_context.get_context())

            status_message = "Completed Copy of HDFS post dqm files for dataset id:" + str(dataset_id)
            self.execution_context.set_context({"post_dqm_location": post_dqm_location})
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            status_message = "Closing spark context"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if spark_context:
                spark_context.stop()
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            log_info[CommonConstants.BATCH_ID]= curr_batch_id
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            CommonUtils().update_batch_status(CommonConstants.AUDIT_DB_NAME, log_info)
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED, CommonConstants.ERROR_KEY: str(exception)}
            raise exception
            #return result_dictionary

    def main(self, dataset_id, batch_id):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for HdfsToS3LoadHandler"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.execute_hdfs_s3_copy(dataset_id, batch_id)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
               raise Exception
            status_message = "Completing the main function for HdfsToS3LoadHandler"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            print str(exception)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            print str(exception)
            #return result_dictionary
            raise exception


if __name__ == '__main__':
    dataset_id = sys.argv[1]
    batch_id=sys.argv[2]
    cluster_id = sys.argv[3]
    workflow_id = sys.argv[4]
    hdfs_s3_handler = HdfsToS3LoadHandler(dataset_id, cluster_id, workflow_id)
    result_dict = hdfs_s3_handler.main(dataset_id,batch_id)
    STATUS_MSG = "\nCompleted execution for CopyHdfsToS3 Utility with status " + json.dumps(result_dict) + "\n"
    sys.stdout.write(STATUS_MSG)