
#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Staging Handler
#  Purpose             :   This module will perform the pre-configured steps before invoking StagingUtility.py.
#  Input Parameters    :   file_master_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   19th November 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   Staging Module development
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
from MySQLConnectionManager import MySQLConnectionManager
from StagingUtility import StagingUtility
from ConfigUtility import JsonConfigUtility
from PySparkUtility import PySparkUtility
from SchemaConverterUtility import SchemaConverterUtility


# all module level constants are defined here
MODULE_NAME = "StagingHandler"
PROCESS_NAME = "Staging"

USAGE_STRING = """
SYNOPSIS
    python StagingHandler.py <file_master_id> <cluster_id> <workflow_id> <process_id> <batch_id> <batch_id_list>

    Where
        input parameters : file_master_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list

"""


class StagingHandler:
    # Default constructor
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the dqm filter and datatype casting (main function)
    # Input              :   file_master_id, cluster_id, workflow_id, process_id, batch_id,batch_id_list
    # Output             :   NA
    # ##################################################################################################################
    def execute_staging(self, file_master_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):

        status_message = ""
        curr_batch_id = batch_id
        log_info = {}
        post_dqm_flag = False
        update_log_file_dtl_flag = True

        try:
            status_message = "Executing dqm filter in StagingHandler"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Input Validations
            if file_master_id is None:
                raise Exception('File Master id is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is not provided")
            self.execution_context.set_context({"file_master_id": file_master_id})
            logger.debug(status_message, extra=self.execution_context.get_context())
            log_info = {CommonConstants.BATCH_ID: batch_id,
                        CommonConstants.PROCESS_NAME: PROCESS_NAME,
                        CommonConstants.DATASET_ID: file_master_id,
                        CommonConstants.CLUSTER_ID: cluster_id,
                        CommonConstants.WORKFLOW_ID: workflow_id,
                        'process_id': process_id
                        }
            data_set_info = CommonUtils().get_dataset_info(file_master_id)
            landing_location = data_set_info['s3_landing_location']
            landing_location = str(landing_location).rstrip('/')

            pre_dqm_location = data_set_info['s3_pre_dqm_location']
            pre_dqm_location = str(pre_dqm_location).rstrip('/')

            post_dqm_location = data_set_info['s3_post_dqm_location']
            post_dqm_location = str(post_dqm_location).rstrip('/')

            configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
            dqm_error_location = str(configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "dqm_s3_error_location"]))

            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, file_master_id)

            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)

            # write a for loop here to make entry in the table for all files under the batch as In-Progress
            file_failed_flag = False
            file_not_processed_flag = False

            for file_detail in file_details:
                # Get detail for each file
                file_id = file_detail['file_id']
                file_name = file_detail['file_name']

                if not file_id or not file_name:
                    raise Exception(CommonConstants.INVALID_FILE_AUDIT_DETAILS)
                log_info = {CommonConstants.BATCH_ID: curr_batch_id, CommonConstants.FILE_ID: file_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                            CommonConstants.FILE_NAME: file_name,
                            CommonConstants.DATASET_ID: file_master_id,
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id
                            }

                # Logic added for restartability
                query_str = "Select file_process_status from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and batch_id={batch_id} and file_id = {file_id} and file_process_name='{staging}'"
                query = query_str.format(audit_db=self.audit_db,
                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                         dataset_id=file_master_id,
                                         cluster_id=cluster_id,
                                         workflow_id=workflow_id,
                                         process_id=process_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         staging=CommonConstants.FILE_PROCESS_NAME_STAGING)

                result = MySQLConnectionManager().execute_query_mysql(query)
                file_process_status = ""
                if result:
                    file_process_status = str(result[0]['file_process_status'])
                if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:
                    if file_process_status == "":
                        file_not_processed_flag = True
                        # Creating process log entry
                        CommonUtils().create_process_log_entry(self.audit_db, log_info)
                        status_message = "Created process log entry for file id " + str(file_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                    if file_process_status == CommonConstants.STATUS_FAILED:
                        file_failed_flag = True

            # move files to s3 at batch level
            try:
                if file_failed_flag or file_not_processed_flag:

                    if pre_dqm_location != 'None':
                        pre_dqm_url = pre_dqm_location.rstrip('/').__add__('/').__add__(
                            CommonConstants.PARTITION_BATCH_ID+"="+ str(batch_id))
                    elif landing_location == 'None' and pre_dqm_location == 'None':
                        status_message = " Landing Location and Pre DQM Location is not configured"
                        raise Exception(status_message)
                    else:
                        pre_dqm_url = landing_location.rstrip('/').__add__('/').__add__(
                            CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id))
                    post_dqm_url = post_dqm_location.rstrip('/').__add__('/').__add__(
                        CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id))

                    file_error_url = dqm_error_location.__add__('/').__add__(CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id))
                    logger.debug(status_message, extra=self.execution_context.get_context())

                    # Invoke Staging Utility - HH deleted , batch_id
                    dqm_filtered_df = StagingUtility().perform_dqm_filter(pre_dqm_url, file_error_url)
                    if post_dqm_location == 'None':
                        post_dqm_flag = True
                        status_message = "Post DQM Location is not configured"
                        raise Exception(status_message)
                    else:
                        if post_dqm_url:
                            fetch_columns_query = "select * from " + self.audit_db + "." + CommonConstants.COLUMN_METADATA_TABLE + " where dataset_id='" + str(
                                file_master_id) + "' order by " + CommonConstants.COLUMN_SEQUENCE_NUMBER
                            column_name_resultset = MySQLConnectionManager().execute_query_mysql(fetch_columns_query,
                                                                                                 False)
                            if not column_name_resultset:
                                raise Exception("Schema is missing in column metadata table")

                            filtered_df_count = dqm_filtered_df.count()
                            if filtered_df_count > 0:
                                schema_converted_df = SchemaConverterUtility().perform_schema_conversion(
                                    dqm_filtered_df,
                                    column_name_resultset)
                                status_message = "Completed the copy of filtered data to post-dqm location"
                                logger.debug(status_message, extra=self.execution_context.get_context())
                                # Code added for data stitching
                                stitch_flag = data_set_info['stitch_flag']
                                status_message = "Data stitching flag value:" + str(stitch_flag)
                                logger.info(status_message, extra=self.execution_context.get_context())
                                if stitch_flag == "Y":
                                    stitch_type = data_set_info['stitch_type']
                                    status_message = "Stitch type:" + str(stitch_type)
                                    unique_column_names = data_set_info['stitch_key_column']
                                    if unique_column_names is None or unique_column_names == "":
                                        status_message = "Stitch key column are not configured"
                                        raise Exception(status_message)
                                    status_message = "Stitch column names:" + str(unique_column_names)
                                    logger.info(status_message, extra=self.execution_context.get_context())
                                    logger.info(status_message, extra=self.execution_context.get_context())
                                    latest_batch_id = CommonUtils().fetch_successful_batch_id_for_dataset(file_master_id)
                                    status_message = "Latest successful batch id fetched:" + str(latest_batch_id)
                                    logger.info(status_message, extra=self.execution_context.get_context())
                                    history_post_dqm_location = post_dqm_location + "/" + CommonConstants.PARTITION_BATCH_ID + "=" + str(
                                        latest_batch_id)
                                    if stitch_type == CommonConstants.SCDTYPE1:
                                        status_message = "SCD type is set to:" + stitch_type
                                        logger.info(status_message, extra=self.execution_context.get_context())
                                        schema_converted_df = StagingUtility().perform_data_stitching_type_1(unique_column_names,
                                                                                                 history_post_dqm_location,
                                                                                                 schema_converted_df, batch_id,file_master_id)

                                    else:
                                        status_message = "Only SCD Type 1 is supported for now"
                                        logger.error(status_message, extra=self.execution_context.get_context())
                                        raise Exception(status_message)
                                schema_converted_df.write.mode('overwrite').format(
                                    'parquet').partitionBy(
                                    CommonConstants.PARTITION_FILE_ID).save(post_dqm_url)
                                status_message = "Completed the copy of filtered data to Staging location"

                                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                                log_info['status_id'] = CommonConstants.SUCCESS_ID
                                record_count = PySparkUtility(self.execution_context).get_parquet_record_count(post_dqm_url)
                                log_info['record_count'] = record_count
                                log_info['message'] = "DQM Filter completed successfully"
                            else:
                                status_message = "No records qualifying DQ checks found. DQM filter operation returned empty Data Frame"
                                raise Exception(status_message)
                else:
                    update_log_file_dtl_flag = False
                    status_message = 'Batch ID : ' + str(
                        batch_id) + ' is already processed'
                    logger.debug(status_message, extra=self.execution_context.get_context())

            except Exception as exception:
                if post_dqm_flag == True or landing_location == 'None':
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SKIPPED
                    log_info['status_id'] = CommonConstants.FAILED_ID
                    log_info[CommonConstants.BATCH_ID] = batch_id
                    log_info[CommonConstants.PROCESS_MESSAGE_KEY] = "Staging Location or Landing is not configured. Skipping Staging."
                else:
                    batches = batch_id_list.split(",")
                    for batch in batches:
                        log_info[CommonConstants.BATCH_ID] = batch
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        log_info['status_id'] = CommonConstants.FAILED_ID
                        log_info['record_count'] = str(0)
                        log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                              "").replace(
                            '"', '')

                        CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                        CommonUtils().update_batch_status(self.audit_db, log_info)
                    log_info[CommonConstants.BATCH_ID] = batch_id
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    log_info['status_id'] = CommonConstants.FAILED_ID
                    log_info['record_count'] = str(0)
                    log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                          "").replace(
                        '"',
                        '')
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    self.execution_context.set_context({"traceback": ""})
                    self.exc_info = sys.exc_info()
                    raise exception

            finally:
                status_message = "In  Finally Block for Staging at Batch Level"
                logger.debug(status_message, extra=self.execution_context.get_context())
                if update_log_file_dtl_flag:
                    # updating process log entry
                    log_info[CommonConstants.BATCH_ID] = curr_batch_id
                    CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
                    status_message = "Updated process log entry for all files in batch id" + str(batch_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                else:
                    status_message = "Files were processed in a previous run, hence process log was not updated"
                    logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            log_info[CommonConstants.BATCH_ID] = batch_id
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info['record_count'] = str(0)
            log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"',
                                                                                                                   '')
            CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
            batches = batch_id_list.split(",")
            for batch in batches:
                log_info[CommonConstants.BATCH_ID] = batch
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info['record_count'] = str(0)
                log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace(
                    '"',
                    '')
                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                CommonUtils().update_batch_status(self.audit_db, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: str(e)}
            raise e


if __name__ == '__main__':
    file_master_id = sys.argv[1]
    cluster_id = sys.argv[2]
    workflow_id = sys.argv[3]
    process_id = sys.argv[4]
    batch_id = sys.argv[5]
    batch_id_list = sys.argv[6]
    if file_master_id is None or cluster_id is None or workflow_id is None or process_id is None or batch_id is None or batch_id_list is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    batch_id = str(batch_id).rstrip("\n\r")
    staging_handler = StagingHandler()
    staging_handler.execute_staging(file_master_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list)
    sys.stdout.write("batch_id=" + batch_id)
