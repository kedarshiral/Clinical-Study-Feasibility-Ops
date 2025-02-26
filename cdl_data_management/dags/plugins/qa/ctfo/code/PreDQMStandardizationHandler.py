__AUTHOR__ = 'ZS Associates'

# Library and external modules declaration
import json
import traceback
import sys
import datetime
import time
import os
import json
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
from PreDQMStandardizationUtility import PreDQMStandardizationUtility
from S3MoveCopyUtility import S3MoveCopyUtility
from PySparkUtility import PySparkUtility
from ConfigUtility import JsonConfigUtility
sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "Pre DQM Standardization"
PROCESS_NAME = "Pre DQM Standardization"

USAGE_STRING = """
SYNOPSIS
    python PreDQMStandardizationHandler.py <dataset_id>  <cluster_id> <workflow_id> <process_id> <batch_id>

    Where
        input parameters : dataset_id , batch_id, cluster_id, workflow_id

"""


class PreDQMStandardizationHandler(object):
    def __init__(self, DATASET_ID=None,BATCH_ID= None, CLUSTER_ID=None, WORKFLOW_ID=None):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": DATASET_ID})
        self.execution_context.set_context({"batch_id": BATCH_ID})
        self.execution_context.set_context({"cluster_id": CLUSTER_ID})
        self.execution_context.set_context({"workflow_id": WORKFLOW_ID})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

        # ########################################### execute_pre_dqm_standization ###########################
        # Purpose            :   Executing the Pre DQM standization
        # Input              :   NA
        # Output             :   NA
        # #########################################################################################

    def execute_pre_dqm_standization(self, dataset_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        status_message = ""
        log_info={}
        no_standardization_flag = False
        rejected_fucntion_list = []
        record_count = 0
        update_log_file_dtl_flag = True
        spark_context = None
        try:
            status_message = "Starting Pre DQM function of module Pre DQM Standization"
            logger.info(status_message, extra=self.execution_context.get_context())
            if dataset_id is None:
                raise Exception('Dataset Id is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"dataset_id": dataset_id, "batch_id": batch_id})
            logger.info(status_message, extra=self.execution_context.get_context())
            log_info = {CommonConstants.BATCH_ID: batch_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.DATASET_ID: dataset_id,
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id
                            }
            # Get landing location and data_set_id for given    data source and subject area
            data_set_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
            landing_location = data_set_info['s3_landing_location']
            landing_location = str(landing_location).rstrip('/')
            pre_dqm_location = data_set_info['s3_pre_dqm_location']
            pre_dqm_location = str(pre_dqm_location).rstrip('/')
            upper_conversion_list = []
            lower_conversion_list = []
            blank_converion_list = []
            left_padding_list = []
            right_padding_list = []
            trim_list = []
            sub_string_list = []
            replace_string_list = []
            zip_extractor_list = []
            date_conversion_list = []
            query_string = "select column_name,function_name,function_params from {automation_db_name}.{standardization_master_table} where dataset_id = {dataset_id}"
            execute_query = query_string.format(automation_db_name=self.audit_db,
                                                standardization_master_table=CommonConstants.STANDARDIZATION_MASTER_TABLE,
                                                dataset_id=dataset_id)

            column_standard_checks = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            if len(column_standard_checks)==0 or column_standard_checks is None:
                no_standardization_flag = True
                file_url_till_batch = ""
                pre_dqm_location_till_batch = ""
                status_message = "Pre-DQM Standardization not configured"
                logger.debug(status_message, extra=self.execution_context.get_context())
                if landing_location == pre_dqm_location:
                    status_message = "Landing Location and Pre DQM Location are same"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                elif landing_location != 'None' and pre_dqm_location != 'None':
                    # Get the complete file url
                    file_url_till_batch = landing_location.__add__('/'+CommonConstants.PARTITION_BATCH_ID+'=').__add__(str(batch_id))
                    pre_dqm_location_till_batch = pre_dqm_location.__add__('/'+CommonConstants.PARTITION_BATCH_ID+'=').__add__(str(batch_id))
                    try:
                        status_message = "Copying data from S3 Landing Location to S3 Pre DQM Location"
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        #S3MoveCopyUtility(self.execution_context).copy_s3_to_s3_recursive_with_s3cmd(file_url_till_batch,
                        #                                                                   pre_dqm_location_till_batch)


                        module_name_for_spark = str(MODULE_NAME + "_dataset_" + dataset_id + "_process_" + process_id)
                        spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark)
                        df = spark_context.read.parquet(file_url_till_batch)


                        df.write.mode('overwrite').format('parquet').partitionBy(CommonConstants.PARTITION_FILE_ID).save(pre_dqm_location_till_batch)



                    except Exception as exception:
                        status_message = "Error in s3 copy"
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception
            else:
                for check in column_standard_checks:
                    check_type = check["function_name"]
                    function_params = check["function_params"]
                    column_name = check[CommonConstants.COLUMN_NAME]
                    if check_type == CommonConstants.TYPE_CONVERSION_FUNC_NAME:
                        if function_params is None:
                            rejected_fucntion_list.append(check)
                        else:
                            json_acceptable_string = function_params.replace("'", "\"")
                            try:
                                param_json = json.loads(json_acceptable_string)
                                if "type" in list(param_json.keys()):
                                    if param_json['type'] == "Upper":
                                        upper_conversion_list.append(column_name)
                                    elif param_json['type'] == "Lower":
                                        lower_conversion_list.append(column_name)
                                    else:
                                        rejected_fucntion_list.append(check)
                                else:
                                    rejected_fucntion_list.append(check)
                            except Exception as exception:
                                rejected_fucntion_list.append(check)

                    elif check_type == CommonConstants.BLANK_CONVERSION_FUNC_NAME:
                        blank_converion_list.append(column_name)

                    elif check_type == CommonConstants.LEFT_PADDING_FUNC_NAME:
                        if function_params is None:
                            rejected_fucntion_list.append(check)
                        else:
                            json_acceptable_string = function_params.replace("'", "\"")
                            try:
                                param_json = json.loads(json_acceptable_string)
                                # {"padding_value":0000,"length":8}
                                if "padding_value" in list(param_json.keys()) and "length" in list(param_json.keys()):
                                    temp_dict = {}
                                    temp_dict[column_name] = param_json
                                    left_padding_list.append(temp_dict)
                                else:
                                    rejected_fucntion_list.append(check)
                            except Exception as e:
                                rejected_fucntion_list.append(check)

###Date Format Conversion

                    elif check_type == CommonConstants.DATE_CONVERSION_FUNC_NAME:
                        if function_params is None:
                            rejected_fucntion_list.append(check)
                        else:
                            json_acceptable_string = function_params.replace("'", "\"")
                            try:
                                param_json = json.loads(json_acceptable_string)

                                if "src" in list(param_json.keys()) and "tgt" in list(param_json.keys()):
                                    temp_dict = {}
                                    temp_dict[column_name] = param_json
                                    date_conversion_list.append(temp_dict)
                                else:
                                    rejected_fucntion_list.append(check)
                            except Exception as e:
                                rejected_fucntion_list.append(check)

                    elif check_type == CommonConstants.RIGHT_PADDING_FUNC_NAME:

                        if function_params is None:
                            rejected_fucntion_list.append(check)
                        else:
                            json_acceptable_string = function_params.replace("'", "\"")
                            try:
                                param_json = json.loads(json_acceptable_string)
                                # {"padding_value":0000,"length":8}
                                if "padding_value" in list(param_json.keys()) and "length" in list(param_json.keys()):
                                    temp_dict = {}
                                    temp_dict[column_name] = param_json
                                    right_padding_list.append(temp_dict)
                                else:
                                    rejected_fucntion_list.append(check)
                            except Exception as e:
                                rejected_fucntion_list.append(check)


                    elif check_type == CommonConstants.TRIM_FUNCTION_FUNC_NAME:

                        trim_list.append(column_name)

                    elif check_type == CommonConstants.SUB_STRING_FUNC_NAME:

                        if function_params is None:
                            rejected_fucntion_list.append(check)
                        else:
                            json_acceptable_string = function_params.replace("'", "\"")
                            try:
                                param_json = json.loads(json_acceptable_string)
                                # {"start_index":2,"length":6}
                                if "start_index" in list(param_json.keys()) and "length" in list(param_json.keys()):
                                    temp_dict = {}
                                    temp_dict[column_name] = param_json
                                    sub_string_list.append(temp_dict)
                                else:
                                    rejected_fucntion_list.append(check)
                            except Exception as exception:
                                rejected_fucntion_list.append(check)

                    elif check_type == CommonConstants.REPLACE_FUNC_NAME:

                        if function_params is None:
                            rejected_fucntion_list.append(check)
                        else:
                            json_acceptable_string = function_params.replace("'", "\"")
                            try:
                                param_json = json.loads(json_acceptable_string)
                                # {"to_repace":".","value":"#"}

                                if "to_replace" in list(param_json.keys()) and "value" in list(param_json.keys()):
                                    temp_dict = {}
                                    temp_dict[column_name] = param_json
                                    replace_string_list.append(temp_dict)
                                else:
                                    rejected_fucntion_list.append(check)
                            except Exception as exception:
                                rejected_fucntion_list.append(check)

                    elif check_type == CommonConstants.ZIP_EXTRACTOR_FUNC_NAME:
                        zip_extractor_list.append(column_name)
                    else:
                        rejected_fucntion_list.append(check)
                    #need to verify the indentation
                    if len(rejected_fucntion_list) != 0:
                        if CommonConstants.EXCEPTION_ON_REJECT_LIST == "True":
                            logger.debug("Rejected Standization Checks" + str(rejected_fucntion_list), extra=self.execution_context.get_context())
                            raise Exception("Wrong Standardization Checks Configured")
                        else:
                            logger.debug("Rejected Standization Checks" + str(rejected_fucntion_list), extra=self.execution_context.get_context())

            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)
            module_name_for_spark = str(
                MODULE_NAME + "_dataset_" + dataset_id + "_process_" + process_id)
            spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,CommonConstants.HADOOP_CONF_PROPERTY_DICT)

            fetch_workflow_name = "select workflow_name from {automation_db_name}.ctl_workflow_master where process_id = {process_id}"
            execute_query = fetch_workflow_name.format(automation_db_name=self.audit_db,
                                                standardization_log_table=CommonConstants.LOG_PRE_DQM_DTL,
                                                process_id=process_id)

            workflow_name_list = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            workflow_name = workflow_name_list[0]['workflow_name']
            fetch_process_name = "select process_name from {automation_db_name}.ctl_cluster_config where process_id = {process_id}"
            process_name_query = fetch_process_name.format(automation_db_name=self.audit_db,
                                                standardization_log_table=CommonConstants.LOG_PRE_DQM_DTL,
                                                process_id=process_id)
            process_name_list = MySQLConnectionManager().execute_query_mysql(process_name_query, False)
            process_name =  process_name_list[0]['process_name']
            file_not_processed_flag = False
            file_failed_flag = False
            for file_detail in file_details:
                file_id = file_detail[CommonConstants.MYSQL_FILE_ID]
                file_name = file_detail[CommonConstants.MYSQL_FILE_NAME]
                log_info = {CommonConstants.BATCH_ID: batch_id,
                            CommonConstants.FILE_ID: file_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                            CommonConstants.FILE_NAME: file_name,
                            CommonConstants.DATASET_ID: dataset_id,
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id
                            }
                # Logic added for restartability
                query_str = "Select file_process_status from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and batch_id={batch_id} and file_id = {file_id} and file_process_name='{pre_dqm}'"
                query = query_str.format(audit_db=self.audit_db,
                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                         dataset_id=dataset_id,
                                         cluster_id=cluster_id,
                                         workflow_id=workflow_id,
                                         process_id=process_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         pre_dqm=CommonConstants.FILE_PROCESS_NAME_PRE_DQM)
                result = MySQLConnectionManager().execute_query_mysql(query)
                file_process_status = ""
                if result:
                    file_process_status = str(result[0]['file_process_status'])
                if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:
                    if file_process_status == "":
                        # Creating process log entry
                        file_not_processed_flag = True
                        CommonUtils().create_process_log_entry(self.audit_db, log_info)
                        status_message = "Created process log entry "
                        logger.debug(status_message, extra=self.execution_context.get_context())
                    if file_process_status == CommonConstants.STATUS_FAILED:
                        file_failed_flag = True
            try:
                if file_not_processed_flag or file_failed_flag:
                    if no_standardization_flag == True:
                        raise Exception
                    if no_standardization_flag == False:
                        # Get the complete file url
                        file_complete_url = landing_location + "/"+CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id)
                        self.execution_context.set_context({'file_complete_path': file_complete_url})
                        status_message = 'Fetched complete file url:' + file_complete_url
                        logger.info(status_message, extra=self.execution_context.get_context())
                        if landing_location == 'None' or  pre_dqm_location == 'None':
                            status_message = 'Landing Location and Pre DQM Location is not configured'
                            logger.info(status_message, extra=self.execution_context.get_context())
                            raise Exception
                        # Invoke Pre DQM Stabdardization Utility
                        else:
                            pre_dqm_url = pre_dqm_location.rstrip('/').__add__('/').__add__(
                                CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id))
                            file_df = spark_context.read.parquet(file_complete_url)
                            PreDQMStandardizationUtility(process_name,workflow_name,process_id,workflow_id, dataset_id,batch_id).perform_pre_dqm_standardization(file_df, upper_conversion_list,
                                                                                           lower_conversion_list,
                                                                                           blank_converion_list,
                                                                                           left_padding_list,
                                                                                           right_padding_list, trim_list,
                                                                                           sub_string_list,
                                                                                           replace_string_list,
                                                                                           zip_extractor_list,
                                                                                           date_conversion_list,
                                                                                           pre_dqm_url,
                                                                                           batch_id, spark_context)
                            status_message = "PRE-DQM Standard Utility Completed"
                            logger.debug(status_message, extra=self.execution_context.get_context())
                            # Update process log and file audit entry to SUCCESS
                            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                            log_info['message']="Pre DQM Completed successfully"
                            # Update process log entry to SUCCESS
                            log_info[CommonConstants.BATCH_ID] = batch_id
                            log_info['status_id'] = CommonConstants.SUCCESS_ID
                            record_count = PySparkUtility(self.execution_context).get_parquet_record_count(pre_dqm_url)
                            log_info['record_count'] = record_count
                    else:
                        status_message = "No Pre - DQM checks configured for dataset id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        raise Exception
                else:
                    update_log_file_dtl_flag = False
                    status_message = 'Files with Batch ID : ' + str(
                        batch_id) + ' are already processed'
                    logger.debug(status_message, extra=self.execution_context.get_context())
            except Exception as exception:
                if no_standardization_flag == True:
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SKIPPED
                    log_info[CommonConstants.PROCESS_MESSAGE_KEY] = "No Standardization Rules Configured, Hence Skipping."
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SKIPPED
                    log_info[CommonConstants.BATCH_ID] = batch_id
                    log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime(
                        '%Y-%m-%d %H:%M:%S')
                    log_info['status_id'] = CommonConstants.FAILED_ID

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
                    log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"',
                                                                                                                   '')
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(exception)
                    self.execution_context.set_context({"traceback": error})
                    logger.error(error, extra=self.execution_context.get_context())
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    self.execution_context.set_context({"traceback": ""})
                    self.exc_info = sys.exc_info()
                    raise exception
            finally:
                if update_log_file_dtl_flag:
                    # updating process log entry
                    CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
                    status_message = "Updated process log entry for all files in batch id" + str(batch_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                else:
                    status_message = "File was processed in a previous run, hence process log was not updated"
                    logger.info(status_message, extra=self.execution_context.get_context())

            status_message = "Pre-DQM Completed for dataset id:" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            log_info[CommonConstants.BATCH_ID] = batch_id
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info['record_count'] = str(0)
            log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"', '')
            CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
            batches = batch_id_list.split(",")
            for batch in batches:
                log_info[CommonConstants.BATCH_ID] = batch
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info['record_count'] = str(0)
                log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"',
                                                                                                                   '')
                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                CommonUtils().update_batch_status(self.audit_db, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: str(exception)}
            raise exception
        finally:
            status_message = "In Finally Block for Standardization Handler"
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Closing spark context"
            logger.info(status_message, extra=self.execution_context.get_context())
            if spark_context:
                spark_context.stop()
            status_message = "Spark Context Closed For DQM Handler"
            logger.info(status_message, extra=self.execution_context.get_context())

    # ############################################# Main ######################################
    # Purpose   : Handles the process of executing Pre DQM Standardization returning the status
    #             and records (if any)
    # Input     : Requires dataset_id,batch id
    # Output    : Returns execution status and records (if any)
    # ##########################################################################################

    def main(self, dataset_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for Pre Dqm Standization"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.execute_pre_dqm_standization(dataset_id, cluster_id, workflow_id, process_id,
                                                                  batch_id, batch_id_list)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for Pre Dqm Standization"
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
            raise exception


if __name__ == '__main__':
    DATASET_ID = sys.argv[1]
    BATCH_ID = sys.argv[5]
    CLUSTER_ID = sys.argv[2]
    WORKFLOW_ID = sys.argv[3]
    PROCESS_ID = sys.argv[4]
    BATCH_ID_LIST = sys.argv[6]
    if DATASET_ID is None or CLUSTER_ID is None or WORKFLOW_ID is None or PROCESS_ID is None or BATCH_ID is None or BATCH_ID_LIST is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    BATCH_ID = str(BATCH_ID).rstrip("\n\r")
    pre_dqm_stand = PreDQMStandardizationHandler(DATASET_ID, CLUSTER_ID, WORKFLOW_ID, BATCH_ID)
    RESULT_DICT = pre_dqm_stand.main(DATASET_ID, CLUSTER_ID, WORKFLOW_ID, PROCESS_ID, BATCH_ID, BATCH_ID_LIST)
    STATUS_MSG = "\nCompleted execution for Pre-DQm Standization Utility with status " + json.dumps(RESULT_DICT) + "\n"
    sys.stdout.write("batch_id=" + str(BATCH_ID))
