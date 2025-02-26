#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import os
import traceback
import json
import argparse
import textwrap
import sys
import boto3
from urllib.parse import urlparse
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager

sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "RepublishUtility"
PROCESS_NAME = "republish"

"""
Module Name         :   RepublishUtility
Purpose             :   This module will read parameters provided and deletes files from S3 locations accordingly
                        parsing and validating the required parameters
Input Parameters    :   Parameters: batch_ids/cycle_ids
Output Value        :   Return Dictionary with status(Failed/Success)
Last changed on     :   21st June 2018
Last changed by     :   Alageshan M
Reason for change   :   RepublishUtility development
Pending Development :
"""


class RepublishUtility(object):

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.CommonUtils = CommonUtils(self.execution_context)
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.mysql = MySQLConnectionManager()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def republish_utility(self, batch_id, cycle_id):
        status_message = "Starting file republish task "
        logger.info(status_message, extra=self.execution_context.get_context())
        message = "Republish Completed Successfully"
        try:
            if batch_id is not None:
                result_dict = self.get_file_process_locations(batch_id, None)
                self.create_logs(batch_id, None)
                s3_locations = result_dict["file_process_locations"]
                if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    status_message = str(result_dict[CommonConstants.PROCESS_MESSAGE_KEY])
                    logger.error(status_message,
                                 extra=self.execution_context.get_context())
                    status = CommonConstants.STATUS_FAILED
                    message = "Process failed in function get_file_process_locations" + status_message
                elif not isinstance(s3_locations, list) or (len(s3_locations) == 0):
                    status_message = "No file process locations found " \
                                     "in ctl_dataset_master for the given file_id,dataset_id"
                    logger.error(status_message,
                                 extra=self.execution_context.get_context())
                    status = CommonConstants.STATUS_FAILED
                    message = "Process failed in function get_file_process_locations"
                else:
                    result_dict = self.delete_file_from_s3(s3_locations[0]["publish_location"])
                    if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                        status = CommonConstants.STATUS_FAILED
                        message = "Process failed in function delete_file_from_s3"
                    else:
                        result_dict = self.s3_to_s3_copy(batch_id=batch_id,
                                                         cycle_id=None,
                                                         source_location=s3_locations[0]["staging_location"],
                                                         target_location=s3_locations[0]["publish_location"])
                        if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                            status = CommonConstants.STATUS_FAILED
                            message = "Process failed in function s3_to_s3_copy"
                        else:
                            status = CommonConstants.STATUS_SUCCEEDED
                self.update_logs(batch_id, None, status, message)
            elif cycle_id is not None:
                self.create_logs(None, cycle_id)
                status = CommonConstants.STATUS_SUCCEEDED
                result_dict = self.get_file_process_locations(None, cycle_id)
                s3_locations = result_dict["file_process_locations"]
                if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    status_message = str(result_dict[CommonConstants.PROCESS_MESSAGE_KEY])
                    logger.error(status_message,
                                 extra=self.execution_context.get_context())
                    status = CommonConstants.STATUS_FAILED
                    message = "Process failed in function get_file_process_locations" + status_message
                elif not isinstance(s3_locations, list) or (len(s3_locations) == 0):
                    status_message = "No file process locations found " \
                                     "in ctl_dataset_master for the given file_id,dataset_id"
                    logger.error(status_message,
                                 extra=self.execution_context.get_context())
                    status = CommonConstants.STATUS_FAILED
                    message = "Process failed in function get_file_process_locations"
                else:
                    for location in s3_locations:
                        result_dict = self.delete_file_from_s3(location["publish_location"])
                        if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                            status = CommonConstants.STATUS_FAILED
                            message = "Process failed in function delete_file_from_s3"
                            break
                        else:
                            result_dict = self.s3_to_s3_copy(batch_id=None, cycle_id=cycle_id,
                                                             source_location=location["staging_location"],
                                                             target_location=location["publish_location"])
                            if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                status = CommonConstants.STATUS_FAILED
                                message = "Process failed in function s3_to_s3_copy"
                                break
                            else:
                                status = CommonConstants.STATUS_SUCCEEDED
                self.update_logs(None, cycle_id, status, message)
            else:
                status_message = "Only one of the inputs can be non-NONE"
                logger.error(status_message)
                raise Exception(status_message)
            status_message = "Successfully completed file republish task "
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dict = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED,
                           CommonConstants.PROCESS_MESSAGE_KEY: status_message}
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            result_dict = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                           CommonConstants.PROCESS_MESSAGE_KEY: str(exception)}
            status_message = "In exception block for function republish_utility. Error: " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            message = "Republish failed with errors: " + str(exception)
            status = CommonConstants.STATUS_FAILED
            if batch_id is not None:
                self.update_logs(batch_id, None, status, message)
            elif cycle_id is not None:
                self.update_logs(None, cycle_id, status, message)
            logger.debug("Returning with result_dict: " + str(result_dict),
                         extra=self.execution_context.get_context())
        return result_dict

    def get_file_process_locations(self, batch_id, cycle_id):
        status_message = "Starting function to get file process locations "
        logger.info(status_message, extra=self.execution_context.get_context())
        s3_locations = []
        status = CommonConstants.RUNNING_STATE
        try:
            if batch_id is not None:
                dataset_id_query_str = "select distinct(dataset_id) " \
                                       "from {audit_db}.{process_log_table} where batch_id={batch_id}"
                dataset_id_query = dataset_id_query_str.format(audit_db=self.audit_db,
                                                               process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                                               batch_id=batch_id)
                dataset_id_result = self.mysql.execute_query_mysql(dataset_id_query)
                dataset_id = dataset_id_result[0]['dataset_id']
                data_set_info = self.CommonUtils.get_dataset_information_from_dataset_id(dataset_id)
                load_type = data_set_info['publish_type']
                if load_type == CommonConstants.LATEST_LOAD_TYPE:
                    publish_location = data_set_info['publish_s3_path']
                    publish_location = str(publish_location).rstrip('/')
                    staging_location = data_set_info['s3_post_dqm_location']
                    staging_location = str(staging_location).rstrip('/')
                    s3_locations.append({"publish_location": publish_location, "staging_location": staging_location})
                else:
                    LoadTypeError("The dataset obtained(" + str(dataset_id) +
                                  ") is not of publish_type '" + str(CommonConstants.LATEST_LOAD_TYPE) + "'")
            else:
                if cycle_id is not None:
                    dataset_id_query_str = "select distinct(dataset_id) from {audit_db}.{dependency_details} " \
                                           "where write_dependency_value={cycle_id} and table_type ='{table_type}' "
                    dataset_id_query = dataset_id_query_str.format(
                        audit_db=self.audit_db,
                        dependency_details=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                        cycle_id=cycle_id,
                        table_type=CommonConstants.TARGET_TABLE_TYPE)
                    dataset_id_result = self.mysql.execute_query_mysql(dataset_id_query)
                    for i in range(0, len(dataset_id_result)):
                        dataset_id = dataset_id_result[i]["dataset_id"]
                        location_query_str = "select dataset_id, table_s3_path, " \
                                             "publish_s3_path from {audit_db}.{dataset_master} " \
                                             "where dataset_id={dataset_id} and dataset_type='processed'"
                        location_query = location_query_str.format(
                            audit_db=self.audit_db,
                            dataset_master=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                            dataset_id=dataset_id)
                        location_result = self.mysql.execute_query_mysql(location_query)
                        if len(location_result) > 0:

                            s3_locations.append({"publish_location": location_result[0]["publish_s3_path"],
                                                 "staging_location": location_result[0]["table_s3_path"]})
                        else:
                            logger.ingo("No data found for Dataset: " + str(dataset_id)
                                        + "or dataset_type is configured as raw",
                                        extra=self.execution_context.get_context())
                else:
                    status_message = "All inputs cannot be NONE"
                    logger.error(status_message)
                    raise Exception(status_message)
                status_message = "File process locations : " + str(s3_locations)
                logger.info(status_message, extra=self.execution_context.get_context())
                status = CommonConstants.STATUS_SUCCEEDED
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            status = CommonConstants.STATUS_FAILED
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            s3_locations = []
        return {CommonConstants.STATUS_KEY: status,
                CommonConstants.PROCESS_MESSAGE_KEY: status_message,
                "file_process_locations": s3_locations}

    def delete_file_from_s3(self, file_path):
        status_message = "Executing S3 remove command  "
        logger.info(status_message, extra=self.execution_context.get_context())
        result_dict = {}
        if file_path is None or len(file_path) == 0:
            raise Exception("Cannot have empty file_path")
        try:
            session = boto3.session.Session()
            s3 = session.client('s3')
            parsed_url = urlparse(file_path)
            s3_bucket = parsed_url.netloc
            file_path = os.path.join(parsed_url.path, "").lstrip("/")
            objects_to_delete = []

            if 'Contents' in s3.list_objects(Bucket=s3_bucket, Prefix=file_path) and \
                    isinstance(s3.list_objects(Bucket=s3_bucket, Prefix=file_path)['Contents'], list) \
                    and len(s3.list_objects(Bucket=s3_bucket, Prefix=file_path)['Contents']) is not None:
                for obj in s3.list_objects(Bucket=s3_bucket, Prefix=file_path)['Contents']:
                    objects_to_delete.append({'Key': obj["Key"]})
                s3_cmd_result = s3.delete_objects(Bucket=s3_bucket, Delete={'Objects': objects_to_delete})
                status_message = "S3 command result : " + str(s3_cmd_result)
                logger.debug(status_message, extra=self.execution_context.get_context())
            else:
                status = "This path does not exist or is empty"
                logger.warn(status, extra=self.execution_context.get_context())
            result_dict[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCEEDED

        except Exception as exception:
            status_message = "In exception block of delete_file_from_s3 function "
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dict[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
        return result_dict

    def s3_to_s3_copy(self, batch_id, cycle_id, source_location, target_location):
        try:
            if batch_id is not None:
                source_location = source_location.rstrip("/") + "/" + \
                                  CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id) + "/"
                target_location = target_location.rstrip("/") + "/" + \
                                  CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id) + "/"

            elif cycle_id is not None:
                data_date = self.get_data_date_from_cycle(cycle_id)
                source_location = source_location.rstrip("/") + "/" + CommonConstants.DATE_PARTITION + "=" + \
                                  str(data_date).replace("-", "") + "/" + \
                                  CommonConstants.CYCLE_PARTITION + \
                                                                "=" + str(cycle_id) + "/"
                target_location = target_location.rstrip("/") + "/" + CommonConstants.DATE_PARTITION + "=" + \
                                  str(data_date).replace("-", "") + "/" + \
                                  CommonConstants.CYCLE_PARTITION + \
                                                                "=" + str(cycle_id) + "/"
            else:
                status_message = "All inputs cannot NONE"
                logger.error(status_message)
                result_dict[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                raise Exception(status_message)
            s3_cp_cmd = "aws s3 cp " + str(source_location) + " " + str(target_location) + " --recursive"
            self.CommonUtils.execute_shell_command(s3_cp_cmd)
            return {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}

    def get_file_ids_by_batch(self, batch_id):
        try:
            file_query_str = "select distinct(file_id) from {audit_db}.{process_log_table} where batch_id={batch_id}"
            file_query = file_query_str.format(audit_db=self.audit_db,
                                               process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                               batch_id=batch_id)
            file_result = self.mysql.execute_query_mysql(file_query)
            status_message = "Files retrieved : " + str(file_result)
            logger.info(status_message, extra=self.execution_context.get_context())
            return file_result
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_data_date_from_cycle(self, cycle_id):
        try:
            status_message = "Starting function to get data_date for cycle_id"
            logger.info(status_message, extra=self.execution_context.get_context())
            data_date_query_str = "select distinct data_date from " \
                                  "{audit_db}.{log_step_dtl} where cycle_id={cycle_id}"
            data_date_query = data_date_query_str.format(audit_db=self.audit_db,
                                                         log_step_dtl=CommonConstants.LOG_STEP_DTL,
                                                         cycle_id=cycle_id)
            data_date_result = self.mysql.execute_query_mysql(data_date_query)
            data_date = str(data_date_result[0]["data_date"])
            status_message = "Data date : " + str(data_date)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return data_date
        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def create_logs(self, batch_id, cycle_id):
        try:
            status_message = "Starting create_logs function "
            logger.info(status_message, extra=self.execution_context.get_context())
            if batch_id is not None:
                log_info_query_str = "select distinct file_name,dataset_id,process_id from" \
                                     " {audit_db}.{process_log_table} where batch_id={batch_id}"
                log_info_query = log_info_query_str.format(
                    audit_db=self.audit_db,
                    process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                    batch_id=batch_id)
                log_result = self.mysql.execute_query_mysql(log_info_query)
                file_ids_result = self.get_file_ids_by_batch(batch_id)
                for i in range(0, len(file_ids_result)):
                    file_id_retrieved = file_ids_result[i]['file_id']
                    process_info = {CommonConstants.FILE_NAME: log_result[0]['file_name'],
                                    'file_id': file_id_retrieved,
                                    CommonConstants.FILE_STATUS: CommonConstants.STATUS_RUNNING,
                                    CommonConstants.DATASET_ID: log_result[0]['dataset_id'],
                                    CommonConstants.BATCH_ID: batch_id,
                                    CommonConstants.CLUSTER_ID: 'NA',
                                    CommonConstants.WORKFLOW_ID: 'NA',
                                    'process_id': log_result[0]['process_id'],
                                    CommonConstants.PROCESS_NAME: PROCESS_NAME
                                    }
                    status_message = "Log info : " + str(process_info)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    self.CommonUtils.create_process_log_entry(self.audit_db, process_info)

            elif cycle_id is not None:
                log_info_query_str = "select distinct process_id,frequency,data_date from" \
                                     " {audit_db}.{log_step_dtl} where cycle_id={cycle_id}"
                log_info_query = log_info_query_str.format(audit_db=self.audit_db,
                                                           log_step_dtl=CommonConstants.LOG_STEP_DTL,
                                                           cycle_id=cycle_id)
                log_result = self.mysql.execute_query_mysql(log_info_query)
                if len(log_result) == 0:
                    raise Exception("No entry found in log_step_dtl for the cycle_id: " + str(cycle_id))
                process_id = log_result[0]["process_id"]
                frequency = log_result[0]["frequency"]
                step_name = PROCESS_NAME
                data_date = log_result[0]["data_date"]
                self.CommonUtils.create_step_audit_entry(process_id, frequency, step_name, data_date, cycle_id)
            else:
                logger.error("Both batch_id and cycle_id cannot be None",
                             extra=self.execution_context.get_context())
                raise Exception("Both batch_id and cycle_id cannot be None")

            status_message = "Completed create_logs function "
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_logs(self, batch_id, cycle_id, status, message):
        try:
            status_message = "Starting update_logs function "
            logger.info(status_message, extra=self.execution_context.get_context())
            if batch_id is not None:
                file_ids_result = self.get_file_ids_by_batch(batch_id)
                for i in range(0, len(file_ids_result)):
                    file_id_retrieved = file_ids_result[i]['file_id']
                    log_info = {CommonConstants.BATCH_ID: batch_id,
                                CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                'message': message,
                                CommonConstants.PROCESS_STATUS: status,
                                CommonConstants.FILE_ID: file_id_retrieved}
                    status_message = "Log info : " + str(log_info)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    self.update_process_status(self.audit_db, log_info)

            else:
                log_info_query_str = "select distinct process_id,frequency,data_date from" \
                                     " {audit_db}.{log_step_dtl} where cycle_id={cycle_id}"
                log_info_query = log_info_query_str.format(audit_db=self.audit_db,
                                                           log_step_dtl=CommonConstants.LOG_STEP_DTL,
                                                           cycle_id=cycle_id)
                log_result = self.mysql.execute_query_mysql(log_info_query)
                process_id = log_result[0]["process_id"]
                frequency = log_result[0]["frequency"]
                step_name = PROCESS_NAME
                data_date = log_result[0]["data_date"]
                self.update_step_audit_entry(process_id, frequency, step_name, data_date, cycle_id, status)
            status_message = "Completed update_logs function "
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def main(self, input_dict):
        try:
            status_message = "Starting the main function for RepublishUtility utility"
            logger.info(status_message, extra=self.execution_context.get_context())
            if input_dict['batch_id'] is not None:
                result_dictionary = self.republish_utility(input_dict['batch_id'], None)
                if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    raise Exception
                status_message = "Completing the main function for RepublishUtility utility"
                logger.info(status_message, extra=self.execution_context.get_context())
            elif input_dict['cycle_id'] is not None:
                result_dictionary = self.republish_utility(None, input_dict['cycle_id'])
                if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    raise Exception
                status_message = "Completing the main function for RepublishUtility utility"
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                logger.error("Both batch_id and cycle_id cannot be None",
                             extra=self.execution_context.get_context())
                raise Exception("Both batch_id and cycle_id cannot be None")
            return result_dictionary
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(str(exception), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_step_audit_entry(self, process_id, frequency, step_name, data_date, cycle_id, status):
        """Updates step audit entry for each process step
        Keyword arguments:
        process_id -- unique id corresponding to each process
        frequency -- process frequency
        step name -- process step name
        data_date -- Data date
        cycle id
        status - step status
        """
        try:
            step_update_audit_status_query = "Update {audit_db}.{step_audit_table} set " \
                                             "step_status='{status}', step_end_time={step_end_time}" \
                                             " where process_id={process_id} and frequency='" \
                                             "{frequency}' and step_name='{step_name}' and " \
                                             "data_date='{data_date}' and cycle_id =" \
                                             " '{cycle_id}' and step_status = '{in_progress_status}'".\
                format(audit_db=self.audit_db, step_audit_table=CommonConstants.LOG_STEP_DTL,
                       process_id=process_id, status=status, step_end_time="NOW()",
                       frequency=frequency, step_name=step_name, data_date=data_date,
                       cycle_id=cycle_id, in_progress_status=CommonConstants.STATUS_RUNNING)
            MySQLConnectionManager().execute_query_mysql(step_update_audit_status_query)
            status_message = "Updated '"+status+ "' status for cycle id:"+str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occurred while updating step audit status for cycle id:" + \
                             str(cycle_id)
            self.execution_context.set_context({"traceback": str(traceback.format_exc())})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_process_status(self, automation_log_database=None, log_info=None):
        """Method to update process status"""
        try:
            status_message = "Starting function to update process log status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info[CommonConstants.BATCH_ID]
            file_id = log_info[CommonConstants.FILE_ID]
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")

            record_count = str(0)
            message = str("'") + str("NA") + str("'")
            if 'record_count' in log_info:
                record_count = str(log_info['record_count'])

            if 'message' in log_info:
                message = str("'") + str(log_info['message']) + str("'")

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{process_log_table} set " \
                           "file_process_status = {process_status}, " \
                           " file_process_end_time = {end_time},record_count = {record_count}, " \
                           " process_message = {message}  where batch_id = {batch_id} and" \
                           " file_id = {file_id} and file_process_name = {process_name} " \
                           "and file_process_status = '{in_progress_status}'"
            query = query_string.format(automation_db_name=automation_log_database,
                                        process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        process_status=process_status, batch_id=batch_id,
                                        file_id=file_id, process_name=process_name,
                                        end_time="NOW() ", message=message,
                                        in_progress_status=CommonConstants.STATUS_RUNNING,
                                        record_count=record_count)

            status_message = "Input query for creating load automation log : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            self.execution_context.set_context({"traceback": str(traceback.format_exc())})
            logger.error(str(exception), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception


def check_mandatory_parameters_exists(input_dict):
    """
    Purpose   :   This method is used to check if all the necessary parameters exist in the passed input dictionary .
    Input     :   Input dictionary containing all the necessary arguments.
    Output    :   Returns True if all the variables exist in the required format
    """
    try:
        if input_dict['batch_id'] is None and input_dict["cycle_id"] is None:
                raise ValueError("Please provide either batch_id or cycle_id")
        return True

    except Exception as e:
        raise e


def get_commandline_arguments():
    """
    Purpose   :   This method is used to get all the commandline arguments .
    Input     :   Console Input
    Output    :   Dictionary of all the command line arguments.
    """
    parser = argparse.ArgumentParser(prog=MODULE_NAME, usage='%(prog)s [command line parameters]',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=textwrap.dedent('''
    command line parameters :

    -f, --file_id : File Id

    OR

    -b, --batch_id : Batch Id

    OR

   -f, --file_id : File Id
   -b, --batch_id :Batch Id

   OR

   -c, --cycle_id : Cycle Id


    command : python RepublishUtility.py -b <batch_ids>
              python RepublishUtility.py -c <cycle_ids>


    The list of command line parameters valid for %(prog)s module.'''),
                                     epilog="The module will exit now!!")

    # List of Mandatory Parameters

    parser.add_argument('-b', '--batch_id', required=False,
                        help='Specify batch_id using -b/--batch_id')

    parser.add_argument('-c', '--cycle_id', required=False,
                        help='Specify cycle_id using -c/--cycle_id')

    cmd_arguments = vars(parser.parse_args())
    return cmd_arguments


def LoadTypeError(error):
        logger.error(error, extra=ExecutionContext().get_context())
        sys.exit(1)


if __name__ == '__main__':
    try:
        commandline_arguments = get_commandline_arguments()
        if check_mandatory_parameters_exists(commandline_arguments):
            republish_utility = RepublishUtility()
            result_dict = republish_utility.main(input_dict=commandline_arguments)
        else:
            raise Exception

        STATUS_MSG = "\nCompleted execution for File Republish Utility with status " + json.dumps(result_dict) + "\n"
        sys.stdout.write(STATUS_MSG)
    except Exception as exp:
        message = "Error occurred in main Error : %s\nTraceback : %s" % (str(exp), str(traceback.format_exc()))
        raise exp