#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import os
import sys
import traceback
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from LogSetup import logger
from LaunchDDLCreationUtility import LaunchDDLCreationUtility

sys.path.insert(0, os.getcwd())

MODULE_NAME = "LaunchDDLCreationHandler"

"""
Module Name         :   LaunchDDLCreationHandler
Purpose             :   This module will create table by invoking DDL Creation utility
Input Parameters    :   process id, frequency, data date, cycle id
Output Value        :   None
Pre-requisites      :   None
Last changed on     :   06-06-2018
Last changed by     :   Sushant Choudhary
Reason for change   :   
"""


class LaunchDDLCreationHandler(object):

    def __init__(self, parent_execution_context=None):
        self.process_id = sys.argv[1]
        self.frequency = sys.argv[2]
        self.data_date = sys.argv[3]
        self.cycle_id = sys.argv[4]
        self.step_name = None
        self.configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = self.configuration.get_configuration( [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

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

    def launch_ddl_creation(self):
        """
            Purpose :   Driver handler method for calling helper functions required for DDL creation
            Input   :   NA
            Output  :   NA
        """
        try:
            # command = "sudo /usr/bin/pip-3.7 uninstall numpy -y"
            # execution_status = CommonUtils().execute_shell_command(command)
            # command1 = "sudo /usr/bin/pip-3.7 uninstall numpy -y"
            # execution_status = CommonUtils().execute_shell_command(command1)
            # command2 = "sudo /usr/bin/pip-3.7 install numpy==1.21.4"
            # execution_status = CommonUtils().execute_shell_command(command2)
            status_message = "Starting DDL Creation step for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            self.step_name = CommonConstants.LAUNCH_DDL_CREATION_STEP
            self.execution_context.set_context({"step_name": self.step_name})
            CommonUtils(self.execution_context).create_step_audit_entry(self.process_id, self.frequency, self.step_name,
                                                       self.data_date,
                                                       self.cycle_id)
            status_message = "Started preparing query to fetch tables to be created for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + \
                             "and data_date:" + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_dataset_details_query = "select {dataset_table}.dataset_id, dataset_type," \
                                          "{process_dependency_table}.table_type from {audit_db}."\
                                          "{process_dependency_table} INNER JOIN {audit_db}.{dataset_table} ON " \
                                          "{process_dependency_table}.dataset_id={dataset_table}.dataset_id where " \
                                          "process_id={process_id} and frequency='{frequency}' and " \
                                          "data_date='{data_date}' and write_dependency_value={cycle_id}". \
                format(audit_db=self.audit_db, dataset_table=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                       process_dependency_table=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                       process_id=self.process_id, data_date=self.data_date, cycle_id=self.cycle_id,
                       frequency=self.frequency)
            logger.debug(fetch_dataset_details_query, extra=self.execution_context.get_context())
            process_dependency_dataset_list = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_dataset_details_query)
            status_message = "Completed executing query to fetch tables to be created for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + \
                             "and data_date:" + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(process_dependency_dataset_list, extra=self.execution_context.get_context())
            for table_details in process_dependency_dataset_list:
                status_message = "Dataset result dict:" + str(table_details)
                logger.debug(status_message, extra=self.execution_context.get_context())
                dataset_id = table_details['dataset_id']
                self.execution_context.set_context({"dataset_id": dataset_id})
                dataset_type = table_details['dataset_type']
                dependency_table_type = table_details['table_type']
                if dataset_type is None:
                    status_message = "Dataset type is not provided for dataset_id:" + str(dataset_id)
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise Exception(status_message)
                status_message = "Dataset id is:" + str(dataset_id) + " and dataset type is:" + str(dataset_type)
                logger.debug(status_message, extra=self.execution_context.get_context())
                fetch_table_details_query = "Select staging_table, file_field_delimiter, table_type, table_name," \
                                            " table_partition_by, table_hdfs_path, hdfs_post_dqm_location, " \
                                            "table_storage_format, table_delimiter,table_analyze_flag, " \
                                            "table_analyze_optional_column_list from {audit_db}.{dataset_table} " \
                                            "where dataset_id={dataset_id}". \
                    format(audit_db=self.audit_db,
                           dataset_table=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                           dataset_id=dataset_id)
                status_message = "Prepared query to fetch table details for dataset_id:" + str(dataset_id)
                logger.debug(status_message, extra=self.execution_context.get_context())
                logger.debug(fetch_table_details_query, extra=self.execution_context.get_context())
                table_details_result = MySQLConnectionManager(self.execution_context).execute_query_mysql(
                        fetch_table_details_query, True)
                status_message = "Completed query to fetch table details for dataset_id:" + str(dataset_id)
                logger.debug(status_message, extra=self.execution_context.get_context())
                logger.debug(table_details_result, extra=self.execution_context.get_context())
                table_analyze_flag = ""
                analyze_column_list = ""
                if dependency_table_type.lower() == CommonConstants.SOURCE_TABLE_TYPE:
                    table_analyze_flag = table_details_result['table_analyze_flag']
                    analyze_column_list = ""
                    if table_analyze_flag is None or table_analyze_flag == "":
                        status_message = "Table analyze flag is not set for dataset_id:" +str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                    elif table_analyze_flag != CommonConstants.ACTIVE_IND_VALUE:
                        status_message = "Table analyze flag is incorrectly configured for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    else:
                        status_message = "Table analyze flag is  set for dataset_id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        analyze_column_list = table_details_result['table_analyze_optional_column_list']
                        if analyze_column_list is None:
                            analyze_column_list = ""
                            status_message = "Analyzing all columns for dataset id:" +str(dataset_id)
                            logger.info(status_message, extra=self.execution_context.get_context())
                        else:
                            status_message = "Analyzing " + analyze_column_list + " for dataset id:" + str(dataset_id)
                            logger.info(status_message, extra=self.execution_context.get_context())
                partition_column_list = ""
                table_type = ""
                if dataset_type.lower() == CommonConstants.RAW_DATASET_TYPE:
                    table_location = table_details_result['hdfs_post_dqm_location']
                    partition_columns = CommonConstants.PARTITION_BATCH_ID + "," + CommonConstants.PARTITION_FILE_ID
                    table_format = CommonConstants.PARQUET_FORMAT
                    partition_column_list = CommonConstants.PARTITION_BATCH_ID + " " + CommonConstants.BATCH_PARTITION_TYPE + "," + \
                                            CommonConstants.PARTITION_FILE_ID + " " + CommonConstants.FILE_PARTITION_TYPE
                    table_name = table_details_result['staging_table']
                    if table_name is None:
                        status_message = "Staging table name is not provided for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    elif "." not in table_name:
                        status_message = "Database name is not provided for dataset_id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        db_name = "default"
                        status_message = "Database name set to " + db_name + " for dataset_id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        table_name = db_name + "." + table_name
                    else:
                        status_message = "Staging Table name is :" + str(table_name)
                        logger.info(status_message, extra=self.execution_context.get_context())
                    table_delimiter = table_details_result['file_field_delimiter']
                    if table_delimiter is None:
                        status_message = "Staging table delimiter is not provided for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                elif dataset_type.lower() == CommonConstants.PROCESSED_DATASET_TYPE:
                    table_type = table_details_result['table_type']
                    if table_type is None:
                        status_message = "Table Type is not provided for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    elif table_type.lower() == CommonConstants.MANAGED_TABLE_TYPE:
                        table_type = ""
                    elif table_type.lower() == CommonConstants.EXTERNAL_TABLE_TYPE:
                        table_type = CommonConstants.EXTERNAL_TABLE_TYPE
                    else:
                        status_message = "Table type is incorrectly configured for dataset id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    table_location = table_details_result['table_hdfs_path']
                    if table_location is None:
                        status_message = "HDFS path is not provided for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    partition_columns = table_details_result['table_partition_by']
                    if partition_columns is None or partition_columns == "":
                        status_message = "No Partition columns for dataset id:" +str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                    else:
                        partition_column_list_array = partition_columns.split(",")
                        for partition_col in range(0, len(partition_column_list_array)):
                            partition_column_list = partition_column_list + partition_column_list_array[partition_col] \
                                                    + " " + CommonConstants.STRING_DATATYPE
                            if partition_col != (len(partition_column_list_array) - 1):
                                 partition_column_list = partition_column_list + ","
                    table_name = table_details_result['table_name']
                    if table_name is None:
                        status_message = "Processed table name is not provided for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    elif "." not in table_name:
                        status_message = "Database name is not provided for dataset_id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        db_name = "default"
                        status_message = "Database name set to " + db_name + " for dataset_id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        table_name = db_name + "." + table_name
                    else:
                        status_message = "Processed Table name is :" +str(table_name)
                        logger.info(status_message, extra=self.execution_context.get_context())
                    table_delimiter = table_details_result['table_delimiter']
                    if table_delimiter is None:
                        status_message = "Processed table delimiter is not provided for dataset_id:" + str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    table_format = table_details_result['table_storage_format']
                    if table_format is None:
                        status_message = "Table format is not provided for dataset_id:" +str(dataset_id)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    elif table_format.upper() == CommonConstants.TABLE_TEXT_FORMAT:
                        table_format = CommonConstants.TEXT_FORMAT
                    elif table_format.upper() == CommonConstants.PARQUET_FORMAT:
                        table_format = CommonConstants.PARQUET_FORMAT
                    else:
                        status_message = "Table format is not supported"
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                else:
                    status_message = "Dataset type is incorrectly configured for dataset id:" + str(dataset_id)
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise Exception(status_message)
                column_name_list = CommonUtils(self.execution_context).fetch_column_details(dataset_id, "column_name")
                if not column_name_list:
                    status_message = "Column names are not configured for dataset_id:" +str(dataset_id)
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise Exception(status_message)
                column_data_type_list = CommonUtils(self.execution_context).fetch_column_details(dataset_id,
                                                                                                 "column_data_type")
                if not column_data_type_list:
                    status_message = "Column data types are not configured for dataset_id:" +str(dataset_id)
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise Exception(status_message)
                table_info = {'table_name': table_name,
                        'column_name_list': column_name_list,
                        'column_data_type_list' : column_data_type_list,
                        'partition_column_list' : partition_column_list,
                        'table_delimiter' : table_delimiter,
                        'table_format' : table_format,
                        'table_location' : table_location,
                        'table_type' : table_type,
                        'table_analyze_flag' : table_analyze_flag,
                        'partition_column_names' : partition_columns,
                        'analyze_column_name_list' : analyze_column_list,
                        }
                status_message = "Table dictionary passed for dataset_id:" + str(dataset_id)
                logger.debug(status_message, extra=self.execution_context.get_context())
                logger.debug(table_info, extra=self.execution_context.get_context())
                LaunchDDLCreationUtility(self.execution_context).create_table(table_info)
            status = CommonConstants.STATUS_SUCCEEDED
        except Exception as exception:
            status = CommonConstants.STATUS_FAILED
            status_message = "Error occured in DDL Creation step for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
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
                                                                             self.data_date, self.cycle_id, status)
            status_message = "Completed executing DDL Creation step for process_id:" + str(self.process_id) + " " \
                             + "and frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) \
                             + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())


if __name__ == '__main__':
    try:
        DDL_HANDLER = LaunchDDLCreationHandler()
        DDL_HANDLER.launch_ddl_creation()
        STATUS_MESSAGE = "Completed execution for Launch DDL Creation Utility"
        sys.stdout.write(STATUS_MESSAGE)
    except Exception as exception:
        raise exception
