#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'


import os
import sys
import traceback
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from ExecutionContext import ExecutionContext
from LogSetup import logger

sys.path.insert(0, os.getcwd())

MODULE_NAME = "LaunchDDLCreationUtility"

"""
Module Name         :   LaunchDDLCreationUtility
Purpose             :   This module will be used for creation of table, repairing partitions and collecting table/column
                        statistics
Input Parameters    :   table dict
Output Value        :   None
Pre-requisites      :   None
Last changed on     :   22-01-2019
Last changed by     :   Nishant Nijaguna
Reason for change   :   Python2 to 3
"""


class LaunchDDLCreationUtility(object):

    def __init__(self, parent_execution_context=None):

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def create_table(self, table_info):
        """
            Purpose :   This method creates table and repair partitions associated with table
            Input   :   Table details dictionary
            Output  :   NA
        """
        try:
            table_name = table_info['table_name']
            column_name_list = table_info['column_name_list']
            column_data_type_list = table_info['column_data_type_list']
            partition_column_list = table_info['partition_column_list']
            table_delimiter = table_info['table_delimiter']
            table_format = table_info['table_format']
            table_location = table_info['table_location']
            table_type = table_info['table_type']
            table_analyze_flag = table_info['table_analyze_flag']
            if partition_column_list != "":
                partition_column_string = "PARTITIONED BY( "+ partition_column_list + ")"
            else:
                partition_column_string = partition_column_list
            with open(CommonConstants.TABLE_TEMPLATE_FILE) as table_template_file:
                table_create_template_string = table_template_file.read()
            column_string = ''
            for column_index in range(0, len(column_name_list)):
                column_name_type = column_name_list[column_index] + ' ' + column_data_type_list[column_index]
                if column_index != (len(column_name_list) - 1):
                    column_name_type = column_name_type + ','
                column_string = column_string + column_name_type

            replacable_properties_dic = {
                '$$table_name': table_name,
                '$$table_type': table_type,
                '$$column_name_datatype_list': column_string,
                '$$table_delimiter': table_delimiter,
                '$$table_format': table_format,
                '$$table_location': table_location,
                '$$partition_column_string': partition_column_string
            }
            table_ddl_statement_query = CommonUtils().replace_values(table_create_template_string,
                                                                     replacable_properties_dic)
            logger.debug(table_ddl_statement_query, extra=self.execution_context.get_context())
            status_message = "Started creating database if not exist"
            logger.debug(status_message, extra=self.execution_context.get_context())
            db_table_array = table_name.split(".")
            database_name = db_table_array[0]
            database_creation_query = "CREATE DATABASE IF NOT EXISTS " + str(database_name)
            CommonUtils().execute_hive_query(database_creation_query)
            status_message = database_name + " database created , if not exists"
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Started creating table:" + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            CommonUtils().execute_hive_query(table_ddl_statement_query)
            status_message = "Completed creating table:" + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            repair_partition_query = "MSCK REPAIR TABLE "+table_name
            status_message = "Started repairing table partition:" + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            # execute repair partition using msck
            CommonUtils().execute_hive_query(repair_partition_query)
            status_message = "Completed repairing table partition:" + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if table_analyze_flag == CommonConstants.ACTIVE_IND_VALUE:
                self.analyze_table(table_info)

        except Exception as exception:
            status_message = "Error in executing DDL script" + str(exception)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message,extra=self.execution_context.get_context())
            raise exception

    def analyze_table(self, table_info):
        """
            Purpose :   This method analyzes table name with partitions
            Input   :   Table Details Dictionary
            Output  :   NA
        """
        try:
            status_message = "Started function to analyze table"
            logger.info(status_message, extra=self.execution_context.get_context())
            partition_columns = table_info['partition_column_names']
            analyze_column_names_list = table_info['analyze_column_name_list']
            table_name = table_info['table_name']
            status_message = "Started preparing query to analyze table:" +str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if partition_columns == "":
                analyze_command_query = "ANALYZE table" + " " + table_name + " " + " COMPUTE STATISTICS FOR " \
                                        "COLUMNS" + " " + analyze_column_names_list
            else:
                analyze_command_query = "ANALYZE table" + " " + table_name + " " + "PARTITION" + \
                                        "(" + partition_columns + ")" + " COMPUTE STATISTICS FOR COLUMNS" + " " + \
                                        analyze_column_names_list
            status_message = "Completed preparing query to analyze table:" + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Query to analyze table is:" + analyze_command_query
            logger.debug(status_message, extra=self.execution_context.get_context())
            CommonUtils().execute_hive_query(analyze_command_query)
            status_message = "Completed analyzing table:" + str(table_name)
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to analyze table"
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error Occured in analyzing table:" + str(table_name)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception
