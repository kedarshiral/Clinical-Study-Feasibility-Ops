#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
AUTHOR = 'ZS Associates'


# ####################################################Module Information################################################
#  Module Name         :   JobExecutorHandler
#  Purpose             :   This module will act as rule executor
#  Input Parameters    :   process id, frequency , step name , data date
#  Output Value        :   None
#  Pre-requisites      :   Cycle log and Step log table
#  Last changed on     :   27-04-2018
#  Last changed by     :   Sushant Choudhary
#  Reason for change   :   Job Executor development
# ######################################################################################################################

import ast
import os
import sys
import subprocess
import datetime
import time
sys.path.insert(0, os.getcwd())
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants
from ExecutionContext import ExecutionContext
from LogSetup import logger

MODULE_NAME = "JobExecutorHandler"


class JobExecutorHandler(object):

    def __init__(self, parent_execution_context=None):
        self.process_id = sys.argv[1]
        self.frequency = sys.argv[2]
        self.data_date = sys.argv[3]
        self.step_name = sys.argv[4]

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_date_time(self):
        #Timestamp format needs to be taken from constants file
        return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    def create_rule_audit_entry(self,rule_id):
        """Inserts rule audit entry for each rule
            Keyword arguments:
            rule_id -- unique id corresponding to each rule
        """
        try:
            status = CommonConstants.STATUS_RUNNING
            rule_audit_entry_query="Insert into {audit_db}.{rule_audit_table} (process_id, frequency, data_date, step_name, rule_id, rule_start_time, rule_status) " \
                       "values('{process_id}', '{frequency}', '{data_date}', '{step_name}', '{rule_id}', '{date_time}', '{status_desc}')".\
                        format(audit_db = CommonConstants.AUDIT_DB_NAME,rule_audit_table = 'log_rule_dtl',
                       process_id=self.process_id,frequency=self.frequency, data_date=self.data_date, step_name=self.step_name,rule_id=str(rule_id),
                       date_time=str(self.get_date_time()),status_desc=status)
            MySQLConnectionManager().execute_query_mysql(rule_audit_entry_query)
            status_message="Rule audit entry inserted"
            logger.info(status_message,extra=self.execution_context.get_context())
        except Exception as  exception:
            status_message = "Error in creating rule audit entry"
            logger.error(status_message,extra=self.execution_context.get_context())
            raise exception

    def create_rule_validation_entry(self,rule_id,command_output_dict):
        """Inserts rule audit summary for validation checks.
            Keyword arguments:
            rule_id -- unique id assigned to each rule
            command_output_dict -- validation check summary output response
            """
        try:
            validation_status = command_output_dict['validation_status']
            validation_msg = command_output_dict['validation_message']
            error_count = command_output_dict['error_record_count']
            error_link = command_output_dict['error_details_path']
            rule_validation_entry_query="Insert into {audit_db}.{rule_audit_table} (process_id, frequency, data_date, step_name, rule_id, " \
                                        "validation_status, validation_message, error_record_count, error_details_path) " \
                             "values('{process_id}', '{frequency}', '{data_date}', '{step_name}', '{rule_id}', '{validation_status}'," \
                                        " '{validation_message}', '{error_record_count}', '{error_details_path}' )".\
                           format(audit_db=CommonConstants.AUDIT_DB_NAME,rule_audit_table='log_rule_validation_dtl',
                    process_id=self.process_id,frequency=self.frequency, data_date=self.data_date, step_name=self.step_name,
                    rule_id=str(rule_id),validation_status = validation_status, validation_message = validation_msg ,
                               error_record_count = error_count, error_details_path = error_link)
            rule_validation_entry_query = MySQLConnectionManager().execute_query_mysql(rule_validation_entry_query)
            status_message="Rule validation check entry inserted"
            logger.info(status_message,extra=self.execution_context.get_context())
        except Exception as exception:
            status_message="Error in creating rule validation check entry"
            logger.error(status_message,extra=self.execution_context.get_context())
            raise exception

    def execute_rule(self):
        """ Actual driving method for calling other helper functions"""
        try:
            status_message = "Starting execute rule function"
            logger.debug(status_message, extra=self.execution_context.get_context())
            # Input Validations
            if self.process_id is None:
                raise Exception('Process Id is not provided')
            if self.step_name is None:
                raise Exception("Step Name is not provided")
            if self.frequency is None:
                raise Exception('Frequency is not provided')
            if self.data_date is None:
                raise Exception('Data date is not provided')
            status_message = "Started preparing query for fetching rule configs"
            logger.debug(status_message, extra=self.execution_context.get_context())
            rule_config_query = "select rule_id,script_name,script_path from audit_information.ctl_rule_config " \
                              "where process_id='"+self.process_id+"' and step_name='"+self.step_name+"' and " \
                                                                                                      "frequency='"+self.frequency+"' and active_flag='Y'"
            rule_config_checks = MySQLConnectionManager().execute_query_mysql(rule_config_query, False)
            # To check whether rule config checks are empty
            if len(rule_config_checks) == 0:
                status_message = "No matching rules to be executed"
                logger.error(status_message , extra=self.execution_context.get_context())
                raise Exception(status_message )
            #Hardcoding cycle id for now
            cycle_id = self.get_date_time()
            for rule in rule_config_checks:
                try:
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    rule_id = rule['rule_id']
                    status_message = "Started executing rule id:" + str(rule_id)
                    self.create_rule_audit_entry(rule_id)
                    script_name = rule['script_name']
                    script_path = rule['script_path']
                    absolute_script_path = script_path+script_name
                    with open(absolute_script_path) as rule_execution_file:
                        property_original_file_str = rule_execution_file.read()
                    replacable_properties_dic = {
                        '$$cycle_id': cycle_id,
                        '$$data_date': self.data_date
                    }
                    modified_file_str = self.replace_values(property_original_file_str, replacable_properties_dic)
                    status_message = "Required parameters replaced for rule:"+str(rule_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    #modified_absolute_path_script=script_path + "_replaced" + script_name
                    with open(absolute_script_path, "w") as modified_rule_execution_file:
                        modified_rule_execution_file.write(modified_file_str)
                    command = 'spark-submit ' + absolute_script_path
                    spark_submit_rule_command = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                             shell=True)
                    command_output, command_error = spark_submit_rule_command.communicate()
                    if spark_submit_rule_command.returncode == 0:
                        status = CommonConstants.STATUS_SUCCEEDED
                        #Validation step name needs to be fetched from Constant file
                        if self.step_name == "Validation Check":
                            command_output_dict = ast.literal_eval(command_output)
                            self.create_rule_validation_entry(rule_id, command_output_dict)
                    else:
                        status = CommonConstants.STATUS_FAILED
                except Exception:
                    status = CommonConstants.STATUS_FAILED
                    status_message = "Exception occured in rule id:"+str(rule_id)
                    logger.error(status_message, extra=self.execution_context.get_context())
                finally:
                    self.update_rule_audit_entry(rule_id, status)
                    status_message = "Completed executing rule:"+str(rule_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            status_message = "Exception occured in execute rule function"
            logger.error(status_message,extra=self.execution_context.get_context())
            raise exception

    def update_rule_audit_entry(self, rule_id, status):
        """Updates rule audit entry status for each rule
            Keyword arguments:
            rule_id -- unique id corresponding to each rule
            status -- rule execution status - Success or Failed
        """
        try:
            update_rule_audit_status_query = "Update {audit_db}.{rule_audit_table} set rule_status='{status}',rule_end_time='{end_time}' where " \
                            "process_id={process_id} and frequency='{frequency}' and step_name='{step_name}' and data_date='{data_date}' and " \
                                             "rule_id = '{rule_id}'".format(
                audit_db=CommonConstants.AUDIT_DB_NAME, rule_audit_table='log_rule_dtl', process_id=self.process_id, status=status,
                end_time=self.get_date_time(), frequency=self.frequency, step_name=self.step_name, data_date=self.data_date, rule_id = rule_id)
            print update_rule_audit_status_query
            result = MySQLConnectionManager().execute_query_mysql(update_rule_audit_status_query)
            status_message = "Updated the status for rule execution"
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured while updating rule audit status"
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def replace_values(self,original_value, replaced_value):
        """ Replaces set of values with new values """
        for key, val in replaced_value.items():
            original_value = original_value.replace(key, val)
        return original_value


if __name__ == '__main__':
    try:
        executorHandler = JobExecutorHandler()
        executorHandler.execute_rule()
        status_msg = "Completed execution for Job Executor Utility"
        sys.stdout.write(status_msg)
    except Exception as exception:
        raise exception
