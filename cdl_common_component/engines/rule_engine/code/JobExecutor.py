#!/usr/bin/python

__AUTHOR__ = 'ZS Associates'

import sys
import os
import subprocess
import traceback
import ast
import re
import json
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from LogSetup import logger

sys.path.insert(0, os.getcwd())

MODULE_NAME = "JobExecutor"

"""
Module Name         :   JobExecutor
Purpose             :   This module will act as rule engine
Input Parameters    :   process id, frequency, step name, data date, cycle id
Output Value        :   None
Pre-requisites      :   None
Last changed on     :   09-08-2018
Last changed by     :   Sushant Choudhary
Reason for change   :   Added functionality to replace parameters provided in property json
"""


class JobExecutor(object):

    def __init__(self, parent_execution_context=None):
        self.process_id = sys.argv[1]
        self.frequency = sys.argv[2]
        self.step_name = sys.argv[3]
        self.data_date = sys.argv[4]
        self.cycle_id = sys.argv[5]
        logger.debug("________________________________")
        logger.debug(self.cycle_id)
        self.configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.validation_error_location = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "validation_s3_error_location"])

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_id": self.process_id})
        self.execution_context.set_context({"frequency": self.frequency})
        self.execution_context.set_context({"data_date": self.data_date})
        self.execution_context.set_context({"cycle_id": self.cycle_id})
        self.execution_context.set_context({"step_name": self.step_name})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def create_rule_audit_entry(self, rule_id, rule_type):
        """
            Purpose :   This method inserts rule audit entry for each rule
            Input   :   Rule Id, Rule Type
            Output  :   NA
        """
        try:
            status = CommonConstants.STATUS_RUNNING
            status_message = "Started preparing query for doing rule audit entry for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:"\
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if rule_type is None:
                rule_audit_entry_query = "Insert into {audit_db}.{rule_audit_table} (process_id, frequency,data_date," \
                                         " cycle_id, step_name, rule_id, rule_start_time, rule_status) " \
                       "values('{process_id}', '{frequency}', '{data_date}',{cycle_id}, '{step_name}', " \
                                     "'{rule_id}', {rule_start_time}, '{status_desc}')".\
                        format(audit_db=self.audit_db, rule_audit_table=CommonConstants.LOG_RULE_DTL,
                       process_id=self.process_id, frequency=self.frequency, data_date=self.data_date,
                               cycle_id=self.cycle_id, step_name=self.step_name, rule_id=str(rule_id),
                       rule_start_time="NOW()", status_desc=status)
            else:
                rule_audit_entry_query = "Insert into {audit_db}.{rule_audit_table} (process_id, frequency, " \
                                         "data_date, cycle_id, step_name, rule_id, rule_start_time, rule_status," \
                                         " rule_type) values('{process_id}', '{frequency}', '{data_date}',{cycle_id}," \
                                         " '{step_name}', '{rule_id}', {rule_start_time}, '{status_desc}'," \
                                         " '{rule_type}')". \
                    format(audit_db=self.audit_db, rule_audit_table=CommonConstants.LOG_RULE_DTL,
                           process_id=self.process_id, frequency=self.frequency, data_date=self.data_date,
                           cycle_id=self.cycle_id, step_name=self.step_name, rule_id=str(rule_id),
                           rule_start_time="NOW()", status_desc=status, rule_type=rule_type)
            logger.debug(rule_audit_entry_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(rule_audit_entry_query)
            status_message = "Completed executing query for doing rule audit entry for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in doing rule audit entry for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def create_rule_validation_entry(self, rule_id, command_output_dict):
        """
            Purpose :   This method inserts rule validation summary entry for each rule
            Input   :   Rule Id, Validation Output Dictionary
            Output  :   NA
        """
        try:
            status_message = "Started preparing query for doing validation rule summary audit entry for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            validation_status = command_output_dict['validation_status']
            validation_msg = command_output_dict['validation_message']
            error_count = command_output_dict['error_record_count']
            error_link = command_output_dict['error_details_path']
            rule_validation_entry_query = "Insert into {audit_db}.{rule_audit_table} (process_id,frequency,data_date," \
                                          "step_name,rule_id,validation_status,validation_message,error_record_count," \
                                          "error_details_path, cycle_id) values('{process_id}','{frequency}'," \
                                          "'{data_date}','{step_name}','{rule_id}'," \
                                          " '{validation_status}','{validation_message}', '{error_record_count}'," \
                                          " '{error_details_path}', {cycle_id} )". \
                format(audit_db=self.audit_db, rule_audit_table=CommonConstants.LOG_VALIDATION_DTL,
                       process_id=self.process_id, frequency=self.frequency, data_date=self.data_date,
                       step_name=self.step_name, cycle_id=self.cycle_id,
                       rule_id=str(rule_id), validation_status=validation_status, validation_message=validation_msg,
                       error_record_count=error_count, error_details_path=error_link)
            logger.debug(rule_validation_entry_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(rule_validation_entry_query)
            status_message = "Completed executing query for doing validation rule summary audit entry for process_id:" \
                             + str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in doing validation rule summary audit entry for process_id:" \
                             + str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def execute_rule(self):
        """
            Purpose :   Driver handler method for calling helper functions required for rule execution associated
                        with input step . It also replaces required variables and performs spark submit for rules
                        available as part of given process id, frequency and step name
            Input   :   NA
            Output  :   NA
        """
        try:
            status_message = "Started executing step:" + self.step_name + " for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            CommonUtils().create_step_audit_entry(self.process_id, self.frequency, self.step_name, self.data_date,
                                                  self.cycle_id)
            status_message = "Started preparing query to fetch rule configs for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id) + " " + "and " \
                            "step name:" + str(self.step_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            rule_config_query = "Select rule_id, rule_type, rule_name, script_name, spark_config, property_json " \
                                "from {audit_db}.{rule_config_table} where process_id={process_id} and " \
                                "frequency='{frequency}' and step_name='{step_name}' and " \
                                "active_flag='{active_flag_value}'".\
                format(audit_db=self.audit_db, rule_config_table=CommonConstants.CTL_RULE_CONFIG, process_id=
            self.process_id, frequency=self.frequency, step_name=self.step_name,
                       active_flag_value=CommonConstants.ACTIVE_IND_VALUE )
            logger.debug(rule_config_query, extra=self.execution_context.get_context())
            rule_config_checks = MySQLConnectionManager(self.execution_context).execute_query_mysql(rule_config_query,
                                                                                                    False)
            status_message = "Completed executing query to fetch rule configs for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id) + " " + "and " \
                            "step name:" + str(self.step_name)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(rule_config_checks, extra=self.execution_context.get_context())
            # To check whether rule config checks are empty
            if not rule_config_checks:
                status_message = "No rules are configured for process_id:" + str(self.process_id) + " " + "and " \
                                "frequency:" + self.frequency + " " + "and data_date:" + str(self.data_date) + " " \
                                 + "and cycle id:" + str(self.cycle_id) + " " + "and step name:" + str(self.step_name)
                logger.warn(status_message, extra=self.execution_context.get_context())
            else:
                for rule in rule_config_checks:
                    try:
                        rule_id = rule['rule_id']
                        rule_type = rule['rule_type']
                        rule_name = rule['rule_name']
                        script_name = rule['script_name']
                        spark_config = rule['spark_config']
                        param_dict = {}
                        property_json = rule['property_json']
                        if property_json is None:
                            property_json = "{}"
                        if json.loads(property_json):
                            param_dict = json.loads(property_json)["job_parameters"]
                        if spark_config is None:
                            spark_config = ""
                        self.create_rule_audit_entry(rule_id, rule_type)
                        absolute_script_path = CommonConstants.EMR_CODE_PATH + "/" + CommonConstants.CONFIG + "/" + \
                                               CommonConstants.JOB_EXECUTOR + "/" + script_name
                        absolute_write_script_path = CommonConstants.EMR_CODE_PATH + "/" + CommonConstants.CODE + "/" \
                                                     + str(rule_id) + "_" + script_name
                        rule_append_space_command = 'sed  "s/^/    /"' + ' ' + absolute_script_path + ' ' + '>' + ' '\
                                                    + absolute_write_script_path
                        rule_modify_command = subprocess.Popen(rule_append_space_command, stdout=subprocess.PIPE,
                                                               stderr=subprocess.PIPE, shell=True)
                        append_space_command_output, append_space_command_error = rule_modify_command.communicate()
                        if rule_modify_command.returncode != 0:
                                logger.error(append_space_command_error, extra=self.execution_context.get_context())
                                status_message = "Exception in updating query file"
                                raise Exception(status_message)
                        logger.debug(append_space_command_output, extra=self.execution_context.get_context())

                        # Initializing job template path based on R and python scripts
                        if script_name.endswith(".py"):
                            job_template_path = CommonConstants.EMR_CODE_PATH+'/' + CommonConstants.CODE + '/' + \
                                            CommonConstants.DW_RULE_TEMPLATE_FILENAME
                        elif script_name.endswith(".R"):
                            job_template_path = CommonConstants.EMR_CODE_PATH+'/' + CommonConstants.CODE + '/' + \
                                            CommonConstants.DW_R_RULE_TEMPLATE_FILENAME
                        else:
                            raise Exception("Invalid extension of rule file identified in script_name")

                        with open(absolute_write_script_path) as rule_execution_file:
                            rule_execution_str = rule_execution_file.read()
                        with open(job_template_path) as rule_template_file:
                            rule_template_str = rule_template_file.read()
                        validation_error_s3_location = self.validation_error_location
                        logger.debug(self.cycle_id)
                        replaceable_properties_dic = {
                            '$$cycle_id': self.cycle_id,
                            '$$data_dt':  self.data_date.replace("-", ""),
                            '$$app_name': rule_name,
                            '$$rule_id': str(rule_id),
                            '$$spark_config': str(spark_config),
                            '$$validation_error_location': validation_error_s3_location
                        }

                        rule_execution_str2 = CommonUtils().replace_values(rule_execution_str,
                                                                           replaceable_properties_dic)

                        replaceable_properties_dic['$$query'] = rule_execution_str2
                        modified_rule_execution_str = CommonUtils().replace_values(rule_template_str,
                                                                                   replaceable_properties_dic)
                        modified_rule_execution_str_param = CommonUtils().replace_values(modified_rule_execution_str,
                                                                                   param_dict)
                        status_message = "Starting to replace the parameters provided in the Property parameter Json"
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        status_message = "Completed replacing the parameters provided in the Property parameter Json"
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        logger.debug(modified_rule_execution_str_param, extra=self.execution_context.get_context())
                        if '$$' in modified_rule_execution_str_param:
                            raise Exception("Invalid parameters provided in the query")

                        status_message = "Parameters replaced for rule matching process_id:" + str(self.process_id) + \
                                         " " + "and frequency:" + self.frequency + " " + "and data_date:" + \
                                         str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + \
                                         "and step name:" + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        status_message = "Started executing rule matching process_id:" + str(self.process_id) + " " +\
                                         "and frequency:" + self.frequency + " " + "and data_date:" + \
                                         str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " +\
                                         "and step name:" + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        with open(absolute_write_script_path, "w") as modified_rule_execution_file:
                            modified_rule_execution_file.write(modified_rule_execution_str_param)

                        if absolute_write_script_path.endswith(".py"):
                            command = CommonConstants.SPARK_BIN_PATH + "spark-submit" + " " + spark_config + " " + \
                                      absolute_write_script_path
                        elif absolute_write_script_path.endswith(".R"):
                            command = CommonConstants.RSCRIPT_BIN_PATH + "Rscript " + absolute_write_script_path
                        else:
                            raise Exception("Invalid file extension identified")

                        status_message = "Spark submit command to be executed:" + command
                        logger.info(status_message, extra=self.execution_context.get_context())
                        self.check_rule_execution_status(command, rule_id, rule_type)
                        status = CommonConstants.STATUS_SUCCEEDED
                    except Exception as exception:
                        status = CommonConstants.STATUS_FAILED
                        status_message = "Exception in execution of rule id:"+str(rule_id)
                        error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                                " ERROR MESSAGE: " + str(traceback.format_exc())
                        self.execution_context.set_context({"traceback": error})
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise exception
                    finally:
                        self.update_rule_audit_entry(rule_id, status)
                        status_message = "Completed executing rule matching process_id:" + str(self.process_id) + \
                                         " " + "and frequency:" + self.frequency + " " + "and data_date:" + \
                                         str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + \
                                         "and step name:" + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
            status = CommonConstants.STATUS_SUCCEEDED
        except Exception as exception:
            status = CommonConstants.STATUS_FAILED
            status_message = "Error occured in executing step:" + self.step_name + " for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception
        finally:
            CommonUtils(self.execution_context).update_step_audit_entry(self.process_id,
                                                                        self.frequency, self.step_name, self.data_date,
                                                                        self.cycle_id, status)
            if status == CommonConstants.STATUS_FAILED:
                CommonUtils(self.execution_context).update_cycle_audit_entry(self.process_id, self.frequency,
                                                                             self.data_date, self.cycle_id, status)
            status_message = "Completed executing step:" + self.step_name + " for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "and cycle id:" + str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())

    def check_rule_execution_status(self, command, rule_id, rule_type):
        """
            Purpose :   This method executes spark application rule
            Input   :   Rule Id, Rule Type
            Output  :   NA
        """
        spark_submit_rule_command = subprocess.Popen(command, stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE,
                                                     shell=True)
        command_output, command_error = spark_submit_rule_command.communicate()
        if spark_submit_rule_command.returncode == 0:
            status_message = "Output returned by  rule:" + str(rule_id) + " is " + \
                             command_output.decode('utf-8')
            logger.debug(status_message, extra=self.execution_context.get_context())
            if rule_type == CommonConstants.VALIDATION_TYPE:
                final_validation_dict_output = "{" + re.search('{(.+?)}', command_output.decode('utf-8')).group(1) + \
                                               "}"
                status_message = "Validation response dict for rule:" + str(rule_id) + " is " + \
                                 final_validation_dict_output
                logger.debug(status_message, extra=self.execution_context.get_context())
                command_output_dict = ast.literal_eval(final_validation_dict_output)
                if 'validation_status' in command_output_dict and 'validation_message' in \
                        command_output_dict and 'error_record_count' in command_output_dict:
                    self.create_rule_validation_entry(rule_id, command_output_dict)
                else:
                    status_message = "One of the required validation response parameter has not been " \
                                     "returned for rule id:" + str(rule_id)
                    raise Exception(status_message)
        else:

            logger.debug(command_error)
            status_message = "Exception occured in  executing rule matching process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + \
                             "and data_date:" + str(self.data_date) + " " + "cycle id:" + \
                             str(self.cycle_id) + " " + "and step name:" + str(self.step_name) + " " + \
                             "and rule_id:" + str(rule_id) + " due to " + str(command_error)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)

    def update_rule_audit_entry(self, rule_id, status):
        """
            Purpose :   This method updates rule execution status for each rule
            Input   :   Rule Id, Status
            Output  :   NA
        """
        try:
            status_message = "Started preparing query for updating rule audit entry for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            update_rule_audit_status_query = "Update {audit_db}.{rule_audit_table} set rule_status='{status}'," \
                                             "rule_end_time={rule_end_time} where process_id={process_id} and " \
                                             "frequency='{frequency}' and step_name='{step_name}' and data_date=" \
                                             "'{data_date}' and rule_id = '{rule_id}' and cycle_id={cycle_id}". \
                format(audit_db=self.audit_db,
                       rule_audit_table=CommonConstants.LOG_RULE_DTL,
                       process_id=self.process_id, status=status,
                       rule_end_time="NOW()",
                       frequency=self.frequency,
                       step_name=self.step_name, cycle_id=self.cycle_id,
                       data_date=self.data_date, rule_id=rule_id)
            logger.debug(update_rule_audit_status_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(update_rule_audit_status_query)
            status_message = "Completed executing query for updating audit entry for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in updating rule audit entry for process_id:" + \
                             str(self.process_id) + " " + "and frequency:" + self.frequency + " " + "and data_date:" \
                             + str(self.data_date) + " " + "cycle id:" + str(self.cycle_id) + " " + "and step name:" \
                             + str(self.step_name) + " " + "and rule_id:" + str(rule_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


if __name__ == '__main__':
    try:
        JOB_EXECUTOR_HANDLER = JobExecutor()
        JOB_EXECUTOR_HANDLER.execute_rule()
        STATUS_MSG = "Completed execution for Job Executor Utility"
        sys.stdout.write(STATUS_MSG)
    except Exception as exception:
        raise exception
