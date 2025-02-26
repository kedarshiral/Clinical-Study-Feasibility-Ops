#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

"""
 Module Name         :   GenerateDWDag
 Purpose             :   This Wrapper Will Generate DW Dag
 Input Parameters    :   Process Id
 Output Value        :   returns the status SUCCESS or FAILURE
 Pre-requisites      :
 Last changed on     :   28 May 2018
 Last changed by     :   Sushant Choudhary
 Reason for change   :

"""

import sys
import os
import json
import logging
import traceback
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from datetime import datetime
from LogSetup import logger
from ExecutionContext import ExecutionContext

MODULE_NAME = "GenerateDWDag"


class GenerateDWDag( object ):

    def __init__(self, parent_execution_context=None):
        self.configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = self.configuration.get_configuration( [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"] )

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


    def get_steps(self, process_id):
        """
        :param process_name: process_id
        :return: steps list
        """
        steps_list = []
        try:
            status_message = "Started preparing query to fetch step list for process id:"+str(process_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            query = "Select step_name from "+self.audit_db+"." + CommonConstants.STEP_WORKFLOW_MASTER_TABLE + \
                    " where process_id="+process_id+" and active_flag='"+CommonConstants.ACTIVE_IND_VALUE+"' order by step_sequence_number ASC"
            print(query)
            status_message = "Completed preparing query to fetch step list for process id:" + str(process_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            step_names = MySQLConnectionManager().execute_query_mysql( query, False )
            status_message = "Completed query execution to fetch step list for process id:" + str(process_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            for step in step_names:
                steps_list.append(step['step_name'])
            return steps_list
        except Exception as exception:
            status_message = "Failed to fetch step list for process id:" + str(process_id)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_frequency(self, process_id):
        try:
            status_message = "Started preparing query to fetch frequency for process id:" + str(process_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            query = "Select frequency from "+self.audit_db+"." + CommonConstants.EMR_CLUSTER_CONFIGURATION_TABLE + \
                    " where process_id="+process_id+" and active_flag='"+CommonConstants.ACTIVE_IND_VALUE+"'"
            print(query)
            status_message = "Completed preparing query to fetch frequency for process id:" + str(process_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            frequency = MySQLConnectionManager().execute_query_mysql( query, False )
            status_message = "Completed query execution to fetch frequency for process id:" + str(process_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            if frequency:
                return frequency[0]['frequency']
            else:
                raise Exception(CommonConstants.PROCESS_NOT_FOUND)
        except Exception as exception:
            status_message = "Failed to fetch frequency for process id:" + str(process_id) + str(traceback.format_exc())
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_workflow_name(self, process_id):
        try:
            status_message = "Started preparing query to fetch workflow name for process id:" + str(process_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            query = "Select workflow_name from "+self.audit_db+"." + CommonConstants.STEP_WORKFLOW_MASTER_TABLE + \
                    " where process_id="+process_id+" and active_flag='"+CommonConstants.ACTIVE_IND_VALUE+"'"
            print(query)
            status_message = "Completed preparing query to fetch workflow name for process id:" + str(process_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            workflow_names = MySQLConnectionManager().execute_query_mysql( query, False )
            status_message = "Completed query execution to fetch workflow name for process id:" + str(process_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            if workflow_names:
                return workflow_names[0]['workflow_name']
            else:
                raise Exception(CommonConstants.WF_NOT_FOUND)

        except Exception as exception:
            status_message = "Failed to fetch workflow name for PID:" + str(process_id) + str(traceback.format_exc())
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def generate_dw_dag(self, process_id):
        """
        :param value: process_id
        :return: Generates DW Dag file
        """
        try:
            dag_name = self.get_workflow_name(process_id)
            frequency = self.get_frequency(process_id)
            step_names = self.get_steps(process_id)
            task_list = ""
            dependency_list = ""
            print("before for")
            for i in range(0, len(step_names)):
                step_var = step_names[i] + ' = PythonOperator(task_id="' + step_names[
                    i] + '",python_callable=dagutils.call_job_executor,op_kwargs={"process_id":PROCESS_ID,"frequency":FREQUENCY, "step_name":"' + \
                           step_names[i] + '"},dag=dag)'
                task_list = task_list + step_var + '\n\n'
                if i == 0:
                    dependency_var = step_names[0] + '.set_upstream(emr_launch)'
                elif i == (len(step_names) - 1):
                    dependency_var = 'emr_terminate.set_upstream(' + step_names[i] + ')'
                else:
                    dependency_var = step_names[i+1] + '.set_upstream(' + step_names[i] + ')'
                dependency_list = dependency_list + dependency_var + '\n'
            dw_template_path = CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.DW_DAG_TEMPLATE_FILE
            dw_file_name = frequency+ '_' + process_id + '.py'
            with open(dw_template_path) as dw_template_file:
                dw_template_string = dw_template_file.read()

            replacable_properties_dic = {
                '$$dag_name': dag_name,
                '$$process_id': process_id,
                '$$frequency': frequency,
                '$$step_task_list': task_list,
                '$$step_dependency_ordering': dependency_list,
                '$$application_dir_path' : CommonConstants.AIRFLOW_CODE_PATH
            }
            modified_dw_process_dag_string = self.replace_values(dw_template_string, replacable_properties_dic)
            print(modified_dw_process_dag_string)
            with open(CommonConstants.AIRFLOW_CODE_PATH+'/'+dw_file_name, "w") as modified_dw_process_dag_file:
                modified_dw_process_dag_file.write(modified_dw_process_dag_string)

        except Exception as exception:
            status_message = "Failed to replace template value for PID:" + str(process_id) + str(traceback.format_exc())
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def replace_values(self,original_value, replaced_value):
        """ Replaces set of values with new values """
        for key, val in list(replaced_value.items()):
            original_value = original_value.replace(key, val)
        return original_value


if __name__ == '__main__':
    process_id = sys.argv[1]
    dw_dag = GenerateDWDag()
    dw_dag.generate_dw_dag(process_id)