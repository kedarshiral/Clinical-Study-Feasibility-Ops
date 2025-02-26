#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Workflow Handler
#  Purpose             :   This module will perform the pre-configured steps before invoking
#                          WorkflowLauncherUtility.py.
#  Input Parameters    :   process name
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   8th January 2017
#  Last changed by     :   Sushant Choudhary
#  Reason for change   :   First version
# ######################################################################################################################

# Library and external modules declaration
import json
import traceback
import sys
import os
sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from WorkflowLauncherUtility import WorkflowLauncherUtility
from MySQLConnectionManager import MySQLConnectionManager

# all module level constants are defined here
MODULE_NAME = "WorflowHandler"
PROCESS_NAME = "Workflow creation and launch"

USAGE_STRING = """
SYNOPSIS
    python WorflowHandler.py process_name

    Where
        input parameters : process_name

"""


class WorflowHandler:
    # Default constructor
    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


    # ########################################### execute_workflow ####################################################
    # Purpose            :   This method will call workflow utility for datasets matching process name
    # Input              :   process name
    # Output             :   NA
    # ##################################################################################################################
    def execute_workflow(self, process_name=None):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        status_message = ""
        try:
            status_message = "Starting cluster check status function of module WorflowHandler"
            logger.info(status_message, extra=self.execution_context.get_context())
            # Input Validations
            if process_name is None:
                raise Exception('Process name is not provided')
            print "*******************"
            self.execution_context.set_context({"process_name": process_name})
            logger.info(status_message, extra=self.execution_context.get_context())
            cluster_check_query = "SELECT "+CommonConstants.EMR_CLUSTER_ID_COLUMN+","+CommonConstants.EMR_MASTER_DNS+" from "+CommonConstants.AUDIT_DB_NAME+"."+CommonConstants.EMR_CLUSTER_DETAILS_TABLE+" where "+CommonConstants.EMR_CLUSTER_PROCESS_COLUMN+"='"+str(process_name)+"' and "+CommonConstants.EMR_CLUSTER_STATUS_COLUMN+"='"+str(CommonConstants.CLUSTER_ACTIVE_STATE)+"'"
            print(cluster_check_query)
            cluster_output_result = MySQLConnectionManager().execute_query_mysql(cluster_check_query, False)

            # To check whether only one cluster is available for given process name
            print(cluster_output_result)
            if len(cluster_output_result)==1:
                cluster_id=cluster_output_result[0]['cluster_id']
                master_node_dns=cluster_output_result[0]['master_node_dns']
                property_file_path=CommonConstants.OOZIE_UNIX_PROPERTY_FILE_PATH
                fetch_dataset_query="Select "+CommonConstants.MYSQL_DATASET_ID+" from "+CommonConstants.AUDIT_DB_NAME+"."+CommonConstants.EMR_PROCESS_WORKKFLOW_MAP_TABLE+" where "+CommonConstants.EMR_PROCESS_WORKFLOW_COLUMN+"='"+str(process_name)+"' and "+CommonConstants.EMR_PROCESS_WORKFLOW_ACTIVE_COLUMN+"='"+str(CommonConstants.WORKFLOW_DATASET_ACTIVE_VALUE)+"'"
                datasetid_list = MySQLConnectionManager().execute_query_mysql(fetch_dataset_query)
                print (datasetid_list)
                if len(datasetid_list) == 0:
                    status_message = "None of the dataset id matches process name:"+str(process_name)
                    raise Exception(status_msg)
                else:
                    WorkflowLauncherUtility().launch_oozie_workflow(property_file_path,master_node_dns,cluster_id,datasetid_list)
            elif len(cluster_output_result) == 0:
                status_message = "None of the cluster is in WAITING state for process name:"+str(process_name)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            elif len(cluster_output_result) > 1:
                status_message = "More than one cluster is in WAITING state for process name:"+str(process_name)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)

            status_message = "Completed Workflow Launching Handler process"
            self.execution_context.set_context({"process_name": process_name})
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED, CommonConstants.ERROR_KEY: str(exception)}
            return result_dictionary


    # ############################################# Main ###############################################################
    # Purpose   : Handles the process of launching oozie workflows and returning the status
    #             and records (if any)
    # Input     : Process Name
    # Output    : Returns execution status and records (if any)
    # ##################################################################################################################

    def main(self, process_name):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for WorkflowLauncherUtility"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.execute_workflow(process_name)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for WorflowHandlerUtility"
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
            print str(exception)
            return result_dictionary

if __name__ == '__main__':
    process_name = sys.argv[1]
    worflow_handler = WorflowHandler()
    result_dict = worflow_handler.main(process_name)
    status_msg = "\nCompleted execution for Worflow Handler Utility with status " + json.dumps(result_dict) + "\n"
    sys.stdout.write(status_msg)