#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Workflow Handler
#  Purpose             :   This module will call emr launcher, hdfs folder creation , code copy from s3 to emr and
#                          WorkflowLauncherUtility.py in sequence.
#  Input Parameters    :   process name
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   14th January 2017
#  Last changed by     :   Sushant Choudhary
#  Reason for change   :   Fix Orchestration issues
# ######################################################################################################################

# Library and external modules declaration
import pysftp
import argparse
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
from TerminateEmrHandler import TerminateEmrHandler
from TerminateEmrUtility import TerminateEmrUtility

# all module level constants are defined here
MODULE_NAME = "OrchestrationUtility"
PROCESS_NAME = "Orchestration"

USAGE_STRING = """
SYNOPSIS
    python OrchestrationUtility.py process_name

    Where
        input parameters : process_name

"""


class OrchestratorUtility:
    # Default constructor
    def __init__(self,process_name):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.process_name=process_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


    # ########################################### execute entire flow ####################################################
    # Purpose            :   This method will call workflow utility for datasets matching process name
    # Input              :   process name
    # Output             :   NA
    # ##################################################################################################################
    def execute_orchestration_flow(self):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        status_message = ""
        try:
            status_message = "Starting function to execute orchestration"
            logger.info(status_message, extra=self.execution_context.get_context())
            # Input Validations
            process_name=self.process_name
            self.execution_context.set_context({"process_name": process_name})
            logger.info(status_message, extra=self.execution_context.get_context())
            #Checking whether there is already active cluster for process name
            cluster_output_resultset=self.fetchClusterStatus(process_name)
            if len(cluster_output_resultset) == 0:
                fetch_dataset_query="Select "+CommonConstants.MYSQL_DATASET_ID+" from "+CommonConstants.AUDIT_DB_NAME+"."+CommonConstants.EMR_PROCESS_WORKKFLOW_MAP_TABLE+" where "+CommonConstants.EMR_PROCESS_WORKFLOW_COLUMN+"='"+str(process_name)+"' and "+CommonConstants.EMR_PROCESS_WORKFLOW_ACTIVE_COLUMN+"='"+str(CommonConstants.WORKFLOW_DATASET_ACTIVE_VALUE)+"'"
                datasetid_list = MySQLConnectionManager().execute_query_mysql(fetch_dataset_query)
                if len(datasetid_list) == 0:
                    status_message = "None of the dataset id matches process name:"+str(process_name)
                    raise Exception(status_message)
                else:
                    status_message="Started executing EMR launcher utility"
                    logger.info(status_message, extra=self.execution_context.get_context())
                    emr_launch_command="python "+CommonConstants.PYTHON_SCRIPT_LOCATION+"EmrClusterLaunchWrapper.py -p "+process_name+" -a start"
                    command_execution_flag=CommonUtils().execute_shell_command(emr_launch_command)
                    #Fetching cluster id and master node dns for active cluster
                    cluster_output_result=self.fetchClusterStatus(process_name)
                    if command_execution_flag and len(cluster_output_result) == 1:
                        cluster_id=None
                        try:
                            status_message = 'Completed executing EMR launcher utility'
                            logger.info(status_message, extra=self.execution_context.get_context())
                            cluster_id=str(cluster_output_result[0]['cluster_id'])
                            master_node_dns=cluster_output_result[0]['master_node_dns']
                            property_file_path=CommonConstants.OOZIE_JOB_SOURCE_LOCATION
                            folder_creation_file_name=CommonConstants.HDFS_FOLDER_CREATION_UTILITY
                            status_message="Started executing hdfs folder creation utility"
                            logger.info(status_message, extra=self.execution_context.get_context())
                            self.execute_utility_remotely(master_node_dns,folder_creation_file_name)
                            status_message="Completed executing hdfs folder creation utility"
                            logger.info(status_message, extra=self.execution_context.get_context())
                            transfer_code_file_name=CommonConstants.TRANSFER_CODE_S3_TO_EMR_UTILITY
                            status_message="Started executing transfer code from S3 to EMR utility"
                            logger.info(status_message, extra=self.execution_context.get_context())
                            self.execute_utility_remotely(master_node_dns,transfer_code_file_name)
                            status_message="Completed executing transfer code from S3 to EMR utility"
                            logger.info(status_message, extra=self.execution_context.get_context())
                            #WorkflowLauncherUtility().launch_oozie_workflow(property_file_path,master_node_dns,cluster_id,datasetid_list)
                        except Exception as exception:
                            status_message="Starting to terminate EMR for cluster id:"+str(cluster_id)
                            logger.info(status_message, extra=self.execution_context.get_context())
                            region_name = TerminateEmrHandler().get_region_for_cluster(cluster_id)
                            TerminateEmrUtility().terminate_emr(cluster_id, region_name)
                            CommonUtils().update_emr_termination_status(cluster_id)
                            status_message="Completed terminating EMR for cluster id:"+str(cluster_id)
                            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
                            self.execution_context.set_context({"traceback": error})
                            logger.error(status_message, extra=self.execution_context.get_context())
                            #result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED, CommonConstants.ERROR_KEY: str(exception)}
                            #return result_dictionary
                            raise exception
                        WorkflowLauncherUtility().launch_oozie_workflow(property_file_path,master_node_dns,cluster_id,datasetid_list)
                    else:
                        status_message="Emr launcher utility failed"
                        raise Exception(status_message)
            elif len(cluster_output_resultset) == 1:
                status_message = "One of the cluster is already in WAITING state for process name:"+str(process_name)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            elif len(cluster_output_resultset) > 1:
                status_message = "More than one cluster is in WAITING state for process name:"+str(process_name)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            status_message = "Completed Orchestration for process:"+process_name
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

    def fetchClusterStatus(self,process_name):
        status_message="Started preparing query to fetch cluster status"
        logger.debug(status_message, extra=self.execution_context.get_context())
        cluster_check_query = "SELECT " + CommonConstants.EMR_CLUSTER_ID_COLUMN + "," + CommonConstants.EMR_MASTER_DNS + " from " + CommonConstants.AUDIT_DB_NAME + "." + CommonConstants.EMR_CLUSTER_DETAILS_TABLE + " where " + CommonConstants.EMR_CLUSTER_PROCESS_COLUMN + "='" + str(process_name) + "' and " + CommonConstants.EMR_CLUSTER_STATUS_COLUMN + "='" + str(
        CommonConstants.CLUSTER_ACTIVE_STATE) + "'"
        cluster_output_result = MySQLConnectionManager().execute_query_mysql(cluster_check_query, False)
        status_message="Executed query to fetch cluster status"
        logger.debug(status_message, extra=self.execution_context.get_context())
        return cluster_output_result

    def execute_utility_remotely(self,master_node_dns,file_name):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        #command='python '+file_name
        command='python '+CommonConstants.OOZIE_JOB_EXECUTION_LOCATION+file_name
        with pysftp.Connection(master_node_dns, username=CommonConstants.EMR_USER_NAME,
                               private_key=CommonConstants.PRIVATE_KEY_LOCATION, cnopts=cnopts) as sftp:
            with sftp.cd(CommonConstants.OOZIE_JOB_EXECUTION_LOCATION):
                sftp.put(CommonConstants.PYTHON_SCRIPT_LOCATION+file_name)
            # execute command
            sftp.execute(command)
            command_execution_status=sftp.execute("echo $?")
            print(command_execution_status)
            print(type(command_execution_status))


    # ############################################# Main ###############################################################
    # Purpose   : Handles the process of execute entire orchestration and returning the status
    #             and records (if any)
    # Input     : Process Name
    # Output    : Returns execution status and records (if any)
    # ##################################################################################################################

    def main(self):
        result_dictionary = None
        status_message = ""
        try:
            #process_name=self.process_name
            status_message = "Starting the main function for OrchestratorUtility"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.execute_orchestration_flow()
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function  OrchestratorUtility"
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
    parser = argparse.ArgumentParser()
    parser.add_argument("process_name", help="Process name for which you want to run Orchestration")
    args = parser.parse_args()
    process_name = args.process_name
    orchestartion_util = OrchestratorUtility(process_name)
    result_dict = orchestartion_util.main()
    status_msg = "\nCompleted execution for Orchestrator Utility with status " + json.dumps(result_dict) + "\n"
    sys.stdout.write(status_msg)