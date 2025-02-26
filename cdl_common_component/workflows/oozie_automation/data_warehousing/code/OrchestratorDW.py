#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   OrchestratorDW
#  Purpose             :   This module will orchestrate entire DW
#  Input Parameters    :   NA
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   18th January 2017
#  Last changed by     :   Sushant Choudhary
#  Reason for change   :   First version
# ######################################################################################################################

# Library and external modules declaration
import json
import pysftp
import traceback
import sys
import os
import argparse
sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
from WorkflowLauncherUtility import WorkflowLauncherUtility
from TerminateEmrHandler import TerminateEmrHandler
from TerminateEmrUtility import TerminateEmrUtility

# all module level constants are defined here
MODULE_NAME = "OrchestratorDW"
PROCESS_NAME = "OrchestratorDW"

USAGE_STRING = """
SYNOPSIS
    python OrchestratorDW.py process_name
"""


class OrchestratorDW:
    # Default constructor
    def __init__(self, process_name):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.process_name = process_name
        self.cluster_id= None
        self.master_node_dns=None
        self.configuration = JsonConfigUtility(CommonConstants.DW_CONFIG_FILE)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


    # ########################################### orchestrate_dw_workflow ####################################################
    # Purpose            : This method orchestrated entire DW flow based on input process name
    # Input              :  NA
    # Output             :   NA
    # ##################################################################################################################
    def orchestrate_dw_workflow(self):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        status_message = ""
        try:
            status_message = "Starting function to execute orchestration DW "
            logger.info(status_message, extra=self.execution_context.get_context())
            # Input Validations
            self.execution_context.set_context({"process_name": process_name})
            logger.info(status_message, extra=self.execution_context.get_context())
            # Checking whether there is already active cluster for process name
            cluster_output_resultset = self.fetchClusterStatus(process_name)
            if len(cluster_output_resultset) == 0:
                status_message = "Started executing EMR launcher utility"
                logger.info(status_message, extra=self.execution_context.get_context())
                emr_launch_command = "python " + CommonConstants.PYTHON_SCRIPT_LOCATION + "EmrClusterLaunchWrapper.py -p " + process_name + " -a start"
                command_execution_flag = CommonUtils().execute_shell_command(emr_launch_command)
                # Fetching cluster id and master node dns for active cluster
                cluster_output_result = self.fetchClusterStatus(process_name)
                if command_execution_flag and len(cluster_output_result) == 1:
                    cluster_id = None
                    try:
                        status_message = 'Completed executing EMR launcher utility'
                        logger.info(status_message, extra=self.execution_context.get_context())
                        self.cluster_id = str(cluster_output_result[0]['cluster_id'])
                        self.execution_context.set_context({"cluster_id": self.cluster_id})
                        self.master_node_dns = cluster_output_result[0]['master_node_dns']
                        status_message = "Starting function to read job properties template:" + CommonConstants.OOZIE_JOB_SOURCE_LOCATION
                        self.execution_context.set_context({"function_status": 'STARTED'})
                        logger.info(status_message, extra=self.execution_context.get_context())
                        property_file_name = CommonConstants.OOZIE_DW_JOB_PROPERTIES_FILE_NAME
                        with open(CommonConstants.OOZIE_JOB_SOURCE_LOCATION + "/" + property_file_name) as oozie_prop_file:
                            property_original_file_str = oozie_prop_file.read()
                        transfer_code_file_name = CommonConstants.TRANSFER_CODE_S3_TO_EMR_UTILITY
                        status_message = "Started executing transfer code from S3 to EMR utility"
                        logger.info(status_message, extra=self.execution_context.get_context())
                        self.execute_utility_remotely(self.master_node_dns, transfer_code_file_name)
                        status_message = "Completed executing transfer code from S3 to EMR utility"
                        logger.info(status_message, extra=self.execution_context.get_context())
                        process_name_dict = self.configuration.get_configuration([CommonConstants.DW_PARAMS_KEY, process_name])
                        database_name = CommonConstants.AUDIT_DB_NAME
                        configuration_table_name = database_name + "." + process_name_dict['configuration_table_name']
                        actuals_table_name = database_name + "." + process_name_dict['actuals_table_name']
                        load_type = process_name_dict['load_type']
                        self.execute_dw_process(configuration_table_name, actuals_table_name,load_type ,property_original_file_str)
                        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
                        return result_dictionary
                    except Exception as exception:
                        status_message="Starting to terminate EMR for cluster id:"+str(self.cluster_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        region_name = TerminateEmrHandler().get_region_for_cluster(self.cluster_id)
                        TerminateEmrUtility().terminate_emr(self.cluster_id, region_name)
                        CommonUtils().update_emr_termination_status(self.cluster_id)
                        status_message="Completed terminating EMR for cluster id:"+str(cluster_id)
                        error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
                        self.execution_context.set_context({"traceback": error})
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise exception
                else:
                    status_message = "Emr launcher utility failed"
                    raise Exception(status_message)
            else:
                status_message = "One of the Cluster is in WAITING state for "+self.process_name
                raise Exception(status_message)
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED, CommonConstants.ERROR_KEY: str(exception)}
            return result_dictionary


    # ########################################### populate actuals ####################################################
    # Purpose            : This method will create job properties based on calculated staging location and cycle id
    # Input              :  cycle id, configuration table name, actuals table name, load type
    # Output             :   NA
    # ##################################################################################################################
    def execute_dw_process(self, configuration_table_name, actuals_table_name,load_type, property_original_file_str):
        status_message = ""
        try:
            status_message = "Starting reading "+process_name+" files"
            logger.info(status_message, extra=self.execution_context.get_context())
            latest_cycle_id = self.fetch_latest_cycle()
            process_name_dict = self.configuration.get_configuration([CommonConstants.DW_PARAMS_KEY, process_name])
            dataset_list = process_name_dict['target_tables']
            status = CommonConstants.IN_PROGRESS_DESC
            self.update_cycle_run_status(latest_cycle_id, self.cluster_id, status)
            if process_name.startswith(CommonConstants.DIM_PREFIX):
                read_cycle_id = CommonConstants.NOT_AVAILABLE_SIGN
            else:
                read_cycle_id= self.fetch_read_cycle_id_dimension(actuals_table_name)
            for dw_target in dataset_list.keys():
                staging_paths = self.fetch_staging_location(load_type, dataset_list[dw_target], latest_cycle_id,
                                                        configuration_table_name, actuals_table_name)
                replacable_properties_dic = {
                    '$$namenode_hostname': self.master_node_dns,
                    '$$jobtracker_hostname': self.master_node_dns,
                    '$$read_cycle_id': read_cycle_id ,
                    '$$write_cycle_id': latest_cycle_id,
                    '$$staging_paths': staging_paths,
                    '$$target_table': dw_target,
                    '$$cluster_id': self.cluster_id,
                    '$$process_name': process_name
            }
                modified_file_str = WorkflowLauncherUtility().replace_values(property_original_file_str,
                                                                         replacable_properties_dic)
                modified_property_filename = dw_target + CommonConstants.MODIFIED_OOZIE_PROPERTY_FILE_NAME
                oozie_property_absolute_path = CommonConstants.OOZIE_JOB_SOURCE_LOCATION + "/" + modified_property_filename
                with open(oozie_property_absolute_path, "w") as modified_oozie_prop_file:
                    modified_oozie_prop_file.write(modified_file_str)
                WorkflowLauncherUtility().launch_oozie_workflow(modified_property_filename, self.master_node_dns, self.cluster_id,
                                                            dw_target)
            status_message = "Completed reading "+process_name+" files"
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_read_cycle_id_dimension(self,actuals_table_name):
        status_message = ""
        try:
            status_message= "Started preparing query to fetch read cycle id for dimension"
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_read_cycle_id_query = "select max(dimension_cycle_id) as dimension_cycle_id from " + actuals_table_name + " where source_type ='"+CommonConstants.DIMENSION_SOURCE_TYPE+"'"
            status_message = "Completed preparing query to fetch read cycle id for dimension"
            logger.debug(status_message, extra=self.execution_context.get_context())
            read_cycle_id_resultset = MySQLConnectionManager().execute_query_mysql(fetch_read_cycle_id_query, False)
            read_cycle_id = str(read_cycle_id_resultset[0]['dimension_cycle_id'])
            status_message = "Completed executing query to fetch read cycle id for dimension"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return read_cycle_id
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


    # ########################################### fetch_staging_location ####################################################
    # Purpose            : This method returns list of staging location for input dataset list
    # Input              :  load type , dataset list , cycle id , configuration tale , actuals table
    # Output             :   NA
    # ##################################################################################################################
    def fetch_staging_location(self,load_type,target_dataset_list,latest_cycle_id,configuration_table_name,actuals_table_name):
        status_message = ""
        try:
            status_message = "Started function to read staging location"
            logger.info(status_message, extra=self.execution_context.get_context())
            staging_path_list = []
            for source_master_id in target_dataset_list:
                if load_type == CommonConstants.FULL_LOAD_TYPE:
                    fetch_staging_path_query = 'select concat(source_stg_location,"/'+CommonConstants.BATCH_ID+'=",batch_id) as staging_path from '+configuration_table_name+' INNER JOIN '+actuals_table_name+' on '+configuration_table_name+'.source_file_master_id='+actuals_table_name+'.source_file_master_id where cycle_id=' + latest_cycle_id + ' and '+configuration_table_name+'.source_file_master_id="' + str(
                        source_master_id) + '"'
                elif load_type == CommonConstants.INCREMENTAL_LOAD_TYPE:
                    fetch_staging_path_query = 'select source_stg_location as staging_path from ' + configuration_table_name + ' INNER JOIN ' + actuals_table_name + ' on ' + configuration_table_name + '.source_file_master_id=' + actuals_table_name + '.source_file_master_id where cycle_id=' + latest_cycle_id + ' and ' + configuration_table_name + '.source_file_master_id="' + str(
                        source_master_id) + '"'
                staging_path_result = MySQLConnectionManager().execute_query_mysql(fetch_staging_path_query, False)
                staging_path = staging_path_result[0]['staging_path']
                if  staging_path.endswith(CommonConstants.NOT_APPLICABLE_SIGN):
                    status_message = "None of batch is successful for "+str(source_master_id)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    continue
                else:
                    staging_path_list.append(staging_path)
            staging_paths = ",".join(staging_path_list)
            status_message = "Completed function to read staging location"
            logger.info(status_message, extra=self.execution_context.get_context())
            return staging_paths
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    # ########################################### fetch_latest_cycle ####################################################
    # Purpose            : This method returns latest cycle id from cycle run table
    # Input              :   NA
    # Output             :   cycle id
    # ##################################################################################################################
    def fetch_latest_cycle(self):
        status_message = ""
        try:
            status_message = "Started preparing query to fetch max cycle id"
            logger.info(status_message, extra=self.execution_context.get_context())
            fetch_max_cycle_id_query = "Select max(cycle_id) AS cycle_id from "+CommonConstants.AUDIT_DB_NAME+"."+CommonConstants.CYCLE_RUN+" where process_name='"+process_name+"' and status='"+CommonConstants.NEW_STATE+"'"
            status_message = "Completed preparing query to fetch max cycle id"
            logger.info(status_message, extra=self.execution_context.get_context())
            max_cycle_id = MySQLConnectionManager().execute_query_mysql(fetch_max_cycle_id_query, False)
            latest_cycle_id = max_cycle_id[0]['cycle_id']
            if latest_cycle_id is None:
                print ("inside cycle id none")
                status_message = "No cycle id is in "+CommonConstants.NEW_STATE+" state for process_name:"+process_name
                raise Exception(status_message)
            status_message = "Completed query execution to fetch max cycle id"
            logger.info(status_message, extra=self.execution_context.get_context())
            return str(latest_cycle_id)
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception

    # ########################################### update_cycle_run_status ####################################################
    # Purpose            : This method will update cycle run status and cluster id for input cycle id
    # Input              :  cycle id, cluster id, status
    # Output             :   NA
    # ##################################################################################################################
    def update_cycle_run_status(self, cycle_id, cluster_id, status):
        status_message = ""
        try:
            status_message = "Starting function to update cycle status for cluster id"+cluster_id
            logger.debug(status_message, extra=self.execution_context.get_context())
            query_string = "UPDATE {automation_db_name}.{cycle_run_table} set status = '{cycle_status}',cluster_id= '{cluster_id}' " \
                           "where cycle_id ={cycle_id} and process_name = '{process_name}'"
            query = query_string.format(automation_db_name = CommonConstants.AUDIT_DB_NAME,
                                        cycle_run_table= CommonConstants.CYCLE_RUN,
                                        cycle_status=status, cluster_id=cluster_id, cycle_id=cycle_id,process_name = process_name)
            status_message = "Input query for updating cycle table entry : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update cycle entry"
            logger.info(status_message, extra=self.execution_context.get_context())
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

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
            command_output = sftp.execute(command)

    # ############################################# Main ###############################################################
    # Purpose   : Handles the process of Orchestrating DW process and returning the status
    #             and records (if any)
    # Input     : Process Name
    # Output    : Returns execution status and records (if any)
    # ##################################################################################################################


    def main(self):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for OrchestratorDW"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.orchestrate_dw_workflow()
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for OrchestratorDW utility"
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
    parser.add_argument("process_name", help="Process name for which you want to run DW Orchestration")
    args = parser.parse_args()
    process_name = args.process_name
    orchestrate_dw = OrchestratorDW(process_name)
    result_dict = orchestrate_dw.main()
    status_msg = "\nCompleted execution for OrchestratorDW utility with status " + json.dumps(result_dict) + "\n"
    sys.stdout.write(status_msg)
