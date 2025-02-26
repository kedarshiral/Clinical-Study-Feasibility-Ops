#!/usr/bin/python
# -*- coding: utf-8 -*-
AUTHOR__ = 'ZS Associates'


# ####################################################Module Information################################################
#  Module Name         :   WorkflowLauncherUtility
#  Purpose             :   This module will creation job xml and launch oozie workflow
#  Input Parameters    :   file_path,master_node_dns,cluster_id,datased_list
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   7th Jan 2017
#  Last changed by     :   Sushant Choudhary
#  Reason for change   :   Workflow Launcher Utility development
# ######################################################################################################################

# Library and external modules declaration
import pysftp
import sys
import os
sys.path.insert(0, os.getcwd())
import traceback
from ExecutionContext import ExecutionContext
from LogSetup import logger
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from CommonUtils import CommonUtils
# all module level constants are defined here
MODULE_NAME = "WorkflowLauncherUtility"


class WorkflowLauncherUtility:
    # Default constructor
    def __init__(self, execution_context=None):
        self.execution_context = ExecutionContext()
        if execution_context is None:
            self.execution_context.set_context({"module_name": MODULE_NAME})
        else:
            self.execution_context = execution_context

        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.env_configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.env_configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.pem_file_location = self.env_configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "pem_file_location"])
        self.oozie_job_source_location = self.env_configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "oozie_job_source_location"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass



    # ########################################### launch worflow ####################################################
    # Purpose            :   Creates job properties and launch workflows
    # Input              :   file_path,master_node_dns,clusterid,dataset list
    # Output             :   NA
    #
    #
    #
    # ##################################################################################################################
    def launch_oozie_workflow(self, job_property_file_path=None,master_node_dns=None,cluster_id=None,dataset_list=None):
        status_message = ""
        try:
            status_message = "Starting function to read job properties template:"+job_property_file_path
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.info(status_message, extra=self.execution_context.get_context())
            file_name=CommonConstants.OOZIE_JOB_PROPERTIES_FILE_NAME
            with open(job_property_file_path+"/"+file_name) as oozie_prop_file:
                original_file_str = oozie_prop_file.read()
            for dataset in dataset_list:
                dataset_id=str(dataset['dataset_id'])
                replacable_properties_dic = {
                '$$namenode_hostname': master_node_dns,
                '$$jobtracker_hostname': master_node_dns,
                '$$cluster_id': cluster_id,
                '$$dataset_id': dataset_id
                       }
                modified_file_str = self.replace_values(original_file_str, replacable_properties_dic)
                #modified_filename=dataset_id+"_"+file_name
                modified_filename=dataset_id+CommonConstants.MODIFIED_OOZIE_PROPERTY_FILE_NAME
                oozie_property_absolute_path=job_property_file_path+"/"+modified_filename
                with open(oozie_property_absolute_path, "w") as modified_oozie_prop_file:
                    modified_oozie_prop_file.write(modified_file_str)
                # Need to specify oozie server url for static cluster
                oozie_command = 'sudo -u '+CommonConstants.OOZIE_EXECUTION_USER+' oozie job http://'+master_node_dns+':'+CommonConstants.OOZIE_SERVER_PORT+'/oozie -config ' + CommonConstants.OOZIE_JOB_EXECUTION_LOCATION + '' + modified_filename + ' -run'
                #Creating sftp connectiion for remote execution on EMR
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None   # disable host key checking
                with pysftp.Connection(master_node_dns, username=CommonConstants.EMR_USER_NAME, private_key=self.pem_file_location,cnopts=cnopts) as sftp:
                    with sftp.cd(CommonConstants.OOZIE_JOB_EXECUTION_LOCATION):
                        sftp.put(self.oozie_job_source_location+''+modified_filename)
                    status_message="Executed oozie command:"+oozie_command
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    #execute oozie command
                    command_output=sftp.execute(oozie_command)
                    if "oozie-oozi-W" in command_output[0]:
                        status_message="Generated oozie workflow id is:"+str(command_output[0])
                        logger.info(status_message, extra=self.execution_context.get_context())
                    else:
                        status_message="Oozie workflow creation failed for:"+str(dataset)
                        logger.info(status_message, extra=self.execution_context.get_context())
                #CommonUtils.execute_shell_command(oozie_command)   #command to execute on static cluster
                status_message = 'Executed sftp command to launch oozie workflow for dataset id:'+dataset_id
                logger.info(status_message, extra=self.execution_context.get_context())
            return True
        except Exception as exception:
            print(exception)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc()+str(exception))
            self.execution_context.set_context({"function_status": 'FAILED', "traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            #raise exception

    # ########################################### replace_values ####################################################
    # Purpose            :   Replace job.properties value with respective dataset id and master host
    # Input              :   original value, replaced value
    # Output             :   NA
    #
    #
    #
    # ##################################################################################################################
    def replace_values(self,original_value, replaced_value):
        for key, val in replaced_value.items():
            original_value = original_value.replace(key, val)
        return original_value