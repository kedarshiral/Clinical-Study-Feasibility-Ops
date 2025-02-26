#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information###########################
#  Module Name         :   TerminateEmrHandler
#  Purpose             :   This module will call terminate emr cluster utility on required condition
#  Input Parameters    :   N/A
#  Output Value        :   N/A
#  Pre-requisites      :
#  Last changed on     :   3rd  Jan 2018
#  Last changed by     :   Pushpendra Singh
#  Reason for change   :   Terminating EMR after all files are processed
# ##################################################################################################

# Library and external modules declaration
import json
import traceback
import sys
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
from TerminateEmrUtility import TerminateEmrUtility
from CommonUtils import CommonUtils
# all module level constants are defined here
MODULE_NAME = "Terminate EMR"
PROCESS_NAME = "Check DB For EMR Status"

class TerminateEmrHandler(object):
    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

        # ########################################### check_emr_status ###########################
        # Purpose            :   Executing the DQM check
        # Input              :   emr_cluster_id
        # Output             :   NA
        # #########################################################################################
    def check_emr_status_for_id(self,emr_cluster_id):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        try:
            status_message = "In Check EMR Status for EMR ID: "+emr_cluster_id
            logger.info(status_message, extra=self.execution_context.get_context())

            query_string = "select cluster_id from {automation_db_name}.emr_cluster_details where cluster_status ='WAITING' and cluster_id='{cluster_id}'"
            execute_query = query_string.format(automation_db_name=CommonConstants.AUDIT_DB_NAME,cluster_id=str(emr_cluster_id))
            running_emr = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            # To check Running EMRs
            if len(running_emr) == 0:
                status_message = "No EMR Running"
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                if(self.check_file_status(emr_cluster_id) == True):
                    region_name = self.get_region_for_cluster(emr_cluster_id)
                    TerminateEmrUtility().terminate_emr(emr_cluster_id, region_name)
                    CommonUtils().update_emr_termination_status(emr_cluster_id)

        except Exception as exception:
            status_message = "EMR Termination Failed For Cluste Id: "+str(emr_cluster_id) + " due to" + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

        status_message = "Emr check Completed For Cluster Id: " + str(emr_cluster_id)
        logger.info(status_message, extra=self.execution_context.get_context())
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        return result_dictionary

        # ########################################### check_emr_status ###########################
        # Purpose            :   Executing the DQM check
        # Input              :   NA
        # Output             :   NA
        # #########################################################################################
    def check_emr_status(self):
        try:
            status_message = "In Chekc EMR Status For All Running Instances"
            logger.info(status_message,extra=self.execution_context.get_context())
            query_string = "select cluster_id from {automation_db_name}.emr_cluster_details where cluster_status ='WAITING'"
            execute_query = query_string.format(automation_db_name=CommonConstants.AUDIT_DB_NAME)
            print(execute_query)
            running_emr = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            print(running_emr)
            # To check Running EMRs
            if len(running_emr) == 0:
                status_message = "No EMRs Running"
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                for running_emr in running_emr:
                    print(running_emr)
                    if(self.check_file_status(running_emr['cluster_id']) == True):
                        emr_cluster_id = running_emr['cluster_id']
                        region_name = self.get_region_for_cluster(emr_cluster_id)
                        print(region_name)
                        print("terminating emr")
                        TerminateEmrUtility().terminate_emr(emr_cluster_id, region_name)
                        CommonUtils().update_emr_termination_status(emr_cluster_id)


        except Exception as exception:
            status_message = "EMR Termination Failed For All Running Instances Due To " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

        status_message = "Emr check Completed For All Running Insatances"
        logger.info(status_message, extra=self.execution_context.get_context())
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        return result_dictionary


    def check_file_status(self,emr_cluster_id):
        try:
            status_message ="In File Status Check For EMR Id: "+emr_cluster_id
            logger.info(status_message,extra=self.execution_context.get_context())
            query_string = "select * from {automation_db_name}.{file_audit_table} where cluster_id='{cluster_id}'"
            execute_query = query_string.format(automation_db_name=CommonConstants.AUDIT_DB_NAME,file_audit_table=CommonConstants.FILE_AUDIT_TABLE,cluster_id=str(emr_cluster_id))
            print(execute_query)
            file_status = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            print(file_status)

            progress_count = 0
            no_file = False
            if len(file_status) == 0:
                no_file = True
                print("No File")
                status_message = "No Files In Audit Table For EMR ID: " + emr_cluster_id
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                for file in file_status:
                    print(file['file_process_status_description'])
                    if file['file_process_status_description'] == CommonConstants.IN_PROGRESS_DESC:
                        progress_count = progress_count + 1

            print(progress_count)

            if progress_count > 0:
                return False
            elif(progress_count == 0 and no_file == True):
                return False
            else:
                return True

        except Exception as exception:
            status_message = "Check File Status In EMR TErmination Failed For Cluster Id: " + str(emr_cluster_id) + " due to" + str(
                exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_region_for_cluster(self,emr_cluster_id):
        try:
            status_message ="In Get Region Name For EMR Id: "+emr_cluster_id
            print(status_message)
            logger.info(status_message,extra=self.execution_context.get_context())
            query_string = "select region_name from {automation_db_name}.emr_cluster_details where cluster_id='{cluster_id}'"
            execute_query = query_string.format(automation_db_name=CommonConstants.AUDIT_DB_NAME,cluster_id=str(emr_cluster_id))
            print(execute_query)
            region_name_result = MySQLConnectionManager().execute_query_mysql(execute_query, True)
            print(str(region_name_result['region_name']))
            return str(region_name_result['region_name'])

        except Exception as exception:
            status_message = "Fetching Region Id For Cluster Id: " + str(emr_cluster_id) + " due to" + str(
                exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    # ############################################# Main ######################################
    # Purpose   : Handles the process of executing DQM queries and returning the status
    #             and records (if any)
    # Input     : Requires dataset_id,batch id
    # Output    : Returns execution status and records (if any)
    # ##########################################################################################

    def main(self):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for Terminate EMR Handler"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.check_emr_status()
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for DQMCheckHandlerUtility"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            print(str(exception))
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return result_dictionary


if __name__ == '__main__':
    Terminate_EMR = TerminateEmrHandler()
    RESULT_DICT = Terminate_EMR.main()
    STATUS_MSG = "\nCompleted execution for EMR Termination Utility with status " + json.dumps(RESULT_DICT) + "\n"
    sys.stdout.write(STATUS_MSG)