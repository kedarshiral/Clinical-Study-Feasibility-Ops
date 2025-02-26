"""This module will call terminate emr cluster utility on required condition"""
#!/usr/bin/python
# -*- coding: utf-8 -*-
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
import os
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
from Terminate_EMR_Cluster import Terminate_EMR
from CommonUtils import CommonUtils
from ConfigUtility import JsonConfigUtility

# all module level constants are defined here
MODULE_NAME = "Terminate EMR"
PROCESS_NAME = "Check DB For EMR Status"


class TerminateEmrHandler(object):
    """This class will call terminate emr cluster utility on required condition"""
    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                         CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

        # ########################################### check_emr_status ###########################
        # Purpose            :   Executing the EMR status check
        # Input              :   emr_cluster_id
        # Output             :   NA
        # #########################################################################################

    def check_emr_status_for_id(self, emr_cluster_id):
        """This function checks for emr status"""
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        try:
            status_message = "In Check EMR Status for EMR ID: " + emr_cluster_id
            logger.info(status_message, extra=self.execution_context.get_context())

            query_string = "select cluster_id from {audit_db_name}.{log_cluster_dtl} " \
                           "where cluster_status !='TERMINATED' and cluster_id='{cluster_id}'"
            execute_query = query_string.format(audit_db_name=self.audit_db,
                                                log_cluster_dtl=
                                                CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                                                cluster_id=str(emr_cluster_id))
            running_emr = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            # To check Running EMRs
            if len(running_emr) == 0:
                status_message = "No EMR Running"
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                region_name = self.get_region_for_cluster(emr_cluster_id)
                terminate_emr_cluster = Terminate_EMR()
                terminate_emr_cluster.terminate_cluster(emr_cluster_id, region_name)
                CommonUtils().update_emr_termination_status(emr_cluster_id, self.audit_db)

        except Exception as exception:
            status_message = "EMR Termination Failed For Cluste Id: " + str(emr_cluster_id) + \
                             " due to" + str(exception) + str(traceback.format_exc())
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

        status_message = "Emr check Completed For Cluster Id: " + str(emr_cluster_id)
        logger.info(status_message, extra=self.execution_context.get_context())
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        return result_dictionary

        # ########################################### check_emr_status ###########################
        # Purpose            :   Executing the EMR Terminate check
        # Input              :   NA
        # Output             :   NA
        # #########################################################################################

    def check_emr_status(self):
        """ this method is executing the EMR Terminate check"""
        try:
            status_message = "In Check EMR Status For All Running Instances"
            logger.info(status_message, extra=self.execution_context.get_context())
            query_string = "select cluster_id from {audit_db_name}.{log_cluster_dtl} " \
                           "where cluster_status ='WAITING'"
            execute_query = query_string.format(audit_db_name=self.audit_db,
                                                log_cluster_dtl=
                                                CommonConstants.EMR_CLUSTER_DETAILS_TABLE)
            running_emr = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            # To check Running EMRs
            if len(running_emr) == 0:
                status_message = "No EMRs Running"
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                for running_emr in running_emr:
                    emr_cluster_id = running_emr['cluster_id']
                    region_name = self.get_region_for_cluster(emr_cluster_id)
                    terminate_emr_cluster = Terminate_EMR()
                    terminate_emr_cluster.terminate_cluster(emr_cluster_id, region_name)
                    CommonUtils().update_emr_termination_status(emr_cluster_id)


        except Exception as exception:
            status_message = "EMR Termination Failed For All Running Instances Due To " + \
                             str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

        status_message = "Emr check Completed For All Running Insatances" + str(traceback.format_exc())
        logger.info(status_message, extra=self.execution_context.get_context())
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        return result_dictionary

    def check_file_status(self, emr_cluster_id):
        """This method checks status of file"""
        try:
            status_message = "In File Status Check For EMR Id: " + emr_cluster_id
            logger.info(status_message, extra=self.execution_context.get_context())
            query_string = "select * from {audit_db_name}.{file_audit_table} " \
                           "where cluster_id='{cluster_id}'"
            execute_query = query_string.format(audit_db_name=self.audit_db,
                                                file_audit_table=CommonConstants.FILE_AUDIT_TABLE,
                                                cluster_id=str(emr_cluster_id))
            file_status = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            progress_count = 0
            no_file = False
            if len(file_status) == 0:
                no_file = True
                status_message = "No Files In Audit Table For EMR ID: " + emr_cluster_id
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                for file in file_status:
                    if file['file_status'] == CommonConstants.IN_PROGRESS_DESC:
                        progress_count = progress_count + 1

            if progress_count > 0:
                return False
            elif (progress_count == 0 and no_file == True):
                return False
            else:
                return True

        except Exception as exception:
            status_message = "Check File Status In EMR TErmination Failed For Cluster Id: " + str(
                emr_cluster_id) + " due to" + str(exception) + str(traceback.format_exc())
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_region_for_cluster(self, emr_cluster_id):
        """This method gets region for cluster"""
        region = None
        try:
            status_message = "In Get Region Name For EMR Id: " + emr_cluster_id
            logger.info(status_message, extra=self.execution_context.get_context())
            query_string = "select process_id from {audit_db_name}.{log_cluster_dtl} " \
                           "where cluster_id='{cluster_id}'"
            execute_query = query_string.format(audit_db_name=self.audit_db,
                                                log_cluster_dtl=
                                                CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                                                cluster_id=str(emr_cluster_id))
            process_result = MySQLConnectionManager().execute_query_mysql(execute_query, True)
            process_id = process_result['process_id']
            if process_id:
                query_string = "select region_name from {audit_db_name}.{ctl_cluster_config} " \
                               "where process_id={process_id}"
                execute_query = query_string.format(audit_db_name=self.audit_db,
                                                    ctl_cluster_config=
                                                    CommonConstants.EMR_CLUSTER_CONFIGURATION_TABLE,
                                                    process_id=str(process_id))
                region_result = MySQLConnectionManager().execute_query_mysql(execute_query, True)
                region = region_result['region_name']
            return region

        except Exception as exception:
            status_message = "Fetching Region Id For Cluster Id: " + str(emr_cluster_id) +\
                             " due to" + str(exception) + str(traceback.format_exc())
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    # ############################################# Main ######################################
    # Purpose   : Handles the process of terminating EMR cluster
    # Input     : Requires dataset_id,batch id
    # Output    : Returns execution status and records (if any)
    # ##########################################################################################

    def main(self, cluster_id=None):
        """This method handles the process of terminating EMR cluster"""
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for Terminate EMR Handler"
            logger.info(status_message, extra=self.execution_context.get_context())
            if cluster_id is None:
                raise Exception("Cluster id is not provided")
            result_dictionary = self.check_emr_status_for_id(cluster_id)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for TerminateEMRHandler"
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
    cluster_id = sys.argv[1]
    TERMINATE_EMR = TerminateEmrHandler()
    RESULT_DICT = TERMINATE_EMR.main(cluster_id)
    STATUS_MSG = "\nCompleted execution for EMR Termination Utility with status "+ \
                 json.dumps(RESULT_DICT) + "\n"
    sys.stdout.write(STATUS_MSG)