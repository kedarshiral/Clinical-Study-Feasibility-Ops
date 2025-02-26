""" Module to Terminate EMR Utility"""

__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information############################
#  Module Name         :   Terminate EMR Utility
#  Purpose             :   This Utility Will Perform EMR Termination For Provided Cluster Id
#  Input Parameters    :   EMR Cluster Id
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   3rd Jan 2018
#  Last changed by     :   Pushpendra Singh
#  Reason for change   :   EMR Termiantion Utility Development
# ##################################################################################################
# Library and external modules declaration
import json
import boto3
from LogSetup import logger
from ExecutionContext import ExecutionContext
from Terminate_EMR_Cluster import Terminate_EMR
import traceback

# all module level constants are defined here
MODULE_NAME = "EMRTerminationUtility"


class TerminateEmrUtility(object):
    """ Class to Terminate EMR Cluster"""

    # Default constructor
    def __init__(self, execution_context=None):
        self.execution_context = ExecutionContext()
        if execution_context is None:
            self.execution_context.set_context({"module_name": MODULE_NAME})
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Method to exit
        """
        pass

    def terminate_emr(self, emr_cluster_id, region_name):
        """
        Method to terminate cluster
        """
        logger.info("Terminating EMR For Id:" + emr_cluster_id, extra=self.execution_context.get_context())
        self.client = boto3.client("lambda", region_name=region_name)
        data = {}
        data['Name'] = emr_cluster_id
        terminate_emr = Terminate_EMR()

        logger.info("Calling Lambda Function to terminate the EMR", extra=self.execution_context.get_context())
        print(json.dumps(data))
        try:
#emr_terminate_lambda=self.client.invoke(FunctionName="lambda_terminate_emr",
# Payload=json.dumps(data))
            emr_terminate = terminate_emr.terminate_cluster(emr_cluster_id)
            logger.info("Cluster Termianted, Cluster Id:" + emr_cluster_id, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "EMR Termination Failed For Cluster Id: " + str(emr_cluster_id) + \
                             " due to" + str(exception) + str(traceback.format_exc())
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception
        return True