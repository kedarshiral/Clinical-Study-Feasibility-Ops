"""This module terminates launched cluster."""
import logging
from LogSetup import logger
import boto3
import traceback


MODULE_NAME = "Terminate_EMR_Cluster"

LOGGER = logging.getLogger('__name__')
HNDLR = logging.StreamHandler()
FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
HNDLR.setFormatter(FORMATTER)


class Terminate_EMR:
    """Class that terminates the launched cluster"""
    def __init__(self, parent_execution_context=None):
        pass

    def terminate_cluster(self, cluster_id, region_name):
        """Method that terminates the launched cluster"""
        try:

            client = boto3.client(
                "emr",
                region_name=region_name
            )
            client.set_termination_protection(JobFlowIds=[cluster_id], TerminationProtected=False)
            response = client.terminate_job_flows(JobFlowIds=[cluster_id])
            LOGGER.info("Cluster Terminated %s", str(response))
        except Exception as error:
            LOGGER.error("Failed to Terminate Cluster %s Error: %s", str(cluster_id), str(error))
            raise error
