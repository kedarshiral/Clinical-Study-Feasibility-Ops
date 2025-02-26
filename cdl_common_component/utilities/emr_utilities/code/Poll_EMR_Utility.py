"""Module to get emr cluster state."""

import boto3
import traceback
import logging
from LogSetup import logger



GLOBAL_SECTION_NAME = "GlobalParameters"
class Emr_State(object):
    """Class to assign emr cluster state and return it."""
    def get_emr_cluster_state(self, jobid, region_name):
        """Method to assign and return cluster state."""
        try:

            client = boto3.client(
                "emr",
                region_name=region_name
            )
            state = client.describe_cluster(ClusterId=jobid)['Cluster']['Status']['State']
            return str(state)
        except Exception as error:
            raise error
