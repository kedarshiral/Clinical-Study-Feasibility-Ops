import boto3
import traceback
from ExecutionContext import ExecutionContext
from LogSetup import logger

GLOBAL_SECTION_NAME = "GlobalParameters"
MODULE_NAME = "Get_EMR_Metadata"

"""Class for fetching EMR Metadata based  on cluster ID"""

class Get_EMR_Metadata(object):

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def get_emr_cluster_state(self, jobid, region_name):
        """Module to fetch the EMR Cluster Status
        @:param
        """
        try:
            client = boto3.client("emr", region_name=region_name)
            res = client.describe_cluster(ClusterId=jobid)
            ready_date_time = str(
                res['Cluster']['Status']['Timeline']['ReadyDateTime']).split(".")[0]
            creation_date_time = str(
                res['Cluster']['Status']['Timeline']['CreationDateTime']).split(".")[0]
            cluster_details = {"Status": res['Cluster']['Status']['State'],
                               "DNS": res['Cluster']['MasterPublicDnsName'],
                               "ReadyDateTime": ready_date_time, "CreationDateTime":
                                   creation_date_time}
            return cluster_details
        except Exception as error:
            status_message = "Error in fetching Cluster Status"
            logger.error(status_message + str(traceback.format_exc()), extra=self.execution_context.get_context())
            raise error
