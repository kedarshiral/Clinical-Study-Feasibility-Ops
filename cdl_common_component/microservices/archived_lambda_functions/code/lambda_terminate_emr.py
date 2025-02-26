# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import boto3
from ConfigUtility import ConfigUtility


ENVIRONMENT_CONFIG_FILE = "setting.conf"
GLOBAL_SECTION_NAME = "GlobalParameters"
MODULE_NAME = "lambda_terminate_emr"


class Terminate_EMR(object):
    configuration = ConfigUtility(ENVIRONMENT_CONFIG_FILE)
    
    def __init__(self,cluster_id):
        
        self.aws_access_key = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "aws_access_key")
        self.aws_secret_key = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "aws_secret_key")
        self.region_name = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "region_name")
        self.cluster_id =  cluster_id
        
    
    def terminate_cluster(self):
        try:
         
                client = boto3.client(
                    "emr",
                     aws_access_key_id=self.aws_access_key,
                     aws_secret_access_key=self.aws_secret_key,
                     region_name=self.region_name
                                          )
                
                
                termination_protection = client.set_termination_protection(JobFlowIds=[self.cluster_id],TerminationProtected=False)
                response = client.terminate_job_flows(JobFlowIds=[self.cluster_id])
                
                print termination_protection         
                print response      
                
        except Exception as e:
                print e
                         


def lambda_handler(event, context):
    cluster_id = event['Name']
    l=Terminate_EMR(cluster_id)
    l.terminate_cluster()
       

