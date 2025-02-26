# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import sys
import boto3
from ConfigUtility import ConfigUtility


ENVIRONMENT_CONFIG_FILE = "setting.conf"
GLOBAL_SECTION_NAME = "GlobalParameters"

class Emr_State(object):
    configuration = ConfigUtility(ENVIRONMENT_CONFIG_FILE)
    
    def __init__(self,event):
        
        self.aws_access_key = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "aws_access_key")
        self.aws_secret_key = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "aws_secret_key")
        self.region_name = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "region_name")
        self.jobid=event["Name"]
        
    
    def get_emr_cluster_state(self):
        #jobid=sys.argv[1]
        #jobid="j-1T5LYZPLTKUJG"
        try:
         
                client = boto3.client(
                    "emr",
                     aws_access_key_id=self.aws_access_key,
                     aws_secret_access_key=self.aws_secret_key,
                     region_name=self.region_name
                                          )
                          
                       
        except Exception as e:
                print e
                         
        res = client.describe_cluster(ClusterId=self.jobid)
        ReadyDateTime= str(res['Cluster']['Status']['Timeline']['ReadyDateTime']).split(".")[0]
        CreationDateTime= str(res['Cluster']['Status']['Timeline']['CreationDateTime']).split(".")[0]
        cluster_details={"Status":res['Cluster']['Status']['State'],"DNS":res['Cluster']['MasterPublicDnsName'],"ReadyDateTime":ReadyDateTime,"CreationDateTime":CreationDateTime } 
        return cluster_details
 
 
def lambda_handler(event, context):        
    obj=Emr_State(event)       
    return obj.get_emr_cluster_state()       
    
    

