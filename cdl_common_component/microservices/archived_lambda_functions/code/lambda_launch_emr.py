# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import boto3
from ConfigUtility import ConfigUtility


ENVIRONMENT_CONFIG_FILE = "setting.conf"
GLOBAL_SECTION_NAME = "GlobalParameters"
MODULE_NAME = "lambda_launch_emr"


class Create_EMR(object):
    configuration = ConfigUtility(ENVIRONMENT_CONFIG_FILE)
    
    def __init__(self,event):
        
        self.subnet_id = event["subnet_id"]
        self.cluster_name = event["cluster_name"]
        self.ami_version = event["release_version"]
        self.num_instance = event["num_core_instances"]
        self.aws_access_key = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "aws_access_key")
        self.aws_secret_key = self.configuration.get_configuration(GLOBAL_SECTION_NAME, "aws_secret_key")
        self.region_name = event["region_name"]
        self.ec2_keyname = event["key_pair_name"]
        self.master_instancetype = event["master_instance_type"]
        self.core_instance_type = event["core_instance_type"]
        self.bootstrap_script = self.configuration.get_configuration(MODULE_NAME, "bootstrap_script")
        #print self.bootstrap_script
    
    def launch_emr_cluster(self):
        try:
            if str(self.ami_version).startswith("emr-4") or str(self.ami_version).startswith("emr-5"):
                instance_market = "ON_DEMAND"
                instance_groups = []
                
                #configuration = [{"Classification": "yarn-site", "Properties": {"yarn.resourcemanager.scheduler.class":"org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"}}]
                
                if instance_market == "ON_DEMAND":
                    instance_groups.append({"Name": "Main node", "Market": "ON_DEMAND", "InstanceRole": "MASTER",
                                            "InstanceType":self.master_instancetype, "InstanceCount": 1, "Configurations": [{"Classification": "yarn-site", "Properties": {"yarn.resourcemanager.scheduler.class":"org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"}}, {"Classification": "oozie-site", "Properties": {"oozie.action.max.output.data":"1580000"}}, {"Classification": "spark", "Properties": {"spark.scheduler.mode": "FAIR"}}]})
                    instance_groups.append({"Name": "Worker nodes", "Market": "ON_DEMAND", "InstanceRole": "CORE",
                                            "InstanceType": self.slave_instance_type, "InstanceCount": int(self.num_instance), "Configurations": [{"Classification": "yarn-site", "Properties": {"yarn.resourcemanager.scheduler.class":"org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"}}, {"Classification": "oozie-site", "Properties": {"oozie.action.max.output.data":"1580000"}}, {"Classification": "spark", "Properties": {"spark.scheduler.mode": "FAIR"}}]})
                 
                try:
                        client = boto3.client(
                                                        "emr",
                                                        aws_access_key_id=self.aws_access_key,
                                                        aws_secret_access_key=self.aws_secret_key,
                                                        region_name=self.region_name
                                                        )
                         
                       
                except Exception as e:
                        print e
 
                try:
                    response = client.run_job_flow(Name=self.cluster_name,ReleaseLabel=self.ami_version,
                                    Instances={'InstanceGroups': instance_groups,'Ec2KeyName': self.ec2_keyname, 'KeepJobFlowAliveWhenNoSteps': True, 'TerminationProtected': True,
                                               'Ec2SubnetId': self.subnet_id},
                                    Applications=[{'Name':'Hive'},{'Name':'Pig'},{'Name':'Ganglia'},{'Name':'Hue'},{'Name':'Spark'}],
                                      VisibleToAllUsers=True, JobFlowRole='EMR_EC2_DefaultRole',
                                     ServiceRole= 'EMR_DefaultRole',
                                     BootstrapActions=[{'Name': 'Install Packages', 'ScriptBootstrapAction':
                                        {'Path': self.bootstrap_script}}]
                                     )
 
                except Exception as e:
                    print e
 
                self.jobid = response['JobFlowId']
                 
                add_Tags = client.add_tags(
                                        ResourceId=self.jobid,
                                        Tags=[
                                            {
                                                'Key':'Name',
                                                'Value':'TestEMR'
                                                },
                                              {
                                                  'Key':'*********',
                                                  'Value':'*********'
                                                  },
                                              {
                                                  'Key':'ClientName',
                                                  'Value':'*********'
                                                  },
                                               
                                              ]
                                               )   
                 
 
            return self.jobid
             
        except Exception as e:
            print "Failed to launch EMR",e
            return "FAILED"
         
         


def lambda_handler(event, context):
    l=Create_EMR(event)
    return l.launch_emr_cluster()
       
      

