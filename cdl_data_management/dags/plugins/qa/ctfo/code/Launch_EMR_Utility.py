
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
"""Module to launch EMR utility"""
# !/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import logging
import boto3
import CommonConstants as CommonConstants
from ConfigUtility import ConfigUtility
from ConfigUtility import JsonConfigUtility
import os
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager

ENVIRONMENT_CONFIG_FILE = "setting.conf"
GLOBAL_SECTION_NAME = "GlobalParameters"
MODULE_NAME = "Launch_EMR_Utility"

logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
hndlr.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


class Create_EMR(object):
    """Class to create EMR"""
    configuration = ConfigUtility(ENVIRONMENT_CONFIG_FILE)

    def __init__(self, event, bucket_name, parent_execution_context=None):
        self.subnet_id = event["subnet_id"]
        self.cluster_name = event["cluster_name"]
        self.ami_version = event["release_version"]
        self.num_core_instance = event["num_core_instances"]
        self.num_task_instance = event["num_task_instances"]
        self.region_name = event["region_name"]
        self.ec2_keyname = event["key_pair_name"]
        self.master_instancetype = event["master_instance_type"]
        self.master_instance_storage = event["master_instance_storage"]
        self.core_instance_type = event["core_instance_type"]
        self.core_instance_storage = event["core_instance_storage"]
        self.task_instance_type = event["task_instance_type"]
        self.task_instance_storage = event["task_instance_storage"]
        self.master_market_type = "ON_DEMAND"
        self.core_market_type = event["core_market_type"]
        self.core_spot_price = event["core_spot_bid_price"]
        self.task_market_type = event["task_market_type"]
        self.task_spot_price = event["task_spot_bid_price"]
        self.property_json = json.loads(event["property_json"])
        # self.property_json = event["property_json"]
        self.region_name = event["region_name"]
        self.service_role = event["role_name"]
        self.jobflow_role = event["instance_profile_name"]
        self.client = None
        self.bucket_name = bucket_name
        # Instance fleet json
        self.instance_fleet = CommonConstants.INSTANCE_FLEET_S3_PATH
        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context

        self.files = []
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"current_module": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.connection = None

    def launch_emr_cluster(self):
        """Method to launch emr cluster"""
        try:
            # configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
            configuration = JsonConfigUtility(
                os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
            bitbucket_username = str(configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "bitbucket_username"]))
            print("bitbucket_username1 = " + str(bitbucket_username))
            bitbucket_secret_password = MySQLConnectionManager().get_secret(
                self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bitbucket_password_secret_name"]),
                self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
            bitbucket_password = bitbucket_secret_password['bitbucket_password']
            if "cc_branch_name" in self.property_json:
                default_cc_branch_name = self.property_json["cc_branch_name"]
            else:
                default_cc_branch_name = configuration.get_configuration([
                    CommonConstants.ENVIRONMENT_PARAMS_KEY, "default_cc_branch_name"])
            if "dm_branch_name" in self.property_json:
                default_dm_branch_name = self.property_json["dm_branch_name"]
            else:
                default_dm_branch_name = configuration.get_configuration([
                    CommonConstants.ENVIRONMENT_PARAMS_KEY, "default_dm_branch_name"])
            environment = str(configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"]))
            if str(self.ami_version).startswith("emr-4") or str(self.ami_version). \
                    startswith("emr-5") or str(self.ami_version).startswith("emr-6"):
                if self.instance_fleet is not None:
                    # Fetching instance fleet config
                    s3_client = boto3.resource('s3')
                    # configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
                    configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
                    bitbucket_username = str(configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bitbucket_username"]))
                    print("bitbucket_username2 = " + str(bitbucket_username))
                    bitbucket_secret_password = MySQLConnectionManager().get_secret(
                        self.configuration.get_configuration(
                            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "bitbucket_password_secret_name"]),
                        self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
                    bitbucket_password = bitbucket_secret_password['bitbucket_password']
                    if "cc_branch_name" in self.property_json:
                        default_cc_branch_name = self.property_json["cc_branch_name"]
                    else:
                        default_cc_branch_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "default_cc_branch_name"])
                    if "dm_branch_name" in self.property_json:
                        default_dm_branch_name = self.property_json["dm_branch_name"]
                    else:
                        default_dm_branch_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "default_dm_branch_name"])
                    environment = str(configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"]))
                    s3_bucket = str(configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"]))
                    content_object = s3_client.Object(s3_bucket,self.instance_fleet)
                    file_content = content_object.get()['Body'].read().decode('utf-8')
                    json_content = json.loads(file_content)
                    instance_details = json_content["Instances"]
                    self.client = boto3.client("emr", region_name=self.region_name)

                else:
                    instance_groups = []

                    if self.master_market_type == "ON_DEMAND":

                        custom_master_properties = {}
                        if "custom_master_instance_properties" in self.property_json:
                            logger.info("Custom properties for master instance type configured "
                                        "hence applying these")
                            custom_master_properties = self.property_json["custom_master_instance" \
                                                                          "_properties"]
                            logger.info("Custom Master Properties found:" + str(custom_master_properties
                                                                                ))
                        logger.info("Master market type is by default considered as: " +
                                    str(self.master_market_type))
                        if self.master_instance_storage is None:
                            master_instance_storage = 64
                        else:
                            master_instance_storage = self.master_instance_storage
                        master_instance_group = {"Name": "Main node", "Market": self.master_market_type,
                                                 "InstanceRole": "MASTER",
                                                 "InstanceType": self.master_instancetype,
                                                 "InstanceCount": 1, "Configurations": [
                                {"Classification": "yarn-site", "Properties": {
                                    "yarn.resourcemanager.scheduler.class":
                                        "org.apache.hadoop.yarn.server."
                                        "resourcemanager.scheduler.fair."
                                        "FairScheduler"}},
                                {"Classification": "spark",
                                 "Properties": {"spark.scheduler.mode": "FAIR"}}],
                                                 "EbsConfiguration": {
                                                     "EbsBlockDeviceConfigs": [
                                                         {
                                                             "VolumeSpecification": {
                                                                 "VolumeType": "gp2",
                                                                 "SizeInGB": master_instance_storage
                                                             }
                                                         }
                                                     ]
                                                 }
                                                 }
                        master_instance_group.update(custom_master_properties)
                        instance_groups.append(master_instance_group)
                        logger.info("Master instance group details: " + str(master_instance_group))
                    else:
                        logger.error("Master Market type is Null or unable to find, atleast master "
                                     "configurations are needed for launching EMR")
                        raise Exception
                    custom_core_properties = {}
                    if "custom_core_instance_properties" in self.property_json:
                        logger.info("Custom properties for Core instance type configured hence"
                                    " applying these")
                        custom_core_properties = self.property_json["custom_core_instance_properties"]
                        logger.info("Custom Core Properties found:" + str(custom_core_properties))
                    logger.info("Core Market type is configured as: " + str(self.core_market_type))
                    if self.core_instance_storage is None:
                        core_instance_storage = 64
                    else:
                        core_instance_storage = self.core_instance_storage
                    if self.core_market_type == "ON_DEMAND":
                        logger.info("Core instance type: " + str(self.core_instance_type) +
                                    " Core Num instances: " + str(self.num_core_instance))
                        core_instance_group = {"Name": "Worker nodes", "Market": self.core_market_type,
                                               "InstanceRole": "CORE",
                                               "InstanceType": self.core_instance_type,
                                               "InstanceCount": int(self.num_core_instance),
                                               "Configurations": [
                                                   {"Classification": "yarn-site", "Properties": {
                                                       "yarn.resourcemanager.scheduler.class":
                                                           "org.apache.hadoop.yarn.server."
                                                           "resourcemanager.scheduler.fair."
                                                           "FairScheduler"}},
                                                   {"Classification": "spark",
                                                    "Properties": {"spark.scheduler.mode": "FAIR"}}],
                                               "EbsConfiguration": {
                                                   "EbsBlockDeviceConfigs": [
                                                       {
                                                           "VolumeSpecification": {
                                                               "VolumeType": "gp2",
                                                               "SizeInGB": core_instance_storage
                                                           }
                                                       }
                                                   ]
                                               }
                                               }
                        core_instance_group.update(custom_core_properties)
                        instance_groups.append(core_instance_group)

                    elif self.core_market_type == "SPOT":
                        logger.info("Core instance type: " + str(self.core_instance_type) +
                                    " Core Num instances: " + str(self.num_core_instance) +
                                    " Core Spot Bid price: " + str(self.core_spot_price))
                        core_instance_group = {"Name": "Worker nodes", "Market": self.core_market_type,
                                               "InstanceRole": "CORE",
                                               "BidPrice": str(float(self.core_spot_price)),
                                               "InstanceType": self.core_instance_type,
                                               "InstanceCount": int(self.num_core_instance),
                                               "Configurations": [{"Classification": "yarn-site",
                                                                   "Properties": {
                                                                       "yarn.resourcemanager.scheduler."
                                                                       "class": "org.apache.hadoop.yarn."
                                                                                "server.resourcemanager."
                                                                                "scheduler.fair."
                                                                                "FairScheduler"}},
                                                                  {"Classification": "spark",
                                                                   "Properties":
                                                                       {"spark.scheduler.mode": "FAIR"}}
                                                                  ],
                                               "EbsConfiguration": {
                                                   "EbsBlockDeviceConfigs": [
                                                       {
                                                           "VolumeSpecification": {
                                                               "VolumeType": "gp2",
                                                               "SizeInGB": core_instance_storage
                                                           }
                                                       }
                                                   ]
                                               }
                                               }
                        core_instance_group.update(custom_core_properties)
                        instance_groups.append(core_instance_group)
                    else:
                        logger.info("Core Market type is Null or unable to find, EMR cluster"
                                    " will not contain any core instances")

                    custom_task_properties = {}
                    if "custom_task_instance_properties" in self.property_json:
                        logger.info("Custom properties for Task instance type configured hence "
                                    "applying these")
                        custom_task_properties = self.property_json["custom_task_instance_properties"]
                        logger.info("Custom Task Properties found:" + str(custom_task_properties))
                    logger.info("Task Market type is configured as: " + str(self.task_market_type))
                    if self.task_instance_storage is None:
                        task_instance_storage = 64
                    else:
                        task_instance_storage = self.task_instance_storage
                    if self.task_market_type == "ON_DEMAND":
                        logger.info("Task instance type: " + str(self.task_instance_type) +
                                    " Task Num instances: " + str(self.num_task_instance))
                        task_instance_group = {"Name": "Worker nodes", "Market": self.task_market_type,
                                               "InstanceRole": "TASK", "InstanceType":
                                                   self.task_instance_type,
                                               "InstanceCount": int(self.num_task_instance),
                                               "Configurations": [
                                                   {"Classification": "yarn-site",
                                                    "Properties": {"yarn.resourcemanager.scheduler."
                                                                   "class": "org.apache.hadoop.yarn."
                                                                            "server.resourcemanager."
                                                                            "scheduler.fair."
                                                                            "FairScheduler"}},
                                                   {"Classification": "spark", "Properties":
                                                       {"spark.scheduler.mode": "FAIR"}}],
                                               "EbsConfiguration": {
                                                   "EbsBlockDeviceConfigs": [
                                                       {
                                                           "VolumeSpecification": {
                                                               "VolumeType": "gp2",
                                                               "SizeInGB": task_instance_storage
                                                           }
                                                       }
                                                   ]
                                               }}
                        task_instance_group.update(custom_task_properties)
                        instance_groups.append(task_instance_group)
                    elif self.task_market_type == "SPOT":
                        logger.info("Task instance type: " + str(self.task_instance_type) +
                                    " Task Num instances: " + str(self.num_task_instance) +
                                    " Task Spot Bid price: " + str(self.task_spot_price))
                        task_instance_group = {"Name": "Worker nodes", "Market": self.task_market_type,
                                               "InstanceRole": "TASK",
                                               "BidPrice": str(float(self.task_spot_price)),
                                               "InstanceType": self.task_instance_type,
                                               "InstanceCount": int(self.num_task_instance),
                                               "Configurations": [
                                                   {"Classification": "yarn-site",
                                                    "Properties": {"yarn.resourcemanager.scheduler."
                                                                   "class":
                                                                       "org.apache.hadoop.yarn.server."
                                                                       "resourcemanager.scheduler.fair"
                                                                       ".FairScheduler"}},
                                                   {"Classification": "spark",
                                                    "Properties": {"spark.scheduler.mode": "FAIR"}}],
                                               "EbsConfiguration": {
                                                   "EbsBlockDeviceConfigs": [
                                                       {
                                                           "VolumeSpecification": {
                                                               "VolumeType": "gp2",
                                                               "SizeInGB": task_instance_storage
                                                           }
                                                       }
                                                   ]
                                               }}

                        task_instance_group.update(custom_task_properties)
                        instance_groups.append(task_instance_group)
                    else:
                        logger.error("Task Market type is Null or unable to find, EMR cluster launched"
                                     " will not contain any Task instances")
                    self.client = boto3.client("emr", region_name=self.region_name)
                    logger.info("Instance groups prepared for launching EMR: " + str(instance_groups))
                if "AutoScalingRole" in self.property_json and "security_conf" in self.property_json:
                    autoscaling_role_name = self.property_json["AutoScalingRole"]
                    logger.info("Applying Autoscaling role: " + str(autoscaling_role_name))
                    if self.instance_fleet is not None:
                        response = self.client.run_job_flow(Name=self.cluster_name,
                                                            ReleaseLabel=self.ami_version,
                                                            AutoTerminationPolicy=self.property_json['AutoTerminationPolicy'],
                                                            LogUri=self.property_json['log_uri'],
                                                            Instances=instance_details,
                                                            Applications=self.property_json[
                                                                "applications"],
                                                            EbsRootVolumeSize=15,
                                                            VisibleToAllUsers=True,
                                                            JobFlowRole=self.jobflow_role,
                                                            ServiceRole=self.service_role,
                                                            Configurations=[
                                                                {
                                                                    "Classification": "spark-env",
                                                                    "Configurations": [
                                                                        {
                                                                            "Classification": "export",
                                                                            "Properties": {
                                                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                            }
                                                                        }
                                                                    ]
                                                                }

                                                            ],
                                                            BootstrapActions=[
                                                                {'Name': 'Install Packages',
                                                                 'ScriptBootstrapAction':
                                                                     {'Path':
                                                                          self.property_json[
                                                                              "cluster_bootstrap_s3_"
                                                                              "location"],
                                                                      'Args': [self.bucket_name,
                                                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                                                      }}, {'Name': 'Install crowstrike and splunk',
                                                                           'ScriptBootstrapAction': {
                                                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                                                            Tags=self.property_json["cluster_tags"],
                                                            AutoScalingRole=autoscaling_role_name,
                                                            SecurityConfiguration=self.property_json["security_conf"]
                                                            )
                        logger.info("EMR Cluster Launched")
                    else:

                        response = self.client.run_job_flow(Name=self.cluster_name,
                                                            ReleaseLabel=self.ami_version,
                                                            AutoTerminationPolicy=self.property_json['AutoTerminationPolicy'],
                                                            LogUri=self.property_json['log_uri'],
                                                            Instances={'InstanceGroups':
                                                                           instance_groups,
                                                                       'Ec2KeyName': self.ec2_keyname,
                                                                       'KeepJobFlowAliveWhenNoSteps':
                                                                           True,
                                                                       'TerminationProtected': False,
                                                                       'Ec2SubnetId': self.subnet_id,
                                                                       'EmrManagedMasterSecurityGroup':
                                                                           self.property_json[
                                                                               "EmrManagedMasterSecurit"
                                                                               "yGroup"],
                                                                       'EmrManagedSlaveSecurityGroup':
                                                                           self.property_json[
                                                                               "EmrManagedSlaveSecurit"
                                                                               "yGroup"],
                                                                       'ServiceAccessSecurityGroup':
                                                                           self.property_json[
                                                                               "ServiceAccessSecurity"
                                                                               "Group"],
                                                                       'AdditionalMasterSecurityGroups':
                                                                           [self.property_json[
                                                                                "AdditionalMasterSecurityGroups"]]},
                                                            Applications=self.property_json[
                                                                "applications"],
                                                            EbsRootVolumeSize=15,
                                                            VisibleToAllUsers=True,
                                                            JobFlowRole=self.jobflow_role,
                                                            ServiceRole=self.service_role,
                                                            Configurations=[
                                                                {
                                                                    "Classification": "spark-env",
                                                                    "Configurations": [
                                                                        {
                                                                            "Classification": "export",
                                                                            "Properties": {
                                                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                            }
                                                                        }
                                                                    ]
                                                                }

                                                            ],
                                                            BootstrapActions=[
                                                                {'Name': 'Install Packages',
                                                                 'ScriptBootstrapAction':
                                                                     {'Path':
                                                                          self.property_json[
                                                                              "cluster_bootstrap_s3_"
                                                                              "location"],
                                                                      'Args': [self.bucket_name,
                                                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                                                      }}, {'Name': 'Install crowstrike and splunk',
                                                                           'ScriptBootstrapAction': {
                                                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                                                            Tags=self.property_json["cluster_tags"],
                                                            AutoScalingRole=autoscaling_role_name,
                                                            SecurityConfiguration=self.property_json["security_conf"]
                                                            )
                        logger.info("EMR Cluster Launched")
                if "AutoScalingRole" not in self.property_json and "security_conf" in self.property_json:
                    logger.info("No autoscaling role configured hence it will not be applied"
                                " while launching EMR")
                    if self.instance_fleet is not None:
                        response = self.client.run_job_flow(Name=self.cluster_name,
                                                            ReleaseLabel=self.ami_version,
                                                            AutoTerminationPolicy=self.property_json['AutoTerminationPolicy'],
                                                            LogUri=self.property_json['log_uri'],
                                                            Instances=instance_details,
                                                            Applications=self.property_json["applications"],
                                                            EbsRootVolumeSize=15,
                                                            VisibleToAllUsers=True,
                                                            JobFlowRole=self.jobflow_role,
                                                            ServiceRole=self.service_role,
                                                            Configurations=[
                                                                {
                                                                    "Classification": "spark-env",
                                                                    "Configurations": [
                                                                        {
                                                                            "Classification": "export",
                                                                            "Properties": {
                                                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                            }
                                                                        }
                                                                    ]
                                                                }

                                                            ],
                                                            BootstrapActions=[
                                                                {'Name': 'Install Packages',
                                                                 'ScriptBootstrapAction':
                                                                     {'Path':
                                                                          self.property_json[
                                                                              "cluster_bootstrap_s3_"
                                                                              "location"],
                                                                      'Args': [self.bucket_name,
                                                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                                                      }}, {'Name': 'Install crowstrike and splunk',
                                                                           'ScriptBootstrapAction': {
                                                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                                                            Steps=self.property_json.get("step_creation_property", []),
                                                            Tags=self.property_json["cluster_tags"],
                                                            SecurityConfiguration=self.property_json["security_conf"]
                                                            )
                        logger.info("EMR Cluster Launched")
                    else:
                        response = self.client.run_job_flow(Name=self.cluster_name,
                                                            ReleaseLabel=self.ami_version,
                                                            AutoTerminationPolicy = self.property_json['AutoTerminationPolicy'],
                                                            LogUri=self.property_json['log_uri'],
                                                            Instances={'InstanceGroups':
                                                                           instance_groups,
                                                                       'Ec2KeyName': self.ec2_keyname,
                                                                       'KeepJobFlowAliveWhenNoSteps':
                                                                           True,
                                                                       'TerminationProtected': False,
                                                                       'Ec2SubnetId': self.subnet_id,
                                                                       'EmrManagedMasterSecurityGroup':
                                                                           self.property_json[
                                                                               "EmrManagedMasterSecurit"
                                                                               "yGroup"],
                                                                       'EmrManagedSlaveSecurityGroup':
                                                                           self.property_json[
                                                                               "EmrManagedSlaveSecurity"
                                                                               "Group"],
                                                                       'ServiceAccessSecurityGroup':
                                                                           self.property_json[
                                                                               "ServiceAccessSecurity"
                                                                               "Group"],
                                                                       'AdditionalMasterSecurityGroups':
                                                                           [self.property_json[
                                                                                "AdditionalMasterSecurityGroups"]]},
                                                            Applications=self.property_json["applications"],
                                                            EbsRootVolumeSize=15,
                                                            VisibleToAllUsers=True,
                                                            JobFlowRole=self.jobflow_role,
                                                            ServiceRole=self.service_role,
                                                            Configurations=[
                                                                {
                                                                    "Classification": "spark-env",
                                                                    "Configurations": [
                                                                        {
                                                                            "Classification": "export",
                                                                            "Properties": {
                                                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                            }
                                                                        }
                                                                    ]
                                                                }

                                                            ],
                                                            BootstrapActions=[
                                                                {'Name': 'Install Packages',
                                                                 'ScriptBootstrapAction':
                                                                     {'Path':
                                                                          self.property_json[
                                                                              "cluster_bootstrap_s3_"
                                                                              "location"],
                                                                      'Args': [self.bucket_name,
                                                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                                                      }}, {'Name': 'Install crowstrike and splunk',
                                                                           'ScriptBootstrapAction': {
                                                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                                                            Steps=self.property_json.get("step_creation_property", []),
                                                            Tags=self.property_json["cluster_tags"],
                                                            SecurityConfiguration=self.property_json["security_conf"]
                                                            )
                        logger.info("EMR Cluster Launched")
                if "AutoScalingRole" in self.property_json and "security_conf" not in self.property_json:
                    autoscaling_role_name = self.property_json["AutoScalingRole"]
                    logger.info("Applying Autoscaling role: " + str(autoscaling_role_name))
                    if self.instance_fleet is not None:

                        response = self.client.run_job_flow(Name=self.cluster_name,
                                                            ReleaseLabel=self.ami_version,
                                                            AutoTerminationPolicy=self.property_json['AutoTerminationPolicy'],
                                                            LogUri=self.property_json['log_uri'],
                                                            Instances=instance_details,
                                                            Applications=self.property_json[
                                                                "applications"],
                                                            EbsRootVolumeSize=15,
                                                            VisibleToAllUsers=True,
                                                            JobFlowRole=self.jobflow_role,
                                                            ServiceRole=self.service_role,
                                                            Configurations=[
                                                                {
                                                                    "Classification": "spark-env",
                                                                    "Configurations": [
                                                                        {
                                                                            "Classification": "export",
                                                                            "Properties": {
                                                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                            }
                                                                        }
                                                                    ]
                                                                }

                                                            ],
                                                            BootstrapActions=[
                                                                {'Name': 'Install Packages',
                                                                 'ScriptBootstrapAction':
                                                                     {'Path':
                                                                          self.property_json[
                                                                              "cluster_bootstrap_s3_"
                                                                              "location"],
                                                                      'Args': [self.bucket_name,
                                                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                                                      }}, {'Name': 'Install crowstrike and splunk',
                                                                           'ScriptBootstrapAction': {
                                                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                                                            Steps=self.property_json.get("step_creation_property", []),
                                                            Tags=self.property_json["cluster_tags"],
                                                            AutoScalingRole=autoscaling_role_name
                                                            )
                        logger.info("EMR Cluster Launched")

                    else:

                        response = self.client.run_job_flow(
                            Name=self.cluster_name,
                            ReleaseLabel=self.ami_version,
                            AutoTerminationPolicy=self.property_json['AutoTerminationPolicy'],
                            LogUri=self.property_json['log_uri'],
                            Instances={'InstanceGroups': instance_groups,
                                       'Ec2KeyName': self.ec2_keyname,
                                       'KeepJobFlowAliveWhenNoSteps':
                                           True,
                                       'TerminationProtected': False,
                                       'Ec2SubnetId': self.subnet_id,
                                       'EmrManagedMasterSecurityGroup':
                                           self.property_json[
                                               "EmrManagedMasterSecurit"
                                               "yGroup"],
                                       'EmrManagedSlaveSecurityGroup':
                                           self.property_json[
                                               "EmrManagedSlaveSecurity"
                                               "Group"],
                                       'ServiceAccessSecurityGroup':
                                           self.property_json[
                                               "ServiceAccessSecurity"
                                               "Group"]},
                            BootstrapActions=[
                                {'Name': 'Install Packages',
                                 'ScriptBootstrapAction':
                                     {'Path':
                                          self.property_json[
                                              "cluster_bootstrap_s3_"
                                              "location"],
                                      'Args': [self.bucket_name,
                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                      }}, {'Name': 'Install crowstrike and splunk',
                                           'ScriptBootstrapAction': {
                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                            Applications=self.property_json["applications"],
                            EbsRootVolumeSize=15,
                            VisibleToAllUsers=True,
                            JobFlowRole=self.jobflow_role,
                            ServiceRole=self.service_role,
                            Configurations=[
                                {
                                    "Classification": "spark-env",
                                    "Configurations": [
                                        {
                                            "Classification": "export",
                                            "Properties": {
                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                            }
                                        }
                                    ]
                                }

                            ],
                            Tags=self.property_json["cluster_tags"],
                            AutoScalingRole='EMR_AutoScaling_DefaultRole'

                        )
                        logger.info("EMR Cluster Launched")

                if "AutoScalingRole" not in self.property_json and "security_conf" not in self.property_json:
                    logger.info("No autoscaling role configured hence it will not be applied"
                                " while launching EMR")
                    if self.instance_fleet is not None:

                        response = self.client.run_job_flow(Name=self.cluster_name,
                                                            ReleaseLabel=self.ami_version,
                                                            AutoTerminationPolicy=self.property_json['AutoTerminationPolicy'],
                                                            LogUri=self.property_json['log_uri'],
                                                            Instances=instance_details,
                                                            Applications=self.property_json["applications"],
                                                            EbsRootVolumeSize=15,
                                                            VisibleToAllUsers=True,
                                                            JobFlowRole=self.jobflow_role,
                                                            ServiceRole=self.service_role,
                                                            Configurations=[
                                                                {
                                                                    "Classification": "spark-env",
                                                                    "Configurations": [
                                                                        {
                                                                            "Classification": "export",
                                                                            "Properties": {
                                                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                            }
                                                                        }
                                                                    ]
                                                                }

                                                            ],
                                                            BootstrapActions=[
                                                                {'Name': 'Install Packages',
                                                                 'ScriptBootstrapAction':
                                                                     {'Path':
                                                                          self.property_json[
                                                                              "cluster_bootstrap_s3_"
                                                                              "location"],
                                                                      'Args': [self.bucket_name,
                                                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                                                      }}, {'Name': 'Install crowstrike and splunk',
                                                                           'ScriptBootstrapAction': {
                                                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                                                            Steps=self.property_json.get("step_creation_property", []),
                                                            Tags=self.property_json["cluster_tags"]
                                                            )
                        logger.info("EMR Cluster Launched")
                    else:
                        response = self.client.run_job_flow(Name=self.cluster_name,
                                                            ReleaseLabel=self.ami_version,
                                                            AutoTerminationPolicy=self.property_json['AutoTerminationPolicy'],
                                                            LogUri=self.property_json['log_uri'],
                                                            Instances={'InstanceGroups':
                                                                           instance_groups,
                                                                       'Ec2KeyName': self.ec2_keyname,
                                                                       'KeepJobFlowAliveWhenNoSteps':
                                                                           True,
                                                                       'TerminationProtected': False,
                                                                       'Ec2SubnetId': self.subnet_id,
                                                                       'EmrManagedMasterSecurityGroup':
                                                                           self.property_json[
                                                                               "EmrManagedMasterSecurit"
                                                                               "yGroup"],
                                                                       'EmrManagedSlaveSecurityGroup':
                                                                           self.property_json[
                                                                               "EmrManagedSlaveSecurity"
                                                                               "Group"],
                                                                       'ServiceAccessSecurityGroup':
                                                                           self.property_json[
                                                                               "ServiceAccessSecurity"
                                                                               "Group"],
                                                                       'AdditionalMasterSecurityGroups':
                                                                           [self.property_json[
                                                                                "AdditionalMasterSecurityGroups"]]},
                                                            Applications=self.property_json["applications"],
                                                            EbsRootVolumeSize=15,
                                                            VisibleToAllUsers=True,
                                                            JobFlowRole=self.jobflow_role,
                                                            ServiceRole=self.service_role,
                                                            Configurations=[
                                                                {
                                                                    "Classification": "spark-env",
                                                                    "Configurations": [
                                                                        {
                                                                            "Classification": "export",
                                                                            "Properties": {
                                                                                "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                            }
                                                                        }
                                                                    ]
                                                                }

                                                            ],
                                                            BootstrapActions=[
                                                                {'Name': 'Install Packages',
                                                                 'ScriptBootstrapAction':
                                                                     {'Path':
                                                                          self.property_json[
                                                                              "cluster_bootstrap_s3_"
                                                                              "location"],
                                                                      'Args': [self.bucket_name,
                                                                               CommonConstants.EMR_CODE_PATH,environment,default_cc_branch_name,default_dm_branch_name,bitbucket_username,bitbucket_password]
                                                                      }}, {'Name': 'Install crowstrike and splunk',
                                                                           'ScriptBootstrapAction': {
                                                                               'Path': "s3://aws-a0023-use1-00-p-s3b-shrd-shr-ec2mgmt01/scripts/ec2provision.sh"}}],
                                                            Steps=self.property_json.get("step_creation_property", []),
                                                            Tags=self.property_json["cluster_tags"]
                                                            )
                        logger.info("EMR Cluster Launched")
                self.jobid = response['JobFlowId']
                return self.jobid

        except Exception as exception:
            logger.error("Failed to launch EMR", exception)
            raise exception


