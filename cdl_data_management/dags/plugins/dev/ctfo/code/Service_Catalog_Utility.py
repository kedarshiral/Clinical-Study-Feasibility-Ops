
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
"""Module to launch EMR """
# !/usr/bin/python
# -*- coding: utf-8 -*-

import time
import json
import logging
import boto3
from botocore.exceptions import ClientError
import CommonConstants as CommonConstants
from ConfigUtility import ConfigUtility
from ConfigUtility import JsonConfigUtility
import os
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
import traceback

logger = logging.getLogger(__name__)
MODULE_NAME = "Service_Catalog_Utility"


class EMRClusterLauncher:

    def __init__(self, event=None, bucket_name=None, parent_execution_context=None):
        self.configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.environment = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                                 , "environment"])
        self.emr_code_path = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                                    , "emr_code"])
        self.bitbucket_username = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                                    , "bitbucket_username"])
        # self.bitbucket_password = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
        #                                                             , "bitbucket_password_secret_name"])
        secret_password = MySQLConnectionManager().get_secret(
            self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                     , "bitbucket_password_secret_name"]),
            self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                     , "s3_region"]))
        print("Here is secret password! ", secret_password)
        self.bitbucket_password = secret_password['Password']
        self.emr_cluster_name = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                                    , "emr_cluster"])
        self.client = None
        self.bucket_name = bucket_name
        self.instance_fleet = CommonConstants.INSTANCE_FLEET_S3_PATH
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.files = []
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"current_module": MODULE_NAME})
        self.connection = None


    def provision_product_async(self, provisioned_product_name, provisioning_parameters,
                                product_name, product_version, environment,
                                role=None, region=None):
        print('Inside provision_product_async')
        provisioning_parameters = [{"Key": list(param.keys())[0], "Value": str(list(param.values())[0])} for param in
                                   provisioning_parameters]

        print('provisioning_parameters = ' + str(provisioning_parameters))
        client = self.get_service_catalog_client()
        print('client= ' + str(client))
        product = self.validate_product(product_name, product_version)
        print('product= ' + str(product))
        print('provisioned_product_name= ' + str(provisioned_product_name))
        print('ProductId' + str(product[0]))
        print('ProvisioningArtifactId' + str(product[1]))
        print('ProvisioningParameters' + str(provisioning_parameters))
        response = client.provision_product(
            ProvisionedProductName=provisioned_product_name,
            ProductId=product[0],
            ProvisioningArtifactId=product[1],
            ProvisioningParameters=provisioning_parameters
        )
        print('response= ' + str(response))
        provisioned_product_id = response['RecordDetail']['ProvisionedProductId']

        status = 'Failed'
        timeout = time.time() + 60 * 10

        while True:
            if time.time() > timeout or status == 'AVAILABLE':
                break
            elif time.time() > timeout or status == 'ERROR':
                response = client.describe_provisioned_product(Id=provisioned_product_id)
                status_message = response['ProvisionedProductDetail']['StatusMessage']
                print("Provisioned product status: {}".format(response['ProvisionedProductDetail']['Status']))
                print(status_message)
                break
            else:
                response = client.describe_provisioned_product(Id=provisioned_product_id)
                status = response['ProvisionedProductDetail']['Status']
                time.sleep(60)
                print("Provisioned product status: {}".format(response['ProvisionedProductDetail']['Status']))

        return provisioned_product_id

    def get_service_catalog_client(self, role=None, region=None):
        if region is None:
            region = 'us-east-1'
        if role is not None:
            return self.assume_role(role).client('servicecatalog', region_name=region)
        else:
            return boto3.client('servicecatalog', region_name=region)

    def product_list(self, role=None, region=None):
        client = self.get_service_catalog_client()
        all_products = []
        next_page_token = None
        while True:
            if next_page_token:
                response = client.search_products(PageToken=next_page_token)
            else:
                response = client.search_products()
            all_products.extend(response['ProductViewSummaries'])

            next_page_token = response.get('NextPageToken')
            if not next_page_token:
                break
        print('Products List=' + str(all_products))
        return all_products



    def artifact_list(self, product_id, role=None, region=None):
        client = self.get_service_catalog_client()
        response = client.describe_product(Id=product_id)
        temp_artifact = [artifact['Name'] for artifact in response['ProvisioningArtifacts']]
        print(f"test artifacts details {temp_artifact}")
        return temp_artifact

    def validate_product(self, product_name, product_version, role=None, region=None):
        client = self.get_service_catalog_client()
        response = client.search_products()
        products = self.product_list()
        print('Products = ' + str(products))
        product_details = []
        product_id = ""
        for val in products:
            if val["Name"] == product_name:
                product_id = val["ProductId"]
                artifacts = self.artifact_list(product_id)
                product_details.append(product_id)
                if product_version in artifacts:
                    response = client.describe_product(Id=product_id)
                    artifact = response['ProvisioningArtifacts'][artifacts.index(product_version)]
                    product_details.append(artifact['Id'])
                else:
                    print("Version not available")
            else:
                print("Product not available")
        return product_details

    def launch_cluster(self):
        print('launch')

    def check(self):
        return "Hello There"

    def terminate_provisioned_product_async(self, provisioned_product_name, role=None, region=None):
        try:
            client = self.get_service_catalog_client()
            response = client.terminate_provisioned_product(
                ProvisionedProductName=provisioned_product_name,
                IgnoreErrors=False
            )
            provisioned_product_id = response['RecordDetail']['ProvisionedProductId']
            return provisioned_product_id
        except Exception as e:
            status_message = "Error Occurred when decomissioning  EMR cluster and Service Catalog. Exiting"
            print("error message : ", status_message)
            raise e

    def check_cluster_status(self,cluster_name):
        try:
            """
            Function : This function is to get the cluster ID for the launched cluster via service catalog
            """
            emr_client = boto3.client('emr')

            # List clusters and filter by status 'WAITING' or 'RUNNING'
            clusters = emr_client.list_clusters(
                ClusterStates=['WAITING'])

            cluster_id = None
            for cluster in clusters['Clusters']:
                if cluster['Name'] == cluster_name:
                    cluster_id = cluster['Id']
                    break

            if cluster_id:
                return cluster_id
        except boto3.exceptions.Boto3Error as e:
            print(f"An error occurred: {e}")

    def managed_scale_policy(self,cluster_id,region_name,min_nodes,max_nodes):
        # Initialize the Boto3 EMR client
        try:
            emr_client = boto3.client('emr', region_name=region_name)
            # Define the managed scaling policy
            managed_scaling_policy = {
                'ClusterId': cluster_id,
                'ManagedScalingPolicy': {
                    'ComputeLimits': {
                        'MinimumCapacityUnits': int(min_nodes),
                        'MaximumCapacityUnits': int(max_nodes),
                        "UnitType": "Instances"
                    }
                }
            }

            # Add managed scaling policy to the EMR cluster
            response = emr_client.put_managed_scaling_policy(**managed_scaling_policy)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return True
            else:
                raise Exception("Failed to apply managed scaling policy: Non-200 HTTP status code received")
        except ClientError as e:
            raise Exception(f"Failed to apply managed scaling policy: {e.response['Error']['Message']}")

    def provision(self,cluster_configs, service_catalog, bucket_name):
        try:
            # provisioning_params = [
            #     {"accountAlias": "a0339"},
            #     {"projectCode": "2181US0002"},`
            #     {"appCode": "ctf"},
            #     {"tenantCode": "incyte"},
            #     {"purpose": "d_m"},
            #     {"requestorEmailId": "kashish.mogha@zs.com"},
            #     {"environment": "prod"},
            #     {"region": "us-east-1"},
            #     {"vpcID": "vpc-0db2c6723056fdf48"},
            #     {"subnetID": "subnet-00d03b0b11875320f"},
            #     {"releaseLabel": "emr-5.36.0"},
            #     {
            #         "scriptLocations": "s3://aws-a0339-use1-00-p-s3b-incy-spo-data01/clinical-data-lake/code/bootstrap/python37_emr_bootstrap.sh aws-a0339-use1-00-p-s3b-incy-spo-data01;/usr/local/airflow/dags/plugins/prod/ctfo;dev;integration;mwaa-dev;SP26212;yXMP3w6jE6******7"},
            #     {"additionals3EMRResources": "us-east-1.elasticmapreduce"},
            #     {"applications": "Spark,JupyterHub,Ganglia,Hive,Hadoop"},
            #     {"steps": ""},
            #     {"keepJobFlowAliveWhenNoSteps": "true"},
            #     {"visibileToAllUsers": "true"},
            #     {"logBucket": "s3://aws-a0339-use1-00-p-s3b-incy-spo-data01/clinical-data-lake/code/emr_logs/"},
            #     {"autoscalingNeeded": "false"},
            #     {"minUnits": "1"},
            #     {"maxUnits": "5"},
            #     {"customConfigurations": "false"},
            #     {"configBucket": "s3://aws-a0339-use1-00-p-s3b-incy-spo-data01/clinical-data-lake/code/bootstrap/"},
            #     {"configKey": "config.json"},
            #     {"clientAccountId": "786061192357"},
            #     {"rootVolumeSize": "100"},
            #     {"keyPair": "aws-a0339-use1-00-p-kpr-incy-shr-emr01"},
            #     {"customAmiId": ""},
            #     {"instanceTypeMaster": "r5.2xlarge"},
            #     {
            #         "emrProfileRoleARN": "arn:aws:iam::381491825997:instance-profile/aws-a0339-use1-00-d-rol-incy-shr-instanceprofile01"},
            #     {"emrSecConfig": ""},
            #     {"additionalMasterEbsRequired": "false"},
            #     {"masterEbsStorageType": "gp2"},
            #     {"masterInstanceEbsSize": "100"},
            #     {"masterEbsIops": ""},
            #     {"masterEbsVolumeCount": "1"},
            #     {"instanceTypeSlave": "r5.xlarge"},
            #     {"coreInstanceCount": "4"},
            #     {"additionalCoreEbsRequired": "false"},
            #     {"coreEbsStorageType": "gp2"},
            #     {"coreInstanceEbsSize": "100"},
            #     {"coreEbsIops": ""},
            #     {"coreEbsVolumeCount": "1"},
            #     {"updateCounter": "01"},
            #     {"count": "1"}
            # ]
            """
            Purpose : This will call service catalog to provision a product which will help to launch EMR
            Input : This will require all the inputs from cluster config table and will fetch base dict from env param
            Output : Firstly it will return a provision product ID and from hat we will be fetching a cluster ID
            Author : Kashish Mogha
            Created Date : 5/23/2024
            """
            provisioned_product_name = cluster_configs['cluster_name'] + CommonConstants.CLIENT_ADDITION
            cluster_details = json.loads(cluster_configs['property_json'])
            service_catalog["projectCode"] = next(val['Value'] for val in cluster_details["cluster_tags"] if val['Key'] == 'ZS_Project_Code')
            service_catalog["purpose"] = cluster_configs['cluster_name']
            service_catalog["environment"] = self.environment
            service_catalog["region"] = cluster_configs['region_name']
            service_catalog["vpcID"] = cluster_configs['vpc_id']
            service_catalog["subnetID"] = cluster_configs['subnet_id']
            service_catalog["releaseLabel"] = cluster_configs['release_version']
            service_catalog["scriptLocations"] = cluster_details['cluster_bootstrap_s3_location'] + ' ' +\
                                                 bucket_name + ';' +self.emr_code_path+';'\
                                                 + self.environment+';'+cluster_details['cc_branch_name']\
                                                 + ';' +cluster_details['dm_branch_name']+';' +\
                                                  self.bitbucket_username + ';' +self.bitbucket_password
            service_catalog["applications"] = ', '.join(val['Name'] for val in cluster_details["applications"])
            service_catalog["logBucket"] = cluster_details['log_uri']
            service_catalog["minUnits"] = CommonConstants.MIN_NODES
            service_catalog["maxUnits"] = CommonConstants.MAX_NODES
            service_catalog["configBucket"] = cluster_details['config_bucket']
            service_catalog["keyPair"] = cluster_configs['key_pair_name']
            service_catalog["instanceTypeMaster"] = cluster_configs['master_instance_type']
            service_catalog["instanceTypeSlave"] = cluster_configs['core_instance_type']
            service_catalog["coreInstanceCount"] = cluster_configs['num_core_instances']

            provisioning_params = [{key: value} for key, value in service_catalog.items()]
            #creating the provisioning param according to cluster configs

            provisioned_product_id = self.provision_product_async(provisioned_product_name, provisioning_params, 'emr-cluster', '1.0.0', self.environment)
            cluster_id = None
            tenantCode = ""
            appCode = ""
            purpose = ""
            count = ""
            for val in provisioning_params:
                if 'tenantCode' in val:
                    tenantCode = val['tenantCode']
                elif 'appCode' in val:
                    appCode = val['appCode']
                elif 'purpose' in val:
                    purpose = val['purpose']
                elif 'count' in val:
                    count = val['count']

            cluster_name = self.emr_cluster_name+"-"+tenantCode+"-"+appCode+"-"+purpose+count
            counter = 0
            while not cluster_id and counter < 5:
                cluster_id = self.check_cluster_status(cluster_name)
                if cluster_id:
                    print(f"Cluster is available. Cluster ID: {cluster_id}")
                    #applying managed cale policy
                    cluster_policy = self.managed_scale_policy(cluster_id,cluster_configs['region_name'],CommonConstants.MIN_NODES,CommonConstants.MAX_NODES)
                    print(f"Manged scale policy applied on the cluster {cluster_id}")
                    return cluster_id
                else:
                    print("Cluster is not yet available. Checking again in 30 seconds...")
                    time.sleep(125)
                    counter = counter + 1
            if counter == 5 and not cluster_id:
                self.terminate_provisioned_product_async(provisioned_product_name)
                raise Exception("Error with the parameters terminated the provisioned product")
        except Exception as e:
            status_message = "Error Occurred when invoking EMR cluster. Exiting"
            print("error message : ", status_message)
            raise e
