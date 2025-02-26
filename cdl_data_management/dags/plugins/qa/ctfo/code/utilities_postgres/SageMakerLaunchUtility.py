"""Handler to ECS task execute"""
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

"""
###################################################### Module Information #########################
#   Module Name         :  SageMakerLaunchUtility
#   Purpose             :   Module to launch SageMaker Insatance
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   NA
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Last changed on     :   24-May-2023
#   Last changed by     :   Shivansh Bhasin
#   Reason for change   :   Development
#################################################################################################
"""

import boto3
import time
import json
import traceback
import requests
import CommonServicesConstants as Constants
from botocore.exceptions import ClientError
from botocore.config import Config
from utils import Utils



def launch_sagemaker_instance(input_data, output_data, environment_, job_name, RoleArn, processing_script,
                              processing_image_uri, site_id, scenario_id, theme_id, optimize_id, user_id,model_run_id):

    return_status = {
        Constants.STATUS_KEY: Constants.STATUS_SUCCESS,
        Constants.RESULT_KEY: {}
    }
    try:
        obj_util = Utils()
        env_config = obj_util.get_env_config_json()
        config = Config(
            retries=dict(
                max_attempts=10
            )
        )
        print("Inside launch")

        processing_job = {
            'ProcessingInputs': [input_data],
            'ProcessingOutputConfig': {
                'Outputs': [output_data]
            },
            'ProcessingJobName': job_name,
            'ProcessingResources': {
                'ClusterConfig': {
                    'InstanceCount': 1,
                    'InstanceType': 'ml.m5.large',
                    'VolumeSizeInGB': 30
                }
            },
            "NetworkConfig": {

                "EnableNetworkIsolation": False,
                "VpcConfig": {
                    "SecurityGroupIds": env_config["SecurityGroupIds"],
                    "Subnets": env_config["Subnets"]
                }
            },
            'RoleArn': RoleArn,
            'AppSpecification': {
                'ImageUri': processing_image_uri,
                'ContainerEntrypoint': ['python3', processing_script, site_id, scenario_id, theme_id, optimize_id,
                                        user_id,model_run_id]
            },
            "StoppingCondition": {
                "MaxRuntimeInSeconds": 7200
            },
            "Environment": {
                "env": environment_
            },
        }

        print("Starting processing job")
        client = boto3.client('sagemaker', config=config)
        response = client.create_processing_job(**processing_job)
        print(response)
        processing_job_id = response['ProcessingJobArn']

        return {
            'processing_job_id': processing_job_id,
            'statusCode': 200,
            'body': 'SageMaker processing job created successfully',
            'status': "SUCCESS"
        }

    except ClientError as e:
        print(traceback.format_exc())
        return_status[Constants.STATUS_KEY] = Constants.STATUS_FAILED
        return_status[Constants.ERROR_KEY] = str(e)
        return return_status

