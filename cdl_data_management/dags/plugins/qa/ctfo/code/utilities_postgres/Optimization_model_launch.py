"""Handler to ECS task execute"""
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

"""
######################################################Module Information#########################
#   Module Name         :   To launch sagemaker
#   Purpose             :   Module to define application logic
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   NA
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Last changed on     :   11-May-2023
#   Last changed by     :   Shivansh Bhasin
#   Reason for change   :   Development
#################################################################################################
"""

# Import Tool Packages
import os
import boto3
import json
import logging
import time
import datetime
import random
import traceback
import uuid
import time
import CommonServicesConstants as Constants
from datetime import datetime
from TableUpdateUtility import UpdateTableLogs
import SageMakerLaunchUtility
# from SQSUtility import Aws_sqs_utility
from utils import Utils
import sys
from utilities.LogSetup import get_logger
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

LOGGING = get_logger()
def lambda_handler(output_sqs):
    """
    Description:
        This function is used to run ECS tasks based on the number of SQS messages
        and number of waiting/failed.
    Usage:
        lambda_handler(event, context)
    Arguments:
        event, context
    Returns: NA
    """
    environment_ = None
    study_id = None
    scenario_id = None
    theme_id = None
    scenario_version = None
    created_by = None
    queue_url = None
    sqs_region = None
    msg_receipt_handle = None
    user_id = ""
    is_cohort = ""
    model_run_id = None

    try:
        obj_util = Utils()

        print("Starting to execute Optimization Model Launch Utility")
        # environment_ = os.environ["environment"]
        env_config = obj_util.get_env_config_json()
        LOGGING.info("env config%s", str(env_config))
        environment_ = env_config["environment"]
        app_config = obj_util.get_application_config_json()
        s3_bucket = env_config[Constants.S3_BUCKET]
        RoleArn = env_config[Constants.ROLE_ARN]
        processing_script = app_config[Constants.KEY_ECS][Constants.KEY_OPTIMIZATION_FILE_PATH]
        processing_image_uri = env_config[Constants.ECR_IMAGE]
        ZS_Requestor = app_config[Constants.KEY_TAG][Constants.KEY_TAG_ZS_REQUESTOR]
        ZS_Environment = app_config[Constants.KEY_TAG][Constants.KEY_TAG_ZS_ENV]
        ZS_Client = app_config[Constants.KEY_TAG][Constants.KEY_TAG_ZS_CLIENT]
        ZS_Project_Code = app_config[Constants.KEY_TAG][Constants.KEY_TAG_ZS_PROJ_CODE]
        ZS_Product = app_config[Constants.KEY_TAG][Constants.KEY_TAG_ZS_PRODUCT]
        ZS_Project_Name = app_config[Constants.KEY_TAG][Constants.KEY_TAG_ZS_PROJ_NM]
        LOGGING.info("env config%s", str(s3_bucket)+str(RoleArn)+str(processing_image_uri))
        tag_list = [
            {
                'key': Constants.KEY_TAG_ZS_REQUESTOR,
                'value': ZS_Requestor
            },
            {
                'key': Constants.KEY_TAG_ZS_ENV,
                'value': ZS_Environment
            },
            {
                'key': Constants.KEY_TAG_ZS_CLIENT,
                'value': ZS_Client
            },
            {
                'key': Constants.KEY_TAG_ZS_PROJ_CODE,
                'value': ZS_Project_Code
            },
            {
                'key': Constants.KEY_TAG_ZS_PRODUCT,
                'value': ZS_Product
            },
            {
                'key': Constants.KEY_TAG_ZS_PROJ_NM,
                'value': ZS_Project_Name
            }
        ]

        # Get the message count from SQS Queue
        # output_msg_count = aws_sqs_utility_obj.get_sqs_message_count(queue_url, sqs_region)
        # if output_msg_count[Constants.RESULT_KEY][Constants.KEY_MESSAGE_COUNT] < 1:
        #     print("No Messages Present in SQS")
        #     return None
        #
        # print("Total messages found in SQS Queue : " + str(
        #     output_msg_count[Constants.RESULT_KEY][Constants.KEY_MESSAGE_COUNT]))
        #
        # threshold_value = app_config[Constants.KEY_ECS][Constants.MAX_TASK_THRESHOLD]

        attrib_name = 'All'

        # Checking count of visible message in SQS , if more than 1 Fetching message from SQS
        # output_sqs = aws_sqs_utility_obj.sqs_receive_message(queue_url, sqs_region,
        #
        #                                                      max_number_of_messages, attrib_name)
        LOGGING.info("Check list type %s", type(output_sqs))
        print(output_sqs)
        LOGGING.info("output_sqs --> %s", str(output_sqs))
        sqs_msg_str = json.loads(output_sqs)
        # msg_receipt_handle = output_sqs[Constants.RESULT_KEY][Constants.KEY_RECEIPT_HANDLE]
        LOGGING.info("Check list type %s", type(output_sqs))

        model_run_id = sqs_msg_str["model_run_id"]
        print(model_run_id)

        if sqs_msg_str is None or len(sqs_msg_str) == 0:
            print("No SQS message is empty")
            raise Exception("No SQS message found")

        if sqs_msg_str:
            input_request = sqs_msg_str
            print("input_request", input_request)



        scenario_id = input_request['scenario_id']
        theme_id = input_request['theme_id']
        optimize_id = input_request['optimize_id']
        site_id = input_request['site_list_id']
        user_id = input_request['user_id']
        created_by = user_id
        print("@@@@@@taglist_requestor:", tag_list[0]['value'])

        # exporting the file to S3
        input_path_1 = "clinical-data-lake/$$environment$$/applications/study_feasibility_optimisation/processed/processing_jobs/$$user_id$$/$$model_run_id$$/"
        input_path = input_path_1.replace("$$user_id$$", input_request["user_id"]).replace("$$model_run_id$$",
                                                                                          model_run_id).replace(
            '$$environment$$', environment_)
        input_path = input_path + "input_file/"
        # Convert Dictionary to JSON String
        data = input_request
        data_string = json.dumps(data, indent=2, default=str)
        # Upload JSON String to an S3 Object
        s3_resource = boto3.resource('s3')
        s3_bucket = s3_resource.Bucket(name=s3_bucket)
        s3_bucket.put_object(
            Key=input_path + 'input_data.json',
            Body=data_string)
        s3_input_path = "s3://" + env_config[Constants.S3_BUCKET] + "/" + input_path + "input_data.json"
        s3_output_path = "s3://" + env_config[Constants.S3_BUCKET] + "/" + input_path_1 + "output_data.json"
        s3_output_path = s3_output_path.replace("$$user_id$$", input_request["user_id"]).replace("$$model_run_id$$",
                                                                                                model_run_id).replace(
            '$$environment$$', environment_)
        print(s3_input_path)
        #  creating input and output dictionary for sagemaker
        input_data = {
            'InputName': 'input',
            'S3Input': {
                'S3Uri': s3_input_path,
                'LocalPath': '/opt/ml/processing/input',
                'S3DataType': 'S3Prefix',
                'S3InputMode': 'File',
                'S3DataDistributionType': 'FullyReplicated',
            }
        }

        output_data = {
            'OutputName': 'output',
            'S3Output': {
                'S3Uri': s3_output_path,
                'LocalPath': '/opt/ml/processing/output',
                'S3UploadMode': 'EndOfJob'
            }
        }

        # Getting json values from message

        scenario_id = input_request[Constants.SCENARIO_ID_KEY]
        theme_id = input_request[Constants.THEME_ID]
        created_by = input_request[Constants.USER_ID]
        # param_estimation_tech = input_request[Constants.KEY_PARAM_ESTIMATION_TECH]
        # forecast_type = input_request[Constants.KEY_FORECAST_TYPE]

        # Get the command path and API URL as per the study feasibility type
        # optimization_file_path = app_config[Constants.KEY_ECS][Constants.KEY_OPTIMIZATION_FILE_PATH]

        id = uuid.uuid1()
        job_name = id.hex

        print(job_name)

        # Launching ECS Task - launching the instance with sagemaker
        output = SageMakerLaunchUtility.launch_sagemaker_instance(input_data, output_data, environment_, job_name, RoleArn,
                                                          processing_script, processing_image_uri, site_id, scenario_id,
                                                          theme_id, optimize_id, user_id,model_run_id)
        print("model Output is")
        print(output)
        processing_job_id = output['processing_job_id']

        print('processing_job_id is')
        print(processing_job_id)
        # Deleting messsage from SQS
        if output[Constants.STATUS_KEY] == Constants.STATUS_SUCCESS:
            # updating status (ecs task ARN) in table::- protocol_ingestion_ds_model_logs
            # ecs_task_arn = output[Constants.RESULT_KEY]["task_arn"]
            created_timestamp = datetime.now()
            description = "Optimization model service launched successfully"
            UpdateTableLogs(environment_).update_log_status(
                status=Constants.STATUS_LAUNCHED, study_id=study_id, scenario_id=scenario_id,
                model_run_id=model_run_id, theme_id=theme_id,
                scenario_version=scenario_version, created_by=created_by,
                description=description, created_timestamp=created_timestamp, processing_job_id=processing_job_id)
            return {
                'processing_job_id': processing_job_id,
                'body': 'SageMaker processing job created successfully',
                'status': "SUCCESS"
            }



    except Exception as error_:
        created_timestamp = datetime.now()
        print(f'Error on line {sys.exc_info()[-1].tb_lineno}')
        print("Failed in Lambda Handler. ERROR : " + str(traceback.print_exc()))
        # Update the status to FAILED in log table

        description = "Failed to run enrollment mode. Error - " + str(error_)
        comment = ""
        if model_run_id is not None:
            UpdateTableLogs(environment_).update_log_status(status=Constants.STATUS_FAILED,
                                                                               study_id=study_id,
                                                                               scenario_id=scenario_id,
                                                                               model_run_id=model_run_id,
                                                                               theme_id=theme_id,
                                                                               scenario_version=scenario_version,
                                                                               created_by=created_by,
                                                                               description=description,
                                                                               created_timestamp=created_timestamp, processing_job_id=processing_job_id)
