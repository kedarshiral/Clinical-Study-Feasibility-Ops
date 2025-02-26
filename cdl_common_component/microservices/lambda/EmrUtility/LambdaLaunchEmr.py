# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import boto3
import json
import logging
# logging.basicConfig(filename='example.log',level=logging.DEBUG)


def validate_tag_keys_in_payload(tags, list_of_tag_keys):
    """
    Validates tag keys and values
    :param tags:
    :param list_of_tag_keys:
    :return:
    """
    try:
        logging.info("Validating Tags...")
        for each_tag_key in list_of_tag_keys:
            tag_key_exists = False
            value_empty_key = True
            for each_tag in tags:
                if each_tag.get('Key', '') == each_tag_key:
                    tag_key_exists = True
                    if each_tag.get('Value', ''):
                        value_empty_key = False
            if not tag_key_exists:
                raise Exception("Mandatory tag key '" + each_tag_key + "' is not present.")
            if value_empty_key:
                raise Exception("Mandatory tag key '" + each_tag_key + "' cannot be empty.")
    except Exception as ex:
        raise Exception(str(ex))


def validate_emr_launch_payload(input_payload):
    """
    Basic validation module for input payload
    :param input_payload:
    :return:
    """
    try:
        list_of_keys = [
            'Region',
            'Name',
            'LogUri',
            'AmiVersion',
            'ReleaseLabel',
            'Instances',
            'BootstrapActions',
            'Applications',
            'VisibleToAllUsers',
            'JobFlowRole',
            'ServiceRole',
            'Tags',
        ]
        list_of_tag_keys = ['ZS_Project_Code']
        for each_key in list_of_keys:
            if not input_payload.get(each_key, ''):
                raise Exception("'" + str(each_key) + "' is a mandatory key.")
        validate_tag_keys_in_payload(input_payload['Tags'], list_of_tag_keys)
        return True
    except Exception as ex:
        raise Exception(str(ex))


def success_response_formatter(result):
    """
    Success response formatting module
    :param result:
    :return:
    """
    try:
        success_response = {
            "status": "SUCCESS",
            "result": result,
            "error": ""
        }
        return success_response
    except Exception as ex:
        raise Exception(str(ex))


def error_response_formatter(error):
    """
    Error response formatting module
    :param error:
    :return:
    """
    error_response = {
        "status": "FAILED",
        "result": dict(),
        "error": str(error)
    }
    return error_response


class EmrLaunch(object):
    """
        EMR Launching module
    """

    def __init__(self, event):
        self.region_name = event['Region']
        self.cluster_name = event['Name']
        self.log_uri = event['LogUri']
        self.ami_version = event['AmiVersion']
        self.release_label = event['ReleaseLabel']
        self.instance_details = event['Instances']
        self.bootstrap_actions = event['BootstrapActions']
        self.applications = event['Applications']
        self.visible_to_all_users = event['VisibleToAllUsers']
        self.job_flow_role = event['JobFlowRole']
        self.service_role = event['ServiceRole']
        self.tags = event['Tags']

    def launch_emr_cluster(self):
        """
        Launches EMR cluster
        :return:
        """
        try:
            logging.info("Attempting to create boto client")
            client = boto3.client('emr', region_name=self.region_name)

            logging.info("Attempting to create cluster")
            result = client.run_job_flow(
                Name=self.cluster_name,
                LogUri=self.log_uri,
                ReleaseLabel=self.ami_version,
                Instances=self.instance_details,
                BootstrapActions=self.bootstrap_actions,
                Applications=self.applications,
                VisibleToAllUsers=self.visible_to_all_users,
                JobFlowRole=self.job_flow_role,
                ServiceRole=self.service_role,
                Tags=self.tags
            )

            job_id = result['JobFlowId']
            logging.debug("Cluster ID: " + str(job_id))

            result = {
                "cluster_id": job_id
            }

            response = success_response_formatter(result)
            return response
        except Exception as ex:
            logging.error("Error occurred while starting job flow: " + str(ex))
            error_message = str(ex)
            raise Exception(error_message)


def lambda_handler(event, context):
    """
    Lambda handler
    :param event:
    :param context:
    :return:
    """
    try:
        validate_emr_launch_payload(event)
        launcher = EmrLaunch(event)
        return launcher.launch_emr_cluster()
    except Exception as ex:
        error_message = "Error occurred while launching cluster: " + str(ex)
        return error_response_formatter(error_message)