# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import boto3
import json
import logging


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


def validate_termination_payload(input_payload):
    """
    Validates payload
    :param input_payload:
    :return:
    """
    try:
        cluster = input_payload.get("cluster_id", "")
        region_name = input_payload.get("aws_region", "")
        if not cluster:
            raise Exception("cluster_id is a mandatory parameter.")
        if not region_name:
            raise Exception("aws_region is a mandatory parameter.")
        return True
    except Exception as ex:
        logging.error(str(ex))
        raise Exception(str(ex))


class EmrTermination(object):
    """
        Termination of EMR cluster
    """
    def __init__(self, event):
        """

        :param event:
        """
        self.region_name = event['aws_region']
        self.cluster_id = event['cluster_id']

    def terminate_emr(self):
        """
        Terminates EMR cluster
        :return:
        """
        try:
            client = boto3.client('emr', region_name=self.region_name)
            client.set_termination_protection(
                JobFlowIds=[self.cluster_id],
                TerminationProtected=False
            )
            client.terminate_job_flows(JobFlowIds=[self.cluster_id])
            success_message = "EMR Termination successful: " + str(self.cluster_id)
            response = success_response_formatter(success_message)
            print(json.dumps(response))
            return response
        except Exception as ex:
            raise Exception(str(ex))


def lambda_handler(event, context):
    try:
        validate_termination_payload(event)
        termation_obj = EmrTermination(event)
        return termation_obj.terminate_emr()
    except Exception as ex:
        error_message = "Termination of EMR failed: " + str(ex)
        logging.error(error_message)
        error_response = error_response_formatter(error_message)
        return error_response