# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import boto3


class EmrStatus(object):

    def __init__(self, payload):
        self.cluster_id = payload["cluster_id"]
        self.region_name = payload["aws_region"]

    def get_status(self):
        """
        Returns state of an EMR cluster
        :return:
        """
        try:
            client = boto3.client('emr', region_name=self.region_name)
            res = client.describe_cluster(ClusterId=self.cluster_id)
            cluster_state = res['Cluster']['Status']['State']
            result = {
                'status': cluster_state
            }
            return success_response_formatter(result)
        except Exception as ex:
            print(str(ex))
            return error_response_formatter("Error occurred while fetching cluster state: " + str(ex))


def validate_emr_status_payload(input_payload):
    """
    Validates input payload
    :param input_payload:
    :return:
    """
    cluster = input_payload.get("cluster_id", "")
    region_name = input_payload.get("aws_region", "")
    if not cluster:
        raise Exception("'cluster_id' is a mandatory parameter.")
    if not region_name:
        raise Exception("'aws_region' is mandatory parameter.")
    return True


def success_response_formatter(result):
    """
    Pushes result into success response template
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
    Pushes error message into error response template
    :param error:
    :return:
    """
    try:
        error_response = {
            "status": "FAILED",
            "result": dict(),
            "error": str(error)
        }
        return error_response
    except Exception as ex:
        raise Exception(str(ex))


def lambda_handler(event, context):
    try:
        validate_emr_status_payload(event)
        status_check = EmrStatus(event)
        return status_check.get_status()
    except Exception as ex:
        error_message = "Error occurred while fetching cluster state: " + str(ex)
        return error_response_formatter(error_message)
