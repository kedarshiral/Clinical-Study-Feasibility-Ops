# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import json
import boto3
import logging


class FetchEmrMetadata(object):

    def __init__(self, payload):
        self.cluster_id = payload['cluster_id']
        self.region_name = payload['aws_region']

    def get_emr_metadata(self):
        """
        Module to fetch the EMR Cluster Status
        :return:
        """
        try:
            client = boto3.client("emr", region_name=self.region_name)
            res = client.describe_cluster(ClusterId=self.cluster_id)
            ready_date_time = str(res['Cluster']['Status']['Timeline']['ReadyDateTime']).split(".")[0]
            creation_date_time = str(res['Cluster']['Status']['Timeline']['CreationDateTime']).split(".")[0]
            cluster_details = {
                "Status": res['Cluster']['Status']['State'],
                "DNS": res['Cluster']['MasterPublicDnsName'],
                "ReadyDateTime": ready_date_time,
                "CreationDateTime": creation_date_time
            }
            return cluster_details
        except Exception as ex:
            raise Exception(str(ex))


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


def lambda_handler(event, context):
    try:
        validate_termination_payload(event)
        metadata_obj = FetchEmrMetadata(event)
        print(success_response_formatter(metadata_obj.get_emr_metadata()))
    except Exception as ex:
        logging.error(str(ex))
        print(error_response_formatter("Error occurred while fetching EMR metadata: " + str(ex)))
