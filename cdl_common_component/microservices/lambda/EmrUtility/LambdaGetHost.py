# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import json
import boto3
import logging


def validate_get_host_payload(payload):
    """
    Validates payload
    :param payload:
    :return:
    """
    try:
        cluster = payload.get("cluster_id", "")
        region_name = payload.get("aws_region", "")
        instance_group_types_list = payload.get("instance_types", [])
        instance_states = payload.get("instance_states", [])
        if not cluster:
            raise Exception("cluster_id is a mandatory parameter.")
        if not region_name:
            raise Exception("aws_region is a mandatory parameter.")
        if not type(instance_group_types_list) is list:
            raise Exception("instance_types has to be list.")
        if not type(instance_states) is list:
            raise Exception("instance_states has to be list.")
        return True
    except Exception as exc:
        logging.error(str(exc))
        raise Exception(str(exc))


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
    except Exception as exc:
        raise Exception(str(exc))


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


def add_optional_keys(input_payload):
    """
    Adds optional keys to payload
    :return:
    """
    try:
        print("Adding optional keys")
        optional_keys = ["instance_states", "instance_types"]
        for each_optional_key in optional_keys:
            input_payload.setdefault(each_optional_key, "")
            input_payload[each_optional_key] = input_payload[each_optional_key].split(",")

            # Checking empty string present in list
            for list_index in range(len(input_payload[each_optional_key])):
                if not input_payload[each_optional_key][list_index]:
                    del(input_payload[each_optional_key][list_index])
        return input_payload
    except Exception as ex:
        logging.error(str(ex))
        raise Exception(str(ex))


class FetchIp(object):
    """
        To fetch IP of EMR cluster machines
    """

    def __init__(self, payload):
        self.cluster_id = payload['cluster_id']
        self.region_name = payload['aws_region']
        self.instance_states = payload.get("instance_states", [])
        self.instance_group_types_list = payload.get("instance_types", [])

    def get_host_ip(self):
        """
        Method to get IPs
        :return:
        """
        try:
            ip_dict = dict()
            client = boto3.client('emr', region_name=self.region_name)
            response = dict()
            if not self.instance_group_types_list:
                if not self.instance_states:
                    response.update(client.list_instances(ClusterId=self.cluster_id))
                else:
                    response.update(
                        client.list_instances(ClusterId=self.cluster_id, InstanceStates=self.instance_states))
            else:
                if not self.instance_states:
                    response.update(client.list_instances(
                        ClusterId=self.cluster_id, InstanceGroupTypes=self.instance_group_types_list))
                else:
                    response.update(
                        client.list_instances(
                            ClusterId=self.cluster_id,
                            InstanceStates=self.instance_states,
                            InstanceGroupTypes=self.instance_group_types_list))

            num_instances = len(response['Instances'])
            if self.instance_states:
                for input_instance_state in self.instance_states:
                    ip_dict.setdefault(input_instance_state, [])
            for index in range(num_instances):
                instance_state = response['Instances'][index]['Status']['State']
                print("Instance State: " + instance_state)
                if self.instance_states:
                    if instance_state in self.instance_states:
                        ip_dict[instance_state].append(response['Instances'][index]['PrivateIpAddress'])
                else:
                    ip_dict.setdefault(instance_state, [])
                    instance_state = response['Instances'][index]['Status']['State']
                    ip_dict.setdefault(instance_state, [])
                    ip_dict[instance_state].append(response['Instances'][index]['PrivateIpAddress'])
            return ip_dict
        except Exception as exc:
            logging.error(str(exc))
            raise Exception(exc)


def lambda_handler(event, context):
    try:
        event = add_optional_keys(event)
        print(event)
        validate_get_host_payload(event)
        ip_fetch_obj = FetchIp(event)
        return success_response_formatter(ip_fetch_obj.get_host_ip())
    except Exception as ex:
        logging.error(str(ex))
        return error_response_formatter("Error occurred while fetching EMR host IPs: " + str(ex))


if __name__ == '__main__':
    input_payload = {
        "cluster_id": "abcdef",
        "aws_region": "us-east-1",
        "instance_states": "RUNNING,BOOTSTRAPPING"
    }

    print(add_optional_keys(input_payload))
