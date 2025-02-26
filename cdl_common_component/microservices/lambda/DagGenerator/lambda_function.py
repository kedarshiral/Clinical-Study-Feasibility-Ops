#!/usr/bin/python

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import pymysql
import json
import boto3
from botocore.exceptions import ClientError
import logging
from configparser import RawConfigParser
import base64

"""
 Module Name         :   Dag Generator
 Purpose             :   This Wrapper Will Generate Dag
 Input Parameters    :   process_id
 Output Value        :   DAG file to S3
 Pre-requisites      :
 Last changed on     :   12 December, 2018
 Last changed by     :   J. Akshay Shankar
 Reason for change   :   To pass dag template as an input argument to Generate DAG python module
"""

MODULE_NAME = "Generate_Dag"
PROCESS_NAME = 'Generate Dag From Template'

logger = logging.getLogger('Generate_dag')
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


CONFIGURATION_FILE = "settings.conf"
parser = RawConfigParser()
with open(CONFIGURATION_FILE) as conf_file:
    parser.readfp(conf_file)


def get_secret(secret_name, region_name):
    """

    :param secret_name:
    :param region_name:
    :return:
    """

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret


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


def replace_text(base_text, replace_dictionary):
    """
    Replaces each key present in replace dictionary with value in dictionary in base text
    :param base_text:
    :param replace_dictionary:
    :return:
    """
    for word, replace_str in replace_dictionary.items():
        if type(replace_str) is list:
            for index in range(len(replace_str)):
                replace_str[index] = replace_str[index].encode('UTF8')
        try:
            int(replace_str)
            replace_str = "\"" + replace_str + "\""
        except Exception:
            pass
        base_text = base_text.replace(word, str(replace_str))
    return base_text


class GenerateDag(object):
    """
    Generates Airflow DAGs using DAG template
    """

    def __init__(self, input_payload):
        """
        Constructor
        :param input_payload:
        """
        try:
            self.process_id = str(input_payload.get("process_id", ""))

            self.dataset_id_list = []
            self.process_name = ""
            self.workflow_name = ""

            self.airflow_code_path = input_payload.get("airflow_code_path", "")
            self.db_name = input_payload.get("db_name", "")

            # Read from secrets manager
            secrets_name = input_payload.get("secret_name", "")
            region = input_payload.get("region_name", "")
            secrets_parameters = json.loads(get_secret(secrets_name, region))
            self.rds_host = secrets_parameters["host"]
            self.port = int(secrets_parameters["port"])
            self.user_name = secrets_parameters["username"]
            self.password = secrets_parameters["password"]

            # Local files
            self.local_dag_template_file = input_payload.get("local_dag_template_file", "")
            self.updated_dag_template_file = input_payload.get("updated_dag_template_file", "")

            self.dag_template = {
                "s3_bucket": input_payload.get("dag_template_s3_bucket", ""),
                "template_path": input_payload.get("dag_template_s3_path", "")
            }
            self.dag_destination = {
                "s3_bucket": input_payload.get("dag_destination_s3_bucket", ""),
                "destination_path": input_payload.get("dag_destination_s3_path", "")
            }

            self.replace_dictionary = {}

        except Exception as ex:
            message = "Error occurred while initialisation"
            logging.error(message + ": " + str(ex))
            raise Exception(message)

    def get_replace_dictionary(self):
        """
        Creates replace dictionary
        :return:
        """
        return {
            "python_scripts_path": self.airflow_code_path,
            "Dataset_Id_List": self.dataset_id_list,
            "p_id": self.process_id,
            "dagname": self.workflow_name + "_" + self.process_id
        }

    def get_dataset_ids(self):
        """
        Fetches dataset ID list from RDS
        :return:
        """
        dataset_id_list = []
        try:

            conn = pymysql.connect(self.rds_host, self.user_name, self.password, self.db_name, self.port)
            query = "Select " + "dataset_id" + " from " + "ctl_workflow_master" + " where " + \
                    "process_id" + "='" + str(self.process_id) + "' and " + "active_flag" + "='" + "Y" + "'"
            with conn.cursor() as cur:
                cur.execute(query)
                conn.close()
            result = cur.fetchall()
            for id_data in result:
                dataset_id_list.append(str(int(id_data[0])))
            return dataset_id_list
        except Exception as ex:
            logging.error("Error occurred while fetching dataset IDs: " + str(ex))
            return None

    def get_process_name(self):
        """
        Fetches process name from RDS
        :return:
        """
        try:
            conn = pymysql.connect(self.rds_host, self.user_name, self.password, self.db_name)
            query = "Select " + "process_name" + " from " + \
                    "ctl_cluster_config" + " where " + "process_id" + "='" + \
                    str(self.process_id) + "' and " + "active_flag" + "='" + \
                    "Y" + "'"
            with conn.cursor() as cur:
                cur.execute(query)
                conn.close()
            process_name = cur.fetchone()[0]
            return process_name

        except Exception as ex:
            logging.error("Error occurred while fetching process name.")
            return None

    def get_workflow_name(self):
        """
        Fetches workflow name from RDS
        :return:
        """
        try:
            conn = pymysql.connect(self.rds_host, self.user_name, self.password, self.db_name)
            query = "Select " + "workflow_name" + " from " + "ctl_workflow_master" + " where " + \
                    "process_id" + "='" + str(self.process_id) + "' and " + "active_flag" + "='" + \
                    "Y" + "'"
            with conn.cursor() as cur:
                cur.execute(query)
                conn.close()
            workflow_name = cur.fetchone()[0]
            return workflow_name
        except Exception as ex:
            logging.error("Error occurred while workflow name.")
            return None

    def validate_input(self):
        """
        Validates input payload
        :return:
        """
        try:
            if not self.process_id:
                status_message = "'Process ID' is a mandatory parameter."
                raise Exception(status_message)

            self.dataset_id_list = self.get_dataset_ids()
            self.process_name = self.get_process_name()
            self.workflow_name = self.get_workflow_name()

            self.replace_dictionary = self.get_replace_dictionary()

            if not self.dataset_id_list or not type(self.dataset_id_list) is list:
                status_message = "'Dataset ID list' corresponding to process ID '"\
                                 + str(self.process_id) + "' could not be found."
                raise Exception(status_message)
            if not self.process_name:
                status_message = "'Process name' corresponding to process ID '"\
                                 + str(self.process_id) + "' could not be found."
                raise Exception(status_message)
            if not self.workflow_name:
                status_message = "'Workflow name' corresponding to process ID '"\
                                 + str(self.process_id) + "' could not be found."
                raise Exception(status_message)
            if self.dag_template:
                if not type(self.dag_template) is dict:
                    raise Exception("'Dag Template' expects data of type dictionary.")
                s3_file_path = self.dag_template.get("template_path", "")
                if not s3_file_path:
                    status_message = "S3 path for DAG template file not specified."
                    raise Exception(status_message)
                else:
                    bucket_name = self.dag_template.get("s3_bucket", "")
                    if not bucket_name:
                        status_message = "S3 bucket for DAG template file is not specified."
                        raise Exception(status_message)
                    else:
                        s3 = boto3.resource('s3')
                        try:
                            s3.Bucket(bucket_name).download_file(s3_file_path, self.local_dag_template_file)
                        except Exception as ex:
                            logging.error(str(ex))
                            status_message = "DAG template provided does not exists or is a directory"
                            raise Exception(status_message)
            if self.dag_destination:
                if not type(self.dag_destination) is dict:
                    raise Exception("'Dag destination' expects data of type dictionary.")
                s3_file_path = self.dag_destination.get("destination_path", "")
                if not s3_file_path:
                    status_message = "S3 path for DAG destination not specified."
                    raise Exception(status_message)
                else:
                    bucket_name = self.dag_destination.get("s3_bucket", "")
                    if not bucket_name:
                        status_message = "S3 bucket for DAG destination is not specified."
                        raise Exception(status_message)

        except Exception as ex:
            logging.error("Error occurred while validating input payload.")
            raise Exception(str(ex))

    def replace_placeholders_in_template(self):
        """
        Replaces placeholders in DAG template and creates DAG file
        :return:
        """
        try:
            dag_template = open(self.local_dag_template_file, 'r')
            content_of_dag_template = dag_template.read()
            updated_dag_content = replace_text(content_of_dag_template, self.replace_dictionary)
            updated_dag = open(self.updated_dag_template_file, 'w')
            updated_dag.write(updated_dag_content)
            updated_dag.close()
        except Exception as ex:
            logging.error("Error occurred while replacing placeholders in DAG template")
            raise Exception(str(ex))

    def push_dag_to_destination_path(self):
        """
        Pushes DAG to destination S3 path
        :return:
        """
        try:
            s3 = boto3.client('s3')
            s3.upload_file(
                self.updated_dag_template_file, self.dag_destination["s3_bucket"],
                self.dag_destination["destination_path"] + "DAG_{}_{}.py".format(
                    self.workflow_name, self.process_id
                ))
            status_message = "DAG generated successfully to S3 bucket: {} in path: {}".format(
                self.dag_destination["s3_bucket"], self.dag_destination["destination_path"])
            response = {
                "s3_bucket": self.dag_destination["s3_bucket"],
                "destination_path": self.dag_destination["destination_path"],
                "message": status_message
            }
            return response
        except Exception as ex:
            logging.error("Error occurred while pushing DAG to S3")
            raise Exception(str(ex))


def lambda_handler(event, context):
    """
    Lambda handler
    :param context:
    :param event:
    :return:
    """
    try:

        # Local DAG template path
        local_dag_template_file = parser.get('PATH', 'local_dag_template_file').encode()

        # Local DAG file
        updated_dag_template_file = parser.get('PATH', 'updated_dag_template_file').encode()

        # Airflow code path
        airflow_code_path = parser.get('PATH', 'airflow_code_path').encode()

        # DAG template in S3
        dag_template_s3_bucket = parser.get('DAG_TEMPLATE', 's3_bucket').encode()
        dag_template_s3_path = parser.get('DAG_TEMPLATE', 'template_path').encode()

        # DAG destination in S3
        dag_destination_s3_bucket = parser.get('DAG_DESTINATION', 's3_bucket').encode()
        dag_destination_s3_path = parser.get('DAG_DESTINATION', 'destination_path').encode()

        # Secrets Manager
        secrets_name = parser.get('SECRETS', 'secret_name').encode()
        region = parser.get('SECRETS', 'region_name').encode()

        # RDS MySQL database name
        db_name = parser.get('DB', 'database').encode()

        event["local_dag_template_file"] = local_dag_template_file
        event["updated_dag_template_file"] = updated_dag_template_file
        event["airflow_code_path"] = airflow_code_path
        event["dag_template_s3_bucket"] = dag_template_s3_bucket
        event["dag_template_s3_path"] = dag_template_s3_path
        event["dag_destination_s3_bucket"] = dag_destination_s3_bucket
        event["dag_destination_s3_path"] = dag_destination_s3_path

        event["secret_name"] = secrets_name
        event["region_name"] = region

        event["db_name"] = db_name

        dag_gen_obj = GenerateDag(event)
        dag_gen_obj.validate_input()
        dag_gen_obj.replace_placeholders_in_template()
        return success_response_formatter(dag_gen_obj.push_dag_to_destination_path())
    except Exception as ex:
        error_message = "Error occurred while generating DAG."
        logging.error(error_message)
        return error_response_formatter(error_message.strip(".") + ": " + str(ex))

