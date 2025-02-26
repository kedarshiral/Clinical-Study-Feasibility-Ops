"""
  Module Name         :   DataIngestionAdapter
  Purpose             :   This module performs below operation:
                              a. extract data from pharma_intelligence data sources
                              b. Perform logging in delta table
  Input               :  Payload ID
  Output              :  Return status SUCCESS/FAILED
  Pre-requisites      :
  Last changed on     :   27th March 2019
  Last changed by     :   Himanshu Khatri
  Reason for change   :   Comments and formatting
"""

import io
import ast
import collections
import copy
import json
import logging
import os
from os import path
import shutil
import re
import sys
import time
import traceback
import urllib.request
import urllib.parse
import urllib.error
from urllib.parse import urlparse
import zipfile
from datetime import datetime
import getopt
import base64
import ntpath
import argparse
import textwrap
import boto3
import requests

# from bs4 import BeautifulSoup


sys.path.insert(0, os.getcwd())

STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "ERROR"
NONE_KEY = "NONE"
RUNNING_KEY = "RUNNING"

APPLICATION_CONFIG_FILE = "application_config.json"
SPARK_CONF_PATH = "spark_input_config_path"
FULL_LOAD = "full"
DELTA_LOAD = "delta"
INGESTION_TABLE_NAME = "ingestion_detail_log"
MODULE_NAME = "DataIngestionUtility"
PROCESS_NAME = "Ingestion"
PARQUET_DATA_DIR = "PARQUET_DATA"
RAW_DATA_DIR = "RAW_DATA"
SCHEMA_FILE_NAME = "drugcompany_schema.json"
SQL_FILE_NAME = "drugcompany.sql"
CONVERTED_JSON_DIR = "CONVERTED_JSON"
DELETED_TRIAL_ID = "DELETED_TRIAL_ID"
DELETED_FILE_NAME = "deleted_id.json"
THRESHOLD_RETRY_LIMIT = 3
CREATE_STRUCT_FILE_NAME = "create_structure_file"
CLUSTER_CONFIG = "cluster_variables.json"
OUTPUT_PARQUET_PATH = "output_parquet_file_path"
FILE_ABS_PATH = path.abspath(os.path.dirname(__file__))
SERVICE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, SERVICE_DIR_PATH)
CODE_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIR_PATH, "../common_utilities"))
sys.path.insert(1, CODE_DIR_PATH)

import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from CustomS3Utility import CustomS3Utility
from DILogSetup import get_logger
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager

logging = get_logger(file_log_name="data_ingestion_log.log", enable_file_log=True)

d = dict()
status_code = 0
step_name = ""


class CommonFunctions:
    """
    Purpose             :   This module performs below operation:
                              a. extract data from pharma_intelligence, ct_gov
                                 and pubmed data sources.
                              b. Perform logging in delta table
    Input               :  a. batch_id
                           b. config_path
                           c. data_source
                           d. delta_refresh
                           e. disease_area
                           f. disease_area_col_value
                           g. load_type
    Output              :   Return status SUCCESS/FAILED
    """

    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.version_repository_flag = self.token_payload = self.extracted_raw_json_path = None
        self.exact_match = self.s3_bucket_name = self.current_timestamp = self.stitch_columns = None
        self.filter_parameters = self.filter_provided_flag = self.local_xml_file_dir = None
        self.get_csv_data = self.delta_refresh_date_str = self.bearer_token = self.fetch_url = None
        self.temp_local_path = self.get_xml_data = self.common_functions = self.stitch_flag = None
        self.common_functions = self.dataframe_columns = self.batch_id = self.s3_deleted_id_path = None
        self.create_alias_mapping_flag = self.s3_deleted_trial_id = self.spark_job_path = None
        self.get_token_url = self.s3_temp_path_ts = self.load_type = self.deleted_id = None
        self.s3_temp_path = self.search_url = self.database_landing_path = self.full_load_url = None
        self.pre_landing_layer = self.landing_layer_batch_id = self.temp_local_path_ts = None
        self.pre_landing_layer_batch_id = self.pre_landing_layer_unzipped_batch_id = None
        self.s3_temp_spark_config_path = self.get_version_data = None
        self.landing_database_name = self.pre_landing_layer_zipped_batch_id = None
        self.disease_area_col_value = self.removed_trial_id_list = self.landing_layer = None
        self.disease_search_name = self.version_repository_file_path = None
        self.full_load_search_url = self.delta_load_url = self.logging_db_name = None
        self.datasource = self.working_database_name = self.application_config = None
        self.auth_token_headers = self.get_history_data = self.s3_temp_path_converted_json = None
        self.s3_temp_path_converted_json = None
        self.s3_temp_path_converted_json = self.pubmed_filter_list = None

    def get_bearer_token(self, get_token_url, token_payload):
        """
        Purpose     :   To get bearer token
        Input       :    get_token_url,auth_token_headers,token_payload
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            status_message = "getting bearer token"
            logging.info(str(status_message))
            #get_token_headers = {"Authorization": auth_token_headers}
            get_token_response = requests.request(
                method="POST", url=get_token_url, data=token_payload
            )

            if str(get_token_response.status_code) == "200":
                bearer_token = get_token_response.json()["access_token"]
                status_message = "Obtained bearer token:" + bearer_token
                logging.debug(str(status_message))

            else:
                status_message = (
                        "Request failed with status_code=" + str(get_token_response.status_code)
                        + ", obtained response=" + get_token_response.text
                )
                raise Exception(status_message)
            status_message = "ending method get_bearer_token"
            logging.info(str(status_message))
            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: bearer_token}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(str(error_message))
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def create_input_json_for_spark(self,
                                    extracted_raw_json_path,
                                    application_config,
                                    batch_id,
                                    load_type,
                                    s3_deleted_id_path=None, stitch_flag=None):

        """
        Purpose     :   To Create input json for spark with input parameters
        Input       :   extracted_raw_json_path ,converted_json_path
        Output      :   Return status SUCCESS/FAILED
        """
        status_message = ""
        try:
            status_message = (
                "starting create_input_json_for_spark with input parameters"
            )
            logging.info(str(status_message))

            current_ts = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S")
            temp_local_path = application_config["adapter_details"]["generic_config"][
                "local_temp_path"
            ]
            s3_temp_path_ts = os.path.join(
                application_config["adapter_details"]["generic_config"]["s3_temp_path"],
                current_ts,
            )
            s3_temp_path_converted_json = os.path.join(
                s3_temp_path_ts, CONVERTED_JSON_DIR
            )

            converted_json_path = s3_temp_path_converted_json
            deleted_json_path = s3_deleted_id_path

            src_s3_flag = "N"
            if extracted_raw_json_path.startswith("s3"):
                src_s3_flag = "Y"

            status_message = (
                    "Input file path for storing converted json= " + converted_json_path
            )
            logging.debug(str(status_message))

            status_message = "Extracted json file path= " + extracted_raw_json_path
            logging.debug(str(status_message))
            urlparse_output = urlparse(converted_json_path)
            converted_json_bucket_name = urlparse_output.netloc
            converted_json_key_path = urlparse_output.path.lstrip("/")

            urlparse_del_output = urlparse(deleted_json_path)
            deleted_json_bucket_name = urlparse_del_output.netloc
            deleted_json_key_path = urlparse_del_output.path.lstrip("/")

            removed_trial_id_list = []
            self.deleted_id = {"id": []}
            if src_s3_flag == "Y":

                output = urlparse(extracted_raw_json_path)
                bucket_name = output.netloc
                key_path = output.path.lstrip("/")
                next_token = None

                while True:
                    if next_token:
                        output = self.s3_client.list_objects_v2(
                            Bucket=bucket_name, Prefix=key_path, ContinuationToken=next_token
                        )
                    else:
                        output = self.s3_client.list_objects_v2(
                            Bucket=bucket_name, Prefix=key_path
                        )

                    if "Contents" in output:
                        for content in output["Contents"]:
                            object_path = content["Key"]
                            status_message = "Converting source json: bucket_name={0}, " \
                                             "object_path={1}".format(bucket_name, object_path)
                            logging.debug(str(status_message))
                            config_object = self.s3_client.get_object(
                                Bucket=bucket_name, Key=object_path
                            )
                            json_data = json.loads(config_object["Body"].read())
                            if load_type == FULL_LOAD:
                                item_data = json_data["items"]
                            elif load_type == DELTA_LOAD:
                                item_data = []
                                item_list = json_data["items"]
                                for item in item_list:
                                    if item.get("type") == 'remove':
                                        self.deleted_id["id"].append(item["id"])
                                    else:
                                        item_data.append(item["data"])

                            else:
                                status_message = "invalid load_type " + load_type
                                raise Exception(status_message)

                            file_name = ntpath.basename(object_path)
                            temp_file_path = converted_json_key_path + "/" + file_name

                            status_message = (
                                    "storing converted json input file s3://" + bucket_name
                                    + "/" + object_path + " to s3://"
                                    + converted_json_bucket_name.rstrip("/") + "/" + temp_file_path
                            )

                            logging.debug(str(status_message))
                            self.s3_client.put_object(
                                Body=json.dumps(item_data), Bucket=converted_json_bucket_name,
                                Key=temp_file_path, ServerSideEncryption="AES256"
                            )
                    if "NextContinuationToken" in output:
                        next_token = output["NextContinuationToken"]
                    else:
                        status_message = (
                            "Next token is not found hence breaking out of while loop"
                        )
                        logging.info(status_message)
                        break
            else:
                for file_name in os.listdir(extracted_raw_json_path):
                    src_file_path = os.path.join(extracted_raw_json_path, file_name)
                    target_file_path = os.path.join(converted_json_path, file_name)
                    read_data = open(src_file_path).read()
                    json_data = json.loads(read_data)
                    if load_type == FULL_LOAD:
                        item_data = json_data["items"]
                    elif load_type == DELTA_LOAD:
                        item_data = []
                        item_list = json_data["items"]
                        for item in item_list:
                            if item.get("type") == 'remove':
                                self.deleted_id["id"].append(item["id"])
                            else:
                                item_data.append(item["data"])
                    else:
                        item_data = json_data

                    status_message = (
                            "storing converted json input file " + file_name + " to " + target_file_path
                    )

                    logging.debug(str(status_message))
                    temp_file_path = converted_json_key_path + "/" + file_name

                    status_message = (
                            "storing converted json input file " + file_name + " to s3://"
                            + converted_json_bucket_name.rstrip("/") + "/" + temp_file_path
                    )
                    logging.debug(str(status_message))
                    self.s3_client.put_object(
                        Body=json.dumps(item_data), Bucket=converted_json_bucket_name,
                        Key=temp_file_path, ServerSideEncryption="AES256"
                    )
            logging.debug(f"delta load - {load_type} stithc flag - {stitch_flag}")
            if load_type == DELTA_LOAD and stitch_flag:
                temp_del_file_path = deleted_json_key_path + "/" + DELETED_FILE_NAME
                status_message = ("storing delete json file "
                                  + DELETED_FILE_NAME + " to s3://"
                                  + deleted_json_bucket_name.rstrip("/") + "/" +
                                  temp_del_file_path
                                  )
                logging.debug(str(status_message))
                self.s3_client.put_object(
                    Body=json.dumps(self.deleted_id), Bucket=deleted_json_bucket_name,
                    Key=temp_del_file_path, ServerSideEncryption="AES256"
                )

            status_message = "completing create_input_json_for_spark"
            logging.info(str(status_message))

            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: converted_json_path}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(str(error_message))
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


class DataIngestionAdapter:
    """
       Purpose             :   This module performs below operation:
                                 a. extract data from pharma_intelligence, ct_gov and pubmed
                                 data sources.
                                 b. Perform logging in delta table
       Input               :  a. batch_id
                              b. config_path
                              c. data_source
                              d. delta_refresh
                              e. disease_area
                              f. disease_area_col_value
                              g. load_type
       Output              :   Return status SUCCESS/FAILED
    """

    def __init__(self):
        self.s3_deleted_id_path = None
        self.data_source = None
        self.common_functions = CommonFunctions()
        self.s3_client = boto3.client("s3")
        self.stitch_flag = self.stitch_columns = None
        self.cluster_id = ""
        self.cluster_launch_flag = "N"
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.configuration = json.load(open(APPLICATION_CONFIG_FILE))
        self.audit_db = self.configuration["adapter_details"]["generic_config"][
            "mysql_db"
        ]

    def extract_data_from_pharma_intelligence(
            self,
            application_config, batch_id, payload_id,
            load_type, disease_area_col_value, filter_json, datasource,
    ):
        """
        Purpose     :   extract data from pharma intelligence function
        Input       :
        Output      :   Status of query execution
        """
        try:
            self.page_count = 0
            status_message = "starting extract_data_from_pharma_intelligence function"
            logging.info(str(status_message))

            if filter_json.get("filter_provided_flag"):
                filter_provided_flag = filter_json["filter_provided_flag"]
                filter_json.pop("filter_provided_flag")
            if filter_json.get("delta_refresh_date_str"):
                delta_refresh_date_str = filter_json["delta_refresh_date_str"]
                filter_json.pop("delta_refresh_date_str")

            for key in filter_json:
                filter_value = filter_json[key]
                filter_value = urllib.parse.quote_plus(filter_value)
                filter_value = filter_value.replace("%", "00").replace("+", "0020")
                filter_json[key] = filter_value
            filter_parameters = filter_json

            logging.info(filter_parameters)
            time.sleep(10)

            if load_type == DELTA_LOAD:
                if not delta_refresh_date_str:
                    status_message = (
                        "Load type is delta but delta_refresh date is not provided"
                    )
                    logging.debug(str(status_message))
                    raise Exception(status_message)
                delta_datetime_object = datetime.strptime(delta_refresh_date_str, "%Y%m%d")
                delta_refresh_date_str = datetime.strftime(delta_datetime_object, "%Y-%m-%d")
                filter_parameters["since"] = delta_refresh_date_str
                logging.debug("delta load timestamp={0}".format(delta_refresh_date_str))

            get_token_url = application_config["adapter_details"][self.data_source]["get_token_url"]
            logging.info(get_token_url)
            token_payload = \
                application_config["adapter_details"][self.data_source]["token_pay_load"]
            # TODO - uncomment below code after secrets have been added to the account
            # secret_password = self.get_secret(application_config["adapter_details"]
            # [self.data_source]["token_pay_load"]["password"],
            #                                   application_config["adapter_details"]
            # ["generic_config"]["s3_region"])
            # TODO following lines to be removed after secrets have been added
            secret_password = {}
            secret_password = MySQLConnectionManager().get_secret(
                self.configuration["adapter_details"]["generic_config"]["adaptor_password_secret_name"],
                self.configuration["adapter_details"]["generic_config"]["s3_region"])
            # TODO lines to be removed end here
            token_payload["password"] = secret_password['citeline_password']
            # token_payload["password"] = base64.b64decode(token_payload["password"])
            # TODO remove below logging, contains credentials
            logging.info(token_payload)
            '''
            auth_token_headers = application_config["adapter_details"][self.data_source][
                "token_header"]
            logging.info(auth_token_headers)
            '''


            full_load_search_url = (
                    application_config["adapter_details"][self.data_source]["host_url"]
                    + application_config["adapter_details"][self.data_source]["search_api_url"]
            )
            delta_load_url = (
                    application_config["adapter_details"][self.data_source]["host_url"]
                    + application_config["adapter_details"][self.data_source]["delta_refresh"]
            )
            full_load_url = (
                    application_config["adapter_details"][self.data_source]["host_url"]
                    + application_config["adapter_details"][self.data_source]["full_load"]
            )
            logging.info(full_load_url)
            logging.info(delta_load_url)
            logging.info(full_load_search_url)
            time.sleep(10)
            pre_landing_layer = self.pre_landing_path
            pre_landing_layer_batch_id = os.path.join(
                application_config["adapter_details"]["generic_config"]["s3_temp_path"],
                "Raw_data_" + batch_id + "_" + payload_id,
            )

            logging.info("Cleaning the temporary Location")

            command = (
                    "aws s3 rm --recursive " + pre_landing_layer_batch_id
            )

            logging.info("command for awscli : " + command)
            # CommonUtils().execute_shell_command(command)
            CustomS3Utility().delete_s3_objects_recursively(os.path.join(pre_landing_layer_batch_id, ""))
            status_message = "Calling get_bearer_token function"
            logging.info(str(status_message))

            output_status = self.common_functions.get_bearer_token(
                get_token_url, token_payload
            )
            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status
            bearer_token = output_status[RESULT_KEY]

            if load_type == FULL_LOAD:
                if filter_provided_flag == "Y":
                    url = full_load_search_url
                    updated_param_list = filter_parameters
                else:
                    url = full_load_url
                    updated_param_list = {}
            elif load_type == DELTA_LOAD:
                url = delta_load_url
                updated_param_list = filter_parameters
            else:
                status_message = "Invalid load type" + load_type
                raise Exception(status_message)

            headers = {
                "Authorization": "Bearer " + bearer_token,
                "Content-Type": "application/xml",
            }

            extracted_raw_json_path = pre_landing_layer_batch_id

            status_message = (
                    "For URL: '" + url + "' Filter details: " + json.dumps(updated_param_list)
            )
            logging.debug(str(status_message))
            count = 0

            retry_count = 1
            next_url = None
            while True:
                current_ts = batch_id

                file_name = (
                        datasource + "_" + disease_area_col_value.replace(" ", "_")
                        + "_" + current_ts + "_" + payload_id + "_" + str(self.page_count) + ".json"
                )
                status_message = "Getting data for page no:" + str(count)
                logging.debug(str(status_message))
                temp_raw_json_file_path = os.path.join(
                    extracted_raw_json_path, file_name
                )
                try:
                    if next_url:
                        status_message = "Next URL:" + next_url
                        logging.debug(str(status_message))
                        response = requests.request(method="GET", url=next_url, headers=headers)
                    else:
                        status_message = "URL:" + url

                        logging.debug(str(status_message))
                        response = requests.request(
                            method="GET", url=url, headers=headers, params=updated_param_list,
                        )

                except:
                    status_message = ("For retry '" + str(retry_count) + "' and page_no=" +
                                      str(count) + ", obtained exception " +
                                      str(traceback.format_exc()))

                    logging.debug(str(status_message))

                    if retry_count > THRESHOLD_RETRY_LIMIT:
                        status_message = "retry limit='{retry_count}' has exceeded threshold " \
                                         "limit={THRESHOLD_RETRY_LIMIT}".format(
                            retry_count=retry_count,
                            THRESHOLD_RETRY_LIMIT=THRESHOLD_RETRY_LIMIT, )

                        raise Exception(status_message)
                    retry_count = retry_count + 1
                    continue

                if int(response.status_code) == 200:
                    retry_count = 1
                    response_json = response.json()
                    if filter_provided_flag == "Y":
                        if "totalRecordCount" not in response_json["meta"]:
                            status_message = "No data found for given filters"
                            raise Exception(status_message)
                        status_message = "Total no of records: " + str(
                            response_json["meta"]["totalRecordCount"]
                        )
                        logging.debug(str(status_message))

                    status_message = (
                            "Ingestion " + datasource + " json data of for page '" + str(count)
                            + "' to " + temp_raw_json_file_path
                    )
                    logging.debug(str(status_message))

                    output = urlparse(temp_raw_json_file_path)
                    bucket_name = output.netloc
                    key_path = output.path.lstrip("/")

                    self.s3_client.put_object(Body=response.text,
                                              Bucket=bucket_name, Key=key_path,
                                              ServerSideEncryption="AES256")

                    pre_landing_raw_path = os.path.join(pre_landing_layer,
                                                        datasource + "_" +
                                                        disease_area_col_value.replace(" ", "_")
                                                        + "_"
                                                        + current_ts + "_" + payload_id + "_" +
                                                        str(self.page_count) + ".json")

                    command = (
                            "aws  s3 cp " + temp_raw_json_file_path + " " + pre_landing_raw_path
                    )

                    logging.info("command for awscli : " + command)
                    # CommonUtils().execute_shell_command(command)
                    CustomS3Utility().copy_s3_to_s3_recursively(temp_raw_json_file_path, pre_landing_raw_path)
                    if "pagination" not in response_json:
                        status_message = "'pagination' key not found in response json"
                        logging.debug(str(status_message))
                        break
                    if "nextPage" not in response_json["pagination"]:
                        status_message = "'nextPage' key not present in response"
                        logging.debug(str(status_message))
                        break

                elif int(response.status_code) == 504:
                    status_message = (
                            "Got status_code '" + str(response.status_code)
                            + "' for retry=" + str(retry_count) + " and page_no=" + str(count)
                    )
                    logging.debug(str(status_message))

                    if retry_count > THRESHOLD_RETRY_LIMIT:
                        status_message = "retry limit='{retry_count}' has exceeded" \
                                         " threshold limit={THRESHOLD_RETRY_LIMIT}".format(
                            retry_count=retry_count,
                            THRESHOLD_RETRY_LIMIT=THRESHOLD_RETRY_LIMIT, )

                        raise Exception(status_message)

                    retry_count = retry_count + 1
                    continue

                else:
                    status_message = (
                            "Request failed with status_code=" + str(response.status_code)
                            + ", obtained response=" + response.text
                    )
                    raise Exception(status_message)

                next_url = response_json["pagination"]["nextPage"]
                count = count + 1
                # if count == 2:
                #    break
                self.page_count = self.page_count + 1

            status_message = "Completing extract_data_from_pharma_intelligence function"
            logging.debug(str(status_message))

            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: extracted_raw_json_path}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(str(error_message))
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def execute_query(
            self,
            table_type=None, query_type=None, batch_id=None, payload_id=None,
            step_name=None, payload_json=None, adapter_id=None, load_type=None, cluster_id=None,
            raw_file_dir=None,
            structured_file_path=None, status=None
    ):
        """
        Purpose     :   Execute query on databricks delta tables
        Input       :   query_type, datasource , step_name , status = failed, success etc.
        Output      :   Status of query execution
        """

        try:
            status_message = "starting execute_query function"
            logging.info(status_message)
            if query_type.lower() == "insert" and table_type.lower() == "smry":
                query = "Insert into {audit_db}.{log_data_acquisition_smry_table} values " \
                        "('{batch_id}'" \
                        ",'{adapter_id}','{payload_id}','{data_source}','{payload_json}','" \
                        "{load_type}','{cluster_id}','{status}'" \
                        ",'{start_time}','{end_time}')".format(
                    audit_db=self.audit_db,
                    log_data_acquisition_smry_table=
                    CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                    batch_id=batch_id, payload_id=payload_id, adapter_id=adapter_id,
                    load_type=load_type, data_source=self.data_source,
                    payload_json=json.dumps(payload_json), cluster_id=cluster_id,
                    status=status,
                    start_time=datetime.utcnow(), end_time="")


            elif query_type.lower() == "insert" and table_type.lower() == "dtl":

                query = (
                    "Insert into {audit_db}.{log_data_acquisition_dtl_table} values ('{batch_id}'"
                    ",'{adapter_id}','{payload_id}','{data_source}','{step_name}', '{raw_file_dir}'"
                    ",'{structured_file_path}','{status}','{start_time}','{end_time}')".format(
                        audit_db=self.audit_db,
                        log_data_acquisition_dtl_table=
                        CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                        batch_id=batch_id, payload_id=payload_id, adapter_id=adapter_id,
                        data_source=self.data_source,
                        step_name=step_name, raw_file_dir=raw_file_dir,
                        structured_file_path=structured_file_path, status=status,
                        start_time=datetime.utcnow(), end_time=""
                    )
                )

            elif query_type.lower() == "update" and table_type.lower() == "smry":

                query_str = (
                    "Update {audit_db}.{log_data_acquisition_smry_table} set status = '{status}' "
                    ", end_time = '{end_time}' where cycle_id = '{batch_id}' and "
                    " source_name = '{data_source}' and payload_id ='{payload_id}'"
                )
                query = query_str.format(
                    audit_db=self.audit_db, status=status,
                    log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                    batch_id=batch_id, data_source=self.data_source, end_time=datetime.utcnow(),
                    payload_id=payload_id
                )

            elif query_type.lower() == "update" and table_type.lower() == "dtl":

                query_str = (
                    "Update {audit_db}.{log_data_acquisition_dtl_table} set status = '{status}' "
                    ", end_time = '{end_time}' where cycle_id = '{batch_id}' and source_name = "
                    "'{data_source}' and step_name = '{step_name}' and payload_id = '{payload_id}'"
                )
                query = query_str.format(
                    audit_db=self.audit_db, status=status,
                    log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                    batch_id=batch_id, data_source=self.data_source, step_name=step_name,
                    payload_id=payload_id, end_time=datetime.utcnow()
                )

            logging.info("Query to insert in the log table: %s", str(query))
            response = MySQLConnectionManager().execute_query_mysql(query, False)
            logging.info("Query output: %s", str(response))

            status_message = "completing execute_query function"
            logging.info(status_message)
            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def create_app_config(self, application_config_dir):
        """
        Purpose     :   To extract application config details
        Input       :   application_config_file = contains all application
                        specific details, env_config_file = contains
                        all environment specific details.
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            status_message = "Calling create_app_config function"
            logging.info(status_message)

            application_config_path = os.path.join(application_config_dir, APPLICATION_CONFIG_FILE)

            logging.debug("application_config_path=" + application_config_path)

            application_config = json.load(open(application_config_path))
            environment_params = application_config["adapter_details"]["generic_config"]

            application_config_str = json.dumps(application_config)
            for key in environment_params:
                application_config_str = application_config_str.replace(
                    "$$" + key,
                    str(application_config["adapter_details"]["generic_config"][key]),
                )

            application_config = ast.literal_eval(application_config_str)

            status_message = (
                    "Application configuration after replacing environment variable"
                    + json.dumps(application_config)
            )
            logging.debug(str(status_message))

            status_message = "Completing create_app_config function"
            logging.info(status_message)
            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: application_config}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def usage(self):
        """
        :def: USAGE() Function
        :return: prints help statement
        """
        msg = """Usage: python DataIngestionAdapter.py -j
        This will trigger the ingestion adapters based on the filters provided by the .
        contains all the filters needed to trigger the respective data source ingestion.
        eg.(trial_trove pubmed and Clinical_trials)."""
        logging.info(str(msg))

    def trigger_data_ingestion(
            self, payload_json, batch_id, payload_id, input_config_path, cluster_id, data_source
    ):
        """
        Purpose     :   This function reads the configuration from given config file path
        Input       :   config file path
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            self.data_source = data_source
            self.pre_landing_path = self.configuration["adapter_details"][self.data_source]["pre_landing_layer"]
            output_status = self.create_app_config(input_config_path)

            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status

            application_config = output_status[RESULT_KEY]
            status_message = "Obtained input config: " + json.dumps(application_config)
            logging.debug(str(status_message))

            current_ts = batch_id

            s3_temp_path_ts = os.path.join(
                application_config["adapter_details"]["generic_config"]["s3_temp_path"], current_ts)

            s3_temp_path_converted_json = os.path.join(s3_temp_path_ts, CONVERTED_JSON_DIR)
            self.s3_deleted_id_path = os.path.join(s3_temp_path_ts, DELETED_TRIAL_ID)
            logging.debug(f"Se deleted id path here - {self.s3_deleted_id_path}")

            failed_flag = True

            load_type = payload_json["load_type"]
            disease_area_col_value = payload_json["disease_area_col_value"]
            filter_payload = payload_json

            if filter_payload.get("load_type"):
                filter_payload.pop("load_type")
            if filter_payload.get("disease_area_col_value"):
                filter_payload.pop("disease_area_col_value")

            output_status = self.extract_data_from_pharma_intelligence(
                application_config, batch_id, payload_id, load_type,
                disease_area_col_value, filter_payload, self.data_source
            )

            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status

            extracted_json_path = output_status[RESULT_KEY]

            output_status = self.common_functions.create_input_json_for_spark(
                extracted_json_path, application_config, batch_id, load_type, self.s3_deleted_id_path, self.stitch_flag
            )

            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status
            converted_json_path = output_status[RESULT_KEY]

            failed_flag = False
            return {SPARK_CONF_PATH: converted_json_path, STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = str(traceback.format_exc())
            logging.debug(str(error_message))
            return {
                SPARK_CONF_PATH: "NONE", STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message,
            }

    def get_secret(self, secret_name, region_name):
        """
        Purpose: This fetches the secret details and decrypts them.
        Input: secret_name is the secret to be retrieved from Secrets Manager.
               region_name is the region which the secret is stored.
        Output: JSON with decoded credentials from Secrets Manager.
        """
        try:
            # Create a Secrets Manager client
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
            logging.info("Fetching the details for the secret name %s", secret_name)
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            logging.info(
                "Fetched the Encrypted Secret from Secrets Manager for %s", secret_name)
        except Exception:
            raise Exception("Unable to fetch secrets.")
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be
            # populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                logging.info("Decrypted the Secret")
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                logging.info("Decrypted the Secret")
            return json.loads(secret)

    def get_payload_json(self, payload_id):

        get_payload_query_str = "select payload_details_file_name,raw_file_dir," \
                                "structured_file_dir" \
                                " from {audit_db}.{adapter_payload_details_table} where " \
                                "payload_id" \
                                " = {payload_id}"

        get_payload_query = get_payload_query_str.format(
            audit_db=self.audit_db,
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE,
            payload_id=payload_id,
        )
        status_message = "Get Payload Query : " + str(get_payload_query)
        print(get_payload_query)
        logging.info(status_message)

        get_payload_result = MySQLConnectionManager().execute_query_mysql(get_payload_query)
        payload_json_file_name = get_payload_result[0]["payload_details_file_name"]

        status_message = "Payload json file name retrived : " + str(payload_json_file_name)

        logging.info(status_message)

        payload_json = json.loads(open(payload_json_file_name, "r").read())

        logging.info(str(payload_json))
        logging.info(get_payload_result[0]["raw_file_dir"])
        logging.info(get_payload_result[0]["structured_file_dir"])

        disease_area_col_list = []

        for dict_data in payload_json["payload_details"]:
            dict_data["raw_file_location"] = get_payload_result[0]["raw_file_dir"]
            dict_data["structured_file_location"] = get_payload_result[0]["structured_file_dir"]
            dict_data["payload_id"] = payload_id

        status_message = (
                "Payload json retrived from s3 for payload id :"
                + payload_id
                + " is - "
                + str(payload_json)
        )
        logging.info(status_message)
        return payload_json

    def check_valid_payload_id(self, payload_id_list):
        output_status = {STATUS_KEY: SUCCESS_KEY}

        get_payload_id_query_str = (
            "select payload_id from {audit_db}.{adapter_payload_details_table} as A"
            " inner join {audit_db}.{adapter_details_table} as B "
            "on A.adapter_id = B.adapter_id where B.source_name = '{data_source}'"
        )

        get_payload_id_query = get_payload_id_query_str.format(
            audit_db=self.audit_db,
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE,
            adapter_details_table=CommonConstants.ADAPTER_DETAILS_TABLE,
            data_source=self.data_source,
        )
        status_message = "Get Payload Query : " + str(get_payload_id_query)
        logging.info(status_message)
        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            get_payload_id_query
        )
        payload_id_db_list = []

        for id in get_payload_result:
            payload_id_db_list.append(str(id["payload_id"]))

        status_message = "Payload id retrived from database : " + str(payload_id_db_list)
        logging.info(status_message)

        status_message = "Payload id retrived from input : " + str(payload_id_list)
        logging.info(status_message)

        for payload in payload_id_list:
            if payload not in payload_id_db_list:
                output_status[STATUS_KEY] = FAILED_KEY
                output_status[ERROR_KEY] = "Payload Id is invalid for this data source"
                return output_status

        return output_status

    def get_adapter_id(self, data_source):
        get_adapter_id_query_str = (
            "select adapter_id from {audit_db}.{adapter_details_table} "
            " where source_name = '{data_source}'"
        )
        get_payload_id_query = get_adapter_id_query_str.format(
            audit_db=self.audit_db,
            adapter_details_table=CommonConstants.ADAPTER_DETAILS_TABLE,
            data_source=data_source,
        )
        print(get_payload_id_query)
        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            get_payload_id_query
        )
        adapter_id = get_payload_result[0]["adapter_id"]
        adapter_id = str(adapter_id)
        print(type(adapter_id), adapter_id)
        return adapter_id

    def get_payload_id(self, input_dict, adapter_id):

        check_payload_id_query_str = (
            "select payload_details_file_name from {audit_db}.{adapter_payload_details_table} "
            " where adapter_id = '{adapter_id}' and payload_details_file_name = '{input_dict}'"
        )

        check_payload_id_query = check_payload_id_query_str.format(
            audit_db=self.audit_db,
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE,
            adapter_id=adapter_id,
            input_dict=input_dict
        )

        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            check_payload_id_query
        )
        if not get_payload_result:
            raise Exception("payload json does not exist, please use valid payload ID")

        get_payload_id_query_str = (
            "select payload_id from {audit_db}.{adapter_payload_details_table} "
            " where payload_details_file_name = '{payload_details_file_name}'"
        )

        get_payload_id_query = get_payload_id_query_str.format(
            audit_db=self.audit_db,
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE,
            payload_details_file_name=input_dict
        )
        print(get_payload_id_query)
        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            get_payload_id_query
        )
        payload_id = get_payload_result[0]["payload_id"]
        payload_id = str(payload_id)
        return payload_id


if __name__ == "__main__":
    try:
        source_name = sys.argv[1]
        IngestionAdaptor_obj = DataIngestionAdapter()
        if str(source_name) == "DRUGCOMPANY":
            data_source = "drugcompany"
            batch_id = sys.argv[2]
            payload_id = sys.argv[3]
            FILE_ABS_PATH = sys.argv[4]
            cluster_id = sys.argv[5]
            f = open('payload_drugcompany.json')
            payload_details = json.load(f)
            sub_payload_list = payload_details['payload_details']
            sub_payload = sub_payload_list[0]
            output = IngestionAdaptor_obj.trigger_data_ingestion(sub_payload, batch_id, payload_id, FILE_ABS_PATH,
                                                                 cluster_id, data_source)
        else:
            DataIngestionAdapter().usage()
            raise Exception(str(traceback.format_exc()))
        if output[STATUS_KEY] == FAILED_KEY:
            raise Exception(output[ERROR_KEY])
        sys.stdout.write(str(output))

    except Exception as exception:
        message = "Error occurred in main Error : %s\nTraceback : %s" % (
            str(exception),
            str(traceback.format_exc()),
        )
        raise exception
