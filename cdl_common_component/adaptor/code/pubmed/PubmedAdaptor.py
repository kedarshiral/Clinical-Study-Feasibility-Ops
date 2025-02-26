# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
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
import base64
import ntpath
import argparse
import textwrap
import boto3
import requests
import xmltodict
from bs4 import BeautifulSoup
import getopt

CURR_PATH = os.getcwd()

UTILITIES_PATH = os.path.abspath(os.path.join(CURR_PATH, "../../utilities/"))
sys.path.insert(0, UTILITIES_PATH)

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
SCHEMA_FILE_NAME = "pubmed_schema.xml"
SQL_FILE_NAME = "pubmed.sql"
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

# from bs4 import BeautifulSoup
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
# from ExecutionContext import ExecutionContext
# from SystemManagerUtility import SystemManagerUtility
from SshEmrUtility import SshEmrUtility
from DILogSetup import get_logger
from EmrManagerUtility import EmrManagerUtility
from ExecutionContext import ExecutionContext

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
        self.s3_resource = boto3.resource('s3')
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
                status_message = ("Request failed with status_code="
                                  + str(get_token_response.status_code)
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


class DataIngestionAdapter:
    """
       Purpose             :   This module performs below operation:
                                 a. extract data from pharma_intelligence, ct_gov and pubmed data
                                  sources.
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
        pass

    def extract_pubmed_data(self, application_config, load_type, batch_id, disease_search_name,delta_refresh_date_str, datasource):
        """
        Purpose     :   To extract pubmed data
        Input       :
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            logging.info("starting method to extract pubmed data")
            status_message = ""
            current_ts = batch_id
            current_timestamp = datetime.utcnow()
            temp_local_path = application_config["adapter_details"]["generic_config"]["local_temp_path"]
            connection_details = "pubmed"
            search_url = application_config["adapter_details"][connection_details]["search_url"]
            delta_load_url = application_config["adapter_details"][connection_details]["delta_refresh_url"]
            fetch_url = application_config["adapter_details"][connection_details]["fetch_url"]
            pubmed_filter_list = application_config["adapter_details"][connection_details]["filter_keys"]
            pre_landing_layer = application_config["adapter_details"][connection_details]["prelanding_path"]
            landing_layer = application_config["adapter_details"][connection_details]["landing_path"]
            pre_landing_layer_batch_id = os.path.join(pre_landing_layer, batch_id)
            if load_type == DELTA_LOAD:
                CommonUtils().make_s3_dir(pre_landing_layer_batch_id)
            # filter_parameters = {"term": disease_search_name}
            filter_parameters = {"term": disease_search_name}
            if load_type == DELTA_LOAD:
                if not delta_refresh_date_str:
                    status_message = "Load type is delta but delta_refresh date is not provided"
                    logging.debug(str(status_message))
                    raise Exception(status_message)
                delta_datetime_object = datetime.strptime(delta_refresh_date_str, '%Y%m%d')
                delta_refresh_date_str = datetime.strftime(delta_datetime_object, "%Y/%m/%d")
                filter_parameters["min_date"] = delta_refresh_date_str
                filter_parameters["max_date"] = datetime.strftime(current_timestamp, "%Y%m%d")
                status_message = "delta start_timestamp={0} and end_timestamp={1}".format(delta_refresh_date_str,
                                                                                          datetime.strftime(
                                                                                              current_timestamp,
                                                                                              "%Y%m%d"))

            pubmed_id_list = []
            start_time = time.time()
            if load_type == DELTA_LOAD:
                base_url = delta_load_url
                logging.debug(delta_load_url)
            else:
                base_url = search_url
                logging.debug(search_url)

            ret_max = 100000

            for key in filter_parameters:
                base_url = base_url.replace("$$" + key, filter_parameters[key])

            for filter in pubmed_filter_list:
                record_count_start = 0
                while True:
                    logging.debug("Getting pubmed_id for filter=" + str(filter) + "retstart=" + str(
                        record_count_start) + " retmax=" + str(ret_max))
                    url = base_url.replace("$$search_filter", filter).replace("$$retstart", str(record_count_start))
                    status_message = "URL={0} for getting pubmed data".format(url)
                    logging.debug(str(status_message))
                    retry_counter_filter = 0
                    while True:
                        try:
                            response = requests.request("GET", url=url, params=filter)
                            status_message = "Rest response output " + str(
                                response.status_code) + " obtained for filter:" + filter
                            logging.debug(str(status_message))
                            if str(response.status_code) == "200":
                                output = xmltodict.parse(response.content)
                                pubmed_record_count = output["eSearchResult"]["Count"]
                                logging.debug(
                                    "No of records count={0} obtained for filter {1}, adding to pubmed_id_list".format(
                                        str(pubmed_record_count), str(filter)))
                                if str(pubmed_record_count) != "0":
                                    id_list = output["eSearchResult"]["IdList"]["Id"]
                                    if not isinstance(id_list, list):
                                        id_list = [id_list]
                                    logging.debug(
                                        "For filter {0} obtained pubmed_id list={1}".format(filter, str(id_list)))
                                    pubmed_id_list.extend(id_list)
                                break
                            if str(response.status_code) != "200":
                                logging.debug(
                                    "obtained response:" + str(response.status_code) + " for retry_counter=" + str(
                                        retry_counter_filter))
                                raise Exception()
                        except:
                            logging.debug("inside exception retry_count=" + str(retry_counter_filter) + "= " + str(
                                traceback.format_exc()))
                            if retry_counter_filter < 3:
                                retry_counter_filter = retry_counter_filter + 1
                                logging.info("Sleeping for 10 seconds, after failure")
                                time.sleep(10)
                                continue
                            else:
                                raise Exception(
                                    "obtained response:" + str(response.status_code) + ", for retry_counter=" + str(
                                        retry_counter_filter))

                    record_count_start = record_count_start + ret_max
                    if int(pubmed_record_count) < record_count_start:
                        logging.debug(
                            "breaking out of while loop due to pubmed_record count={0} is less than ret_max={1}".format(
                                str(pubmed_record_count), str(ret_max)))
                        break
            pubmed_id_list = list(set(pubmed_id_list))
            logging.debug("count of total obtained pubmed_id=" + str(len(pubmed_id_list)))
            logging.info(status_message)

            status_message = "pubmed_id list=" + str(pubmed_id_list)
            logging.debug(status_message)

            i = 0
            for pubmed_id in pubmed_id_list:
                url = fetch_url.replace("$$pubmed_id", pubmed_id)
                retry_counter = 0
                while True:
                    try:
                        logging.debug("getting data for pubmed_id={0} using url={1}".format(pubmed_id, url))
                        response = requests.request("GET", url=url, params=filter)
                        status_message = "Rest response output " + str(
                            response.status_code) + " obtained for pubmed_id:" + pubmed_id
                        logging.debug(str(status_message))
                        if str(response.status_code) == "200":
                            file_name = "PMID" + pubmed_id + ".xml"
                            xml_file_path = os.path.join(pre_landing_layer_batch_id, file_name)
                            logging.debug("Writing {pubmed_id} xml file to {path}".format(pubmed_id=pubmed_id,
                                                                                          path=xml_file_path))

                            remove_tags = ["<i>", "</i>", "<sup>", "</sup>", "<sub>", "</sub>", "<u>", "</u>"]
                            xml_data = response.text
                            for tag in remove_tags:
                                xml_data = xml_data.replace(tag, "")

                            CommonUtils().put_s3_object(xml_file_path, xml_data)
                            i = i + 1
                            logging.debug("count=" + str(i))
                            break
                        if str(response.status_code) != "200":
                            status_message = "obtained response:" + str(
                                response.status_code) + " for retry_counter=" + str(retry_counter)
                            logging.debug(status_message)
                            raise Exception()
                    except:
                        logging.debug("inside exception retry_count=" + str(retry_counter) + "= " + str(
                            traceback.format_exc()))
                        if retry_counter < 3:
                            retry_counter = retry_counter + 1
                            logging.info("Sleeping for 10 seconds, after failure")
                            time.sleep(10)
                            continue
                        else:
                            raise Exception(
                                "obtained response:" + str(response.status_code) + ", for retry_counter=" + str(
                                    retry_counter))
                # if i == 100:
                #     break

            end_time = time.time()
            time_taken = end_time - start_time
            hours, rest = divmod(time_taken, 3600)
            minutes, seconds = divmod(rest, 60)
            status_message = "Total time hours={hours}, minutes={minutes}, seconds={seconds}".format(hours=hours,
                                                                                                     minutes=minutes,
                                                                                                     seconds=seconds)
            logging.debug(str(status_message))
            status_message = "Completing extract_pubmed_data function"
            logging.info(str(status_message))
            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: pre_landing_layer_batch_id}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
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

    def call_create_structured_file(
            self, create_struct_dict_list, application_config, cluster_id, payload_id, batch_id
    ):
        """
        Purpose     :   To call notebook CreateStructuredFile with input file
        Input       :   s3_temp_path_converted_json = json file containing all
                       input configuration details
        Output      :   return status key
        """
        try:
            status_message = "staring call_create_structured_file function"
            logging.info(status_message)
            status_message = "extracting data from config file"
            logging.info(status_message)

            current_ts = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S")
            s3_temp_path_ts = os.path.join(
                application_config["adapter_details"]["generic_config"]["s3_temp_path"], current_ts
            )
            landing_layer = self.landing_path

            s3_temp_spark_config_path = os.path.join(s3_temp_path_ts, "spark_input_config_"
                                                     + datetime.strftime(datetime.utcnow(),
                                                                         "%Y%m%d%H%M%S") + ".json", )

            spark_input_conf_list = []

            emr_keypair_path = application_config["adapter_details"]["generic_config"][
                "private_key_loaction"
            ]
            emr_code_path = application_config["adapter_details"]["generic_config"]["emr_code_path"]
            emr_region_name = application_config["adapter_details"]["generic_config"]["aws_region"]

            S3_SCHEMA_LOCATION = os.path.join(s3_temp_path_ts, SCHEMA_FILE_NAME)
            S3_SQL_LOCATION = os.path.join(s3_temp_path_ts, SQL_FILE_NAME)

            command_to_copy_schema_file_to_s3 = (
                    "aws s3 cp --debug " + SCHEMA_FILE_NAME + " " + S3_SCHEMA_LOCATION)

            command_to_copy_sql_file_to_s3 = (
                    "aws s3 cp --debug " + SQL_FILE_NAME + " " + S3_SQL_LOCATION)

            logging.info(
                "command for awscli : " + command_to_copy_schema_file_to_s3
            )

            CommonUtils().execute_shell_command(command_to_copy_schema_file_to_s3)
            logging.info("command for awscli : " + command_to_copy_sql_file_to_s3)
            CommonUtils().execute_shell_command(command_to_copy_sql_file_to_s3)
            self.s3_parquet_output_path = ""

            for dict_data in create_struct_dict_list:
                spark_input_config = {}

                self.s3_parquet_output_path = (
                        landing_layer + "/" + self.data_source + "_"
                        + dict_data["disease_area_col_value"].replace(" ", "_")
                        + "_" + batch_id + "_" + payload_id + ".parquet")

                spark_input_config["s3_schema_path"] = S3_SCHEMA_LOCATION
                spark_input_config["spark_query_path"] = S3_SQL_LOCATION
                if self.load_type == DELTA_LOAD and self.stitch_flag:
                    spark_input_config["stitching_columns"] = dict_data["stitching_columns"]
                    spark_input_config["deleted_id_path"] = os.path.join(self.s3_deleted_id_path, DELETED_FILE_NAME)
                    spark_input_config["step_name"] = CREATE_STRUCT_FILE_NAME
                spark_input_config["s3_input_data_path"] = dict_data["path_extracted_json"]
                spark_input_config["s3_parquet_output_path"] = self.s3_parquet_output_path
                spark_input_config["data_source"] = dict_data["datasource"]
                spark_input_config["disease_name"] = dict_data["disease_area_col_value"]
                spark_input_config["temp_s3_path"] = \
                    application_config["adapter_details"]["generic_config"]["s3_temp_path"]

                spark_input_conf_list.append(spark_input_config)

            spark_input_config_file = {"spark_config": spark_input_conf_list}
            logging.info(spark_input_config_file)

            output = urlparse(s3_temp_spark_config_path)

            status_message = (
                    "Writing spark input config file to "
                    + s3_temp_spark_config_path + " with content: " + json.dumps(
                spark_input_config_file))

            logging.info(status_message)

            CommonUtils().put_s3_object(
                s3_temp_spark_config_path, json.dumps(spark_input_config_file)
            )

            logging.debug(
                "Calling notebook CreateStructuredFile with input file="
                + str(s3_temp_spark_config_path)
            )

            emr_code_path = os.path.join(emr_code_path, "adaptor", "common_utilities")
            emr_code_path = os.path.join(emr_code_path, "CreateStructuredFile.py")

            argument_list = [
                "/usr/lib/spark/bin/spark-submit", "--packages",
                "com.databricks:spark-xml_2.10:0.4.1", emr_code_path, s3_temp_spark_config_path,
            ]

            status_message = "Argument list SSH_EMR_Utility job=" + json.dumps(argument_list)
            logging.debug(status_message)

            command = '/usr/lib/spark/bin/spark-submit --packages ' \
                      'com.databricks:spark-xml_2.10:0.4.1 {0} -j "{1}"'.format(
                emr_code_path, s3_temp_spark_config_path)

            logging.debug(command)

            try:
                # output_status = SystemManagerUtility().execute_command(
                #     cluster_id, command
                # )
                emr_ssh_secret_name = application_config["adapter_details"]["generic_config"]["emr_ssh_secret"]
                fetch_emr_password = CommonUtils().get_secret(emr_ssh_secret_name, emr_region_name)
                emr_password = fetch_emr_password["password"]
                output_status = SshEmrUtility().execute_command(
                    cluster_id, emr_region_name, emr_password, command
                )
                status_message = "Final output: " + str(output_status)
                logging.debug(str(status_message))

                if not re.search('{"status": "SUCCESS"}', status_message):
                    return {STATUS_KEY: FAILED_KEY, OUTPUT_PARQUET_PATH: None,
                            ERROR_KEY: "Failed spark job"}

                logging.info("Spark submitt done successfully")
                command_to_remove_schema_file_to_s3 = "aws s3 rm " + S3_SCHEMA_LOCATION
                command_to_remove_sql_file_to_s3 = "aws s3 rm " + S3_SQL_LOCATION
                logging.info("command for awscli : " + command_to_remove_schema_file_to_s3)
                CommonUtils().execute_shell_command(command_to_remove_schema_file_to_s3)
                logging.info("command for awscli :  " + command_to_remove_sql_file_to_s3)
                CommonUtils().execute_shell_command(command_to_remove_sql_file_to_s3)

                return {
                    STATUS_KEY: SUCCESS_KEY,
                    OUTPUT_PARQUET_PATH: self.s3_parquet_output_path,
                }
            except:
                status_message = "ERROR - " + str(traceback.format_exc())
                logging.error(status_message)
                raise Exception(status_message)

        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {
                STATUS_KEY: FAILED_KEY, OUTPUT_PARQUET_PATH: None, ERROR_KEY: error_message
            }

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
                    + json.dumps(application_config))

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
            self, payload_json, batch_id, payload_id, input_config_path, cluster_id
    ):
        """
        Purpose     :   This function reads the configuration from given config file path
        Input       :   config file path
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            status_message = "Calling main function"
            logging.info(status_message)
            cluster_id = str(cluster_id)

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
            delta_refresh_date_str = payload_json["delta_refresh_date_str"]
            self.load_type = load_type
            disease_area_col_value = payload_json["disease_area_col_value"]
            filter_payload = payload_json

            if filter_payload.get("load_type"):
                filter_payload.pop("load_type")
            if filter_payload.get("disease_area_col_value"):
                filter_payload.pop("disease_area_col_value")

            # output_status = self.extract_pubmed_data(application_config, load_type, batch_id, disease_search_name,
            #                                          delta_refresh_date_str, datasource)
            output_status = self.extract_pubmed_data(application_config, load_type, batch_id, disease_area_col_value,
                                                     delta_refresh_date_str,self.data_source)
            print("output_status--->>>>", output_status)

            logging.debug("output_status--->>>>" + str(output_status))

            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status

            extracted_json_path = output_status[RESULT_KEY]

            # output_status = self.common_functions.create_input_json_for_spark(
            #     extracted_json_path, application_config, batch_id, load_type, self.s3_deleted_id_path, self.stitch_flag
            # )

            # if output_status[STATUS_KEY] == FAILED_KEY:
            #     return output_status
            # converted_json_path = output_status[RESULT_KEY]

            failed_flag = False
            # return {SPARK_CONF_PATH: converted_json_path, STATUS_KEY: SUCCESS_KEY}
            return {SPARK_CONF_PATH: extracted_json_path, STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = str(traceback.format_exc())
            logging.debug(str(error_message))
            return {
                SPARK_CONF_PATH: "NONE", STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message,
            }

    def call_terminate_cluster(self, cluster_id, region_name):
        try:
            logging.info("Terminate_cluster method called")

            logging.debug("cluster_id --> " + str(cluster_id))
            EmrManagerUtility().terminate_cluster(cluster_id, region_name)
            logging.info("Cluster terminated")
        except Exception:
            raise Exception(str(traceback.format_exc()))

    def call_launch_cluster(self, data_source_name):
        try:
            cluster_type = self.configuration["adapter_details"][data_source_name]["cluster_type"]
            file_read = open("../common_utilities/" + CLUSTER_CONFIG, "r")
            json_data = json.load(file_read)
            file_read.close()

        except Exception:
            raise Exception("File not found error")

        try:
            logging.info("launch_cluster method called for dag")

            emr_payload = json_data["cluster_configurations"][cluster_type]
            region_name = emr_payload["Region"]

            returned_json = EmrManagerUtility().launch_emr_cluster(
                emr_payload, region_name, data_source_name
            )

            logging.debug("Json for EMR --> " + str(returned_json))
            cluster_id = returned_json[RESULT_KEY]
            logging.debug("cluster_id --> " + str(cluster_id))
            logging.info("Cluster launched")

            return cluster_id

        except Exception:
            error_message = str(traceback.format_exc())
            logging.error("Cluster is not launched " + error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

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
            # Depending on whether the secret is a string or binary,
            # one of these fields will be populated.
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
                                " from {audit_db}.{adapter_payload_details_table}" \
                                " where payload_id" \
                                " = {payload_id}"

        get_payload_query = get_payload_query_str.format(
            audit_db=self.audit_db,
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE,
            payload_id=payload_id,
        )
        status_message = "Get Payload Query : " + str(get_payload_query)
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
            # if not dict_data.get("load_type") or dict_data["load_type"] == "" or
            #  type(dict_data["load_type"]!=str):
            #     raise Exception("load_type key/value must be present or must not be
            # empty or must be str")
            # if not dict_data.get("disease_area_col_value") or
            # dict_data["disease_area_col_value"] == "" \
            #         or type(dict_data["disease_area_col_value"]) != str:
            #     raise Exception("disease_area_col_value key/value must be
            #  present or must not be empty or must be str")
            #
            # if dict_data["disease_area_col_value"] not in disease_area_col_list and not
            # disease_area_col_list:
            #     raise Exception("Please provide only one disease are column value")
            # else:
            #     disease_area_col_list.append(dict_data["disease_area_col_value"])

            dict_data["raw_file_location"] = get_payload_result[0]["raw_file_dir"]
            dict_data["structured_file_location"] = get_payload_result[0]["structured_file_dir"]
            dict_data["payload_id"] = payload_id

        status_message = (
                "Payload json retrived from s3 for payload id :"
                + payload_id
                + " is - "
                + str(payload_json))

        logging.info(status_message)
        return payload_json

    def check_valid_payload_id(self, payload_id_list):
        """
        Purpose     :   this function checks the payload id provided is valid or not
        Input       :   payload id list
        Output      :   valid/invalid status of payload id
        """
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

    def check_mandatory_parameters_exists(self, input_dict):
        """
        Purpose   :   This method is used to check if all the necessary parameters
                      exist in the passed input dictionary .
        Input     :   Input dictionary containing all the necessary arguments.
        Output    :   Returns True if all the variables exist in the required format
        """
        try:
            flag = True
            if input_dict["payload_id_list"] is None:
                has_incompatible_arg = True
                if has_incompatible_arg:
                    status_message = """Required inputs cannot be None
                                        command :python IngestionAdaptor.py -p
                    """
                    logging.error(
                        status_message, extra=self.execution_context.get_context()
                    )
                    raise ValueError("All inputs cannot be None")
            return flag

        except Exception as exception:
            logging.error(
                str(traceback.format_exc()), extra=self.execution_context.get_context()
            )
            raise exception

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

    def main(self, input_dict):
        batch_id = payload_id = aws_region = ""
        try:
            status_message = "Starting the main function for Ingestion Adaptor"
            logging.info(status_message)
            self.data_source = "pubmed"
            self.adapter_id = self.get_adapter_id(self.data_source)
            input_dict = input_dict["payload_id_list"]
            payload_ids = self.get_payload_id(input_dict, self.adapter_id)
            extract_data_step_name = "extract_data"
            create_structure_step_name = CREATE_STRUCT_FILE_NAME

            if payload_ids is not None and payload_ids != "":
                payload_id_list = payload_ids.split(",")
                if len(payload_id_list) > 1:
                    ERROR_MSG = "Please provide one payload at a time!"
                    raise Exception(ERROR_MSG)

            output_status = self.check_valid_payload_id(payload_id_list)

            if output_status[STATUS_KEY] == FAILED_KEY:
                logging.info(output_status[ERROR_KEY])
                return output_status

            payload_json_list = []

            for payload_id in payload_id_list:
                payload_json = self.get_payload_json(payload_id)
                payload_json_list.append(payload_json["payload_details"])

            logging.info(payload_json_list)

            output_status = self.create_app_config(FILE_ABS_PATH)
            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status
            application_config = output_status[RESULT_KEY]
            aws_region = application_config["adapter_details"]["generic_config"]["aws_region"]
            status_message = "Obtained input config: " + json.dumps(application_config)
            logging.info(status_message)

            batch_id = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S")
            cluster_id = ""
            filter_provided_flag = "N"

            for payload_json in payload_json_list:
                logging.info(json.dumps(payload_json))
                payload_id = str(payload_json[0]["payload_id"])
                create_struct_dict_list = []
                if not payload_json[0].get("load_type"):
                    raise Exception("load_type key must be present")
                if not payload_json[0].get("disease_area_col_value"):
                    raise Exception("disease_area_col_value key must be present")
                if not payload_json[0].get("delta_refresh_date_str"):
                    pass
                delta_refresh_date_str = str(payload_json[0]["load_type"])
                load_type = str(payload_json[0]["load_type"])
                self.load_type = load_type
                if load_type == DELTA_LOAD and not payload_json[0].get("delta_refresh_date_str"):
                    get_max_batch_id_query_str = (
                        "select max(cycle_id) as cycle_id from {audit_db}."
                        "{log_data_acquisition_smry_table} where source_name = "
                        " '{source_name}' and ( load_type = '{full_load_type}' or "
                        " load_type = '{delta_load_type}' )"
                        " and status = '{status}' "
                    )

                    query = get_max_batch_id_query_str.format(
                        audit_db=self.audit_db, status=SUCCESS_KEY, full_load_type=FULL_LOAD,
                        log_data_acquisition_smry_table=
                        CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                        source_name=self.data_source, delta_load_type=DELTA_LOAD
                    )

                    logging.info("Query to insert in the log table: %s", str(query))
                    get_max_batch_id_query_output = MySQLConnectionManager().execute_query_mysql(
                        query)
                    logging.info("Query output: %s", str(get_max_batch_id_query_output))

                    if not get_max_batch_id_query_output[0]['cycle_id']:
                        status_message = "No latest batch found for this payload id " \
                                         "in log table. Please enter the" \
                                         "delta refresh date in payload for delta load "
                        logging.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message + str(traceback.format_exc()))
                    latest_batch_id = get_max_batch_id_query_output[0]['cycle_id']

                if load_type == FULL_LOAD and len(payload_json[0]) > 5:
                    filter_provided_flag = "Y"

                payload_json[0]["filter_provided_flag"] = filter_provided_flag

                if load_type == FULL_LOAD and filter_provided_flag == "N":
                    data_load_type = FULL_LOAD
                elif load_type == DELTA_LOAD:
                    data_load_type = DELTA_LOAD
                else:
                    data_load_type = "filtered load"

                output_query = self.execute_query(
                    query_type="insert", table_type="smry", batch_id=batch_id,
                    payload_id=payload_id, adapter_id=self.adapter_id,
                    payload_json=json.dumps(payload_json), load_type=data_load_type,
                    cluster_id=cluster_id, status=RUNNING_KEY
                )
                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                self.page_count = 0
                if len(payload_json) > 1:
                    logging.info("Multiple Json found for this payload!")

                self.inserted_query = True
                for sub_payload in payload_json:
                    dict_data = {}
                    logging.info(sub_payload)

                    self.pre_landing_path = sub_payload["raw_file_location"]
                    self.landing_path = sub_payload["structured_file_location"]
                    if sub_payload["load_type"] == DELTA_LOAD and not sub_payload.get(
                            "delta_refresh_date_str"):
                        logging.info(
                            "Delta load is requested but no refresh rate has been given so taking"
                            " the latest batch as refresh rate")
                        sub_payload["delta_refresh_date_str"] = None
                    if sub_payload.get("stitching_flag"):
                        self.stitch_flag = sub_payload["stitching_flag"]
                        self.stitch_columns = sub_payload["stitching_columns"]
                    if sub_payload["load_type"] == DELTA_LOAD and sub_payload[
                        "delta_refresh_date_str"] == None:
                        # sub_payload["delta_refresh_date_str"] = latest_batch_id[:8]
                        pass

                    if self.inserted_query:
                        output_query = self.execute_query(
                            query_type="insert", table_type="dtl", batch_id=batch_id,
                            adapter_id=self.adapter_id, payload_id=payload_id,
                            step_name=extract_data_step_name,
                            raw_file_dir=self.pre_landing_path, structured_file_path="",
                            status=RUNNING_KEY
                        )
                        if output_query[STATUS_KEY] == FAILED_KEY:
                            raise Exception(output_query[ERROR_KEY])

                    self.inserted_query = False
                    sub_payload.pop("raw_file_location")
                    sub_payload.pop("structured_file_location")
                    sub_payload.pop("payload_id")
                    if self.stitch_flag:
                        sub_payload.pop("stitching_flag")
                        sub_payload.pop("stitching_columns")

                    dict_data["disease_area_col_value"] = sub_payload["disease_area_col_value"]

                    output = self.trigger_data_ingestion(
                        sub_payload, batch_id, payload_id, FILE_ABS_PATH, cluster_id
                    )

                    if output[STATUS_KEY] == FAILED_KEY:
                        output_query = self.execute_query(
                            query_type="update", table_type="smry",
                            batch_id=batch_id, payload_id=payload_id, status=FAILED_KEY,
                        )

                        if output_query[STATUS_KEY] == FAILED_KEY:
                            raise Exception(output_query[ERROR_KEY])

                        output_query = self.execute_query(
                            query_type="update", table_type="dtl", batch_id=batch_id,
                            payload_id=payload_id, step_name=extract_data_step_name,
                            status=FAILED_KEY,
                        )

                        if output_query[STATUS_KEY] == FAILED_KEY:
                            raise Exception(output_query[ERROR_KEY])

                        raise Exception(output[ERROR_KEY])

                    logging.debug("final output: " + str(json.dumps(output)))

                    dict_data["path_extracted_json"] = output[SPARK_CONF_PATH]
                    dict_data["datasource"] = self.data_source
                    if self.load_type == DELTA_LOAD and self.stitch_flag:
                        dict_data["stitching_columns"] = self.stitch_columns

                    create_struct_dict_list.append(dict_data)

                output_query = self.execute_query(
                    query_type="update", table_type="dtl",
                    batch_id=batch_id, step_name=extract_data_step_name,
                    payload_id=payload_id, status=SUCCESS_KEY,
                )

                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                logging.info("Launching cluster ...")
                self.cluster_id = self.call_launch_cluster(data_source_name=self.data_source)
                self.cluster_launch_flag = "Y"
                cluster_id = self.cluster_id
                # cluster_id = 'j-2W1F2ODJG311Z'
                query_str = (
                    "Update {audit_db}.{log_data_acquisition_smry_table} set "
                    "cluster_id = '{cluster_id}' "
                    ", end_time = '{end_time}' where cycle_id = '{batch_id}' and "
                    " source_name = '{data_source}' and payload_id ='{payload_id}'"
                )
                output_query = query_str.format(
                    audit_db=self.audit_db, cluster_id=cluster_id,
                    log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                    batch_id=batch_id, data_source=self.data_source, end_time=datetime.utcnow(),
                    payload_id=payload_id
                )

                logging.info("Query to insert in the log table: %s", str(output_query))
                response = MySQLConnectionManager().execute_query_mysql(output_query, False)
                logging.info("Query output: %s", str(response))

                logging.info(
                    "Create structured dictionary list ------->>  " + str(create_struct_dict_list)
                )

                output_query = self.execute_query(
                    query_type="insert", table_type="dtl", batch_id=batch_id,
                    payload_id=payload_id, adapter_id=self.adapter_id,
                    step_name=create_structure_step_name,
                    raw_file_dir=self.pre_landing_path, structured_file_path=self.landing_path,
                    status=RUNNING_KEY
                )

                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                output_status = self.call_create_structured_file(
                    create_struct_dict_list, application_config, cluster_id,
                    payload_id, batch_id
                )

                logging.debug(str(output_status))
                if output_status[STATUS_KEY] == FAILED_KEY:

                    output_query = self.execute_query(
                        query_type="update", table_type="smry",
                        batch_id=batch_id, status=FAILED_KEY, payload_id=payload_id,
                    )

                    if output_query[STATUS_KEY] == FAILED_KEY:
                        raise Exception(output_query[ERROR_KEY])

                    output_query = self.execute_query(
                        query_type="update", table_type="dtl",
                        batch_id=batch_id, step_name=create_structure_step_name,
                        payload_id=payload_id, status=FAILED_KEY,
                    )

                    if output_query[STATUS_KEY] == FAILED_KEY:
                        raise Exception(output_query[ERROR_KEY])

                    return output_status

                output_parquet_file_path = output_status[OUTPUT_PARQUET_PATH]

                logging.info(
                    "output_parquet_file_path : " + str(output_parquet_file_path)
                )

                logging.info("ingestion completed for payload!")

                output_query = self.execute_query(
                    query_type="update", table_type="smry",
                    batch_id=batch_id, status=SUCCESS_KEY, payload_id=payload_id,
                )

                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                query_str = (
                    "Update {audit_db}.{log_data_acquisition_dtl_table} set status = '{status}' ,"
                    " structured_file_path = '{structured_file_path}', end_time = '{end_time}' "
                    "where cycle_id = '{batch_id}' and source_name = '{data_source}' and "
                    "step_name = '{step_name}' and payload_id ='{payload_id}'"
                )

                query = query_str.format(
                    audit_db=self.audit_db, status=SUCCESS_KEY,
                    structured_file_path=output_parquet_file_path,
                    log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                    batch_id=batch_id, data_source=self.data_source,
                    step_name=create_structure_step_name, payload_id=payload_id,
                    end_time=datetime.utcnow(),
                )

                logging.info("Query to insert in the log table: %s", str(query))
                response = MySQLConnectionManager().execute_query_mysql(query, False)
                logging.info("Query output: %s", str(response))

                logging.info("Calling Termination of cluster !!")

                # if self.cluster_launch_flag == "Y":
                #     self.call_terminate_cluster(self.cluster_id, aws_region)

                self.cluster_launch_flag = "N"

            logging.info("Completed main function")
            output_status[STATUS_KEY] = SUCCESS_KEY
            return output_status

        except Exception:
            output_query = self.execute_query(
                query_type="update", table_type="smry",
                batch_id=batch_id, payload_id=payload_id, status=FAILED_KEY,
            )
            logging.debug(output_query)
            if output_query[STATUS_KEY] == FAILED_KEY:
                raise Exception(output_query[ERROR_KEY])
            error_msg = str(traceback.format_exc())
            logging.debug("error_msg = " + error_msg)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_msg}
        finally:
            logging.info("In finally block , terminating the cluster if launched")
            # if self.cluster_launch_flag == "Y" and aws_region != "":
            #     self.call_terminate_cluster(self.cluster_id, aws_region)


def get_commandline_arguments():
    """
    Purpose   :   This method is used to get all the commandline arguments .
    Input     :   Console Input
    Output    :   Dictionary of all the command line arguments.
    """
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        usage="%(prog)s [command line parameters]",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """

   -p, --payload_id_list : Payload Id list


    command : python IngestionAdaptor.py -p



    The list of command line parameters valid for %(prog)s module."""
        ),
        epilog="The module will exit now!!",
    )

    # List of Mandatory Parameters

    parser.add_argument(
        "-p",
        "--payload_id_list",
        required=False,
        help="Specify comma separated payload ids using -p/--payload_id_list",
    )

    cmd_arguments = vars(parser.parse_args())
    return cmd_arguments


if __name__ == "__main__":
    try:
        commandline_arguments = get_commandline_arguments()
        IngestionAdaptor_obj = DataIngestionAdapter()
        data_source = "pubmed"
        if IngestionAdaptor_obj.check_mandatory_parameters_exists(
                commandline_arguments
        ):
            output = IngestionAdaptor_obj.main(input_dict=commandline_arguments)
        else:
            DataIngestionAdapter().usage()
            raise Exception(str(traceback.format_exc()))
        if output[STATUS_KEY] == FAILED_KEY:
            raise Exception(output[ERROR_KEY])
        STATUS_MSG = "\nCompleted execution for Adaptor with status " + str(output)
        sys.stdout.write(STATUS_MSG)

    except Exception as exception:
        message = "Error occurred in main Error : %s\nTraceback : %s" % (
            str(exception),
            str(traceback.format_exc()),
        )
        raise exception
