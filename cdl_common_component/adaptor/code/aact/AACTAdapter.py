
"""
  Module Name         :   DataIngestionAdapter
  Purpose             :   This module performs below operation:
                              a. extract data from pharma_intelligence,ct_gov and pubmed
                              data sources.
                              b. Perform logging in delta table
  Input               :    Payload ID
  Output              :   Return status SUCCESS/FAILED
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
from datetime import datetime
import argparse
import textwrap
import boto3
import requests


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
CONVERTED_JSON_DIR = "CONVERTED_JSON"
DELETED_TRIAL_ID = "DELETED_TRIAL_ID"
THRESHOLD_RETRY_LIMIT = 3
CLUSTER_CONFIG = "cluster_variables.json"
OUTPUT_PARQUET_PATH = "output_parquet_file_path"
FILE_ABS_PATH = path.abspath(os.path.dirname(__file__))
SERVICE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, SERVICE_DIR_PATH)
CODE_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIR_PATH, "../common_utilities"))
sys.path.insert(1, CODE_DIR_PATH)
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from SqoopIngestor import SqoopIngestor
# from SshEmrUtility import SshEmrUtility
from DILogSetup import get_logger
from CustomS3Utility import CustomS3Utility
from FileTransferUtility import FileTransferUtility
from EmrManagerUtility import EmrManagerUtility
from ExecutionContext import ExecutionContext
# from ConfigUtility import JsonConfigUtility
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
        self.exact_match = self.s3_bucket_name = self.current_timestamp = None
        self.filter_parameters = self.filter_provided_flag = self.local_xml_file_dir = None
        self.get_csv_data = self.delta_refresh_date_str = self.bearer_token = self.fetch_url = None
        self.temp_local_path = self.get_xml_data = self.common_functions = None
        self.common_functions = self.dataframe_columns = self.batch_id = None
        self.create_alias_mapping_flag = self.s3_deleted_trial_id = self.spark_job_path = None
        # self.get_token_url = self.s3_temp_path_ts = self.load_type = None
        self.s3_temp_path = self.search_url = self.database_landing_path = self.full_load_url = None
        self.pre_landing_layer = self.landing_layer_batch_id = self.temp_local_path_ts = None
        self.pre_landing_layer_batch_id = self.pre_landing_layer_unzipped_batch_id = None
        self.s3_temp_spark_config_path = self.get_version_data = None
        self.landing_database_name = self.pre_landing_layer_zipped_batch_id = None
        # self.disease_area_col_value = self.removed_trial_id_list = self.landing_layer = None
        # self.disease_search_name = self.version_repository_file_path = None
        self.full_load_search_url = self.delta_load_url = self.logging_db_name = None
        self.datasource = self.working_database_name = self.application_config = None
        self.auth_token_headers = self.get_history_data = self.s3_temp_path_converted_json = None
        self.s3_temp_path_converted_json = self.pubmed_filter_list = None
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


class DataIngestionAdapter:
    """
       Purpose             :   This module performs below operation:
                                 a. extract data from pharma_intelligence,
                                  ct_gov and pubmed data sources.
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
                        start_time=datetime.utcnow(), end_time="")

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

    def trigger_aact_ingestion(self, application_config, filter_payload, batch_id, cluster_id,
                               datasource):
        """
        Purpose     :   This function used for get data from AACT postgres into hdfs in parquet
                        format
        Input       :   application_config,cluster_id,batch_id,datasource
        Output      :   Return status SUCCESS/FAILED
        """
        status_message = "Starting method to extract_aact_data"
        logging.info(status_message)
        STATUS_KEY = "status"
        try:
            # target_hdfs_path = "/home/hadoop/sqoop1/"
            target_hdfs_path = application_config["adapter_details"]["aact"]["target_hdfs_path"]
            emr_region_name = application_config["adapter_details"]["aact"]["emr_region_name"]
            emr_keypair_path = application_config["adapter_details"]["generic_config"][
                "private_key_loaction"
            ]
            logging.info(filter_payload)
            tableName = filter_payload["table_name"]
            condition = filter_payload["condition"]
            parameters = filter_payload["params"]
            columns = filter_payload["columns"]
            logging.info(tableName)
            hdfs_target_path = os.path.join(target_hdfs_path, tableName + "_" +
                                            str(self.counter)  + "_" + self.payload_id)
            hdfs_target_path = hdfs_target_path + "/{batch_id}/".format(batch_id=batch_id)
            self.counter = self.counter + 1
            self.s3_parquet_output_path = (
                self.landing_path + "/" + "AACT_" + tableName.upper() + "/" + datasource +
                "_" + tableName + "_" + batch_id[:14] + "_" + self.payload_id + ".parquet")
            query = None
            where = None
            if "aact_db_name" in application_config["adapter_details"]["aact"]:
                aact_db_name = application_config["adapter_details"]["aact"]["aact_db_name"]
            else:
                logging.error("Mandatory key: aact_db_name not found in input")
                raise Exception("Mandatory key: aact_db_name not found in input")
            if "aact_host_name" in application_config["adapter_details"]["aact"]:
                aact_host_name = application_config["adapter_details"]["aact"]["aact_host_name"]
                if not aact_host_name:
                    logging.error("aact_host_name not provided which is a mandatory field")
                    raise Exception("aact_host_name not provided which is a mandatory field")
            else:
                logging.error("Mandatory key: hostName not found in input")
                raise Exception("Mandatory key: hostName not found in input")
            if "aact_port_number" in application_config["adapter_details"]["aact"]:
                aact_port_number = application_config["adapter_details"]["aact"]["aact_port_number"]
                if not aact_port_number:
                    logging.error("Port number not provided which is a mandatory field")
                    raise Exception("Port number not provided which is a mandatory field")
            else:
                logging.error("Mandatory key: aact_host_name not found in input")
                raise Exception("Mandatory key: aact_host_name not found in input")
            if "aact_database_name" in filter_payload:
                aact_database_name = filter_payload["aact_database_name"]
                if not aact_database_name:
                    logging.error("aact_database_name name not provided which is a mandatory field")
                    raise Exception("aact_database_name name not provided which is a mandatory "
                                    "field")
            else:
                logging.error("Mandatory key: aact_database_name not found in input")
                raise Exception("Mandatory key: aact_database_name not found in input")
            if "aact_username" in application_config["adapter_details"]["aact"]:
                aact_username = application_config["adapter_details"]["aact"]["aact_username"]
                if not aact_username:
                    logging.error("aact_username not provided which is a mandatory field")
                    raise Exception("aact_username not provided which is a mandatory field")
            else:
                logging.error("Mandatory key: aact_username not found in input")
                raise Exception("Mandatory key: aact_username not found in input")
            if "aact_password" in application_config["adapter_details"]["aact"]:
                aact_password = application_config["adapter_details"]["aact"]["aact_password"]
            else :
                try:
                    secret_password = {}
                    secret_password = MySQLConnectionManager().get_secret(
                        self.configuration["adapter_details"]["generic_config"]["adaptor_password_secret_name"],
                        self.configuration["adapter_details"]["generic_config"]["s3_region"])
                    aact_password = secret_password['aact_password']
                except:
                    logging.error("aact_password not provided which is a mandatory field")
                    raise Exception("aact_password not provided which is a mandatory field")

            if "aact_column_datatype_mapping" in application_config["adapter_details"]["aact"]:
                aact_column_datatype_mapping = \
                    application_config["adapter_details"]["aact"]["aact_column_datatype_mapping"]
            else:
                aact_column_datatype_mapping = {}
                logging.error("AACT datatype config: aact_column_datatype_mapping not "
                              "found in input")
            if "query" in application_config["adapter_details"]["aact"]:
                query = application_config["adapter_details"]["aact"]["query"]
                query = query.lstrip('"').rstrip('"')
                query = query.lstrip("'").rstrip("'")
            if "condition" in filter_payload:
                where = condition
                #where = where.replace("'", '"')
            if "params" in filter_payload:
                parameter = parameters
            get_emr_ip_response = CommonUtils().get_emr_ip_list(cluster_id=cluster_id,
                                                                region=emr_region_name)
            emr_ip = get_emr_ip_response[RESULT_KEY][0]
            if tableName in aact_column_datatype_mapping:
                column_datatype_dict = aact_column_datatype_mapping[tableName]
                # Col1=String, Col2=String
                parameter = "--map-column-java " + ",".join(["{}={}".
                                                             format(x, column_datatype_dict[x])
                                                             for x in column_datatype_dict])
            if "columns" in filter_payload:
                columns = filter_payload["columns"]
                if not columns:
                    logging.error("column name not provided which is a mandatory field")
                    raise Exception("column name not provided which is a mandatory "
                                    "field")



            # aact credentials
            sqoop_config = {"dbType": "postgresql", "clusterId": cluster_id,
                            "clusterHost": str(emr_ip),
                            "password": aact_password,
                            "username": aact_username, "host": aact_host_name,
                            "port": aact_port_number, "dbname": aact_database_name,
                            "targetPath": hdfs_target_path,
                            "tableNames": [tableName], "outputFileType": "parquet",
                            "query": "", "whereClause": where,
                            "params": parameter,"columns": columns}

            logging.debug("Trigger sqoop ingestor with the payload - {}"
                          .format(json.dumps(sqoop_config)))

            time.sleep(5)
            try:
                sqoop_ingestion_object = SqoopIngestor(sqoop_config)
                validation = sqoop_ingestion_object.input_validation(sqoop_config)
                response = sqoop_ingestion_object.ingest_data(validation)
                logging.info(str(response))
                if response[0]["importStatus"] is False or \
                        response[0]["file_transferred_path"] == FAILED_KEY:
                    return {STATUS_KEY: FAILED_KEY, ERROR_KEY: response}
                command_to_copy_parquet_file_to_s3 = (
                    "aws s3 cp " +  response[0]["file_transferred_path"] + " "
                    + self.s3_parquet_output_path
                )

                logging.info(
                    "command for awscli : " + command_to_copy_parquet_file_to_s3
                )

                # CommonUtils().execute_shell_command(command_to_copy_parquet_file_to_s3)
                CustomS3Utility().copy_s3_to_s3_recursively(response[0]["file_transferred_path"], self.s3_parquet_output_path)

                logging.debug("Sqoop utility was executed successfully")
            except Exception as e:
                status_message = "Sqoop ingestion failed"
                STATUS_KEY = FAILED_KEY
                logging.error(status_message + "ERROR - " + str(traceback.format_exc()))
                raise Exception(status_message)

            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: self.s3_parquet_output_path, "tableName": tableName}

        except:
            error_message = str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message, "tableName": tableName}

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
            self, payload_json, batch_id, input_config_path, cluster_id
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



            failed_flag = True

            filter_payload = payload_json


            output_status = self.trigger_aact_ingestion(application_config, filter_payload,
                                                        batch_id, cluster_id,
                                                        self.data_source)
            logging.info("testing aact" + output_status[STATUS_KEY], output_status["tableName"])
            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status

            output_s3_path = output_status[RESULT_KEY]

            failed_flag = False
            return {OUTPUT_PARQUET_PATH: output_s3_path, STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = str(traceback.format_exc())
            logging.debug(str(error_message))
            return {OUTPUT_PARQUET_PATH: "NONE", STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}
        finally:
            if failed_flag:
                logging.info("failed")

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


    def get_payload_json(self, payload_id):

        get_payload_query_str = "select payload_details_file_name,raw_file_dir," \
                                "structured_file_dir" \
                                " from {audit_db}.{adapter_payload_details_table} " \
                                "where payload_id" \
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
        print("###",get_adapter_id_query_str)
        get_payload_id_query = get_adapter_id_query_str.format(
            audit_db=self.audit_db,
            adapter_details_table=CommonConstants.ADAPTER_DETAILS_TABLE,
            data_source=data_source,
        )
        logging.debug("Query to get the paylod id - {}".format(get_payload_id_query))
        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            get_payload_id_query
        )
        adapter_id = get_payload_result[0]["adapter_id"]
        adapter_id = str(adapter_id)
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
        logging.debug("Query to get the payload id - {}".format(get_payload_id_query))
        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            get_payload_id_query
        )
        payload_id = get_payload_result[0]["payload_id"]
        payload_id = str(payload_id)
        return payload_id


    def main(self, input_dict):
        aws_region = ""
        try:
            status_message = "Starting the main function for Ingestion Adaptor"
            logging.info(status_message)
            self.data_source = "aact"
            self.adapter_id = self.get_adapter_id(self.data_source)
            input_dict = input_dict["payload_id_list"]
            payload_ids = self.get_payload_id(input_dict, self.adapter_id)
            extract_data_step_name = "extract_data"

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

            batch_date = datetime.now()
            batch_id = str(batch_date.strftime("%Y%m%d%I%M%S%f"))
            cluster_id = ""
            data_load_type = "filtered load"
            for payload_json in payload_json_list:
                logging.info(json.dumps(payload_json))
                self.payload_id = str(payload_json[0]["payload_id"])
                self.counter = 0

                logging.debug("Payload JSON - {}".format(json.dumps(payload_json)))
                output_query = self.execute_query(
                    query_type="insert", table_type="smry", batch_id=batch_id,
                    adapter_id=self.adapter_id, payload_id=self.payload_id,
                    payload_json=json.dumps(payload_json), load_type=data_load_type,
                    cluster_id=cluster_id, status=RUNNING_KEY
                )
                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                self.page_count = 0
                logging.info("Launching cluster ...")
                self.cluster_id = self.call_launch_cluster(data_source_name=self.data_source)
                self.cluster_launch_flag = "Y"

                if len(payload_json) > 1:
                    logging.info("Multiple Json found")

                self.inserted_query = True
                for sub_payload in payload_json:
                    # dict_data = {}
                    logging.info(sub_payload)
                    self.pre_landing_path = sub_payload["raw_file_location"]
                    self.landing_path = sub_payload["structured_file_location"]

                    if self.inserted_query:
                        output_query = self.execute_query(
                            query_type="insert", table_type="dtl", batch_id=batch_id,
                            adapter_id=self.adapter_id, payload_id=self.payload_id,
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

                    # dict_data["disease_area_col_value"] = sub_payload["disease_area_col_value"]

                    cluster_id = self.cluster_id
                    query_str = (
                        "Update {audit_db}.{log_data_acquisition_smry_table} set"
                        " cluster_id = '{cluster_id}' "
                        ", end_time = '{end_time}' where cycle_id = '{batch_id}' and "
                        " source_name = '{data_source}' and payload_id ='{payload_id}'"
                    )
                    output_query = query_str.format(
                        audit_db=self.audit_db, cluster_id=cluster_id,
                        log_data_acquisition_smry_table=
                        CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                        batch_id=batch_id, data_source=self.data_source, end_time=datetime.utcnow(),
                        payload_id=self.payload_id
                    )

                    logging.info("Query to insert in the log table: %s", str(output_query))
                    response = MySQLConnectionManager().execute_query_mysql(output_query, False)
                    logging.info("Query output: %s", str(response))

                    output = self.trigger_data_ingestion(
                        sub_payload, batch_id, FILE_ABS_PATH, cluster_id
                    )

                    if output[STATUS_KEY] == FAILED_KEY:
                        output_query = self.execute_query(
                            query_type="update", table_type="smry",
                            batch_id=batch_id, payload_id=self.payload_id, status=FAILED_KEY,
                        )

                        if output_query[STATUS_KEY] == FAILED_KEY:
                            raise Exception(output_query[ERROR_KEY])

                        output_query = self.execute_query(
                            query_type="update", table_type="dtl", batch_id=batch_id,
                            payload_id=self.payload_id, step_name=extract_data_step_name,
                            status=FAILED_KEY,
                        )

                        if output_query[STATUS_KEY] == FAILED_KEY:
                            raise Exception(output_query[ERROR_KEY])

                        raise Exception(output[ERROR_KEY])


                    logging.debug("final output: " + str(json.dumps(output)))


                output_query = self.execute_query(
                    query_type="update", table_type="dtl",
                    batch_id=batch_id, step_name=extract_data_step_name,
                    payload_id=self.payload_id, status=SUCCESS_KEY,
                )

                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                # output_query = self.execute_query(
                #     query_type="update", table_type="smry",
                #     batch_id=batch_id, status=SUCCESS_KEY, payload_id=self.payload_id
                # )
                # if output_query[STATUS_KEY] == FAILED_KEY:
                #     raise Exception(output_query[ERROR_KEY])

                logging.info("Calling Termination of cluster !!")

            # if self.cluster_launch_flag == "Y":
            #     self.call_terminate_cluster(self.cluster_id, aws_region)

            self.cluster_launch_flag = "N"


            logging.info("Completed main function")
            output_status[STATUS_KEY] = SUCCESS_KEY
            return output_status

        except Exception as exception:
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
        data_source = "aact"
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

