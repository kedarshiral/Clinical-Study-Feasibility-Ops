"""
  Module Name         :   eudract_ingestor
  Purpose             :   This module performs below operation:
                                  a. extract data from eudract data source.(in .txt format)
                                  b. process the extracted data to convert it into .csv format
                                  c. call CreateStructuredFile.py to convert the data in .csv format into .parquet files
                                  d. Perform logging in
                                        a. log_data_acquisition_dtl table
                                        b. log_data_acquisition_smry table
  Input               :    a. payload_id_list (comma separated string)
  Output              :   Return status SUCCESS/FAILED
  Pre-requisites      :
  Last changed on     :   23rd April
  Last changed by     :   Rakhi Darshi
  Reason for change   :   Comments and formatting
"""
import csv
import json
import textwrap
import sys
import os
import traceback
import argparse
import re
import ast
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import boto3
from os import path
import requests
MODULE_NAME = "eudract_ingestor.py"
APPLICATION_CONFIG_FILE = "application_config.json"
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "error"
NONE_KEY = "NONE"
RUNNING_KEY = "RUNNING"
DISEASE_AREA_NAME_KEY = "disease_area_name"
PROCESS_NAME = "Ingestion"
OUTPUT_PARQUET_PATH = "output_parquet_file_path"
CREATE_STRUCTURE_STEP_NAME = "create_structure_file"
EXTRACT_DATA_STEP_NAME = "extract_data"
LOAD_TYPE_KEY = "load_type"
FULL_LOAD = "full"
DELTA_LOAD = "delta"
DISEASE_AREA_COLUMN_VALUE_KEY = "disease_area_col_value"
FILTERED_LOAD = "filtered_load"
DELTA_REFRESH_DATE_KEY = "delta_refresh_date_str"
RAW_FILE_LOCATION_KEY = "raw_file_location"
STRUCTURED_FILE_LOCATION_KEY = "structured_file_location"
PAYLOAD_ID_KEY = "payload_id"
FILE_ABS_PATH = path.abspath(os.path.dirname(__file__))
PATH_OF_CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
PATH_OF_UTILITIES_DIRECTORY = os.path.abspath(os.path.join(PATH_OF_CURRENT_DIRECTORY, "../common_utilities/"))
CLUSTER_CONFIG = os.path.join(PATH_OF_UTILITIES_DIRECTORY,"cluster_variables.json")
sys.path.insert(0, PATH_OF_UTILITIES_DIRECTORY)
from ExecutionContext import ExecutionContext
# from ConfigUtility import JsonConfigUtility
from CommonUtils import CommonUtils
from EmrManagerUtility import EmrManagerUtility
from SystemManagerUtility import SystemManagerUtility
from DILogSetup import get_logger
logging = get_logger(file_log_name="eudract_ingestion_log.log", enable_file_log=True)
from SshEmrUtility import SshEmrUtility
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants
# from FileTransferUtility import FileTransferUtility
APP_CONFIG_PATH = "application_config.json"
PAYLOAD_DETAILS_PATH = "payload_eudract.json"

class eudract_ingestor:
    """
        Purpose             :   This module performs below operation:
                                  a. extract data from eudract data source.(in .txt format)
                                  b. process the extracted data to convert it into .csv format
                                  c. convert the data in .csv format into .parquet files
                                  d. Perform logging in
                                        a. log_data_acquisition_dtl table
                                        b. log_data_acquisition_smry table

        """

    def __init__(self):
        self.logging = get_logger()
        self.s3_client = boto3.client("s3")
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.phantomjs_service_log_path = CommonConstants.PHANTOMJS_SERVICE_LOG_PATH
        self.phantomjs_executable_path = CommonConstants.PHANTOMJS_EXECUTABLE_PATH
        create_app_config_status = self.create_app_config(PATH_OF_CURRENT_DIRECTORY)
        logging.debug(create_app_config_status)
        if create_app_config_status[STATUS_KEY] == FAILED_KEY:
            raise Exception(create_app_config_status[FAILED_KEY])
        self.app_config_data = create_app_config_status[RESULT_KEY]
        self.audit_db = self.app_config_data["adapter_details"]["generic_config"]["mysql_db"]
        self.pre_landing_path = ""
        self.landing_path = ""
        self.emr_region_name = ""
        self.cluster_id = ""
        self.launch_cluster_flag = "N"
        self.s3_parquet_output_path = ""
        logging.debug(str(os.getcwd()))
        payload_config_file_data = open(PAYLOAD_DETAILS_PATH, 'r').read()
        self.payload_config_data = json.loads(payload_config_file_data)
        self.download_url = self.app_config_data["adapter_details"]["eudract"]["download_url"]
        self.search_url = self.app_config_data["adapter_details"]["eudract"]["search_url"]
        self.local_directory_text_path = \
            self.app_config_data["adapter_details"]["eudract"]["local_directory_text_path"]
        self.local_directory_csv_path = \
            self.app_config_data["adapter_details"]["eudract"]["local_directory_csv_path"]
        self.file_prefix = \
            self.app_config_data["adapter_details"]["eudract"]["file_prefix"]
        self.data_source = self.app_config_data["adapter_details"]["eudract"]["datasource"]

    def call_launch_cluster(self, data_source_name):
        """
                Purpose     :   To launch cluster
                Input       :   data_source_name
                Output      :   Return cluster_id
                """
        try:
            logging.info("Starting Function call_launch_cluster ... ")
            cluster_type = self.app_config_data["adapter_details"][data_source_name]["cluster_type"]
            logging.debug(cluster_type)
            try:
                file_read = open(CLUSTER_CONFIG, "r")
            except Exception:
                raise Exception("File not found - {}".format(CLUSTER_CONFIG))
            json_data = json.load(file_read)
            file_read.close()
            emr_payload = json_data["cluster_configurations"][cluster_type]
            self.emr_region_name = emr_payload["Region"]
            returned_json = EmrManagerUtility().launch_emr_cluster(
                emr_payload, self.emr_region_name
            )
            logging.debug(returned_json)
            if returned_json[STATUS_KEY] == FAILED_KEY:
                raise Exception(returned_json[ERROR_KEY])
            logging.debug("Json for EMR --> " + str(returned_json))
            cluster_id = returned_json[RESULT_KEY]
            logging.debug("cluster_id --> " + str(cluster_id))
            logging.debug("Cluster launched")
            logging.info("Completing Function call_launch_cluster ... ")
            return cluster_id
        except Exception:
            raise Exception(str(traceback.format_exc()))

    def call_terminate_cluster(self, cluster_id):
        """
            Purpose     :   To terminate cluster
            Input       :   cluster_id
            Output      :   -
        """
        try:
            logging.info("Starting Function call_terminate_cluster ... ")
            logging.debug("cluster_id --> " + str(cluster_id))
            status = EmrManagerUtility().terminate_cluster(cluster_id, self.emr_region_name)
            logging.debug(status)
            if status[STATUS_KEY] == FAILED_KEY:
                raise Exception(status[ERROR_KEY])
            logging.info("Completing Function call_terminate_cluster ... ")
        except Exception:
            raise Exception(str(traceback.format_exc()))

    def create_app_config(self, application_config_dir):
        """
        Purpose     :   To extract application config details
        Input       :   application_config_file = contains all application
                        specific details, env_config_file = contains
                        all environment specific details.
        Output      :   Return status SUCCESS/FAILED
        """
        status_message = ""
        try:
            logging.info("Starting Function create_app_config ... ")
            status_message = "Calling create_app_config function"
            application_config_path = os.path.join(application_config_dir, APPLICATION_CONFIG_FILE)
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
            status_message = "Completing Function create_app_config ... "
            logging.info("Completing Function create_app_config ... ")
            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: application_config}
        except Exception:
            error_message = status_message + ": " + str(traceback.format_exc())
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def check_valid_payload_id(self, payload_id_list):
        """
            Purpose     :   Checks if payload id received from commandline argument
                            is valid or not
            Input       :   payload_id_list received from commandline argument
            Output      :   Returns Status key
        """
        try:
            logging.info("Starting Function check_valid_payload_id ... ")
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
            logging.debug(status_message)
            get_payload_result = MySQLConnectionManager().execute_query_mysql(
                get_payload_id_query
            )
            logging.debug(get_payload_result)
            payload_id_db_list = []
            for payload_id_dict in get_payload_result:
                payload_id_db_list.append(str(payload_id_dict["payload_id"]))
            status_message = "Payload id retrieved from database : " + str(payload_id_db_list)
            logging.debug(status_message)
            status_message = "Payload id retrieved from input : " + str(payload_id_list)
            logging.debug(status_message)
            for payload in payload_id_list:
                if payload not in payload_id_db_list:
                    output_status[STATUS_KEY] = FAILED_KEY
                    output_status[ERROR_KEY] = "Payload Id is invalid for this data source"
                    return output_status
            logging.info("Completing Function check_valid_payload_id ... ")
            return output_status
        except Exception:
            raise Exception(str(traceback.format_exc()))

    def get_payload_json(self, payload_id):
        """
            Purpose     :   To get payload_json corresponding to payload_id
            Input       :   payload_id received
            Output      :   Returns payload_json
        """
        try:
            self.logging.info("Starting Function get_payload_json ... ")
            latest_batch_id = ""
            get_payload_query_str = "select payload_details_file_name,raw_file_dir,structured_file_dir" \
                                    " from {audit_db}.{adapter_payload_details_table} where payload_id" \
                                    " = {payload_id}"

            get_payload_query = get_payload_query_str.format(
                audit_db=self.audit_db,
                adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE,
                payload_id=payload_id,
            )
            status_message = "Get Payload Query : " + str(get_payload_query)
            logging.debug(status_message)
            get_payload_result = MySQLConnectionManager().execute_query_mysql(get_payload_query)
            logging.debug(get_payload_result)
            payload_json_file_name = get_payload_result[0]["payload_details_file_name"]
            logging.debug(payload_json_file_name)
            status_message = "Payload json file name retrived : " + str(payload_json_file_name)
            logging.debug(status_message)
            payload_json = json.loads(open(payload_json_file_name, "r").read())
            logging.debug(payload_json)
            #to check only one disease_area in all sub_payload
            if "payload_details" not in payload_json:
                raise Exception("'payload_details' key must be present in payload file ")

            if len(payload_json["payload_details"]) == 0:
                raise Exception("payload cannot be empty")
            list_of_disease_areas = []
            list_of_disease_areas.append(payload_json["payload_details"][0][DISEASE_AREA_COLUMN_VALUE_KEY])

            for dict_data in payload_json["payload_details"]:
                if LOAD_TYPE_KEY not in dict_data:
                    raise Exception(LOAD_TYPE_KEY + " key must be present in payload ")
                else:
                    if dict_data[LOAD_TYPE_KEY] == "" or type(dict_data[LOAD_TYPE_KEY]) != str:
                        raise Exception("Value of load_type key should not be empty and must be string ")

                if DISEASE_AREA_COLUMN_VALUE_KEY not in dict_data:
                    raise Exception(DISEASE_AREA_COLUMN_VALUE_KEY + " key must be present in payload ")
                else:
                    if dict_data[DISEASE_AREA_COLUMN_VALUE_KEY] == "" or \
                            type(dict_data[DISEASE_AREA_COLUMN_VALUE_KEY])!=str:
                        raise Exception("Value of disease_area_col_value key should not be empty and must be string ")

                if dict_data[DISEASE_AREA_COLUMN_VALUE_KEY] not in list_of_disease_areas:
                    logging.debug("sub_payload = " + str(dict_data))
                    raise Exception("Every sub payload should have the same disease area column value")

                load_type = str(dict_data[LOAD_TYPE_KEY])
                if load_type == FULL_LOAD and len(dict_data) > 2:
                    logging.debug("data_load_type = "+str(load_type))
                    data_load_type = FILTERED_LOAD
                elif load_type == FULL_LOAD and len(dict_data) == 2:
                    logging.debug("data_load_type = "+str(load_type))
                    data_load_type = FULL_LOAD
                elif load_type == DELTA_LOAD and DELTA_REFRESH_DATE_KEY in dict_data:
                    logging.debug("data_load_type = "+str(load_type))
                    data_load_type = DELTA_LOAD
                elif (load_type == DELTA_LOAD and DELTA_REFRESH_DATE_KEY not in dict_data) or \
                        (load_type == DELTA_LOAD and DELTA_REFRESH_DATE_KEY ==""):
                    # then get max batch_id from table
                    latest_batch_id_result = self.get_max_batch_id()
                    logging.debug("data_load_type = "+str(load_type))
                    data_load_type = DELTA_LOAD
                    if latest_batch_id_result[STATUS_KEY] == SUCCESS_KEY:
                        latest_batch_id = latest_batch_id_result[RESULT_KEY]
                        dict_data["delta_refresh_date_str"] = latest_batch_id[:8]
                else:
                    raise Exception("enter a valid payload")

                dict_data["raw_file_location"] = get_payload_result[0]["raw_file_dir"]
                dict_data["structured_file_location"] = get_payload_result[0]["structured_file_dir"]
                dict_data["payload_id"] = payload_id
            status_message = (
                "Payload json retrived from s3 for payload id :"
                + payload_id
                + " is - "
                + str(payload_json)
            )
            self.logging.debug(status_message)
            self.logging.info("Completing Function get_payload_json ... ")
            return payload_json
        except Exception:
            raise Exception(str(traceback.format_exc()))

    def download_eudract(self, sub_payload_json, batch_id, counter, payload_id):
        """
            Purpose     :   To download data from eudract in .txt files based on payload json and upload to s3
            Input       :   payload_json, batch_id , counter, payload_id (used for naming downloaded files)
            Output      :   Returns Status key
        """
        try:
            self.logging.info("Starting Function download_eudract ... ")
            self.local_directory_text_path = self.local_directory_text_path.replace("$$batch_id", batch_id)
            self.file_prefix = self.file_prefix.replace("$$batch_id", batch_id)
            disease_area = sub_payload_json[DISEASE_AREA_COLUMN_VALUE_KEY].replace(" ","_")
            self.file_prefix = self.file_prefix.replace("$$disease_area_col_name", disease_area)
            if not os.path.exists(self.local_directory_text_path.strip()):
                os.makedirs(self.local_directory_text_path)

            destination = sub_payload_json["raw_file_location"]
            self.logging.debug(destination)
            logging.debug(str(self.phantomjs_service_log_path).rstrip("/"))
            logging.debug(str(CommonConstants.PHANTOMJS_LOG_FILE_NAME))
            logging.debug(str(self.phantomjs_executable_path).rstrip("/")+ "/")
            driver = webdriver.PhantomJS(service_log_path=str(self.phantomjs_service_log_path).rstrip("/") + "/" +
                                         CommonConstants.PHANTOMJS_LOG_FILE_NAME, executable_path=
                                         str(self.phantomjs_executable_path).rstrip("/") + "/" + "phantomjs",
                                         service_args=['--ignore-ssl-errors=true', '--ssl-protocol=any',
                                                       '--web-security=false'])
            payload = {'query': '', 'page': "0", 'mode': 'current_page'}

            if sub_payload_json[LOAD_TYPE_KEY] == FULL_LOAD and len(sub_payload_json)>5:
                for key, value in sub_payload_json.items():
                    if (key in DISEASE_AREA_COLUMN_VALUE_KEY) or (key in RAW_FILE_LOCATION_KEY) or \
                            (key in STRUCTURED_FILE_LOCATION_KEY) or (key in PAYLOAD_ID_KEY) or \
                        (key in LOAD_TYPE_KEY) or (key in DISEASE_AREA_NAME_KEY):
                        continue
                    payload[key] = value
                if DISEASE_AREA_NAME_KEY in sub_payload_json:
                    payload['query'] = sub_payload_json[DISEASE_AREA_NAME_KEY]

            if sub_payload_json[LOAD_TYPE_KEY] == DELTA_LOAD:
                payload[DELTA_REFRESH_DATE_KEY] = sub_payload_json[DELTA_REFRESH_DATE_KEY]

            # filter_provided_status = sub_payload_json["filter_provided_flag"]
            # if filter_provided_status == "Y":
            #     for key, value in sub_payload_json["filters"].items():
            #         payload[key] = value

            logging.debug("payload query = "+str(payload))
            logging.debug("search url ="+self.search_url)
            request_data = requests.get(self.search_url, params=payload, verify=False)
            logging.debug("search url = "+str(request_data.url))
            self.logging.debug(request_data.url)
            driver.get(request_data.url)
            select = Select(driver.find_element_by_id('dContent'))
            select.select_by_visible_text("Full Trial Details")
            driver.find_element_by_id('submit-download').click()
            pages = driver.find_element_by_xpath("//div[@id='tabs-1']/div[1]").text
            logging.debug(pages)
            last_page = pages.split('Displaying page 1 of')[1].strip().replace(".", "").replace(",", "")

            self.logging.debug(last_page)
            string_in_file = ''
            local_path = self.local_directory_text_path + self.file_prefix + \
                         "_" + payload_id + "_" +str(counter) + ".txt"
            self.logging.debug("source path = "+local_path)
            # we find last page and then iterate through last page to first page
            for index in range(int(last_page), 0, -1):  # last page =2   i=1,2
                payload["page"] = str(index)
                request_data = requests.get(self.download_url, params=payload, verify=False)
                logging.debug("download url = " + str(request_data.url))
                self.logging.debug(request_data.url)
                string_in_file += request_data.text.replace('\r', '*_delimiter*')
                with open(local_path, "ab+") as output_file:
                    output_file.write(string_in_file.encode('utf-8'))
                    output_file.close()
            local_path = str(local_path.strip())
            response = CommonUtils().upload_s3_object(local_path.strip(), destination.strip())
            self.logging.debug(response)
            if response[STATUS_KEY] == FAILED_KEY:
                raise Exception(response[ERROR_KEY])
            self.logging.info("Completing Function download_eudract ... ")
            return {STATUS_KEY: SUCCESS_KEY}
        except Exception:
            error_msg = str(traceback.format_exc())
            return{STATUS_KEY : FAILED_KEY, ERROR_KEY : error_msg}

    def pre_process_eudract(self, sub_payload_json, batch_id, counter, payload_id):
        """
            Purpose     :   To convert data downloaded in .txt format to .csv files and upload to s3
            Input       :   payload_json, batch_id , counter, payload_id
            Output      :   Returns Status key
        """
        try:
            self.logging.info("Starting Function preprocess_eudract ... ")
            status_message = "Starting to pre-process eudract files"
            self.logging.debug(status_message)
            self.local_directory_csv_path = self.local_directory_csv_path.replace("$$batch_id", batch_id)
            self.local_directory_text_path = self.local_directory_text_path.replace("$$batch_id", batch_id)
            self.file_prefix = self.file_prefix.replace("$$batch_id", batch_id)
            disease_area = sub_payload_json[DISEASE_AREA_COLUMN_VALUE_KEY].replace(" ", "_")
            self.file_prefix = self.file_prefix.replace("$$disease_area_col_name", disease_area)
            configuration_columns = open("column_config.json", 'r').read()
            configuration_columns = json.loads(configuration_columns)

            if not os.path.exists(self.local_directory_csv_path.strip()):
                os.makedirs(self.local_directory_csv_path)
                # self.logging.debug(status)

            list_of_configuration_columns = []
            for text_column, csv_columns in configuration_columns.items():
                list_of_configuration_columns.append(csv_columns)

            for filename in os.listdir(self.local_directory_text_path):
                trials_in_file = \
                open(os.path.join
                     (self.local_directory_text_path, filename), 'r').read().split('Summary*_delimiter*')[1:]
                list_of_trials_dict = []
                for trial in trials_in_file:
                    cols = trial.split('*_delimiter*')
                    trial_dict = dict()
                    trial_dict["B.1.1 Name of Sponsor".lower()] = ""
                    trial_dict["D.8.3 Pharmaceutical form of the placebo".lower()] = ""
                    for col in cols:
                        try:
                            (str1, str2) = col.split(':', 1)
                            str1 = str1.strip()
                            str2 = str2.strip()
                            if str1.strip() == "B.1.1 Name of Sponsor" or str1.strip() == \
                                    "D.8.3 Pharmaceutical form of the placebo":
                                trial_dict[str1.lower()] = str2.replace('\n', ';') + ";" + trial_dict[str1.lower()]
                            else:
                                trial_dict[str1.lower()] = str2.replace('\n', ';')

                        except ValueError:
                            trial_dict[col] = ""
                    input_dict = dict()

                    for columns, csv_columns in configuration_columns.items():
                        columns = columns.strip().lower()
                        if columns in trial_dict:
                            input_dict[csv_columns] = trial_dict[columns]
                        else:
                            input_dict[csv_columns] = ""
                    list_of_trials_dict.append(input_dict)

                local_path = self.local_directory_csv_path + self.file_prefix + "_" + payload_id + "_"+str(counter) \
                             + ".csv"
                self.logging.debug("source path = "+local_path)
                local_path = local_path.strip()
                # destination = "s3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/clinical_data_lake/dev/eudract/raw_files/"
                destination = sub_payload_json["raw_file_location"]
                destination_file_name = destination+self.file_prefix +"_" + payload_id + "_" + str(counter) +".csv"

                csv_tmp_path_location = self.app_config_data["adapter_details"]["eudract"]["csv_tmp_path_location"]
                csv_tmp_path_location = os.path.join(csv_tmp_path_location, batch_id)
                csv_tmp_path_location = os.path.join(csv_tmp_path_location, payload_id)

                with open(local_path, 'w') as output_file:
                    dict_writer = csv.DictWriter(output_file, list_of_configuration_columns)
                    dict_writer.writeheader()
                    dict_writer.writerows(list_of_trials_dict)
                    response = CommonUtils().upload_s3_object(local_path.strip(), destination.strip())
                    self.logging.debug(response)
                    if response[STATUS_KEY] == FAILED_KEY:
                        raise Exception(response[ERROR_KEY])
                    status_message = CommonUtils().copy_file_s3(destination_file_name, csv_tmp_path_location)
                    if status_message[STATUS_KEY] == FAILED_KEY:
                        raise Exception(status_message[ERROR_KEY])

            local_directory_path = self.app_config_data["adapter_details"]["eudract"]["local_directory_path"]
            local_directory_path = local_directory_path.replace("$$batch_id", batch_id)
            cmd = "rm -r "+ local_directory_path
            self.logging.debug(cmd)
            status = os.system(cmd)
            self.logging.debug(status)

            self.logging.info("Completing Function preprocess_eudract ... ")
            return {STATUS_KEY: SUCCESS_KEY}
        except Exception:
            # self.logging.debug ("Exception in pre-processing eudract!")
            error_msg = str(traceback.format_exc())
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_msg}

    def execute_query(
            self, table_type=None, query_type=None, batch_id=None,adapter_id=None, payload_id=None,
            step_name=None, payload_json=None, load_type=None, cluster_id=None, raw_file_dir=None,
            structured_file_path=None, status=None
    ):
        """
        Purpose     :   Execute query on databricks delta tables
        Input       :   query_type, datasource , step_name , status = failed, success etc.
        Output      :   Status of query execution
        """
        try:
            query = ""
            logging.info("Starting Function execute_query ... ")
            if query_type.lower() == "insert" and table_type.lower() == "smry":
                query = "Insert into {audit_db}.{log_data_acquisition_smry_table} values ('{batch_id}'" \
                        ",'{adapter_id}','{payload_id}','{data_source}','{payload_json}','{load_type}','{cluster_id}','{status}'" \
                        ",'{start_time}','{end_time}')".format(
                            audit_db=self.audit_db,
                            log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                            batch_id=batch_id,adapter_id=self.adapter_id, payload_id=payload_id, load_type=load_type, data_source=self.data_source,
                            payload_json=json.dumps(payload_json), cluster_id=cluster_id, status=status,
                            start_time=datetime.utcnow(), end_time="")

            elif query_type.lower() == "insert" and table_type.lower() == "dtl":

                query = (
                    "Insert into {audit_db}.{log_data_acquisition_dtl_table} values ('{batch_id}'"
                    ",'{adapter_id}','{payload_id}','{data_source}','{step_name}', '{raw_file_dir}'"
                    ",'{structured_file_path}','{status}','{start_time}','{end_time}')".format(
                        audit_db=self.audit_db,
                        log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                        batch_id=batch_id,adapter_id=self.adapter_id, payload_id=payload_id, data_source=self.data_source,
                        step_name=step_name, raw_file_dir=raw_file_dir,
                        structured_file_path=structured_file_path, status=status, start_time=datetime.utcnow(),
                        end_time=""
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
                logging.debug("Query to insert in the log table: ", str(query))
            response = MySQLConnectionManager().execute_query_mysql(query, False)
            logging.debug("Query output: ", str(response))

            logging.info("Completing Function execute_query ... ")
            return {STATUS_KEY: SUCCESS_KEY}
        except Exception:
            error_message = str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def check_mandatory_parameters_exists(self, input_dict):
        """
        Purpose   :   This method is used to check if all the necessary parameters
                      exist in the passed input dictionary .
        Input     :   Input dictionary containing all the necessary arguments.
        Output    :   Returns True if all the variables exist in the required format
        """
        try:
            flag = True
            print("********************************************************************************")
            print(input_dict)
            print("********************************************************************************")

            print(input_dict["payload_id_list"])
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

    def main(self,input_dict):
        """
                Purpose     :   Orchestrate EudraCT ingestion using following methods -
                                a. download_eudract - downloads data(in .txt files)
                                b. pre_process_eudract - converts the .txt files into .csv files
                                c. logging in the tables (detail and summary)
                                d. launch and terminate cluster functions to launch and terminate cluster
                                e. call_create_structured_file - to create parquet files from csv files
                Input       :   no input parameters
                Output      :   returns status key
        """
        batch_id = ""
        payload_id = ""
        status_message = ""
        try:
            status_message = "Starting the main function for Ingestion Adaptor"
            logging.info(status_message)
            self.data_source = "eudract"
            self.adapter_id = self.get_adapter_id(self.data_source)
            print(input_dict,"###################################################################################")
            input_dict = input_dict["payload_id_list"]
            payload_ids = self.get_payload_id(input_dict, self.adapter_id)

            batch_id = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S")
            self.logging.debug("batch_id "+batch_id)
            self.logging.debug("app_config_data = "+str(self.app_config_data))

            payload_id_list = []
            if payload_ids is not None and payload_ids != "":
                payload_id_list = payload_ids.split(",")
            # there must be only one payload_id in commandline_argument
            if len(payload_id_list) > 1:
                status_message = "only one payload_id must be given as command line argument"
                raise Exception(status_message)

            output_status = self.check_valid_payload_id(payload_id_list)
            logging.debug(output_status)
            if output_status[STATUS_KEY] == FAILED_KEY:
                raise Exception(output_status[ERROR_KEY])

            payload_json_list = []
            for payload_id in payload_id_list:
                payload_json = self.get_payload_json(payload_id)
                logging.debug(payload_json)
                payload_json_list.append(payload_json["payload_details"])

            self.cluster_id = self.call_launch_cluster(data_source_name=self.data_source)
            # self.cluster_id = "j-28HENYH5GSQ8B"
            self.launch_cluster_flag = "Y"
            cluster_id = self.cluster_id
            adapter_id = self.adapter_id

            query_str = (
                "Update {audit_db}.{log_data_acquisition_smry_table} "
                "set cluster_id = '{cluster_id}' "
                ", end_time = '{end_time}' where cycle_id = '{batch_id}' and "
                " source_name = '{data_source}' and payload_id ='{payload_id}'"
            )
            output_query = query_str.format(
                audit_db=self.audit_db, cluster_id=cluster_id,
                log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                batch_id=batch_id, data_source=self.data_source, end_time=datetime.utcnow(),
                payload_id=payload_id
            )

            create_struct_dict_list = []
            latest_batch_id = ""
            # data download in csv
            for payload_json in payload_json_list:
                counter = 0
                payload_id = str(payload_json[0]["payload_id"])
                output_query = self.execute_query(
                    query_type="insert", table_type="smry", batch_id=batch_id,adapter_id = self.adapter_id, payload_id=payload_id,
                    payload_json=json.dumps(payload_json), load_type=payload_json[0][LOAD_TYPE_KEY],
                    cluster_id=self.cluster_id, status=RUNNING_KEY
                )
                logging.debug(output_query)
                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                try:
                    output_query = self.execute_query(
                        query_type="insert", table_type="dtl", batch_id=batch_id,
                        adapter_id=self.adapter_id,payload_id=payload_id, step_name=EXTRACT_DATA_STEP_NAME,
                        raw_file_dir=self.pre_landing_path, structured_file_path="",
                        status=RUNNING_KEY
                    )
                    logging.debug(output_query)
                    if output_query[STATUS_KEY] == FAILED_KEY:
                        raise Exception(output_query[ERROR_KEY])

                    for sub_payload_json in payload_json:
                        self.pre_landing_path = sub_payload_json["raw_file_location"]
                        self.landing_path = sub_payload_json["structured_file_location"]
                        dict_data = {}
                        payload_id = sub_payload_json["payload_id"]
                        dict_data["parquet_final_path"] = sub_payload_json["structured_file_location"]
                        dict_data["payload_id"] = sub_payload_json["payload_id"]
                        dict_data["cycle_id"] = batch_id
                        dict_data["disease_area_col_value"] = sub_payload_json["disease_area_col_value"]
                        create_struct_dict_list.append(dict_data)
                        # to update raw_file_location in dtl table
                        # query_str = (
                        #     "Update {audit_db}.{log_data_acquisition_dtl_table} set raw_file_dir = "
                        #     "'{raw_file_dir}' "
                        #     "where cycle_id = '{batch_id}' and source_name = "
                        #     "'{data_source}' and step_name = '{step_name}' and payload_id = '{payload_id}'"
                        # )
                        # query = query_str.format(
                        #     audit_db=self.audit_db,
                        #     log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                        #     raw_file_dir=self.pre_landing_path,
                        #     batch_id=batch_id, data_source=self.data_source, step_name=EXTRACT_DATA_STEP_NAME,
                        #     payload_id=payload_id, end_time=datetime.utcnow()
                        # )
                        # logging.debug("Query to update the log table: ", str(query))
                        # response = MySQLConnectionManager().execute_query_mysql(query, False)
                        # logging.debug("Query output: ", str(response))

                    #     output = self.download_eudract\
                    #         (sub_payload_json, batch_id=batch_id, counter=counter, payload_id=payload_id)
                    #     logging.debug(output)
                    #     if output[STATUS_KEY] == FAILED_KEY:
                    #         raise Exception(output[ERROR_KEY])
                    #     # preprocess
                    #     output = self.pre_process_eudract\
                    #         (sub_payload_json, batch_id, counter, payload_id)
                    #     logging.debug(output)
                    #     if output[STATUS_KEY] == FAILED_KEY:
                    #         raise Exception(output[ERROR_KEY])
                    #     counter = counter + 1
                    #
                    # output_query = self.execute_query(
                    #     query_type="update", table_type="dtl",
                    #     batch_id=batch_id, step_name=EXTRACT_DATA_STEP_NAME,
                    #     payload_id=payload_id, status=SUCCESS_KEY
                    # )
                    # logging.debug(output_query)
                    # if output_query[STATUS_KEY] == FAILED_KEY:
                    #     raise Exception(output_query[ERROR_KEY])
                except Exception:
                    output_query = self.execute_query(
                        query_type="update", table_type="dtl", batch_id=batch_id,
                        payload_id=payload_id, step_name=EXTRACT_DATA_STEP_NAME,
                        status=FAILED_KEY,
                    )
                    logging.debug(output_query)
                    raise Exception(output_query[ERROR_KEY])

            # logging.info("Query to insert in the log table: %s", str(output_query))
            # response = MySQLConnectionManager().execute_query_mysql(output_query, False)
            # logging.info("Query output: %s", str(response))
            logging.info("================Executing job in Spark client mode================")
            logging.info("cluster_id is ", cluster_id)
            trigger_data_cmd = "cd /usr/local/airflow/dags/plugins/dev/ctfo/code/adaptor/common_utilities; python3 " \
                               "EudraCTAdapter.py ECT " + " " + str(batch_id) + " " + str(payload_id) \
                               + " " + str(adapter_id) + " " + str(cluster_id)
            logging.info("trigger_data_cmd is ", trigger_data_cmd)
            response, output_string = SystemManagerUtility().fetch_file_name(cluster_id, trigger_data_cmd)
            logging.info("EMR Command is executed and the command response is ", response)
            # result_list = list(output_string.split("\n"))
            # output = self.trigger_data_ingestion(
            #     sub_payload, batch_id, payload_id, FILE_ABS_PATH, cluster_id
            # )

            if response == FAILED_KEY:
                output_query = self.execute_query(
                    query_type="update", table_type="smry",
                    batch_id=batch_id, payload_id=payload_id, status=FAILED_KEY,
                )

                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                output_query = self.execute_query(
                    query_type="update", table_type="dtl", batch_id=batch_id,
                    payload_id=payload_id, step_name=EXTRACT_DATA_STEP_NAME,
                    status=FAILED_KEY,
                )

                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

            logging.info("Calling call_create_structured_file function ")
            output = self.call_create_structured_file(create_struct_dict_list=create_struct_dict_list,
                                                      cluster_id=cluster_id, cycle_id=batch_id)
            if output[STATUS_KEY] == FAILED_KEY:
                raise Exception(output[ERROR_KEY])
            logging.debug(output)
            if output[STATUS_KEY] == FAILED_KEY:
                raise Exception(output[ERROR_KEY])

            logging.info("Calling Termination of cluster !!")

            if self.launch_cluster_flag == "Y":
                self.call_terminate_cluster(self.cluster_id)

            self.launch_cluster_flag = "N"

            logging.info("Completing Function main ... ")
            return {STATUS_KEY: SUCCESS_KEY}
        except Exception:
            output_query = self.execute_query(
                query_type="update", table_type="smry",
                batch_id=batch_id, payload_id=payload_id, status=FAILED_KEY,
            )
            logging.debug(output_query)
            if output_query[STATUS_KEY] == FAILED_KEY:
                raise Exception(output_query[ERROR_KEY])
            error_msg = status_message+str(traceback.format_exc())
            self.logging.debug("error_msg = " + error_msg)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_msg}
        finally:
            logging.info("In finally block , terminating the cluster if launched")
            # if self.launch_cluster_flag == "Y":
            #     self.call_terminate_cluster(self.cluster_id)

    def call_create_structured_file(
            self, create_struct_dict_list, cluster_id, cycle_id
    ):
        """
        Purpose     :   To call notebook CreateStructuredFile with input file
        Input       :   create_struct_dict_list, cluster_id, cycle_id
        Output      :   return status key and final parquet output path
        """
        payload_id = ""
        try:
            status_message = "Starting Function call_create_structured_file ... "
            logging.info(status_message)
            status_message = "extracting data from config file"
            logging.debug(status_message)
            s3_tmp_path = self.app_config_data["adapter_details"]["eudract"]["s3_tmp_path"]
            s3_tmp_path = os.path.join(s3_tmp_path, "spark_config")
            s3_temp_path_ts = os.path.join(s3_tmp_path, cycle_id)
            logging.debug(s3_temp_path_ts)
            s3_temp_spark_config_path = os.path.join(
                s3_temp_path_ts, "spark_input_config_"
                + datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S") + ".json",
            )
            logging.debug(s3_temp_spark_config_path)
            emr_keypair_path = self.app_config_data["adapter_details"]["generic_config"][
                "private_key_loaction"
            ]
            self.logging.debug(str(self.app_config_data))
            self.logging.debug("logging app config"+str(self.app_config_data["adapter_details"]["generic_config"]))
            emr_code_path = self.app_config_data["adapter_details"]["generic_config"]["emr_code_path"]
            self.logging.debug(str(self.app_config_data))
            emr_region_name = self.app_config_data["adapter_details"]["generic_config"]["aws_region"]
            payload_id_list = []
            for dict_data in create_struct_dict_list:
                payload_id = dict_data["payload_id"]
                spark_input_config = {}
                spark_input_conf_list = []
                self.s3_parquet_output_path = dict_data["parquet_final_path"]
                logging.debug(self.s3_parquet_output_path)

                # update smry table
                if dict_data["payload_id"] not in payload_id_list:
                    query_str = (
                        "Update {audit_db}.{log_data_acquisition_smry_table} set cluster_id = '{cluster_id}' "
                        " where cycle_id = '{batch_id}' and "
                        " source_name = '{data_source}' and payload_id ='{payload_id}'"
                    )
                    output_query = query_str.format(
                        audit_db=self.audit_db, cluster_id=cluster_id,
                        log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                        batch_id=cycle_id, data_source=self.data_source,
                        payload_id=dict_data["payload_id"]
                    )

                    logging.debug("Query to update in the log table: ", str(output_query))
                    # TODO check response of MySQLConnectionManager
                    response = MySQLConnectionManager().execute_query_mysql(output_query, False)
                    logging.debug(response)
                    output_query = self.execute_query(
                        query_type="insert", table_type="dtl", batch_id=cycle_id,
                        payload_id=dict_data["payload_id"], step_name=CREATE_STRUCTURE_STEP_NAME,
                        raw_file_dir=self.pre_landing_path, structured_file_path=self.landing_path,
                        status=RUNNING_KEY
                    )
                    logging.debug(output_query)
                    if output_query[STATUS_KEY] == FAILED_KEY:
                        raise Exception(output_query[ERROR_KEY])

                payload_id_list.append(dict_data["payload_id"])

                spark_input_config["csv_temp_path"] = \
                    self.app_config_data["adapter_details"]["eudract"]["csv_tmp_path_location"]
                spark_input_config["parquet_temp_path"] = \
                    self.app_config_data["adapter_details"]["eudract"]["parquet_temp_path"]
                spark_input_config["parquet_final_path"] = dict_data["parquet_final_path"]
                spark_input_config["payload_id"] = dict_data["payload_id"]
                spark_input_config["cycle_id"] = dict_data["cycle_id"]
                spark_input_config["data_source"] = self.app_config_data["adapter_details"]["eudract"]["datasource"]
                spark_input_config["spark_query_path"] = ""
                spark_input_config["s3_schema_path"] = ""
                spark_input_config[DISEASE_AREA_COLUMN_VALUE_KEY] = dict_data[DISEASE_AREA_COLUMN_VALUE_KEY]
                spark_input_conf_list.append(spark_input_config)
            spark_input_config_file = {"spark_config": spark_input_conf_list,
                                       "drop_duplicates":
                                           self.app_config_data["adapter_details"]["eudract"]["drop_duplicates"]}

            logging.debug(spark_input_config_file)
            # output = urlparse(s3_temp_spark_config_path)
            status_message = ("Writing spark input config file to " + s3_temp_spark_config_path + " with content: "
                              +json.dumps(spark_input_config_file))
            logging.info(status_message)

            status = CommonUtils().put_s3_object(s3_temp_spark_config_path, json.dumps(spark_input_config_file))
            if status[STATUS_KEY] == FAILED_KEY:
                raise Exception(status[ERROR_KEY])

            logging.debug(
                "Calling notebook CreateStructuredFile with input file="
                + str(s3_temp_spark_config_path)
            )

            emr_code_path = os.path.join(emr_code_path, "code", "adaptor", "common_utilities")
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
            # output_status = SshEmrUtility().execute_command(
            #     cluster_id, emr_region_name, emr_keypair_path, command
            # )

            output_status = SystemManagerUtility().execute_command(cluster_id, command)
            status_message = "Final output: " + str(output_status)
            logging.debug(str(status_message))

            # if not re.search('{"status": "SUCCESS"}', status_message):
            if not re.search('Success', status_message):
                return {STATUS_KEY: FAILED_KEY, OUTPUT_PARQUET_PATH: None,
                        ERROR_KEY: "Failed spark job"}

            # if output_status[STATUS_KEY] == FAILED_KEY:
            #     raise Exception(output_status[ERROR_KEY])
            #
            # status_message = "Final output: " + str(output_status)
            # logging.debug(str(status_message))

            logging.debug("Spark submit done successfully")
            status = CommonUtils().delete_s3_object(s3_temp_path_ts)
            logging.debug(status)

            if status[STATUS_KEY] == FAILED_KEY:
                raise Exception(status[ERROR_KEY])
            logging.debug("spark config folder deleted successfully")
            self.logging.debug(status)

            query_str = (
                "Update {audit_db}.{log_data_acquisition_dtl_table} set status = '{status}' ,"
                " structured_file_path = '{structured_file_path}', end_time = '{end_time}' "
                "where cycle_id = '{batch_id}' and source_name = '{data_source}' and "
                "step_name = '{step_name}' and payload_id ='{payload_id}'"
            )

            query = query_str.format(
                audit_db=self.audit_db,
                status=SUCCESS_KEY,
                structured_file_path=dict_data["parquet_final_path"],
                log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                batch_id=cycle_id, data_source=self.data_source,
                step_name=CREATE_STRUCTURE_STEP_NAME, payload_id=dict_data["payload_id"],
                end_time=datetime.utcnow(),
            )

            logging.debug("Query to insert in the log table: ", str(query))
            response = MySQLConnectionManager().execute_query_mysql(query, False)
            logging.debug("Query output: ", str(response))

            output_query = self.execute_query(
                query_type="update", table_type="smry",
                batch_id=cycle_id, status=SUCCESS_KEY, payload_id=dict_data["payload_id"],
            )
            logging.debug(output_query)
            if output_query[STATUS_KEY] == FAILED_KEY:
                raise Exception(output_query[ERROR_KEY])

            csv_tmp_path_location = self.app_config_data["adapter_details"]["eudract"]["csv_tmp_path_location"]
            logging.debug(csv_tmp_path_location)
            csv_tmp_path_location = os.path.join(csv_tmp_path_location, cycle_id)
            logging.debug(csv_tmp_path_location)
            if not csv_tmp_path_location.endswith("/"):
                csv_tmp_path_location = csv_tmp_path_location + "/"
            logging.debug(csv_tmp_path_location)
            status = CommonUtils().delete_s3_object(csv_tmp_path_location)
            logging.debug(status)

            if status[STATUS_KEY] == FAILED_KEY:
                raise Exception(status[ERROR_KEY])
            logging.debug("s3 tmp csv folder deleted successfully")
            self.logging.debug(status)
            logging.info("Completing Function call_create_structured_file ... ")
            return {STATUS_KEY:SUCCESS_KEY, OUTPUT_PARQUET_PATH:self.s3_parquet_output_path}
        except Exception:
            output_query = self.execute_query(
                query_type="update", table_type="dtl",
                batch_id=cycle_id, step_name=CREATE_STRUCTURE_STEP_NAME,
                payload_id=payload_id, status=FAILED_KEY,
            )
            logging.debug(output_query)
            if output_query[STATUS_KEY] == FAILED_KEY:
                raise Exception(output_query[FAILED_KEY])

            error_message = str(traceback.format_exc())
            logging.error(error_message)
            return {
                STATUS_KEY: FAILED_KEY, OUTPUT_PARQUET_PATH: None, ERROR_KEY: error_message
            }

    def get_max_batch_id(self):
        status_message = ""
        try:
            get_max_batch_id_query_str = (
                "select max(cycle_id) as cycle_id from {audit_db}."
                "{log_data_acquisition_smry_table} where source_name = "
                " '{data_source}' and ( load_type = '{full_load_type}' or "
                " load_type = '{delta_load_type}' )"
                " and status = '{status}' "
            )
            query = get_max_batch_id_query_str.format(
                audit_db=self.audit_db, status=SUCCESS_KEY, full_load_type=FULL_LOAD,
                log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                data_source=self.data_source, delta_load_type=DELTA_LOAD
            )
            logging.info("Query to select from the log table: ", str(query))
            get_max_batch_id_query_output = MySQLConnectionManager().execute_query_mysql(query)
            logging.info("Query output: ", str(get_max_batch_id_query_output))

            if not get_max_batch_id_query_output[0]['cycle_id']:
                status_message = "No latest batch found for this payload id " \
                                 "in log table. Please enter the" \
                                 "delta refresh date in payload for delta load "
                logging.debug(status_message)
                raise Exception(status_message + str(traceback.format_exc()))
            latest_batch_id = get_max_batch_id_query_output[0]['cycle_id']
            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: latest_batch_id}
        except Exception:
            status_message = (status_message+str(traceback.format_exc()))


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
        IngestionAdaptor_obj = eudract_ingestor()
        data_source = "EduraCT"
        if IngestionAdaptor_obj.check_mandatory_parameters_exists(
                commandline_arguments
        ):
            output = IngestionAdaptor_obj.main(input_dict=commandline_arguments)
            logging
        else:
            eudract_ingestor().usage()
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
