"""
  Module Name         :   DataIngestionAdapter
  Purpose             :   This module performs below operation:
                              a. extract data from who datasource.
                              b. Perform logging in delta table
                              c. call module on emr which create parquet files.
  Input               :    configurations needed for initiating download process
  Output              :   Return status SUCCESS/FAILED
  Pre-requisites      :   lynx logs files must be present at cwd to download data
  Last changed on     :   24th April 2019
  Last changed by     :   Harshal Taware
  Reason for change   :   Comments and formatting
"""

import datetime
import traceback
import logging
import json
import ast
from datetime import datetime
import argparse
import textwrap
import sys
import os
from os import path
from urllib.parse import urlparse
import re
import copy
sys.path.insert(0, os.getcwd())
import time
from lxml import etree

STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCESS"
ERROR_KEY = "error"
MODULE_NAME = "DataIngestionUtility"
PROCESS_NAME = "Ingestion"
RUNNING_KEY = "RUNNING"
FULL_LOAD = "full"
DELTA_LOAD = "delta"
LOAD_TYPE_KEY = "load_type"
DISEASE_AREA_COLUMN_VALUE_KEY = "disease_area_col_value"
DELTA_REFRESH_DATE_KEY = "delta_refresh_date_str"

FILTERED_LOAD="filtered load"
extract_data_step_name = "extract_data"
create_structure_step_name = "create_structure_file"
S3_SQL_LOCATION = ""
OUTPUT_PARQUET_PATH = "output_parquet_file_path"
SQL_FILE_NAME = "who.sql"
S3_SCHEMA_LOCATION = ""
APPLICATION_CONFIG_FILE = "application_config.json"
ENVIRONMENT_CONFIG_FILE = "environment_variable.json"
SPARK_CONF_PATH = "spark_input_config_path"
FILE_ABS_PATH = path.abspath(os.path.dirname(__file__))
SERVICE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, SERVICE_DIR_PATH)
CODE_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIR_PATH, "../common_utilities"))
sys.path.insert(1, CODE_DIR_PATH)
CLUSTER_CONFIG = os.path.join(CODE_DIR_PATH,"cluster_variables.json")

from DILogSetup import get_logger
from ExecutionContext import ExecutionContext
import CommonConstants_bkp as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from EmrManagerUtility import EmrManagerUtility
from CommonUtils import CommonUtils
from SshEmrUtility import SshEmrUtility
logging = get_logger(file_log_name="data_ingestion_log.log", enable_file_log=True)


class DataIngestionAdapter():

    def __init__(self):
        self.xml_tag_replace = {'<TrialID />': '<TrialID>NULL</TrialID>',
                     '<Last_Refreshed_on />': '<Last_Refreshed_on>NULL</Last_Refreshed_on>',
                     '<Public_title />': '<Public_title>NULL</Public_title>',
                     '<Scientific_title />': '<Scientific_title>NULL</Scientific_title>',
                     '<Acronym />': '<Acronym>NULL</Acronym>',
                     '<Primary_sponsor />': '<Primary_sponsor>NULL</Primary_sponsor>',
                     '<Date_registration />': '<Date_registration>NULL</Date_registration>',
                     '<Prospective_registration />': '<Prospective_registration>NULL</Prospective_registration>',
                     '<Export_date />': '<Export_date>NULL</Export_date>',
                     '<Source_Register />': '<Source_Register>NULL</Source_Register>',
                     '<web_address />': '<web_address>NULL</web_address>',
                     '<Recruitment_Status />': '<Recruitment_Status>NULL</Recruitment_Status>',
                     '<other_records />': '<other_records>NULL</other_records>',
                     '<Inclusion_agemin />': '<Inclusion_agemin>NULL</Inclusion_agemin>',
                     '<Inclusion_agemax />': '<Inclusion_agemax>NULL</Inclusion_agemax>',
                     '<Inclusion_gender />': '<Inclusion_gender>NULL</Inclusion_gender>',
                     '<Date_enrollement />': '<Date_enrollement>NULL</Date_enrollement>',
                     '<Target_size />': '<Target_size>NULL</Target_size>',
                     '<Study_type />': '<Study_type>NULL</Study_type>',
                     '<Study_design />': '<Study_design>NULL</Study_design>',
                     '<Phase />': '<Phase>NULL</Phase>',
                     '<Countries />': '<Countries>NULL</Countries>',
                     '<Contact_Firstname />': '<Contact_Firstname>NULL</Contact_Firstname>',
                     '<Contact_Lastname />': '<Contact_Lastname>NULL</Contact_Lastname>',
                     '<Contact_Address />': '<Contact_Address>NULL</Contact_Address>',
                     '<Contact_Email />': '<Contact_Email>NULL</Contact_Email>',
                     '<Contact_Tel />': '<Contact_Tel>NULL</Contact_Tel>',
                     '<Contact_Affiliation />': '<Contact_Affiliation>NULL</Contact_Affiliation>',
                     '<Inclusion_Criteria />': '<Inclusion_Criteria>NULL</Inclusion_Criteria>',
                     '<Exclusion_Criteria />': '<Exclusion_Criteria>NULL</Exclusion_Criteria>',
                     '<Condition />': '<Condition>NULL</Condition>',
                     '<Intervention />': '<Intervention>NULL</Intervention>',
                     '<Primary_outcome />': '<Primary_outcome>NULL</Primary_outcome>',
                     '<Secondary_outcome />': '<Secondary_outcome>NULL</Secondary_outcome>',
                     '<Secondary_ID />': '<Secondary_ID>NULL</Secondary_ID>',
                     '<Source_Support />': '<Source_Support>NULL</Source_Support>',
                     '<Secondary_Sponsor />': '<Secondary_Sponsor>NULL</Secondary_Sponsor>'
                     }

        self.current_timestamp = datetime.utcnow()
        self.batch_id = datetime.strftime(self.current_timestamp, "%Y%m%d%H%M%S")
        # self.batch_id = "20190927061028"
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.configuration = json.load(open(APPLICATION_CONFIG_FILE))
        self.audit_db = self.configuration["adapter_details"]["generic_config"]["mysql_db"]
        self.local_path = self.configuration["adapter_details"]["who"]["local_path"]
        self.s3_temp_path = self.configuration["adapter_details"]["generic_config"]["s3_temp_path"]
        self.cluster_id = ""
        self.region_name = ""
        self.data_source = "who"
        self.who_url = self.configuration["adapter_details"]["who"]["who_url"]

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

    def set_download_parameters(self, payload_json, filter_key):
        if filter_key == "diseasehierarchy":
            self.who_file_names = self.configuration["adapter_details"]["who"]["who_disease_filter"]["who_file_names"]
            self.who_lynx_names = self.configuration["adapter_details"]["who"]["who_disease_filter"]["who_lynx_names"]
        if filter_key == "phase":
            if payload_json[filter_key]:
                value = payload_json[filter_key]
                self.who_file_names = self.configuration["adapter_details"]["who"]["who_phase_filter"][value][
                    "who_file_names"]
                self.who_lynx_names = self.configuration["adapter_details"]["who"]["who_phase_filter"][value][
                    "who_lynx_names"]
        if filter_key == "load_type" and payload_json["load_type"] == "delta":
            self.who_file_names = self.configuration["adapter_details"]["who"]["who_delta_load_filter"][
                "who_file_names"]
            self.who_lynx_names = self.configuration["adapter_details"]["who"]["who_delta_load_filter"][
                "who_lynx_names"]
        if filter_key == "load_type" and payload_json["load_type"] == "full":
            logging.debug("getting full load data")
            self.who_file_names = self.configuration["adapter_details"]["who"]["who_full_load_filter"][
                "who_file_names"]
            self.who_lynx_names = self.configuration["adapter_details"]["who"]["who_full_load_filter"][
                "who_lynx_names"]

        if filter_key == "delta_refresh_date_str" and payload_json["delta_refresh_date_str"] != "":
            self.who_file_names = self.configuration["adapter_details"]["who"]["who_delta_load_filter"][
                "who_file_names"]
            self.who_lynx_names = self.configuration["adapter_details"]["who"]["who_delta_load_filter"][
                "who_lynx_names"]

    def pre_process_who(self, pre_landing_path, who_file_names, application_config, batch_id, payload_id, counter,disease_area_col_value):
        """
        Purpose     :   this function used to convert downloaded raw to csv format
        Input       :   pre_landing_path = path where raw files are downloaded
                        who_file_names = downloaded files names from lynx script command
                        batch_id and payload_id
        Output      :   Return status SUCCESS/FAILED and prelanding layer path where csv gets extracted.
        """
        try:
            status_message = "Starting to pre-process who files with inputs - {},{}".format(pre_landing_path, who_file_names)
            logging.info(status_message)
            for who_file_name in who_file_names:
                who_file_name = who_file_name+batch_id
                file_name = who_file_name +'.xml'
                logging.debug("File name - {}".format(file_name))
                exists = os.path.isfile(self.local_path + file_name)
                logging.debug("Checking if file ({}) exists, status - {}".format(str(self.local_path) + str(file_name),
                                                                                 exists))

                if exists:
                    # create a copy of the original file
                    source_file_name = os.path.join(self.local_path, file_name)
                    file_name = "WHO_{disease}_{batch}_{payload}_{counter}.xml".format(
                        disease=disease_area_col_value, batch=batch_id, payload=payload_id, counter=counter)
                    target_file_name = os.path.join(self.local_path, file_name)
                    logging.debug("Copying file")
                    copy_command = "cp {} {}".format(source_file_name, target_file_name)
                    logging.debug("Executing copy command - {}".format(copy_command))
                    os.system(copy_command)

                    # replace tags of file using 'sed' command
                    # for key in self.xml_tag_replace:
                    #     logging.debug("Replacing {} in file with {}".format(key, self.xml_tag_replace[key]))
                    #     sed_command = "sed -i 's+{}+{}+g' {}".format(key, self.xml_tag_replace[key], target_file_name)
                    #     logging.debug("Executing replace command - {}".format(sed_command))
                    #     os.system(sed_command)
                    #     logging.debug("Command executed successfully")
                    # logging.debug("All xml tags were replaced successfully")


                    logging.debug("Removing file - {}".format(source_file_name))
                    os.system("rm "+ source_file_name)
                    status_message = "removed {}".format(source_file_name)
                    logging.debug(str(status_message))

                    # copying final xml file to s3
                    temp_s3_path = os.path.join(self.s3_temp_path, batch_id)
                    logging.debug("Uploading file to s3, source: {}, destination: {}".format(target_file_name,
                                                                                             temp_s3_path))
                    upload_result = CommonUtils().upload_s3_object(target_file_name, temp_s3_path)
                    logging.debug("Removing local xml file")
                    remove_command = "rm {}".format(target_file_name)
                    logging.debug("Command to remove file - {}".format(remove_command))
                    os.system(remove_command)
                    logging.debug("Removed local xml file successfully")
                    if upload_result[STATUS_KEY] == SUCCESS_KEY:
                        logging.debug("Uploaded file to s3 successfully")
                        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: os.path.join(temp_s3_path, file_name)}
                    raise Exception("Failed to upload xml file to s3, ERROR - {}".format(upload_result[ERROR_KEY]))
                else:
                    return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: ""}


        except Exception:
            error_message = str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


    def download_who(self, pre_landing_path, who_lynx_names, batch_id, payload_id, counter,disease_area_col_value):
        """
        Purpose     :   this function used to download raw data for who in xml fromat
        Input       :   pre_landing_path = path where raw files are downloaded
                        who_lynx_names = downloaded files names from lynx script command
                        batch_id and payload_id
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            logging.info("Starting to download WHO files.")
            logging.debug(who_lynx_names)
            for each in who_lynx_names.split(","):
                download_file = each.split(";")[0].strip() + batch_id
                lynx_file = each.split(";")[1].strip()
                logging.debug("table name: " + str(download_file))
                logging.debug("lynx file name: " + str(lynx_file))

                command = "lynx -cmd_script={0}".format(self.local_path) + lynx_file.strip() + "  {0} > /tmp/{1}".format(
                    self.who_url, str(lynx_file))
                logging.debug(str(command))
                output = os.system(command)
                logging.debug("Command output - {}".format(output))
                # os.system("ls " + self.local_path)
                logging.debug("@@@@@@@@@@\t{}".format(self.local_path+lynx_file.strip()))
                if os.path.isfile(self.local_path+lynx_file.strip()):
                    logging.debug("@@@@@@@@@@@@@")
                    if not os.path.exists(self.local_path + "temp"):
                        os.mkdir(self.local_path + "temp")
                    self.local_path_temp = self.local_path + "temp/" + "who_" + disease_area_col_value.replace(" ", "_") + "_" + batch_id + "_" + str(payload_id) + "_" + str(
                        counter) + ".xml"
                    logging.debug("********")
                    copy_file_command = "cp " + self.local_path + download_file.strip() + ".xml" + " " + \
                                        self.local_path_temp
                    logging.debug("Command to copy file has been created - {}".format(copy_file_command))
                    logging.debug("**********")
                    copy_status = os.system(copy_file_command)
                    if copy_status != 0:
                        raise Exception("Failed to copy file")
                    # self.pre_landing_path_s3 = pre_landing_path
                    # self.local_path_file = self.local_path + download_file.strip() + ".xml"
                    # logging.debug("Uploading xml file to s3, source: {}, target: {}".format(
                    #     self.local_path_temp, self.pre_landing_path_s3))
                    # output = CommonUtils().upload_s3_object(self.local_path_temp, self.pre_landing_path_s3)
                    # # os.system("rm "+self.local_path+lynx_file.strip())
                    # if output[STATUS_KEY] == FAILED_KEY:
                    #     raise Exception
                    # logging.debug(str(output))
                else:
                    logging.info("files are not present for this {0} filters".format(lynx_file))

            logging.info("Downloaded all the who files!")
            return {STATUS_KEY:SUCCESS_KEY}

        except Exception as e:
            error_message = "Failed to download source file, ERROR - "+str(traceback.format_exc(e))
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def trigger_data_ingestion(self, payload_json, batch_id, payload_id, filter_key, input_config_path, pre_landing_path, counter,disease_area_col_value):
        """
        Purpose     :   This function calls all the data ingestion processes for who
        Input       :   payload_json, batch_id, payload_id,filter_key, input_config_path,pre_landing_path,
        Output      :   Return status SUCCESS/FAILED and SPARK CONFIGURATION PATH
        """
        failed_flag = True
        try:
            status_message = "Calling trigger_data_ingestion function"
            logging.info(status_message)

            output_status = self.create_app_config(input_config_path)

            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status

            application_config = output_status[RESULT_KEY]
            status_message = "Obtained input config: " + json.dumps(application_config)
            # logging.debug(str(status_message))
            logging.debug("%%%%%%%" + filter_key)
            logging.debug(str(json.dumps(payload_json)))

            self.set_download_parameters(payload_json, filter_key)
            logging.debug("Download parameters have been set successfully")
            logging.debug(str(self.who_file_names) + "\t" + str(self.who_lynx_names))

            output_status = self.download_who(pre_landing_path, self.who_lynx_names, batch_id, payload_id, counter,
                                              disease_area_col_value)

            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status

            # sys.exit()
            output_status = self.pre_process_who(pre_landing_path, self.who_file_names, application_config, batch_id,
                                                 payload_id, counter,disease_area_col_value)
            if output_status[STATUS_KEY] == FAILED_KEY:
                return output_status

            failed_flag = False
            return {SPARK_CONF_PATH: output_status[RESULT_KEY], STATUS_KEY: SUCCESS_KEY}
        except Exception as e:
            error_message = "unable to download and pre process data, ERROR - "+str(traceback.format_exc(e))
            logging.debug(str(error_message))
            return {
                SPARK_CONF_PATH: "NONE", STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message,
            }
        finally:
            if failed_flag:
                logging.info("failed")

    def usage(self):
        """
        :def: USAGE() Function
        :return: logging.debugs help statement
        """
        msg = """Usage: python DataIngestionAdapter.py -j
        This will trigger the ingestion adapters based on the filters provided by the .
        contains all the filters needed to trigger the respective data source ingestion.
        eg.(trial_trove pubmed and Clinical_trials)."""
        logging.info(str(msg))

    def execute_query(self, table_type=None, query_type=None, batch_id=None, adapter_id=None, payload_id=None,
                      load_type=None, step_name=None, payload_json=None, cluster_id=None, raw_file_dir=None,
                      structured_file_path=None, status=None):
        """
        Purpose     :   Execute query on databricks delta tables
        Input       :   query_type, datasource , step_name , status = failed, success etc.
        Output      :   Status of query execution
        """

        try:
            status_message = "starting execute_query function"
            logging.info(status_message)
            if query_type.lower() == "insert" and table_type.lower() == "smry":
                query = "Insert into {audit_db}.{log_data_acquisition_smry_table} values ('{batch_id}'" \
                        ",'{adapter_id}', '{payload_id}','{data_source}','{payload_json}','{load_type}','{cluster_id}'," \
                        "'{status}'" \
                        ",'{start_time}','{end_time}')".format(
                    audit_db=self.audit_db, log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                    batch_id=batch_id, adapter_id=adapter_id, payload_id=payload_id, load_type=load_type,
                    data_source=self.data_source, payload_json=json.dumps(payload_json), cluster_id=cluster_id,
                    status=status, start_time=datetime.utcnow(), end_time="")

            elif query_type.lower() == "insert" and table_type.lower() == "dtl":

                query = (
                    "Insert into {audit_db}.{log_data_acquisition_dtl_table} values ('{batch_id}'"
                    ",'{adapter_id}', '{payload_id}','{data_source}','{step_name}', '{raw_file_dir}'"
                    ",'{structured_file_path}','{status}','{start_time}','{end_time}')".format(
                        audit_db=self.audit_db,
                        log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE,
                        adapter_id=adapter_id, batch_id=batch_id, payload_id=payload_id, data_source=self.data_source,
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

            logging.info("Query to insert in the log table: {}".format(str(query)))
            response = MySQLConnectionManager().execute_query_mysql(query, False)
            logging.info("Query output: {}".format(str(response)))

            status_message = "completing execute_query function"
            logging.info(status_message)
            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def call_create_structured_file(self, create_struct_dict_list,
                                    application_config, cluster_id, payload_id, batch_id):
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

            S3_SQL_LOCATION = os.path.join(s3_temp_path_ts, SQL_FILE_NAME)

            command_to_copy_sql_file_to_s3 = (
                "aws s3 cp " + SQL_FILE_NAME + " " + S3_SQL_LOCATION
            )

            logging.info("command for awscli : " + command_to_copy_sql_file_to_s3)
            CommonUtils().execute_shell_command(command_to_copy_sql_file_to_s3)

            s3_temp_spark_config_path = os.path.join(
                s3_temp_path_ts, "spark_input_config_"
                + datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S") + ".json",
            )
            spark_input_conf_list = []

            emr_keypair_path = application_config["adapter_details"]["generic_config"][
                "private_key_loaction"
            ]
            emr_code_path = application_config["adapter_details"]["generic_config"]["emr_code_path"]

            self.s3_parquet_output_path = ""

            for dict_data in create_struct_dict_list:
                spark_input_config = {}
                disease_area_col_value = dict_data["disease_area_col_value"]
                self.s3_parquet_output_path = (
                    landing_layer + "/" + self.data_source + "_" + disease_area_col_value.replace(" ", "_") + "_" + batch_id + "_" + payload_id + ".parquet"
                )
                spark_input_config["s3_schema_path"] = S3_SCHEMA_LOCATION
                spark_input_config["spark_query_path"] = S3_SQL_LOCATION
                spark_input_config["s3_input_data_path"] = dict_data["path_extracted_json"]
                spark_input_config["s3_parquet_output_path"] = self.s3_parquet_output_path
                spark_input_config["data_source"] = dict_data["datasource"]
                spark_input_config["disease_name"] = dict_data["disease_area_col_value"]
                spark_input_config["temp_s3_path"] = application_config["adapter_details"]["generic_config"]["s3_temp_path"]

                spark_input_conf_list.append(spark_input_config)

            spark_input_config_file = {"spark_config": spark_input_conf_list}
            logging.info(spark_input_config_file)

            output = urlparse(s3_temp_spark_config_path)

            status_message = (
                "Writing spark input config file to "
                + s3_temp_spark_config_path + " with content: "+ json.dumps(spark_input_config_file)
            )

            logging.info(status_message)

            CommonUtils().put_s3_object(
                s3_temp_spark_config_path, json.dumps(spark_input_config_file)
            )

            logging.debug(
                "Calling notebook CreateStructuredFile with input file="
                + str(s3_temp_spark_config_path)
            )

            emr_code_path = os.path.join(emr_code_path, "adapter/common_utilities")
            emr_code_path = os.path.join(emr_code_path, "CreateStructuredFile.py")

            argument_list = [
                "/usr/lib/spark/bin/spark-submit", "--packages",
                "com.databricks:spark-xml_2.10:0.4.1", emr_code_path, s3_temp_spark_config_path,
            ]

            status_message = "Argument list SSH_EMR_Utility job=" + json.dumps(argument_list)
            logging.debug(status_message)

            command = '/usr/lib/spark/bin/spark-submit --packages ' \
                      'com.databricks:spark-xml_2.10:0.4.1 {0} -j "{1}"'.format(emr_code_path, s3_temp_spark_config_path)
            logging.debug(command)

            try:
                logging.debug("Calling utility to ssh into emr and execute spark command with inputs - {},{},{},{}".format(
                    cluster_id, self.region_name, emr_keypair_path, command))
                output_status = SshEmrUtility().execute_command(
                    cluster_id, self.region_name, emr_keypair_path, command
                )
                status_message = "Final output: " + str(output_status)
                logging.debug(str(status_message))

                if not re.search('{"status": "SUCCESS"}', status_message):
                    return {STATUS_KEY: FAILED_KEY, OUTPUT_PARQUET_PATH: None,
                            ERROR_KEY:"Failed spark job"}

                logging.info("Spark submit done successfully")
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

    def download_lynx_log_file(self, input, batch_id,latest_batch_id=None):
        """
        Purpose     :   this function used download lynx log files
        Input       :   input = filter key foe which data need to extract.
                        latest_batch_id and batch_id
        Output      :   Return status SUCCESS/FAILED
        """
        logging.info("calling download_lynx_log_file function with inputs - {},{},{}".format(
            str(input), str(batch_id), str(latest_batch_id)))
        logging.debug(str(input) + "*************")
        if "phase" in input:
            phase = input.strip()
            # phase = "phase0"
            key = """key phase0
            key phase1
            key phase2
            key phase3
            key phase4"""
            key_list = key.split("\n")
            phase_variale = []
            for key_value in key_list:
                if phase in key_value:
                    phase_variale.append(key_value.replace(key_value[-6:], "^J"))
                else:
                    phase_variale.append(key_value.replace(key_value[-6:], "Down Arrow"))
            phase_parameters = '\n'.join(phase_variale)
            command_template_path = self.local_path + "phase_std.txt"
            download_commands_template = open(command_template_path, 'rb').read()
            download_commands = download_commands_template.decode('utf-8').replace("$$phase_key", phase_parameters)
            end_value_parameter = ""
            end_value = phase[-1] + batch_id
            for character in end_value:
                end_value_parameter = end_value_parameter + "key " + character + "\n"
            end_value_parameter = end_value_parameter[0:-1]
            download_commands = download_commands.replace("$$variable", end_value_parameter)
            download_commands.encode(encoding='UTF-8')
            download_commands_file_path = self.local_path + input + ".txt"
            download_commands_file = open(download_commands_file_path, 'w')
            download_commands_file.write(download_commands)
        elif input == "delta":
            current_ts = datetime.strftime(datetime.utcnow(), "%d/%m/%Y")
            full_date = latest_batch_id[0:-6]
            year = full_date[0:4]
            month = full_date[4:6]
            day = full_date[6:8]
            start_date = day + "/" + month + "/" + year
            start_date = start_date
            end_date = current_ts
            start_date_parameter = ""
            for character in start_date:
                start_date_parameter = start_date_parameter + "key " + character + "\n"
            start_date_parameter = start_date_parameter[0:-1]
            end_date_parameter = ""
            for character in end_date:
                end_date_parameter = end_date_parameter + "key " + character + "\n"
            end_date_parameter = end_date_parameter[0:-1]
            command_template_path = self.local_path + "delta_load_std.txt"
            download_commands_template = open(command_template_path, 'rb').read()
            download_commands = download_commands_template.decode('utf-8').replace("$$start_date", start_date_parameter)
            download_commands = download_commands.replace("$$end_date", end_date_parameter)
            end_value_parameter = ""
            end_value = batch_id
            for character in end_value:
                end_value_parameter = end_value_parameter + "key " + character + "\n"
            end_value_parameter = end_value_parameter[0:-1]
            download_commands = download_commands.replace("$$variable", end_value_parameter)
            download_commands_file_path = self.local_path + "delta_load" + ".txt"
            download_commands_file = open(download_commands_file_path, 'w')
            download_commands_file.write(download_commands)
        elif input == "delta_refresh_date_str":
            current_ts = datetime.strftime(datetime.utcnow(), "%d/%m/%Y")
            start_date = latest_batch_id
            end_date = current_ts
            start_date_parameter = ""
            for character in start_date:
                start_date_parameter = start_date_parameter + "key " + character + "\n"
            start_date_parameter = start_date_parameter[0:-1]
            end_date_parameter = ""
            for character in end_date:
                end_date_parameter = end_date_parameter + "key " + character + "\n"
            end_date_parameter = end_date_parameter[0:-1]
            command_template_path = self.local_path + "delta_load_std.txt"
            download_commands_template = open(command_template_path, 'rb').read()
            download_commands = download_commands_template.decode('utf-8').replace("$$start_date", start_date_parameter)
            download_commands = download_commands.replace("$$end_date", end_date_parameter)
            end_value_parameter = ""
            end_value = batch_id
            for character in end_value:
                end_value_parameter = end_value_parameter + "key " + character + "\n"
            end_value_parameter = end_value_parameter[0:-1]
            download_commands = download_commands.replace("$$variable", end_value_parameter)
            download_commands_file_path = self.local_path + "delta_load" + ".txt"
            download_commands_file = open(download_commands_file_path, 'w')
            download_commands_file.write(download_commands)
        elif input == "full":
            logging.debug("downloading data for full load")
            command_template_path = self.local_path + "full_load_std.txt"
            download_commands_template = open(command_template_path, 'rb').read()
            end_value_parameter = ""
            end_value = batch_id
            for character in end_value:
                end_value_parameter = end_value_parameter + "key " + character + "\n"
            end_value_parameter = end_value_parameter[0:-1]
            download_commands = download_commands_template.decode('utf-8').replace("$$variable", end_value_parameter)
            download_commands_file_path = self.local_path + "full_load" + ".txt"
            download_commands_file = open(download_commands_file_path, 'w')
            download_commands_file.write(download_commands)
        else:
            line = input
            disease_area_parameter = ""
            for character in line:
                if character == " ":
                    character = "<space>"
                disease_area_parameter = disease_area_parameter + "key " + character + "\n"
            disease_area_parameter = disease_area_parameter[0:-1]
            command_template_path = self.local_path + "disease_area_std.txt"
            download_commands_template = open(command_template_path, 'rb').read()
            download_commands = download_commands_template.decode('utf-8').replace("$$disease_area_key", disease_area_parameter)
            end_value_parameter = ""
            end_value = batch_id
            for character in end_value:
                end_value_parameter = end_value_parameter + "key " + character + "\n"
            end_value_parameter = end_value_parameter[0:-1]
            download_commands = download_commands.replace("$$variable", end_value_parameter)
            download_commands_file_path = self.local_path + "disease_area" + ".txt"
            download_commands_file = open(download_commands_file_path, 'w')
            download_commands_file.write(download_commands)
        logging.info("completing download_lynx_log_file function")
        return download_commands_file_path

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
            "on A.adapter_id = B.adapter_id where LOWER(B.source_name) = '{data_source}'"
        )

        get_payload_id_query = get_payload_id_query_str.format(
            audit_db=self.audit_db,
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE,
            adapter_details_table=CommonConstants.ADAPTER_DETAILS_TABLE,
            data_source=self.data_source.lower(),
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

    def get_payload_id(self,input_dict, adapter_id):

        check_payload_id_query_str = (
            "select payload_details_file_name from {audit_db}.{adapter_payload_details_table} "
            " where adapter_id = '{adapter_id}' and payload_details_file_name = '{input_dict}'"
        )

        check_payload_id_query = check_payload_id_query_str.format(
            audit_db="audit_information",
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE, adapter_id=adapter_id,
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
            audit_db="audit_information",
            adapter_payload_details_table=CommonConstants.PAYLOAD_DETAILS_TABLE, payload_details_file_name=input_dict
        )
        print(get_payload_id_query)
        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            get_payload_id_query
        )
        payload_id = get_payload_result[0]["payload_id"]
        payload_id = str(payload_id)
        return payload_id

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
        logging.debug("Query to get the payload id - {}".format(get_payload_id_query))
        get_payload_result = MySQLConnectionManager().execute_query_mysql(
            get_payload_id_query
        )
        adapter_id = get_payload_result[0]["adapter_id"]
        adapter_id = str(adapter_id)
        return adapter_id

    def get_payload_json(self, payload_id):
        """
            Purpose     :   To get payload_json corresponding to payload_id
            Input       :   payload_id received
            Output      :   Returns payload_json
        """
        try:

            get_payload_query_str = "select payload_details_file_name,raw_file_dir,structured_file_dir" \
                                    " from {audit_db}.{adapter_payload_details_table} where payload_id" \
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

            status_message = "Payload json file name retrieved : " + str(payload_json_file_name)

            logging.info(status_message)

            payload_json = json.loads(open(payload_json_file_name, "r").read())

            logging.info(str(payload_json))

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
                        logging.debug("sub_payload = "+str(dict_data))
                        raise Exception("Value of disease_area_col_value key should not be empty and must be string ")

                if dict_data[DISEASE_AREA_COLUMN_VALUE_KEY] not in list_of_disease_areas:
                    raise Exception("Every sub payload should have the same disease area column value")

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

        except Exception:
            raise Exception(str(traceback.format_exc()))

    def call_terminate_cluster(self,cluster_id):
        """
        Purpose     :   this function terminate the EMR cluster
        Input       :   clutser id
        Output      :   ""
        """
        try:
            logging.info("Terminate_cluster method called")

            logging.debug("cluster_id --> " + str(cluster_id))
            EmrManagerUtility().terminate_cluster(cluster_id, self.region_name)
            logging.info("Cluster terminated")
        except Exception:
            raise Exception(str(traceback.format_exc()))

    def call_launch_cluster(self, data_source_name):
        """
        Purpose     :   this function launch EMR cluster
        Input       :   data source name
        Output      :   returen status and cluster id
        """
        try:
            cluster_type = self.configuration["adapter_details"][data_source_name]["cluster_type"]
            file_read = open(CLUSTER_CONFIG, "r")
            json_data = json.load(file_read)
            file_read.close()
            emr_payload = json_data["cluster_configurations"][cluster_type]
            self.region_name = emr_payload["Region"]
            returned_json = EmrManagerUtility().launch_emr_cluster(
                emr_payload, self.region_name
            )
            logging.debug("Json for EMR launch --> " + str(returned_json))
            self.cluster_id = returned_json[RESULT_KEY]
            # self.cluster_id = "j-1RT1CUYGFTYR8"
            logging.debug("cluster_id --> " + str(self.cluster_id))
            logging.info("Cluster launched")
            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: self.cluster_id}

        except Exception:
            error_message = str(traceback.format_exc())
            logging.error("Cluster is not launched " + error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def validate_date(self,d):
        try:
            datetime.strptime(d, '%d/%m/%Y')
            return d
        except Exception:
            raise Exception("Date should be in %d/%m/%Y format")

    def main(self, input_dict):
        """
        Purpose   :   this function call all the processes sequentially
        Input     :   Input dictionary containing all the necessary arguments.
        """
        batch_id = latest_batch_id= payload_id = ""
        self.cluster_launch_flag = "N"
        try:
            status_message = "Starting the main function for Ingestion Adaptor"
            logging.info(status_message)
            # payload_ids = input_dict["payload_id_list"]
            adapter_id = self.get_adapter_id(self.data_source)
            payload_ids = self.get_payload_id(input_dict['payload_id_list'], adapter_id)
            if payload_ids is not None and payload_ids != "":
                payload_id_list = payload_ids.split(",")
                if len(payload_id_list) > 1 :
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

            status_message = "Obtained input config: " + json.dumps(application_config)
            logging.info(status_message)

            batch_id = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S")
            # batch_id = "20190927061028"
            filter_provided_flag = "N"
            for payload_json in payload_json_list:
                logging.info(json.dumps(payload_json))
                payload_id = str(payload_json[0]["payload_id"])
                create_struct_dict_list = []
                if not payload_json[0].get("load_type"):
                    raise Exception("load_type key must be present")
                if not payload_json[0].get("disease_area_col_value"):
                    raise Exception("disease_area_col_value key must be present")

                load_type = str(payload_json[0]["load_type"])
                if load_type == DELTA_LOAD:
                    get_max_batch_id_query_str = (
                        "select max(cycle_id) as cycle_id from {audit_db}."
                        "{log_data_acquisition_smry_table} where source_name = "
                        " '{source_name}' and ( load_type = '{full_load_type}' or "
                        " load_type = '{delta_load_type}' )"
                        " and status = '{status}' "
                    )
                    query = get_max_batch_id_query_str.format(
                        audit_db=self.audit_db, status=SUCCESS_KEY, full_load_type=FULL_LOAD,
                        log_data_acquisition_smry_table=CommonConstants.LOG_DATA_ACQUISITION_SMRY_TABLE,
                        source_name=self.data_source, delta_load_type=DELTA_LOAD
                    )
                    logging.info("Query to insert in the log table: %s", str(query))
                    get_max_batch_id_query_output = MySQLConnectionManager().execute_query_mysql(query)
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
                    data_load_type = FILTERED_LOAD

                output_query = self.execute_query(
                    query_type="insert", table_type="smry", batch_id=batch_id, adapter_id=adapter_id,
                    payload_id=payload_id, payload_json=json.dumps(payload_json), load_type=data_load_type,
                    cluster_id=self.cluster_id, status=RUNNING_KEY
                )
                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])

                self.page_count = 0
                if len(payload_json) > 1:
                    logging.info("Multiple Json found for this payload!")
                self.inserted_query = True
                counter = 0
                for sub_payload in payload_json:
                    logging.debug(sub_payload)
                    dict_data = {}
                    logging.info(sub_payload)
                    self.pre_landing_path = sub_payload["raw_file_location"]
                    self.landing_path = sub_payload["structured_file_location"]
                    if sub_payload.get("payload_id"):
                        sub_payload.pop("payload_id")
                    if sub_payload.get("raw_file_location"):
                        sub_payload.pop("raw_file_location")
                    if sub_payload.get("structured_file_location"):
                        sub_payload.pop("structured_file_location")
                    if sub_payload.get("disease_area_col_value"):
                        dict_data["disease_area_col_value"] = sub_payload["disease_area_col_value"]
                        sub_payload.pop("disease_area_col_value")
                    if sub_payload.get("filter_provided_flag"):
                        sub_payload.pop("filter_provided_flag")
                    if sub_payload.get("delta_refresh_date_str"):
                        date_validate = sub_payload.get("delta_refresh_date_str")
                        logging.debug(date_validate,"************")
                        self.validate_date(date_validate)

                    logging.debug("getting sub_payload value" + str(sub_payload))

                    logging.info("Load type configured - {}".format(sub_payload.get("load_type")))

                    if sub_payload.get("load_type") == DELTA_LOAD:
                        for key in sub_payload.copy():
                            self.filter_key = key
                            payload_json_str = json.dumps(sub_payload)
                            if ("delta_refresh_date_str" in payload_json_str and
                                    sub_payload["delta_refresh_date_str"] != ""):
                                if (sub_payload.get("load_type")):
                                    sub_payload.pop("load_type")
                                    for key in sub_payload:
                                        output = self.download_lynx_log_file(key, batch_id, sub_payload[key])
                                        logging.debug(str(output))
                            else:
                                logging.debug(sub_payload[key],"batch_id",batch_id,"latest_batch_id",latest_batch_id)
                                if (sub_payload.get("delta_refresh_date_str")):
                                    sub_payload.pop("delta_refresh_date_str")
                                output = self.download_lynx_log_file(sub_payload[key], batch_id, latest_batch_id)
                                logging.debug(str(output))
                                break
                    else:
                        if len(sub_payload) > 1:
                            latest_batch_id = ""
                            if len(sub_payload) > 2:
                                raise Exception("multiple filter values are found, who only accepts the single filter at a time")
                            logging.debug("two key found")
                            copy_payload = sub_payload.copy()
                            if (copy_payload.get("load_type")):
                                copy_payload.pop("load_type")
                            logging.debug(copy_payload)
                            copy_payload_value = list(copy_payload.values())
                            copy_payload_key = list(copy_payload.keys())
                            self.filter_key = copy_payload_key[0]
                            logging.debug("key passed to download lynx file----------> {}".format(self.filter_key))
                            logging.debug(str(copy_payload_key) + " " + str(copy_payload_value))
                            # sys.exit()
                            output = self.download_lynx_log_file(copy_payload_value[0], batch_id, latest_batch_id)
                            logging.debug(str(output))
                        else:
                            logging.debug("one key found")
                            logging.debug("#############", sub_payload)
                            if len(sub_payload) > 2:
                                raise Exception(
                                    "multiple filter values are found, who only accepts the single filter at a time")
                            copy_payload = sub_payload.copy()
                            copy_payload_value = list(copy_payload.values())
                            logging.debug(copy_payload_value[0])
                            copy_payload_key = list(copy_payload.keys())
                            logging.debug("copy_payload_key - {}".format(copy_payload_key))
                            self.filter_key = copy_payload_key[0]
                            logging.debug("key passed to download lynx file---------->{}".format(self.filter_key))
                            output = self.download_lynx_log_file(copy_payload_value[0], batch_id, latest_batch_id)
                            logging.debug(str(output))

                    if self.inserted_query:
                        output_query = self.execute_query(
                            query_type="insert", table_type="dtl", batch_id=batch_id, adapter_id=adapter_id,
                            payload_id=payload_id, step_name=extract_data_step_name,
                            raw_file_dir=self.pre_landing_path, structured_file_path="",
                            status=RUNNING_KEY
                        )
                        if output_query[STATUS_KEY] == FAILED_KEY:
                            raise Exception()

                    self.inserted_query = False
                    disease_area_col_value = dict_data["disease_area_col_value"]
                    output = self.trigger_data_ingestion(sub_payload, batch_id, payload_id, self.filter_key,
                                                         FILE_ABS_PATH, self.pre_landing_path, counter,disease_area_col_value)
                    counter = counter + 1

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

                        raise Exception(output_status[ERROR_KEY])

                    logging.debug("final output: " + str(json.dumps(output)))

                    dict_data["path_extracted_json"] = output[SPARK_CONF_PATH]
                    dict_data["datasource"] = self.data_source

                    create_struct_dict_list.append(dict_data)

                output_query = self.execute_query(
                    query_type="update", table_type="dtl",
                    batch_id=batch_id, step_name=extract_data_step_name,
                    payload_id=payload_id, status=SUCCESS_KEY,
                )

                if output_query[STATUS_KEY] == FAILED_KEY:
                    raise Exception(output_query[ERROR_KEY])
                logging.info("Launching cluster ...")
                output = self.call_launch_cluster(data_source_name=self.data_source)
                self.cluster_id = output[RESULT_KEY]
                # TODO - update value of cluster_launch_flag
                self.cluster_launch_flag = "N"
                cluster_id = self.cluster_id
                query_str = (
                    "Update {audit_db}.{log_data_acquisition_smry_table} set cluster_id = '{cluster_id}' "
                    ", end_time = '{end_time}' where cycle_id = '{batch_id}' and "
                    " source_name = '{data_source}' and payload_id ='{payload_id}'"
                )
                output_query = query_str.format(
                    audit_db=self.audit_db, cluster_id=self.cluster_id,
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
                    query_type="insert", table_type="dtl", batch_id=batch_id, adapter_id=adapter_id,
                    payload_id=payload_id, step_name=create_structure_step_name,
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
                #     self.call_terminate_cluster(self.cluster_id)


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
            # if self.cluster_launch_flag == "Y":
            #     self.call_terminate_cluster(self.cluster_id)

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
        IngestionAdaptor_obj.check_mandatory_parameters_exists(commandline_arguments)
        output = IngestionAdaptor_obj.main(input_dict=commandline_arguments)
        STATUS_MSG = "\nCompleted execution for Adaptor with status " + str(output)
        sys.stdout.write(STATUS_MSG)
        # if output[STATUS_KEY] == FAILED_KEY:
        #     DataIngestionAdapter().call_terminate_cluster()
    except Exception as exception:
        DataIngestionAdapter().usage()
        logging.error(str(traceback.format_exc()))
        message = "Error occurred in main Error : %s\nTraceback : %s" % (
            str(exception),
            str(traceback.format_exc()),
        )
        raise exception
