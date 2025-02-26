#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

import sys
import os
sys.path.insert(0, os.getcwd())
import re
import logging
import argparse
import json
import time
from urllib.parse import urlparse
import requests
import boto3
import traceback
from SalesForceUtility import SalesForceIngestor

SERVICE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, SERVICE_DIR_PATH)
CODE_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIR_PATH, "../common_utilities"))
sys.path.insert(1, CODE_DIR_PATH)

from EmrManagerUtility import EmrManagerUtility
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from SystemManagerUtility import SystemManagerUtility

APPLICATION_CONFIG_FILE = "application_config.json"
MODULE_NAME = "SalesForce Wrapper"
PROCESS_NAME = "SalesForce Data Download Orchestrator"
LOG_CLUSTER_TABLE = "log_cluster_dtl"
CLUSTER_CONFIG = "cluster_variables_sf.json"
ADAPTER_PAYLOAD_CONTROL_TABLE = "ctl_adapter_payload_details"
ADAPTER_CONFIG_TABLE = "ctl_adapter_details"

STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "ERROR"


class SalesForceWrapper(object):

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": MODULE_NAME})
        self.configuration = json.load(open(APPLICATION_CONFIG_FILE))
        self.data_source_name = "salesforce"
        self.emr_region_name = self.configuration["adapter_details"]["generic_config"]["aws_region"]
        self.emr_code_path = self.configuration["adapter_details"]["generic_config"]["emr_code_path"]
        self.emr_key_path = self.configuration["adapter_details"]["generic_config"]["private_key_loaction"]

    def get_emr_mode_flag(self):
        emr_mode_flag = self.configuration["adapter_details"][self.data_source_name]["emr_mode_flag"]
        if emr_mode_flag.lower() == 'y' or emr_mode_flag.lower() == 'n':
            return emr_mode_flag.lower()
        else:
            print("There is no valid emr_mode_flag configured in the app config hence going forward with EC2 execution")
            emr_mode_flag_default = "y"
            return emr_mode_flag_default

    def execute_command_emr(self, payload_id=None, cluster_id=None):
        command = "python3 " + CommonConstants.AIRFLOW_CODE_PATH + "/adapter/salesforce/SalesForceUtility.py -p " \
                  + str(payload_id)
        output_status = SystemManagerUtility().execute_command(cluster_id,
                                                        command)
        print(output_status)

    def call_launch_cluster(self):
        try:
            cluster_type = self.configuration["adapter_details"][self.data_source_name]["cluster_type"]
            file_read = open("../common_utilities/" + CLUSTER_CONFIG, "r")
            json_data = json.load(file_read)
            file_read.close()

        except Exception:
            raise Exception("File not found error")

        try:
            logging.info("launch_cluster method called for process")

            emr_payload = json_data["cluster_configurations"][cluster_type]
            region_name = emr_payload["Region"]

            returned_json = EmrManagerUtility().launch_emr_cluster(
                emr_payload, region_name, self.data_source_name
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


if __name__ == "__main__":
    try:
        PARSER = argparse.ArgumentParser(description="VEEVA_WRAPPER")
        PARSER.add_argument("-p", "--payload_id_list", help="The payload_id_list to be executed", required=False)
        ARGS = vars(PARSER.parse_args())

        payload_id_input = ARGS['payload_id_list']
        payload_id_list = payload_id_input.split(',')

        WrapperObj = SalesForceWrapper()
        emr_mode_flag = WrapperObj.get_emr_mode_flag()

        app_file = json.load(open(APPLICATION_CONFIG_FILE))
        emr_region_name = app_file["adapter_details"]["generic_config"]["aws_region"]
        emr_code_path = app_file["adapter_details"]["generic_config"]["emr_code_path"]
        emr_key_path = app_file["adapter_details"]["generic_config"]["private_key_loaction"]

        payload_exclusion_list = []

        for payload_id in payload_id_list:
            try:
                get_payload_query = "select * from " + ADAPTER_PAYLOAD_CONTROL_TABLE + " where payload_id={payload_id}".format \
                    (payload_id=payload_id)
                get_payload_result = MySQLConnectionManager().execute_query_mysql(get_payload_query)

                adapter_id = get_payload_result[0]['adapter_id']

                get_adapter_id = "select adapter_id,source_name from " + ADAPTER_CONFIG_TABLE + " where adapter_id={adapter_id}".\
                    format(adapter_id=adapter_id)
                adapter_id_result = MySQLConnectionManager().execute_query_mysql(get_adapter_id)

                if adapter_id_result[0]['source_name'].lower() != "salesforce" or get_payload_result[0]['active_flag'] == "N":
                    raise Exception

            except Exception as ex:
                print("The payload ID will not be processed either due to misconfiguration or incorrect source "
                      "type or active flag = N - The payload ID is:")
                print(payload_id)
                payload_exclusion_list.append(payload_id)

        for payload_id in payload_exclusion_list:
            payload_id_list.remove(payload_id)

        if emr_mode_flag.lower() == "y":
            cluster_id = WrapperObj.call_launch_cluster()
            for payload_id in payload_id_list:
                get_payload_query = "select * from " + ADAPTER_PAYLOAD_CONTROL_TABLE + " where payload_id={payload_id}".format \
                    (payload_id=payload_id)
                get_payload_result = MySQLConnectionManager().execute_query_mysql(get_payload_query)
                config_file_path = get_payload_result[0]["payload_details_file_name"]
                config_file = json.loads(str(open(config_file_path, 'r').read()))
                command = 'cd ' + CommonConstants.AIRFLOW_CODE_PATH + \
                          '/adapter/salesforce/ && python3 SalesForceUtility.py -p ' + str(payload_id) + \
                          ' -d "' + str(config_file) + '"' + ' -c ' + str(cluster_id)
                output_status = SystemManagerUtility().execute_command(cluster_id,  command)
        else:
            for payload_id in payload_id_list:
                get_payload_query = "select * from " + ADAPTER_PAYLOAD_CONTROL_TABLE + " where payload_id={payload_id}".format \
                    (payload_id=payload_id)
                get_payload_result = MySQLConnectionManager().execute_query_mysql(get_payload_query)
                config_file_path = get_payload_result[0]["payload_details_file_name"]
                config_file = json.loads(str(open(config_file_path, 'r').read()))
                command = 'python3 SalesForceUtility.py -p ' + str(payload_id)
                os.system(command)

    except Exception as ex:
        ERROR = "Check for error in execution of wrapper. The error is: %s" % str(ex)
        print(ERROR)
