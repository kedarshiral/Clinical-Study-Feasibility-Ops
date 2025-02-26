#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Doc_Type        :
Tech Description:    This is to check if data refresh has been completed or not
Pre_requisites  :
Inputs          :    NA
Outputs          :   Dictionary response
Command to execute : Execute via route in browser e.g. http://<host_ip>:<port>/<routes>
Config_file     :
"""

__author__ = 'ZS Associates'

import json
import os
import sys
import boto3
import subprocess
import pandas as pd
from datetime import datetime, timedelta

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../utilities/"))
sys.path.insert(0, UTILITIES_DIR_PATH)

import CommonServicesConstants as CommonServicesConstants
from LogSetup import get_logger
from utils import Utils
import user_activity_backup_queries as ubq


LOGGING = get_logger()
UTILS_OBJ = Utils()


def data_backup():
    try:
        app_config_dict = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(app_config_dict)
        present_date = datetime.today()
        present_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        present_date = datetime.combine(present_date, datetime.min.time())
        start_date = present_date - timedelta(days=app_config_dict["ctfo_services"]["time_interval_in_days"])
        end_date = present_date - timedelta(seconds=1)

        # present_date = present_date.strftime("%Y-%m-%d %H:%M:%S")
        start_timestamp = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_timestamp = end_date.strftime("%Y-%m-%d %H:%M:%S")

        present_date_name = present_date.strftime("%Y_%m_%d_%H_%M_%S")
        start_date_name = start_date.strftime("%Y_%m_%d_%H_%M_%S")
        end_date_name = end_date.strftime("%Y_%m_%d_%H_%M_%S")

        LOGGING.info("Date Calculations Done")

        select_query = ubq.backup_select_query
        select_query = UTILS_OBJ.replace_environment_details(app_config_dict, select_query)
        select_query = select_query.replace('$$start_time$$', start_timestamp).replace('$$end_time$$', end_timestamp)
        LOGGING.info("Select Query : {}".format(select_query))

        select_query_output = UTILS_OBJ.execute_query(select_query,
                                               UTILS_OBJ.host,
                                               UTILS_OBJ.port, UTILS_OBJ.username, UTILS_OBJ.password,
                                               UTILS_OBJ.database_name)
        LOGGING.info("Select Query Output : {}".format(json.dumps(select_query_output)))


        if select_query_output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
            raise Exception(" Exception while running Query")

        # make a data frame from JSON
        data_frame = pd.DataFrame(select_query_output["result"])

        # make a file with data
        target_file_name = 'user_activity_backup_start_{start_date}_end_{end_date}_present_time_{present_time}.csv'.format(start_date=start_date_name,
                                                                                               end_date=end_date_name, present_time=present_time)

        LOGGING.info("target_file_name : {}".format(target_file_name))

        target_directory_suffix = 'user_activity'

        target_path = os.path.join(CommonServicesConstants.user_activity_file_path, target_directory_suffix)

        LOGGING.info("target_path : {}".format(target_path))

        if not os.path.exists(target_path):
            os.makedirs(target_path)

        target_file = os.path.join(target_path, target_file_name)

        LOGGING.info("target_file : {}".format(target_file))

        data_frame.to_csv(target_file)

        df = pd.read_csv(target_file)
        LOGGING.info(df)

        # Upload the file to S3
        bucket_name = app_config_dict["ctfo_services"]["s3_details"]["bucket_name"]
        LOGGING.info("bucket_name : {}".format(bucket_name))

        s3_path = app_config_dict["ctfo_services"]["s3_target_file"].split(bucket_name)[1]
        s3_path = s3_path.replace('/', '', 1)
        LOGGING.info("s3_path : {}".format(s3_path))

        relative_s3_path = "user_activity/{present_date}/{file_name}".format(present_date=present_date_name,
                                                                              file_name=target_file_name)
        LOGGING.info("relative_s3_path : {}".format(relative_s3_path))

        s3_full_path = os.path.join(s3_path, relative_s3_path)
        LOGGING.info("S3 Full Path : {}".format(s3_full_path))

        s3 = boto3.resource('s3')
        key = s3_full_path

        LOGGING.info("Received request to copy file to s3 - " + target_file)
        LOGGING.info("key - " + key)

        s3.meta.client.upload_file(target_file, bucket_name, key)

        # cmd = '/usr/local/bin/aws s3 mv ' + "'" + target_file + "'" + " " + "'" + s3_full_path + "'"
        # LOGGING.info("Command to Move Export summary file (Excel) to S3 %s", str(cmd))
        #
        # shell_out_put = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        # out, err = shell_out_put.communicate()
        # shell_out_put.wait()
        #
        # if int(shell_out_put.returncode) == 0:
        #     LOGGING.info(f"Moved Export summary Template file (Excel) to S3. "
        #                  f"S3 Location:- {s3_full_path}. "
        #                  f"{out, err}")
        # else:
        #     LOGGING.info(f"{out, err}")
        #     raise Exception(f"Error while moving Export summary Template file (Excel) to S3. {out, err}")

        # Generating presign url
        # presigned_url = "/usr/local/bin/aws s3 presign " + "'" + s3_full_path + "'" + " " + "--expires-in 900"
        # LOGGING.info("Command to Generate presign url %s", presigned_url)
        #
        # shell_output = subprocess.Popen(presigned_url, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        # out, err = shell_output.communicate()
        # shell_output.wait()
        # if int(shell_output.returncode) == 0:
        #     url = str(out.decode('ascii'))
        #     url = url.rstrip('\n')
        #     LOGGING.info(f"Successfully Generated Presigned url. "
        #                  f"URL:- {url}")
        # else:
        #     LOGGING.info(f"{out, err}")
        #     raise Exception(f"Error while Generating Presigned url. {out, err}")

        # Return to UI
        LOGGING.info("Success Response - URL Extracted")

        # Delete date from the table
        delete_query = ubq.backup_delete_query
        delete_query = UTILS_OBJ.replace_environment_details(app_config_dict, delete_query)
        delete_query = delete_query.replace('$$start_time$$', start_timestamp).replace('$$end_time$$', end_timestamp)
        delete_query_output = UTILS_OBJ.execute_query(delete_query,
                                                      UTILS_OBJ.host,
                                                      UTILS_OBJ.port, UTILS_OBJ.username, UTILS_OBJ.password,
                                                      UTILS_OBJ.database_name)
        if delete_query_output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
            raise Exception(" Exception while running Query")

    except Exception as e:
        LOGGING.info(str(e))

