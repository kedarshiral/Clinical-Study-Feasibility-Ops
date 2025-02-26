#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

######################################################Module Information###########################################
#   Module Name         :   FTPMoveCopyUtility
#   Purpose             :   Copy data from FTP to s3
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate and object of this class and call the class functions
#   Predecessor module  :   This module is specific to FTP
#   Successor module    :   NA
#   Last changed on     :   7 May 2021
#   Last changed by     :   Sukhwinder Singh
#   Reason for change   :   NA
###################################################################################################################

import traceback
import pysftp
import os
import sys
import boto3
import subprocess
import base64
import json
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

sys.path.insert(0, os.getcwd())
from ConfigUtility import JsonConfigUtility
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
import logging
from logging.handlers import RotatingFileHandler

MODULE_NAME = "FTPMoveCopyUtility"
__handler_type__ = "rotating_file_handler"
__max_bytes__ = 50000000
__backup_count__ = 2
__time_format__ = "%Y-%m-%d %H:%M:%S"

class FTPMoveCopyUtility():
    def __init__(self):
        self.logger = self.get_logger()
        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.aws_region_name = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "ftp_aws_region"])
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.mysql_conn = MySQLConnectionManager()


    def get_logger(self):
        """
        Purpose: To create the self.logger file
        Input: logging parameters
        Output: self.logger
        """
        log_file_name = CommonConstants.FTP_LOG_FILE_NAME
        log_level = CommonConstants.FTP_LOG_LEVEL
        time_format = __time_format__
        handler_type = __handler_type__
        max_bytes = __max_bytes__
        backup_count = __backup_count__
        log_file_path = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "logs/")
        if not os.path.isdir(log_file_path):
            os.makedirs(log_file_path)
        complete_log_file_name = os.path.join(log_file_path, log_file_name)
        log_file = os.path.join(complete_log_file_name + '.log')
        __logger__ = logging.getLogger(complete_log_file_name)
        __logger__.setLevel(log_level.strip().upper())
        debug_formatter = '%(asctime)s - %(levelname)-6s - [%(threadName)5s:%(filename)5s:%(funcName)5s():''%(lineno)s] - %(message)s'
        formatter_string = '%(asctime)s - %(levelname)-6s- - %(message)s'

        if log_level.strip().upper() == log_level:
            formatter_string = debug_formatter

        formatter = logging.Formatter(formatter_string, time_format)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        if __logger__.hasHandlers():
            __logger__.handlers.clear()
        __logger__.addHandler(console_handler)

        if str(handler_type).lower() == "rotating_file_handler":
           handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
           handler.setFormatter(formatter)
           __logger__.addHandler(handler)

        else:
           hdlr_service = logging.FileHandler(log_file)
           hdlr_service.setFormatter(formatter)
           __logger__.addHandler(hdlr_service)
        return __logger__

    def execute_shell_command(self, command):
        """
                Purpose     :   This function is to execute a shell command
                Input       :   command
                Output      :   Status
        """
        return_status = {}
        try:
            status_message = "Started executing shell command " + command
            self.logger.info(status_message)
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            if standard_output:
                standard_output_line = standard_output.decode("utf-8")

            elif standard_error:
                standard_error_line = standard_error.decode("utf-8")

            if command_output.returncode == 0:
                return_status[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                return_status[CommonConstants.RESULT_KEY] = standard_output_line
                return return_status
            else:
                return_status[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                return_status[CommonConstants.ERROR_KEY] = "The command '{0}' has failed. ".format(command) + \
                                str(standard_error_line)
                return return_status

        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logger.error(error)
            return_status[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            return_status[CommonConstants.ERROR_KEY] = error
            return return_status

    def get_password(self, secret_name, region_name):
        """

        :param secret_name:
        :param region_name:
        :return:
        """
        return_status = {}
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
            self.logger.info("Fetching the details for the secret name %s", secret_name)
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            self.logger.info(
                "Fetched the Encrypted Secret from Secrets Manager for %s", secret_name)

            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                self.logger.info("Decrypted the Secret Key to get the FTP Password from AWS Secret Manager")
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                self.logger.info("Decrypted the Secret Key to get the FTP Password from AWS Secret Manager")
            get_secret = json.loads(secret)
            return_status[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            return_status[CommonConstants.RESULT_KEY] = get_secret[CommonConstants.FTP_SECRET_PASSWORD_TAG_KEY]
            return return_status
        except Exception as e:
            error = ' Failed while decrypting the secret key. ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logger.error(error)
            return_status[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            return_status[CommonConstants.ERROR_KEY] = error
            return return_status

    def copy_ftp_to_s3_with_boto3(self, ftp_file_path, target_file_name, ftp_connection=None, source_system=None,
                                  source_platform=None):
        """
             Purpose   :   This method is used to download FTP files and upload that to s3
             Input     :   ftp_file_path, target_file_name
             Output    :   success message
         """
        response = dict()
        status_message = ""
        try:
            status_message = "Started function to copy all the files from FTP : " + str(ftp_file_path) + \
                             " to s3 location : " + str(target_file_name)
            self.logger.info(status_message)
            if ftp_connection:
                conn = ftp_connection
            else:
                # Getting the FTP Connection
                ftp_query = "select {connection_json_col} from {db}.{connection_tbl} where {src_system_col} = " \
                            "'{src_system_val}' and {source_platform_col} = '{source_platform_val}'".format(
                    connection_json_col=CommonConstants.CONNECTION_JSON_COL,
                    db=self.audit_db,
                    connection_tbl=CommonConstants.DATA_ACQUISITION_CONNECTION_TBL,
                    src_system_col=CommonConstants.SRC_SYSTEM_COL,
                    src_system_val=source_system,
                    source_platform_col=CommonConstants.SOURCE_PLATFORM_COL,
                    source_platform_val=source_platform)
                ftp_query_res = self.mysql_conn.execute_query_mysql(ftp_query, False)
                connection_details = json.loads(ftp_query_res[0][CommonConstants.CONNECTION_JSON_COL])
                ftp_server_details = connection_details[CommonConstants.FTP_SERVER_KEY]
                ftp_username = connection_details[CommonConstants.FTP_USERNAME_KEY]
                ftp_password_secret = connection_details[CommonConstants.FTP_PWD_SECRET_NAME_KEY]
                get_ftp_password = self.get_password(secret_name=ftp_password_secret,
                                                                  region_name=self.aws_region_name)
                if get_ftp_password[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    return get_ftp_password[CommonConstants.ERROR_KEY]
                ftp_password = str(get_ftp_password[CommonConstants.RESULT_KEY]).strip()
                conn = pysftp.Connection(ftp_server_details, username=ftp_username, password=ftp_password,
                                         cnopts=cnopts)

            bucket_name = CommonUtils().get_s3_bucket_name(target_file_name)
            s3_folder_path = CommonUtils().get_s3_folder_path(target_file_name)
            local_ftp_path = CommonConstants.LOCAL_FTP_PATH
            if not os.path.isdir(local_ftp_path):
                os.mkdir(local_ftp_path)
            if not os.path.isdir(local_ftp_path):
                raise Exception("The provided local path '{path}' is not available and is not getting created".format(path=local_ftp_path))
            local_ftp_path = "/" + local_ftp_path.strip("/") + "/" + ftp_file_path.rsplit("/", 1)[-1]
            conn.get(ftp_file_path, local_ftp_path)
            # getting the record counts in source file
            file_type = local_ftp_path.rsplit(".", 1)[-1]
            if file_type == "csv" or file_type == "tsv" or file_type == "txt":
                command = "wc -l {0}".format(local_ftp_path)
                record_count_res = self.execute_shell_command(command=command)
                if record_count_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    return record_count_res[CommonConstants.ERROR_KEY]
                else:
                    record_count_res = record_count_res[CommonConstants.RESULT_KEY]
                    record_count = int(record_count_res.split(" ")[0])
            elif file_type == "parquet":
                command = "parquet-tools csv {0} | wc -l".format(local_ftp_path)
                record_count_res = self.execute_shell_command(command=command)
                if record_count_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    return record_count_res[CommonConstants.ERROR_KEY]
                else:
                    record_count_res = record_count_res[CommonConstants.RESULT_KEY]
                    record_count = int(record_count_res.strip("\n"))
            else:
                record_count = None
            self.logger.info("The record count of file '{file}' is: '{count}'".format(file=ftp_file_path, count=record_count))
            s3 = boto3.client('s3')
            s3.upload_file(local_ftp_path, bucket_name, s3_folder_path)
            status_message = "The file copy process executed successfuly"
            self.logger.info(status_message)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = record_count
            return response
        except Exception as e:
            error = "Failed while copying the file '{file}' to S3 location '{loc}'" + \
                    " ERROR MESSAGE: ".format(file=ftp_file_path, loc=target_file_name) + str(e) + "\n" + str(traceback.format_exc())
            self.logger.error(status_message)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response
        finally:
            local_ftp_path = CommonConstants.LOCAL_FTP_PATH
            local_ftp_path = "/" + local_ftp_path.strip("/") + "/" + ftp_file_path.rsplit("/", 1)[-1]
            os.system("sudo rm -f {local_ftp_path}".format(local_ftp_path=local_ftp_path))


    def copy_ftp_data_to_s3_with_boto3(self,connection_id, ftp_file_path, target_file_name):
        """
             Purpose   :   This method is used to read FTP data and copy that to s3
             Input     :   connection_id(for FTP connections), ftp_file_path, target_file_name
             Output    :   success message
         """
        try:
            status_message = "Started function to copy all the files from FTP : " + str(ftp_file_path) + " to s3 location : " \
                + str(target_file_name)
            self.logger.info(status_message)
            ftp_conn_details = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                 "ftp_connection_details"])
            ftp_connection_details_for_conn_id = ftp_conn_details[str(connection_id)]
            ftp_cred_params = ftp_connection_details_for_conn_id
            ftp_server_details = ftp_cred_params[CommonConstants.FTP_SERVER]
            ftp_username = ftp_cred_params[CommonConstants.FTP_USERNAME]
            ftp_secret_name = ftp_cred_params[CommonConstants.FTP_SECRET_NAME]

            secret_password = MySQLConnectionManager().get_secret(ftp_secret_name,
                self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
            ftp_password = secret_password['ftp_password_secret_name']

            conn = pysftp.Connection(ftp_server_details, username=ftp_username, password=ftp_password,
                                     cnopts=cnopts)

            status_message = "Created FTP connection object with provided connection details"
            self.logger.info(status_message)
            bucket_name = CommonUtils().get_s3_bucket_name(target_file_name)
            s3_folder_path = CommonUtils().get_s3_folder_path(target_file_name)

            file_content = conn.open(ftp_file_path)
            s3 = boto3.client('s3')
            s3.upload_fileobj(file_content, bucket_name, s3_folder_path)
            status_message = 'Executed FTP to s3 copy successfully '
            self.logger.debug(status_message)

        except Exception as e:
            error = "ERROR occured while saving the ftp data on s3." + \
                " ERROR MESSAGE: " + str(traceback.format_exc())
            self.logger.error(error)
            raise e




