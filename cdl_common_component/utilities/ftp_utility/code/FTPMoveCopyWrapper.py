#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

######################################################Module Information################################################
#   Module Name         :   FTPMoveCopyWrapper
#   Purpose             :   Copy data from FTP to s3 and logging the status of files
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate and object of this class and call the class functions
#   Predecessor module  :   This module is specific to FTP
#   Successor module    :   NA
#   Last changed on     :   11 May 2021
#   Last changed by     :   Sukhwinder Singh
#   Reason for change   :   NA
########################################################################################################################


import traceback
import pysftp
import os
import sys
import json
import argparse

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

sys.path.insert(0, os.getcwd())
from datetime import datetime
from ConfigUtility import JsonConfigUtility
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from Pattern_Validator import PatternValidator
from FTPMoveCopyUtility import FTPMoveCopyUtility

MODULE_NAME = "FTPMoveCopyWrapper"

ftp_copy_file_obj = FTPMoveCopyUtility()
logger = ftp_copy_file_obj.get_logger()

class FTPMoveCopyWrapper():
    """

    """
    def __init__(self):
        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.aws_region_name = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "ftp_aws_region"])
        self.mysql_conn = MySQLConnectionManager()

    def get_source_details(self, source_system, source_platform):
        """

        :param source_system:
        :param source_platform:
        :return:
        """
        response = dict()
        try:
            source_details_query = "select {source_platform}, {src_system},  {source_location}, " \
                                   "{target_location}, {file_pattern}, {output_file_format}, {geography_id}, " \
                                   "{output_file_suffix} from {db}.{table_name} where lower({src_system_col}) = '{src_system_val}' " \
                                   " and lower({src_platform_col}) = '{src_platform_val}' and  " \
                                   " lower({active_flag_col}) = '{active_flag_val}'".format(
                source_platform=CommonConstants.SOURCE_PLATFORM_COL,
                src_system=CommonConstants.SRC_SYSTEM_COL,
                source_location=CommonConstants.SOURCE_LOCATION_COL,
                target_location=CommonConstants.TARGET_LOCATION_COL,
                file_pattern=CommonConstants.FILE_PATTERN_COL,
                output_file_format=CommonConstants.OUTPUT_FILE_FORMAT_COL,
                geography_id=CommonConstants.GEOGRAPHY_ID_COL,
                output_file_suffix=CommonConstants.OUTPUT_FILE_SUFFIX_COL,
                db=self.audit_db,
                table_name=CommonConstants.FTP_CNTRL_TABLE,
                src_system_col=CommonConstants.SRC_SYSTEM_COL,
                src_system_val=source_system.lower(),
                src_platform_col=CommonConstants.SOURCE_PLATFORM_COL,
                src_platform_val=source_platform.lower(),
                active_flag_col=CommonConstants.FTP_ACTIVE_FLAG_COL,
                active_flag_val=CommonConstants.FTP_ACTIVE_FLAG_VAL.lower()
            )
            source_details_res = self.mysql_conn.execute_query_mysql(source_details_query, False)
            if len(source_details_res) == 0 or source_details_res[0] is None:
                error = "No records are present in control table for source system '{}'".format(source_system)
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                response[CommonConstants.RESULT_KEY] = "Error"
                response[CommonConstants.ERROR_KEY] = error
                return response
            else:
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = source_details_res
                return response
        except Exception as e:
            error = "Failed while getting the details from control table'. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def get_source_files_list(self, ftp_connection, source_location):
        """

        :param ftp_connection:
        :param source_location:
        :return:
        """
        response = dict()
        try:
            directory_present = ftp_connection.isdir(source_location)
            if directory_present:
                source_files_list = ftp_connection.listdir(source_location)

                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = source_files_list
                return response
            else:
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                error = "The provide ftp source location '{loc}' is not available".format(loc=source_location)
                logger.error(error)
                response[CommonConstants.RESULT_KEY] = "Error"
                response[CommonConstants.ERROR_KEY] = error
                return response

        except Exception as e:
            error = "Failed while getting the list of source files from source location'. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def log_start_process(self, cycle_id, geography_id, source_platform_val, src_system_val,
                          source_file_location, target_location, output_file_format, validation_status=None):
        """

        :param cycle_id:
        :param geography_id:
        :param source_platform_val:
        :param src_system_val:
        :param source_file_format_val:
        :param source_file_location:
        :param target_location:
        :param output_file_format:
        :param validation_status:
        :return:
        """
        response = dict()
        try:
            if validation_status:
                status_val = validation_status
            else:
                status_val = CommonConstants.IN_PROGRESS_STATUS_KEY
            start_query = "insert into {db}.{log_ftp_tbl} ({geography_id}, {cycle_id}, {source_platform}, " \
                          "{src_system},  {source_file_location}, " \
                          "{target_location}, {output_file_format}, {status}, {load_start_time},  " \
                          "{insert_by}, {insert_date}) values({geography_id_val}, {cycle_id_val}, " \
                          " '{source_platform_val}', '{src_system_val}' ,  " \
                          "'{source_file_location_val}', '{target_location_val}', " \
                          "'{output_file_format_val}', '{status_val}', now(), '{insert_by_val}', " \
                          "now())".format(
                db=self.audit_db,
                log_ftp_tbl=CommonConstants.FTP_LOG_TABLE,
                geography_id=CommonConstants.GEOGRAPHY_ID_LOG_COL,
                cycle_id=CommonConstants.CYCLE_ID_LOG_COL,
                source_platform=CommonConstants.SOURCE_PLATFORM_LOG_COL,
                source_platform_val=source_platform_val,
                src_system_val=src_system_val,
                src_system=CommonConstants.SRC_SYSTEM_LOG_COL,
                source_file_location=CommonConstants.SOURCE_FILE_LOCATION_LOG_COL,
                target_location=CommonConstants.TARGET_FILE_LOCATION_LOG_COL,
                output_file_format=CommonConstants.OUTPUT_FILE_FORMAT_LOG_COL,
                status=CommonConstants.STATUS_LOG_COL,
                load_start_time=CommonConstants.LOAD_START_TIME_LOG_COL,
                insert_by=CommonConstants.INSERT_BY_LOG_COL,
                insert_date=CommonConstants.INSERT_DATE_LOG_COL,
                geography_id_val=geography_id,
                cycle_id_val=cycle_id,
                source_file_location_val=source_file_location,
                target_location_val=target_location,
                output_file_format_val=output_file_format,
                status_val=status_val,
                insert_by_val=CommonConstants.INSERT_BY_LOG_VAL_KEY
            )
            self.mysql_conn.execute_query_mysql(start_query, False)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            return response
        except Exception as e:
            error = "Failed while putting the process start log in FTP log table'. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def check_file_processed(self, source_file_location):
        """

        :param source_file_location:
        :return:
        """
        response = dict()
        try:
            check_query = "select count(*) as count from {db}.{log_ftp_tbl} where " \
                          "{source_file_location}='{source_file_location_val}' " \
                          " and {status_col} = '{status_complete_val}'".format(
                db=self.audit_db,
                log_ftp_tbl=CommonConstants.FTP_LOG_TABLE,
                source_file_location=CommonConstants.SOURCE_FILE_LOCATION_LOG_COL,
                source_file_location_val=source_file_location,
                status_col=CommonConstants.STATUS_LOG_COL,
                status_complete_val=CommonConstants.FTP_COMPLETED_KEY
            )
            logger.info("Checking source file {0} in log table".format(source_file_location))
            check_query_res = self.mysql_conn.execute_query_mysql(check_query, False)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = check_query_res[0]["count"]
            return response
        except Exception as e:
            error = "Failed while checking the status of file '{file}'. " \
                    "ERROR is: {error}".format(file=source_file_location,
                                               error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def log_update_process(self, cycle_id_val, updated_status, source_file_location, target_file_location, record_count_val=None):
        """

        :param cycle_id_val:
        :param updated_status:
        :param source_file_location:
        :param target_file_location:
        :return:
        """
        response = dict()
        try:
            if record_count_val:
                log_query = "update {db}.{log_ftp_tbl} set {status_col} = '{updated_status}', " \
                               " {record_count_col} = {record_count_val}, {load_end_time_col} = now() " \
                               "where {cycle_id} = {cycle_id_val} and " \
                               "{source_file_location}='{source_file_location_val}' " \
                               "and {target_location}='{target_location_val}'"
            else:
                log_query = "update {db}.{log_ftp_tbl} set {status_col} = '{updated_status}', " \
                           "{load_end_time_col} = now() where {cycle_id} = {cycle_id_val} " \
                           "and {source_file_location}='{source_file_location_val}' " \
                           "and {target_location}='{target_location_val}'"
            update_query = log_query.format(
                    db=self.audit_db,
                    log_ftp_tbl=CommonConstants.FTP_LOG_TABLE,
                    status_col=CommonConstants.STATUS_LOG_COL,
                    load_end_time_col=CommonConstants.LOAD_END_TIME_LOG_COL,
                    cycle_id=CommonConstants.CYCLE_ID_LOG_COL,
                    target_location=CommonConstants.TARGET_FILE_LOCATION_LOG_COL,
                    target_location_val=target_file_location,
                    cycle_id_val=cycle_id_val,
                    updated_status=updated_status,
                    source_file_location=CommonConstants.SOURCE_FILE_LOCATION_LOG_COL,
                    source_file_location_val=source_file_location,
                    record_count_col=CommonConstants.RECORD_COUNT_LOG_COL,
                    record_count_val=record_count_val
                )
            self.mysql_conn.execute_query_mysql(update_query, False)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            return response
        except Exception as e:
            error = "Failed while updating the process log in ftp log table. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def main(self, source_system, source_platform):
        """

        :param source_system:
        :param source_platform:
        :return:
        """
        response = dict()
        try:
            # Generating the cycle id.
            cycle_id = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            status_message = "cycle id is : {0}".format(cycle_id)
            logger.info(status_message)

            # Getting the FTP Configs from control table
            get_ftp_details = self.get_source_details(source_system=source_system, source_platform=source_platform)
            if get_ftp_details[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception(get_ftp_details[CommonConstants.ERROR_KEY])

            ftp_details_res = get_ftp_details[CommonConstants.RESULT_KEY]
            status_message = "The given ftp configuration details are: {0}".format(ftp_details_res)
            logger.info(status_message)

            # Creating the FTP Connection
            ftp_query = "select {connection_json_col} from {db}.{connection_tbl} where {src_system_col} = " \
                            "'{src_system_val}' and {source_platform_col} = '{source_platform_val}' " \
                            " and lower({active_flag_col}) = '{active_flag_val}' ".format(
                    connection_json_col=CommonConstants.CONNECTION_JSON_COL,
                    db=self.audit_db,
                    connection_tbl=CommonConstants.DATA_ACQUISITION_CONNECTION_TBL,
                    src_system_col=CommonConstants.SRC_SYSTEM_COL,
                    src_system_val=source_system,
                    source_platform_col=CommonConstants.SOURCE_PLATFORM_COL,
                    source_platform_val=source_platform,
                    active_flag_col=CommonConstants.FTP_ACTIVE_FLAG_COL,
                    active_flag_val=CommonConstants.FTP_ACTIVE_FLAG_VAL.lower()
                )
            ftp_query_res = self.mysql_conn.execute_query_mysql(ftp_query, False)
            connection_details = json.loads(ftp_query_res[0][CommonConstants.CONNECTION_JSON_COL])
            ftp_server_details = connection_details[CommonConstants.FTP_SERVER_KEY]
            ftp_username = connection_details[CommonConstants.FTP_USERNAME_KEY]
            ftp_password_secret = connection_details[CommonConstants.FTP_PWD_SECRET_NAME_KEY]
            get_ftp_password = ftp_copy_file_obj.get_password(secret_name=ftp_password_secret,
                                                                  region_name=self.aws_region_name)

            if get_ftp_password[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                return get_ftp_password[CommonConstants.ERROR_KEY]
            ftp_password = str(get_ftp_password[CommonConstants.RESULT_KEY]).strip()
            ftp_connection = pysftp.Connection(ftp_server_details, username=ftp_username, password=ftp_password,
                                                   cnopts=cnopts)
            status_message = "Created FTP connection object with provided connection details successfully"
            logger.info(status_message)
            pattern_validator_obj = PatternValidator()
            for ftp_details in ftp_details_res:
                source_platform = ftp_details[CommonConstants.SOURCE_PLATFORM_COL]
                src_system = ftp_details[CommonConstants.SRC_SYSTEM_COL]
                output_file_suffix = ftp_details[CommonConstants.OUTPUT_FILE_SUFFIX_COL]
                source_location = ftp_details[CommonConstants.SOURCE_LOCATION_COL]
                target_location = ftp_details[CommonConstants.TARGET_LOCATION_COL]
                file_pattern = ftp_details[CommonConstants.FILE_PATTERN_COL]
                output_file_format = ftp_details[CommonConstants.OUTPUT_FILE_FORMAT_COL]
                geography_id = ftp_details[CommonConstants.GEOGRAPHY_ID_COL]
                file_suffix = None
                if output_file_suffix != "" and output_file_suffix is not None:
                    output_file_suffix = output_file_suffix.replace("YYYY","%Y").replace("yyyy","%Y").replace("yy","%y").replace("YY",
                                                                    "%y").replace("mm","%m").replace("dd","%d").replace("DD",
                                                                    "%d").replace("MM","%M").replace("HH","%H").replace("hh",
                                                                    "%H").replace("SS","%S").replace("ss","%S").replace("bbb",
                                                                    "%b").replace("BBB","%b").replace("pp","%p").replace("PP","%p")


                    file_suffix = datetime.utcnow().strftime(output_file_suffix)

                
                file_count = 0
                file_processed = 0
                try:
                    if len(source_location.rsplit("/", 1)[-1].rsplit(".", 1)) != 1:
                        status_message = "The given source location is file"
                        logger.info(status_message)
                        pattern_validates = pattern_validator_obj.patter_validator(source=source_location,
                                                                                   pattern=file_pattern)
                        if pattern_validates:
                            file_count += 1
                            status_message = "The file '{file}' has been validated with the pattern '{pattern}'".format(file=source_location, pattern=file_pattern)
                            logger.info(status_message)
                            check_file = self.check_file_processed(source_file_location=source_location)
                            if check_file[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                raise Exception(check_file[CommonConstants.ERROR_KEY])
                            file_exist = check_file[CommonConstants.RESULT_KEY]
                            if file_exist != 0:
                                status_message = "The file '{0}' has already been processed".format(source_location)
                                logger.info(status_message)
                            else:
                                file_processed += 1
                                if file_suffix == "" or file_suffix is None:
                                    target_file_name = target_location.strip("/") + "/" + \
                                                   source_location.rsplit("/", 1)[-1]
                                else:
                                    target_file_name = target_location.strip("/") + "/" + \
                                                   source_location.rsplit("/", 1)[-1].rsplit(".",1)[0] + "_" + \
                                                   file_suffix + "." + source_location.rsplit("/", 1)[-1].rsplit(".", 1)[-1]
                                start_log = self.log_start_process(cycle_id=cycle_id, geography_id=geography_id,
                                                                   source_platform_val=source_platform,
                                                                   src_system_val=src_system,
                                                                   source_file_location=source_location,
                                                                   target_location=target_file_name,
                                                                   output_file_format=output_file_format)
                                if start_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                    raise Exception(start_log[CommonConstants.ERROR_KEY])

                                ftp_copy_res = ftp_copy_file_obj.copy_ftp_to_s3_with_boto3(ftp_file_path=source_location,
                                                                                           target_file_name=target_file_name,
                                                                                           ftp_connection=ftp_connection)
                                if ftp_copy_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                    status_message = "Failed to copy the file '{0}'.".format(source_location)
                                    logger.error(status_message)
                                    update_log = self.log_update_process(cycle_id_val=cycle_id,
                                                            updated_status=CommonConstants.FTP_FAILED_KEY,
                                                            source_file_location=source_location,
                                                                target_file_location=target_file_name)
                                    if update_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                        raise Exception(update_log[CommonConstants.ERROR_KEY])
                                    raise Exception(ftp_copy_res[CommonConstants.ERROR_KEY])

                                else:
                                    status_message = "File '{0}' from FTP has been copied to '{1}' S3 location Successfully".format(
                                        source_location, target_file_name
                                    )
                                    logger.info(status_message)
                                    record_count = ftp_copy_res[CommonConstants.RESULT_KEY]
                                    update_log = self.log_update_process(cycle_id_val=cycle_id,
                                                            updated_status=CommonConstants.FTP_COMPLETED_KEY,
                                                            source_file_location=source_location,
                                                                target_file_location=target_file_name,
                                                                         record_count_val=record_count)
                                    if update_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                        raise Exception(update_log[CommonConstants.ERROR_KEY])
                    else:
                        status_message = "The given source location '{0}' is folder".format(source_location)
                        logger.info(status_message)

                        get_file_list = self.get_source_files_list(ftp_connection=ftp_connection,
                                                                   source_location=source_location)
                        if get_file_list[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                            raise Exception(get_file_list[CommonConstants.ERROR_KEY])
                        file_list = get_file_list[CommonConstants.RESULT_KEY]
                        status_message = "The files present in folder are : {0}".format(file_list)
                        logger.info(status_message)
                        for file in file_list:
                            pattern_validates = pattern_validator_obj.patter_validator(source=file, pattern=file_pattern)
                            if pattern_validates:
                                file_count += 1
                                status_message = "The file '{file}' has been validated with the pattern '{pattern}'".format(file=file, pattern=file_pattern)
                                logger.info(status_message)
                                ftp_file_path = "/" + source_location.strip("/") + "/" + file
                                if file_suffix == "" or file_suffix is None:
                                    target_file_name = target_location.strip("/") + "/" + file.rsplit("/", 1)[-1]
                                else:
                                    target_file_name = target_location.strip("/") + "/" + file.rsplit("/", 1)[-1].rsplit(".",1)[0] + \
                                                    "_" + file_suffix + "." + file.rsplit("/", 1)[-1].rsplit(".", 1)[-1]

                                check_file = self.check_file_processed(source_file_location=ftp_file_path)
                                if check_file[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                    raise Exception(check_file[CommonConstants.ERROR_KEY])
                                file_exist = check_file[CommonConstants.RESULT_KEY]
                                if file_exist != 0:
                                    status_message = "The file '{0}' has already been processed".format(ftp_file_path)
                                    logger.info(status_message)
                                else:
                                    file_processed += 1
                                    start_log = self.log_start_process(cycle_id=cycle_id, geography_id=geography_id,
                                                                       source_platform_val=source_platform,
                                                                       src_system_val=src_system,
                                                                       source_file_location=ftp_file_path,
                                                                       target_location=target_file_name,
                                                                       output_file_format=output_file_format)
                                    if start_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                        raise Exception(start_log[CommonConstants.ERROR_KEY])
                                    ftp_copy_res = ftp_copy_file_obj.copy_ftp_to_s3_with_boto3(ftp_file_path=ftp_file_path,
                                                                                               target_file_name=target_file_name,
                                                                                               ftp_connection=ftp_connection)
                                    if ftp_copy_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                        status_message = "Failed to copy the file '{0}'.".format(ftp_file_path)
                                        logger.error(status_message)
                                        update_log = self.log_update_process(cycle_id_val=cycle_id,
                                                                updated_status=CommonConstants.FTP_FAILED_KEY,
                                                                source_file_location=ftp_file_path,
                                                                target_file_location=target_file_name)
                                        if update_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                            raise Exception(update_log[CommonConstants.ERROR_KEY])

                                    else:
                                        status_message = "File '{0}' from FTP has been copied to '{1}' S3 location Successfully".format(
                                            ftp_file_path, target_file_name)
                                        logger.info(status_message)
                                        record_count = ftp_copy_res[CommonConstants.RESULT_KEY]
                                        update_log = self.log_update_process(cycle_id_val=cycle_id,
                                                                updated_status=CommonConstants.FTP_COMPLETED_KEY,
                                                                source_file_location=ftp_file_path,
                                                                target_file_location=target_file_name,
                                                                record_count_val=record_count)
                                        if update_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                            raise Exception(update_log[CommonConstants.ERROR_KEY])

                except Exception as e:
                    error = "Failed while copying the files from FTP to S3'. " \
                            "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
                    logger.error(error)
                    response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                    response[CommonConstants.RESULT_KEY] = "Error"
                    response[CommonConstants.ERROR_KEY] = error

                if file_count == 0:
                    pattern_validation_log = self.log_start_process(cycle_id=cycle_id, geography_id=geography_id,
                                                                    source_platform_val=source_platform,
                                                                    src_system_val=src_system,
                                                       source_file_location=source_location,
                                                       target_location=target_location,
                                                       output_file_format=output_file_format,
                                                        validation_status=CommonConstants.PATTERN_NOT_FOUND_STATUS)
                    if pattern_validation_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                raise Exception(pattern_validation_log[CommonConstants.ERROR_KEY])
                    logger.info("The provided file pattern '{pattern}' didn't match with any of the file names " \
                               " present in source folder '{src_folder}'".format(pattern=file_pattern, src_folder=source_location))
                if file_processed == 0 and file_count != 0:
                    file_processed_log = self.log_start_process(cycle_id=cycle_id, geography_id=geography_id,
                                                                    source_platform_val=source_platform,
                                                                    src_system_val=src_system,
                                                       source_file_location=source_location,
                                                       target_location=target_location,
                                                       output_file_format=output_file_format,
                                                        validation_status=CommonConstants.ALL_FILES_ALREADY_EXIST)
                    if file_processed_log[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                raise Exception(file_processed_log[CommonConstants.ERROR_KEY])
                    logger.info("All the files present in source folder '{src_folder}' with file pattern '{pattern}' " \
                               " have already been processed".format(pattern=file_pattern, src_folder=source_location))

        except Exception as e:
            error = "The main method has failed'. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            raise e
        finally:
            ftp_connection.close()


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Copying files from FTP to S3")
        parser.add_argument("-sc", "--source_system",
                            help="Data Source system is required to know from which data source we have to extract data",
                            required=True)
        parser.add_argument("-sp", "--source_platform", help="Database name is required", required=True)
        args = vars(parser.parse_args())
        SOURCE_SYSTEM = args['source_system']
        SOURCE_PLATFORM = args['source_platform']
    except Exception as exception:
        error = "ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
        logger.error(error)
        raise exception

    obj = FTPMoveCopyWrapper()
    obj.main(source_system=SOURCE_SYSTEM, source_platform=SOURCE_PLATFORM)

