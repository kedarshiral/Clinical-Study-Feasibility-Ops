#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

######################################################Module Information################################################
#   Module Name         :   RDBMSCopyUtility
#   Purpose             :   Copy data from RDBMS tables to s3 
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate and object of this class and call the class methods
#   Predecessor module  :   This module is specific to RDBMS
#   Successor module    :   NA
#   Last changed on     :   27 May 2021
#   Last changed by     :   Sukhwinder Singh
#   Reason for change   :   NA
########################################################################################################################

import datetime
import json
import pymysql
import base64
import os
import sys
from datetime import timezone
import traceback
import boto3
import getpass
import subprocess
from ConfigUtility import JsonConfigUtility
import logging
from logging.handlers import RotatingFileHandler
import CommonConstants as CommonConstants

MODULE_NAME = "RDBMSCopyUtility"

class RDBMSCopyUtility(object):
    """

    """

    def __init__(self, cycle_id, spark_session):
        """
        Purpose: Main handler for saving the file in S3 by fetching the data from oracle and teradata through spark
        """
        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.cycle_id = cycle_id
        self.spark = spark_session
        self.mysql_host = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_host"])
        self.mysql_username = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_username"])
        self.mysql_port = str(
            self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_port"]))
        self.mysql_database = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.aws_region = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "rdbms_aws_region"])
        self.utility_obj = RDBMSCommonUtilities()
        self.logger = self.utility_obj.get_logger()
        self.mysql_password = self.utility_obj.decode_password(
            password=self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "rds_password_secret_name"]),
            region_name=self.aws_region)

    def rdbms_connection_details(self, src_system, source_platform):
        response = dict()
        try:
            conn_detail_query = "select {connection_json_col} from {db}.{connection_tbl} where {src_system_col} = " \
                                "'{src_system_val}' and lower({source_platform_col}) = '{source_platform_val}' " \
                                " and lower({active_flag_col}) = '{active_flag_val}' ".format(
                connection_json_col=CommonConstants.RDBMS_CONNECTION_JSON_COL,
                db=self.mysql_database,
                connection_tbl=CommonConstants.DATA_ACQUISITION_CONNECTION_TBL,
                src_system_col=CommonConstants.RDBMS_SRC_SYSTEM_COL,
                src_system_val=src_system.lower(),
                active_flag_col=CommonConstants.RDBMS_ACTIVE_FLAG_COL,
                active_flag_val=CommonConstants.RDBMS_ACTIVE_FLAG.lower(),
                source_platform_col=CommonConstants.RDBMS_SOURCE_PLATFORM_COL,
                source_platform_val=source_platform.lower())
            conn_detail_query_res = self.utility_obj.mysql_utilty(mysql_host=self.mysql_host,
                                                                  mysql_username=self.mysql_username,
                                                                  mysql_password=self.mysql_password,
                                                                  mysql_database=self.mysql_database,
                                                                  query=conn_detail_query,
                                                                  dict_flag="Y", insert_update_key="N")
            conn_details = conn_detail_query_res[CommonConstants.RESULT_KEY][CommonConstants.RDBMS_CONNECTION_JSON_COL]
            self.logger.info(
                f"The Connection details for source system '{src_system}' and source platform '{source_platform}' are '{conn_details}'")
            if conn_details != "" and conn_details is not None:
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = conn_details
                return response
            else:
                if conn_details == "":
                    error = "The provided connection details for src_system '{src_system}' and " \
                            "source_platform '{source_platform}' are '{details}'".format(
                        src_system=src_system,
                        source_platform=source_platform,
                        details="Empty")
                else:
                    error = "The provided connection details for src_system '{src_system}' and " \
                            "source_platform '{source_platform}' are '{details}'".format(
                        src_system=src_system,
                        source_platform=source_platform,
                        details=conn_details)
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                response[CommonConstants.METHOD_KEY] = "rdbms_connection_details"
                response[CommonConstants.RESULT_KEY] = "Error"
                response[CommonConstants.ERROR_KEY] = error
                return response
        except Exception as e:
            error = "Failed while getting the connection details from connection table. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            # self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "rdbms_connection_details"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def read_from_database(self, each_data):
        """
        Purpose: Main function for connecting to databases and saving the file to s3
        Input: Connection list
        """
        get_prev_max_time = "{}"
        get_current_max_time = "{}"
        override_flag = "N"
        start_time = str(datetime.datetime.now(timezone.utc).strftime(CommonConstants.RDBMS_START_TIME_FORMAT))
        response = dict()
        save_df_to_s3_res_flag = True
        try:
            src_system = each_data[CommonConstants.RDBMS_SRC_SYSTEM_COL].lower()
            source_platform = each_data[CommonConstants.RDBMS_SOURCE_PLATFORM_COL]
            get_connection_details = self.rdbms_connection_details(src_system=src_system,
                                                                   source_platform=source_platform)
            if get_connection_details[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception(str(get_connection_details))
            connection_details = get_connection_details[CommonConstants.RESULT_KEY]
            query = ""
            if each_data[CommonConstants.RDBMS_ACTIVE_FLAG_COL].lower() == CommonConstants.RDBMS_ACTIVE_FLAG.lower():
                self.logger.info("inside read database method")
                # start_time = str(datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
                output_file_delimiter = each_data[CommonConstants.RDBMS_OUTPUT_FILE_DELIMITER_COL]
                file_query = each_data[CommonConstants.RDBMS_QUERY_COL]
                header_flag = str(each_data[CommonConstants.RDBMS_HEADER_FLAG_COL])
                output_file_suffix = each_data[CommonConstants.RDBMS_OUTPUT_FILE_SUFFIX_COL]
                query_description = each_data[CommonConstants.RDBMS_QUERY_DESCRIPTION_COL]
                database_list = str(each_data[CommonConstants.RDBMS_DATABASE_NAME_COL]).split(",")
                table_list = str(each_data[CommonConstants.RDBMS_OBJECT_NAME_COL]).split(",")
                target_directory = None
                if output_file_suffix != "" and output_file_suffix is not None:
                    output_file_suffix = output_file_suffix.replace("YYYY", "%Y").replace("yyyy", "%Y").replace("yy",
                                                                                                                "%y").replace(
                        "YY",
                        "%y").replace("mm", "%m").replace("dd", "%d").replace("DD",
                                                                              "%d").replace("MM", "%M").replace("HH",
                                                                                                                "%H").replace(
                        "hh",
                        "%H").replace("SS", "%S").replace("ss", "%S").replace("bbb",
                                                                              "%b").replace("BBB", "%b").replace("pp",
                                                                                                                 "%p").replace(
                        "PP", "%p")

                    target_directory = datetime.datetime.utcnow().strftime(output_file_suffix)

                target_folder_location = each_data[CommonConstants.RDBMS_TARGET_LOCATION_COL].strip("/") + "/" + \
                                         str(each_data[CommonConstants.RDBMS_OBJECT_NAME_COL]) + "/"
                if target_directory == "" or target_directory is None: 
                    output_file_name = str(each_data[CommonConstants.RDBMS_OUTPUT_FILE_NAME_COL]) + "." + \
                                       str(each_data[CommonConstants.RDBMS_OUTPUT_FILE_FORMAT_COL])
                else:
                    output_file_name = str(each_data[CommonConstants.RDBMS_OUTPUT_FILE_NAME_COL]) + "_" + \
                                       target_directory + "." + str(each_data[CommonConstants.RDBMS_OUTPUT_FILE_FORMAT_COL])
                    
                try:
                    if each_data[CommonConstants.RDBMS_LOAD_TYPE_COL].lower() == CommonConstants.RDBMS_INCREMENTAL_KEY.lower() or \
                            each_data[CommonConstants.RDBMS_LOAD_TYPE_COL].lower() == CommonConstants.RDBMS_DELTA_KEY.lower():
                        self.logger.info("inside incremental load process ")
                        # check the proper conf for incremental load
                        if ((each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL] is None or 
                                each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL] == "") and 
                                (each_data[CommonConstants.RDBMS_QUERY_COL] is None or 
                                each_data[CommonConstants.RDBMS_QUERY_COL] == "")) or len(
                                database_list) > 1 or len(table_list) > 1:
                            raise Exception("please provide proper information for incremental load")
                        query_to_fetch_latest_load_time = "select max(load_start_time) as ls from {db}.{log_table_name} " \
                                                          " where status ='{success_status}'".format(db=self.mysql_database, 
                                                                                              success_status=CommonConstants.RDBMS_STATUS_COMPLETE_KEY,
                                                                                              log_table_name=CommonConstants.RDBMS_LOG_TABLE_NAME)
                        get_load_time_res = self.utility_obj.mysql_utilty(mysql_host=self.mysql_host,
                                                                          mysql_username=self.mysql_username,
                                                                          mysql_password=self.mysql_password,
                                                                          mysql_database=self.mysql_database,
                                                                          query=query_to_fetch_latest_load_time,
                                                                          dict_flag="Y", insert_update_key="N")
                        if get_load_time_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                            raise Exception(str(get_load_time_res))
                        get_load_time = get_load_time_res[CommonConstants.RESULT_KEY]

                        load_time = get_load_time['ls']
                        if file_query != "" and file_query is not None:
                            query = file_query
                        else:
                            query_res = self.utility_obj.file_reader(CommonConstants.AIRFLOW_CODE_PATH,
                                                                     each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL])
                            if query_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                raise Exception(str(query_res))
                            query = query_res[CommonConstants.RESULT_KEY]

                        multiple_increment_column = each_data[CommonConstants.RDBMS_INCREMENTAL_COL_NAME_COL].split(",")
                        current_time_max = dict()
                        for each_increment_col in multiple_increment_column:
                            each_increment_col = each_increment_col.strip()
                            temp_dict_curr = dict()
                            table_name = each_data[CommonConstants.RDBMS_OBJECT_NAME_COL]

                            query_to_fetch_latest_time_from_rdbms = "select max({incremental_load_column}) as d from {db_name}.{table_name}".format(
                                incremental_load_column=each_increment_col, db_name=each_data[CommonConstants.RDBMS_DATABASE_NAME_COL],
                                table_name=table_name)
                            df_last_refrsh_time_res = RDBMSCommonUtilities().get_spark_dataframe(
                                spark=self.spark, query=query_to_fetch_latest_time_from_rdbms,
                                region_name=self.aws_region,
                                source_platform=source_platform,
                                connection_details=connection_details)
                            if df_last_refrsh_time_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                raise Exception(str(df_last_refrsh_time_res))
                            df_last_refrsh_time = df_last_refrsh_time_res[CommonConstants.RESULT_KEY]

                            data = df_last_refrsh_time.collect()[0]['d']
                            temp_dict_curr[each_increment_col] = str(data)
                            current_time_max.update(temp_dict_curr)
                            self.logger.info("The maximum value for {incremental_load_column} in table "
                                             "{db_name}.{table_name} is : {value}".format(
                                incremental_load_column=each_increment_col, db_name=each_data[CommonConstants.RDBMS_DATABASE_NAME_COL],
                                table_name=table_name, value=data))
                            self.logger.info("The provided override_last_refresh_date value is: '{value}'".format(
                                value=each_data[CommonConstants.RDBMS_OVERRIDE_REFRESH_DATE_COL]))
                            if each_data[CommonConstants.RDBMS_OVERRIDE_REFRESH_DATE_COL] == "" or each_data[
                                CommonConstants.RDBMS_OVERRIDE_REFRESH_DATE_COL] is None:
                                self.logger.info("Fetching the curr_max_refresh_timestamp value from log table")
                                query_to_fetch_latest_time_from_log = "select max(curr_max_refresh_timestamp) as prevtime " \
                                                                      " from {db}.{log_table_name}  where cycle_id = " \
                                                                      " (select max(cycle_id) from {db}.{log_table_name} " \
                                                                      " where object_name='{table_name}' and status in " \
                                                                      " ('{success_status}') and src_system='{src_system}') " \
                                                                      " and object_name='{table_name}'".format(
                                    db=self.mysql_database,
                                    log_table_name=CommonConstants.RDBMS_LOG_TABLE_NAME, success_status=CommonConstants.RDBMS_STATUS_COMPLETE_KEY,
                                    table_name=each_data[CommonConstants.RDBMS_OBJECT_NAME_COL], src_system=each_data[CommonConstants.RDBMS_SRC_SYSTEM_COL])
                                get_prev_max_time_res = self.utility_obj.mysql_utilty(mysql_host=self.mysql_host,
                                                                                      mysql_username=self.mysql_username,
                                                                                      mysql_password=self.mysql_password,
                                                                                      mysql_database=self.mysql_database,
                                                                                      query=query_to_fetch_latest_time_from_log,
                                                                                      dict_flag="Y",
                                                                                      insert_update_key="N")
                                if get_prev_max_time_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                    raise Exception(str(get_prev_max_time_res))
                                get_prev_max_time = get_prev_max_time_res[CommonConstants.RESULT_KEY]
                                get_prev_max_time = json.loads(get_prev_max_time['prevtime'])

                                query = query.replace("$$load_date", str(load_time)).replace('"',"'")
                                if get_prev_max_time[each_increment_col] is None:

                                    query = query.replace("'$$" + each_increment_col + "'", "null").replace(
                                        "$$database_name", str(each_data[CommonConstants.RDBMS_DATABASE_NAME_COL]))
                                else:
                                    date_to_fetch_increment = str(get_prev_max_time[each_increment_col])
                                    # date_to_fetch_increment = get_timestamp_to_date.split(" ")
                                    query = query.replace("$$" + each_increment_col,
                                                          date_to_fetch_increment).replace(
                                        "$$database_name", str(each_data[CommonConstants.RDBMS_DATABASE_NAME_COL]))
                                self.logger.info("The incremental query is : {0}".format(query))
                            else:
                                self.logger.info("Fetching the data from table {db_name}.{table_name} for "
                                                 " override_last_refresh_date value {value}".format(
                                    table_name=table_name, db_name=each_data[CommonConstants.RDBMS_DATABASE_NAME_COL],
                                    value=each_data[CommonConstants.RDBMS_OVERRIDE_REFRESH_DATE_COL]),
                                )
                                get_current_max_time = json.loads(each_data[CommonConstants.RDBMS_OVERRIDE_REFRESH_DATE_COL])
                                query = query.replace("$$load_date", str(load_time)).replace('"',"'")
                                if get_current_max_time[each_increment_col] == 'None':
                                    query = query.replace("'$$" + each_increment_col + "'", "null").replace(
                                        "$$database_name", str(each_data[CommonConstants.RDBMS_DATABASE_NAME_COL]))
                                else:
                                    query = query.replace("$$" + each_increment_col,
                                                          str(get_current_max_time[each_increment_col])).replace(
                                        "$$database_name", str(each_data[CommonConstants.RDBMS_DATABASE_NAME_COL]))
                                get_prev_max_time = dict()
                                self.logger.info("The incremental query is : {0}".format(query))
                        get_current_max_time = current_time_max
                        query_to_update = "update {table_name} set override_last_refresh_date = Null where object_name='{object_name}';".format(
                            table_name=CommonConstants.RDBMS_CONTROL_TABLE, object_name=each_data[CommonConstants.RDBMS_OBJECT_NAME_COL])
                        self.utility_obj.mysql_utilty(mysql_host=self.mysql_host, mysql_username=self.mysql_username,
                                                      mysql_password=self.mysql_password, mysql_database=self.mysql_database,
                                                      query=query_to_update, insert_update_key="y", dict_flag="N")

                    # condition for full load
                    else:
                        self.logger.info("inside full load process ")
                        if len(database_list) > 1 or len(table_list) > 1 and each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL] is None:
                            raise Exception("please provide proper information for full load")
                        if file_query is not None and file_query != '':
                            query = file_query
                        elif each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL] is not None and \
                                each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL] != '' :
                            full_query_res = self.utility_obj.file_reader(CommonConstants.AIRFLOW_CODE_PATH,
                                                                 each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL])
                            if full_query_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                raise Exception(str(full_query_res))
                            query = full_query_res[CommonConstants.RESULT_KEY]
                        else:
                            query = "select * from {database_name}.{table_name}".format(
                                database_name=each_data[CommonConstants.RDBMS_DATABASE_NAME_COL],
                                table_name=each_data[CommonConstants.RDBMS_OBJECT_NAME_COL])
                        self.logger.info("The full load query is : {0}".format(query),
                                         )

                    # execute the query through spark jdbc
                    df_res = self.utility_obj.get_spark_dataframe(spark=self.spark, query=query,
                                                                  region_name=self.aws_region,
                                                                  source_platform=source_platform,
                                                                  connection_details=connection_details)
                    if df_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                        raise Exception(df_res)
                    df = df_res[CommonConstants.RESULT_KEY]

                    record_count = df.count()
                    load_method = CommonConstants.SQOOP_ENABLE_FLAG.lower()
                    hdfs_path = "/data/" + each_data[CommonConstants.RDBMS_OBJECT_NAME_COL] + "/" + str(self.cycle_id) + "/"
                    if load_method == "y":
                        sqoop_load_res = self.utility_obj.sqoop_utility(spark=self.spark, query=query,
                                                                        source_platform=source_platform,
                                                                        connection_details=connection_details,
                                                                        region_name=self.aws_region, df=df,
                                                                        delimiter=each_data[CommonConstants.RDBMS_OUTPUT_FILE_DELIMITER_COL],
                                                                        table_name=each_data[CommonConstants.RDBMS_OBJECT_NAME_COL],
                                                                        hdfs_path=hdfs_path)
                        if sqoop_load_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                            raise Exception(str(sqoop_load_res))

                    save_df_to_s3_res = self.save_to_s3(cycle_id=self.cycle_id, dataframe=df,
                                                        query_file_name=each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL],
                                                        output_file_delimiter=output_file_delimiter,
                                                        start_time=start_time,
                                                        target_location=target_folder_location,
                                                        src_system=each_data[CommonConstants.RDBMS_SRC_SYSTEM_COL],
                                                        source_platform=each_data[CommonConstants.RDBMS_SOURCE_PLATFORM_COL],
                                                        output_file_name=output_file_name,
                                                        query=query,
                                                        query_description=query_description,
                                                        object_name=each_data[CommonConstants.RDBMS_OBJECT_NAME_COL],
                                                        load_type=each_data[CommonConstants.RDBMS_LOAD_TYPE_COL],
                                                        incremental_load_column_name=each_data[
                                                            CommonConstants.RDBMS_INCREMENTAL_COL_NAME_COL],
                                                        curr_max_refresh_timestamp=get_current_max_time,
                                                        prev_max_refresh_timestamp=get_prev_max_time,
                                                        database_name=each_data[CommonConstants.RDBMS_DATABASE_NAME_COL],
                                                        output_file_format=each_data[CommonConstants.RDBMS_OUTPUT_FILE_FORMAT_COL],
                                                        override_flag=override_flag,
                                                        header_flag=header_flag,
                                                        geography_id=each_data[CommonConstants.RDBMS_GEOGRAPHY_ID_COL],
                                                        record_count=record_count,
                                                        load_method=load_method, hdfs_path=hdfs_path)
                    if save_df_to_s3_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                        save_df_to_s3_res_flag = False
                        raise Exception(str(save_df_to_s3_res))

                except Exception as e:
                    error = "Failed while getting the data from database table. " \
                            "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
                    response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                    response[CommonConstants.METHOD_KEY] = "read_from_database"
                    response[CommonConstants.RESULT_KEY] = "Error"
                    response[CommonConstants.ERROR_KEY] = error
                    end_time = str(datetime.datetime.now(timezone.utc).strftime(CommonConstants.RDBMS_END_TIME_FORMAT))
                    if save_df_to_s3_res_flag is True:
                        RDBMSCommonUtilities().rds_execute_query(mysql_host=self.mysql_host,
                                                             mysql_username=self.mysql_username,
                                                             mysql_password=self.mysql_password,
                                                             mysql_database=self.mysql_database,
                                                             cycle_id=self.cycle_id,
                                                             query_file_name=each_data[CommonConstants.RDBMS_QUERY_FILE_NAME_COL],
                                                             status=CommonConstants.RDBMS_STATUS_FAILED_KEY,
                                                             step_name="Failed to extract the data due to {}".format(e),
                                                             start_time=start_time, end_time=end_time,
                                                             target_location=target_folder_location,
                                                             source_platform=each_data[CommonConstants.RDBMS_SOURCE_PLATFORM_COL],
                                                             output_file_name=output_file_name,
                                                             src_system=each_data[CommonConstants.RDBMS_SRC_SYSTEM_COL],
                                                             query=query,
                                                             query_description=query_description,
                                                             object_name=each_data[CommonConstants.RDBMS_OBJECT_NAME_COL],
                                                             load_type=each_data[CommonConstants.RDBMS_LOAD_TYPE_COL],
                                                             incremental_load_column_name=each_data[
                                                                 CommonConstants.RDBMS_INCREMENTAL_COL_NAME_COL],
                                                             curr_max_refresh_timestamp=get_current_max_time,
                                                             prev_max_refresh_timestamp=get_prev_max_time,
                                                             database_name=each_data[CommonConstants.RDBMS_DATABASE_NAME_COL],
                                                             output_file_format=each_data[CommonConstants.RDBMS_OUTPUT_FILE_FORMAT_COL],
                                                             override_flag=override_flag, record_count="",
                                                             geography_id=each_data[CommonConstants.RDBMS_GEOGRAPHY_ID_COL])
                    self.logger.error(response)
                    raise Exception
        except Exception as e:
            error = "Failed while copying the data to s3. The  " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()) +
                                                     '\n' + str(response))
            self.logger.error(error)
            raise Exception

    def save_to_s3(self, cycle_id, dataframe, src_system, database_name, object_name, incremental_load_column_name, 
                   target_location, load_type, output_file_format, output_file_delimiter, query, query_file_name, 
                   query_description, prev_max_refresh_timestamp, curr_max_refresh_timestamp, start_time, override_flag,
                   source_platform, output_file_name, header_flag,
                   geography_id, record_count, load_method, hdfs_path, date_format=None, timestamp_format=None):
        """
        Purpose: Function for saving the files to S3
        Input: cycle_id, start_time, data_frame, target_location, file_name
        """
        response = dict()
        try:
            self.logger.info("Saving the data to s3 ")
            file_name_to_save = output_file_name
            step_name = "file does not save to s3 due to no updated data records have been found"
            status = CommonConstants.RDBMS_STATUS_SKIPPED_KEY
            if header_flag.lower() == "y":
                header = 'true'
            else:
                header = 'false'
            if record_count:
                self.logger.info("record count is non zero")
                step_name = "file successfully saved to s3"
                status = CommonConstants.RDBMS_STATUS_COMPLETE_KEY
                if load_method == "y":
                    self.logger.info("inside sqoop save to s3")
                    df_read_hdfs = self.spark.sql("select * from {} limit 10".format(object_name))
                    self.logger.info(f"Hive result is {df_read_hdfs}")

                    df_read_hdfs.coalesce(1).write.mode('overwrite').format(output_file_format.lower()).option('header',
                                                                                                               header).option(
                        'delimiter', output_file_delimiter).save(target_location + "temp/")
                    file_list = os.popen("aws s3 ls " + target_location + "temp/").read().replace("\n", "").split(" ")
                    for filename in file_list:
                        if filename.endswith(output_file_format.lower()):
                            os.system(
                                "aws s3 mv " + target_location + "temp/" + filename + " " + target_location + file_name_to_save)
                            os.system("aws s3 rm " + target_location + "temp/" + " --recursive")
                    os.system("hdfs dfs -rm -r " + hdfs_path)
                else:
                    self.logger.info("inside spark save to s3")
                    self.logger.info(f"The s3 target location is {target_location}")
                    dataframe.coalesce(1).write.mode('overwrite').format(output_file_format.lower()).option('header',
                                                                                                            header).option(
                        'delimiter', output_file_delimiter).save(target_location + "temp/")
                    file_list = os.popen("aws s3 ls " + target_location + "temp/").read().replace("\n", "").split(" ")
                    self.logger.info("The s3 file_list is : {0}".format(file_list),
                                     )
                    for filename in file_list:
                        if filename.endswith(output_file_format.lower()):
                            os.system(
                                "aws s3 mv " + target_location + "temp/" + filename + " " + target_location + file_name_to_save)
                            os.system("aws s3 rm " + target_location + "temp/" + " --recursive")
            end_time = str(datetime.datetime.now(timezone.utc).strftime(CommonConstants.RDBMS_END_TIME_FORMAT))
            self.logger.info("The target file name is '{}'".format(target_location))
            RDBMSCommonUtilities().rds_execute_query(mysql_host=self.mysql_host, mysql_username=self.mysql_username,
                                                     mysql_password=self.mysql_password,
                                                     mysql_database=self.mysql_database,
                                                     cycle_id=self.cycle_id,
                                                     query_file_name=query_file_name,
                                                     status=status,
                                                     step_name=step_name,
                                                     start_time=start_time, end_time=end_time,
                                                     target_location=target_location,
                                                     src_system=src_system,
                                                     source_platform=source_platform,
                                                     output_file_name=output_file_name,
                                                     query=query,
                                                     query_description=query_description,
                                                     object_name=object_name, load_type=load_type,
                                                     incremental_load_column_name=incremental_load_column_name,
                                                     curr_max_refresh_timestamp=curr_max_refresh_timestamp,
                                                     prev_max_refresh_timestamp=prev_max_refresh_timestamp,
                                                     database_name=database_name,
                                                     output_file_format=output_file_format, override_flag=override_flag,
                                                     record_count=record_count, geography_id=geography_id)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            return response
        except Exception as e:
            error = "Failed while saving the data on S3. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "save_to_s3"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            end_time = str(datetime.datetime.now(timezone.utc).strftime(CommonConstants.RDBMS_END_TIME_FORMAT))
            RDBMSCommonUtilities().rds_execute_query(mysql_host=self.mysql_host, mysql_username=self.mysql_username,
                                                     mysql_password=self.mysql_password,
                                                     mysql_database=self.mysql_database,
                                                     cycle_id=cycle_id,
                                                     query_file_name=query_file_name,
                                                     status=CommonConstants.RDBMS_STATUS_FAILED_KEY,
                                                     step_name="error while saving the fiile in s3 " + str(e),
                                                     start_time=start_time, end_time=end_time,
                                                     target_location=target_location,
                                                     source_platform=source_platform,
                                                     output_file_name=output_file_name,
                                                     src_system=src_system,
                                                     query=query,
                                                     query_description=query_description,
                                                     object_name=object_name, load_type=load_type,
                                                     incremental_load_column_name=incremental_load_column_name,
                                                     curr_max_refresh_timestamp=curr_max_refresh_timestamp,
                                                     prev_max_refresh_timestamp=prev_max_refresh_timestamp,
                                                     database_name=database_name,
                                                     output_file_format=output_file_format, override_flag=override_flag,
                                                     record_count="", geography_id=geography_id)
            return response

class RDBMSCommonUtilities(object):

    def __init__(self):
        """
        Purpose: *Utility for creating the connection for oracle and teradata through spark
                 *Utility for create log entry in RDS
                 *Utility for mysql
        Input: Conneection parameters
        Output: Connection object
        """
        self.logger = self.get_logger()

    def decode_password(self, password, region_name):
        """
        
        :param password: 
        :param region_name: 
        :return: 
        """
        response = dict()
        try:
            # Create a Secrets Manager client
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
            self.logger.info("Fetching the details for the secret name %s", password,
                             )
            get_secret_value_response = client.get_secret_value(
                SecretId=password
            )
            self.logger.info(
                "Fetched the Encrypted Secret from Secrets Manager for %s", password,
            )
        except Exception as e:
            error = "Failed while getting the password from secret manager. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "decode_password"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                self.logger.info("Decrypted the Secret")
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                self.logger.info("Decrypted the Secret")
            get_secret = json.loads(secret)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = get_secret[CommonConstants.RDBMS_SECRET_PASSWORD_TAG_KEY]
            return get_secret[RDBMS_SECRET_PASSWORD_TAG_KEY]

    def file_reader(self, directory_path, file_name):
        """
        Purpose:   To read the query from local EMR and make a list
        Input: Path of directory contains the query
        Output: Query list
        """
        response = dict()
        try:
            directory_path = "/" + directory_path.strip("/") + "/"
            if "s3" in file_name:
                read_s3_path = file_name.split("/")
                read_file_name = read_s3_path[-1]
                os.system("aws s3 cp " + file_name + " " + directory_path)
                file_name = read_file_name
            with open(directory_path + file_name, 'r') as stream:
                query_data = stream.read()
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = query_data
            return response

        except Exception as e:
            error = "Failed while reading the query file. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            # self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "file_reader"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def mysql_utilty(self, mysql_host, mysql_username, mysql_password, mysql_database,
                     query, dict_flag, insert_update_key):
        """
        
        :param mysql_host: 
        :param mysql_username: 
        :param mysql_password: 
        :param mysql_database: 
        :param query: 
        :param dict_flag: 
        :param insert_update_key: 
        :return: 
        """
        response = dict()
        try:
            mysql_client = pymysql.connect(host=mysql_host, user=mysql_username,
                                           password=mysql_password, db=mysql_database)
            cursor = mysql_client.cursor()
            if insert_update_key.lower() == "y":
                query_ouput = cursor.execute(query)
                mysql_client.commit()
                return query_ouput
            cursor.execute(query)
            resp = cursor.fetchall()
            desc = cursor.description
            final_list = []
            if resp is not None:
                for each_version in resp:
                    temp_dict = dict()
                    for index, item in enumerate(desc):
                        if isinstance(each_version[index], bytes):
                            temp_dict[item[0].lower()] = each_version[index].decode()
                        else:
                            temp_dict[item[0].lower()] = each_version[index]
                    final_list.append(temp_dict)
            if dict_flag.lower() == "y":
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = temp_dict
                return response
            else:
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = final_list
                return response
        except Exception as e:
            error = "Failed while executing the query '{query}' " \
                    "ERROR is: {error}".format(query=query, error=str(e) + '\n' + str(traceback.format_exc()))
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "mysql_utilty"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            raise e

    def rds_execute_query(self, mysql_host, mysql_username, mysql_password, mysql_database,
                          cycle_id, source_platform, src_system, status, step_name, database_name, object_name,
                          incremental_load_column_name, target_location, load_type, output_file_name,
                          output_file_format, query, query_file_name, query_description, prev_max_refresh_timestamp,
                          curr_max_refresh_timestamp, start_time, end_time, override_flag, record_count, geography_id):
        """
        Purpose: Function for creating the log entry in RDS
        Input: Connection parameters
        Output: Connection object
        """
        response = dict()
        try:
            table = CommonConstants.RDBMS_LOG_TABLE_NAME
            current_user = getpass.getuser()
            curr_max_refresh_timestamp = json.dumps(curr_max_refresh_timestamp)
            prev_max_refresh_timestamp = json.dumps(prev_max_refresh_timestamp)
            query = 'INSERT INTO `' + table + '` (`cycle_id`,`geography_id`, `source_platform`, `src_system`, `status`, ' \
                                              '`comments`, `database_name`, `object_name`, `incremental_load_column_name`, ' \
                                              '`target_location`, `output_file_name`, `output_file_format`, `load_type`, `query`, ' \
                                              '`query_file_name`,`query_description`, `prev_max_refresh_timestamp`, ' \
                                              '`curr_max_refresh_timestamp`,`load_start_time`,`load_end_time`, `override_flag`,' \
                                              '`record_count`,`insert_by`,`update_by`,`insert_date`,`update_date`) ' \
                                              'VALUES (' + '"' + str(cycle_id) + '"' + ', ' + '"' + str(
                geography_id) + '"' + ' , ' + '"' + str(
                source_platform) + '"' + ' , ' + '"' + str(
                src_system) + '"' + ' , ' + '"' + str(status) + '"' + ' , ' + '"' + str(
                step_name.replace('"',"'")) + '"' + ' , ' + '"' + str(database_name) + '"' + ' , ' + '"' + str(
                object_name) + '"' + ' , ' + '"' + str(incremental_load_column_name) + '"' + ' , ' + '"' + str(
                target_location) + '"' + ' , ' + '"' + str(
                output_file_name) + '"' + ' , ' + '"' + str(
                output_file_format) + '"' + ' , ' + '"' + str(
                load_type) + '"' + ' , ' + '"' + str(
                query.replace('"',"'")) + '"' + ' , ' + '"' + str(
                query_file_name) + '"' + ' , ' + '"' + str(
                query_description.replace('"',"'")) + '"' + ' , ' + "'" + prev_max_refresh_timestamp + "'" + ' , ' + str(
"'") + curr_max_refresh_timestamp + "'" + ' , ' + '"' + str(
                start_time) + '"' + ' , ' + '"' + end_time + '"' + ' , ' + '"' + str(
                override_flag) + '"' + ' , ' + '"' + str(
                record_count) + '"' + ' , ' + '"' + str(
                current_user) + '"' + ' , ' + '"' + str(
                current_user) + '"' + ' , ' + "NOW()" + ' , ' + "NOW()" + ');'
            self.mysql_utilty(mysql_host=mysql_host, mysql_username=mysql_username,
                              mysql_password=mysql_password, mysql_database=mysql_database,
                              query=query, insert_update_key="y", dict_flag="N")

            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            return response
        except Exception as e:
            error = "Failed while inserting the log record in log table. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            # self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "rds_execute_query"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def get_spark_dataframe(self, spark, query, region_name, source_platform, connection_details):
        """
        Purpose: Function for creating the connection for oracle through spark
        Input: Connection parameters
        Output: Connection object
        """
        response = dict()
        try:
            self.logger.info(f"The connection_details are : {connection_details}")
            connection_details = json.loads(connection_details)
            if source_platform.lower() == CommonConstants.RDBMS_ORACLE_KEY:
                host = connection_details[CommonConstants.RDBMS_ORACLE_HOST_KEY]
                port = connection_details[CommonConstants.RDBMS_ORACLE_PORT_KEY]
                database_name = connection_details[CommonConstants.RDBMS_ORACLE_DATABASE_NAME_KEY]
                username = connection_details[CommonConstants.RDBMS_ORACLE_USERNAME_KEY]
                password = self.decode_password(password=connection_details[CommonConstants.RDBMS_ORACLE_PASSWORD_KEY],
                                                region_name=region_name)
                _query_ = query.replace(';', '')
                self.logger.info("Executing the oracle query : {0}".format(_query_))
                database_df = spark.read.format("jdbc").options(
                    url="jdbc:oracle:thin:@" + host + ":" + port + "/" + database_name,
                    user=username, dsn="dns_nm",
                    password=password,
                    driver=CommonConstants.RDBMS_ORACLE_JDBC_DRIVER, query=_query_).load()
                self.logger.info("The query on Oracle has been executed using spark JDBC successfully")
            elif source_platform.lower() == CommonConstants.RDBMS_MYSQL_KEY.lower() or \
                    source_platform.lower() == CommonConstants.RDBMS_RDS_KEY.lower():
                host = connection_details[CommonConstants.RDBMS_MYSQL_HOST_KEY]
                port = connection_details[CommonConstants.RDBMS_MYSQL_PORT_KEY]
                database_name = connection_details[CommonConstants.RDBMS_MYSQL_DATABASE_NAME_KEY]
                username = connection_details[CommonConstants.RDBMS_MYSQL_USERNAME_KEY]
                password = self.decode_password(password=connection_details[CommonConstants.RDBMS_MYSQL_PASSWORD_KEY],
                                                region_name=region_name)
                self.logger.info(f"rds host is  : {host}")
                _query_ = "(" + query.replace(';', '') + ") abc"
                jdbc_url = "jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}".format(
                    jdbcHostname=host,
                    jdbcPort=port,
                    jdbcDatabase=database_name
                )
                driver = CommonConstants.RDBMS_MYSQL_JDBC_DRIVER
                self.logger.info("Executing the mysql query :" + str(_query_))
                database_df = spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("driver", driver) \
                    .option("dbtable", _query_) \
                    .option("user", username) \
                    .option("password", password) \
                    .load()
                self.logger.info("The query on RDS has been executed using spark JDBC successfully")
            elif source_platform.lower() == CommonConstants.RDBMS_TERADATA_KEY.lower():
                host = connection_details[CommonConstants.RDBMS_TERADATA_HOST_KEY]
                database_name = connection_details[CommonConstants.RDBMS_TERADATA_DATABASE_NAME_KEY]
                username = connection_details[CommonConstants.RDBMS_TERADATA_USERNAME_KEY]
                password = self.decode_password(password=connection_details[CommonConstants.RDBMS_TERADATA_PASSWORD_KEY],
                                                region_name=region_name)
                _query_ = "(" + query.replace(';', '') + ") abc"
                self.logger.info("Executing the teradata query : {0}".format(_query_),
                                 )
                jdbc_url = "jdbc:teradata://{jdbcHostname}/DATABASE={jdbcDatabase},LOGMECH=LDAP".format(
                    jdbcHostname=host,
                    jdbcDatabase=database_name
                )
                self.logger.info("The teradata jdbc url is {0}".format(jdbc_url))
                driver = CommonConstants.RDBMS_TERADATA_JDBC_DRIVER
                database_df = spark.read \
                    .format('jdbc') \
                    .option('driver', driver) \
                    .option('url', jdbc_url) \
                    .option('dbtable', _query_) \
                    .option('user', username) \
                    .option('password', password) \
                    .load()
                self.logger.info("The query on Teradata has been executed using spark JDBC successfully")
            else:
                raise Exception("This utility supports only 'oracle', 'mysql', 'rds', 'teradata' databases "
                                "and the provided database is '{database_type}'".format(
                    database_type=source_platform))

            load_method = CommonConstants.SQOOP_ENABLE_FLAG.lower()
            table_dtype = database_df.dtypes

            dtype_dict = dict()
            for each_value in table_dtype:
                temp_dict = dict()
                temp_dict[each_value[0]] = each_value[1]
                dtype_dict.update(temp_dict)
            for column_name, data_type in dtype_dict.items():
                if data_type.lower() == "decimal(38,10)":
                    database_df = database_df.withColumn(column_name, database_df[column_name].cast('Decimal(38,6)'))
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = database_df
            return response
        except Exception as e:
            error = "Failed while getting the data from database table. The query is: {query}" \
                    "ERROR is: {error}".format(query=query, error=str(e) + '\n' + str(traceback.format_exc()))
            # self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "spark_oracle"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def sqoop_utility(self, spark, query, source_platform, connection_details, region_name, delimiter, df, 
                      table_name, hdfs_path):
        """
        Purpose: Function for creating the connection for oracle through spark
        Input: Connection parameters
        Output: Connection object
        """
        response = dict()
        try:
            self.logger.info("inside the sqoop utility")
            if "where" in query or "WHERE" in query:
                query = query + " and \$CONDITIONS"
            else:
                query = query + " where \$CONDITIONS"
            if source_platform.lower() == CommonConstants.RDBMS_ORACLE_KEY.lower():
                host = connection_details[CommonConstants.RDBMS_ORACLE_HOST_KEY]
                port = connection_details[CommonConstants.RDBMS_ORACLE_PORT_KEY]
                database_name = connection_details[CommonConstants.RDBMS_ORACLE_DATABASE_NAME_KEY]
                username = connection_details[CommonConstants.RDBMS_ORACLE_USERNAME_KEY]
                password = self.decode_password(password=connection_details[CommonConstants.RDBMS_ORACLE_PASSWORD_KEY], region_name=region_name)
                url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + database_name
                os.environ['HADOOP_CLASSPATH'] = CommonConstants.RDBMS_ORACLE_JDBC_JAR_FILE_PATH
                os.system("export HADOOP_CLASSPATH=$HADOOP_CLASSPATH")
                sqoop_command = r"""sqoop import  --libjars {jdbc_jar} 
                --driver {driver} --connect {url} --username {username} 
                --password {password}  --query "{query} " --target-dir '{hdfs_path}' --hive-drop-import-delims 
                -m 1 --fields-terminated-by "{delimiter}" --null-string '\\N' --null-non-string '\\N' --map-column-java  """
                sqoop_command_formated = sqoop_command.format(url=url, username=username, password=password, query=query,
                                                              hdfs_path=hdfs_path, driver=CommonConstants.RDBMS_ORACLE_JDBC_DRIVER,
                                                              jdbc_jar=CommonConstants.RDBMS_ORACLE_JDBC_JAR_FILE_PATH,
                                                              delimiter=delimiter)

            elif source_platform.lower() == CommonConstants.RDBMS_MYSQL_KEY.lower() or source_platform.lower() == CommonConstants.RDBMS_RDS_KEY.lower():
                connection_details = json.loads(connection_details)
                host = connection_details[CommonConstants.RDBMS_MYSQL_HOST_KEY]
                port = connection_details[CommonConstants.RDBMS_MYSQL_PORT_KEY]
                database_name = connection_details[CommonConstants.RDBMS_MYSQL_DATABASE_NAME_KEY]
                username = connection_details[CommonConstants.RDBMS_MYSQL_USERNAME_KEY]
                password = self.decode_password(password=connection_details[CommonConstants.RDBMS_MYSQL_PASSWORD_KEY], region_name=region_name)
                os.environ['HADOOP_CLASSPATH'] = CommonConstants.RDBMS_MYSQL_JDBC_JAR_FILE_PATH
                os.system("export HADOOP_CLASSPATH=$HADOOP_CLASSPATH")
                url = "jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}".format(
                    jdbcHostname=host,
                    jdbcPort=port,
                    jdbcDatabase=database_name)
                sqoop_command = r"""sqoop import  --libjars {jdbc_jar} 
                --driver {driver} --connect {url} --username {username} --password {password}  
                --query "{query} " --target-dir '{hdfs_path}' --hive-drop-import-delims 
                -m 1 --fields-terminated-by "{delimiter}" --null-string '\\N' --null-non-string '\\N' --map-column-java  """
                sqoop_command_formated = sqoop_command.format(url=url, username=username, password=password, query=query,
                                                              hdfs_path=hdfs_path,
                                                              jdbc_jar=CommonConstants.RDBMS_MYSQL_JDBC_JAR_FILE_PATH,
                                                              driver=CommonConstants.RDBMS_MYSQL_JDBC_DRIVER,
                                                              delimiter=delimiter)
            elif source_platform.lower() == CommonConstants.RDBMS_TERADATA_KEY.lower():
                host = connection_details[CommonConstants.RDBMS_TERADATA_HOST_KEY]
                database_name = connection_details[CommonConstants.RDBMS_TERADATA_DATABASE_NAME_KEY]
                username = connection_details[CommonConstants.RDBMS_TERADATA_USERNAME_KEY]
                password = self.decode_password(password=connection_details[CommonConstants.RDBMS_TERADATA_PASSWORD_KEY],
                                                region_name=region_name)
                url = "jdbc:teradata://{jdbcHostname}/DATABASE={jdbcDatabase},LOGMECH=LDAP".format(
                    jdbcHostname=host,
                    jdbcDatabase=database_name
                )
                os.environ['HADOOP_CLASSPATH'] = CommonConstants.RDBMS_TERADATA_JDBC_JAR_FILE_PATH
                os.system("export HADOOP_CLASSPATH=$HADOOP_CLASSPATH")
                sqoop_command = r"""sqoop import  --libjars {jdbc_jar} 
                --driver {driver} --connect {url} --username {username} --password {password}  
                --query "{query} " --target-dir '{hdfs_path}' --hive-drop-import-delims 
                -m 1 --fields-terminated-by "{delimiter}" --null-string '\\N' --null-non-string '\\N' --map-column-java  """
                sqoop_command_formated = sqoop_command.format(url=url, username=username, password=password, query=query,
                                                              hdfs_path=hdfs_path,
                                                              jdbc_jar=CommonConstants.RDBMS_TERADATA_JDBC_JAR_FILE_PATH,
                                                              driver=CommonConstants.RDBMS_TERADATA_JDBC_DRIVER,
                                                              delimiter=delimiter)

            object_schema = df.schema
            types = [f.dataType for f in df.schema.fields]
            columns = (','.join([field.simpleString() for field in object_schema])).replace(':', ' ')
            columns_map_java = (','.join([field.simpleString() for field in object_schema])).replace(':', '=')
            for each_data_type in types:
                lower_data_value = str(each_data_type).lower()
                split_data = str(each_data_type).lower().split(",")
                if "decimal" in lower_data_value:
                    check_value = split_data[0].split("(")
                    changed_type = "decimal(" + check_value[1] + "," + split_data[1]
                    if "0" in split_data[1]:
                        if int(check_value[1]) > 8:
                            columns = columns.replace(changed_type, "Biginteger")
                            columns_map_java = columns_map_java.replace(changed_type, "String")
                        else:
                            columns = columns.replace(changed_type, "Integer")
                            columns_map_java = columns_map_java.replace(changed_type, "String")
                    else:
                        columns = columns.replace(changed_type, changed_type)
                        columns_map_java = columns_map_java.replace(changed_type, "String")
            columns_map_java = columns_map_java.replace("timestamp", "String").replace("string", "String")

            sqoop_command_run = sqoop_command_formated + columns_map_java
            self.logger.info(f"The sqoop command is : {sqoop_command_run}")
            process = subprocess.Popen(sqoop_command_run, shell=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            out, err = process.communicate()

            spark.sql("drop table if exists {table_name}".format(table_name=table_name))
            ddl = """
                            CREATE EXTERNAL TABLE {table_name} ({columns})
                            ROW FORMAT DELIMITED
                              FIELDS TERMINATED BY '|'
                            STORED AS INPUTFORMAT
                              'org.apache.hadoop.mapred.TextInputFormat'
                            OUTPUTFORMAT
                              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                            LOCATION
                              '{location}'
                            """
            ddl_to_run = ddl.format(table_name=table_name, columns=columns, location=hdfs_path)
            spark.sql(ddl_to_run)
            self.logger.info("ddl creation done")
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            #            response[CommonConstants.RESULT_KEY] = database_df
            return response

        except Exception as e:
            error = "Failed while getting the data from database table. The query is: {query}" \
                    "ERROR is: {error}".format(query=query, error=str(e) + '\n' + str(traceback.format_exc()))
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "sqoop_utility"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def get_logger(self):
        """
        Purpose: To create the self.logger file
        Input: logging parameters
        Output: self.logger
        """
        log_file_name = CommonConstants.RDBMS_LOG_FILE_NAME
        log_level = CommonConstants.RDBMS_LOG_LEVEL
        time_format = CommonConstants.RDBMS_LOG_DATETIME_FORMAT
        handler_type = CommonConstants.RDBMS_LOG_FILE_HANDLER
        max_bytes = CommonConstants.RDBMS_LOG_MAX_BYTES
        backup_count = CommonConstants.RDBMS_LOG_BACKUP_COUNTS
        log_file_path = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.RDBMS_LOG_RELATIVE_PATH_DIRECTORY)
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


