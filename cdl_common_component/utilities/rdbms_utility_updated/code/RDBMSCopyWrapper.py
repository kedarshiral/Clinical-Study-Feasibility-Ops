#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

######################################################Module Information################################################
#   Module Name         :   RDBMSCopyWrapper
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
from pyspark.sql.session import SparkSession
import traceback
import argparse
from datetime import timezone
from ConfigUtility import JsonConfigUtility
from multiprocessing.pool import ThreadPool
from RDBMSCopyUtility import RDBMSCopyUtility, RDBMSCommonUtilities
import CommonConstants as CommonConstants

MODULE_NAME = "RDBMSCopyWrapper"
utility_obj = RDBMSCommonUtilities()
logger = utility_obj.get_logger()

class RDBMSCopyWrapper(object):
    """

    """
    def __init__(self,):
        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.cycle_id = str(datetime.datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f'))
        self.mysql_database = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.mysql_host = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_host"])
        self.mysql_username = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_username"])
        self.aws_region = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "rdbms_aws_region"])
        self.mysql_port = str(
                self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_port"]))
        self.mysql_database = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.mysql_password = utility_obj.decode_password(
                password=self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "rds_password_secret_name"]),
                region_name=self.aws_region)

    def create_spark_session(self):
        """
        Purpose: To create one spark session.
        :return:
        """
        response = dict()
        try:
            logger.info("Creating a spark session")
            spark = SparkSession.builder.appName(
                CommonConstants.RDBMS_FTP_SPARK_APP_NAME).enableHiveSupport().getOrCreate()
            spark.sparkContext.setLogLevel(CommonConstants.RDBMS_SPARK_LOG_LEVEL)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = spark
            return response
        except Exception as e:
            error = "Error occurred while creating the spark session. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            # logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "create_spark_session"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def check_data_processed(self, data_parameters):
        """

        :param source_file_location:
        :return:
        """
        response = dict()
        try:
            load_type = data_parameters[CommonConstants.RDBMS_LOAD_TYPE_COL].lower()
            query = "select * from {db_name}.{table_name}".format(
                db_name=data_parameters[CommonConstants.RDBMS_DATABASE_NAME_COL],
                table_name=data_parameters[CommonConstants.RDBMS_OBJECT_NAME_COL])
            if load_type == CommonConstants.RDBMS_FULL_LOAD_KEY.lower():
                if data_parameters[CommonConstants.RDBMS_QUERY_COL] != "" and data_parameters[CommonConstants.RDBMS_QUERY_COL] is not None:
                    query = data_parameters[CommonConstants.RDBMS_QUERY_COL]
                    checking_query = "select count(*) as row_count, {target_location_col} from {db}.{log_table_name} where " \
                                     "{src_platform_col} = '{src_platform_val}' and {src_system_col} = '{src_system_val}' " \
                                     "and {database_name_col} = '{database_name_val}' and {object_name_col} = " \
                                     "'{object_name_val}' and {status_col} = '{status_val}' and  " \
                                     "{query_col} = '{query_val}'".format(
                        db=self.mysql_database,
                        log_table_name=CommonConstants.RDBMS_LOG_TABLE_NAME,
                        src_platform_col=CommonConstants.RDBMS_SOURCE_PLATFORM_COL,
                        src_platform_val=data_parameters[CommonConstants.RDBMS_SOURCE_PLATFORM_COL],
                        src_system_col=CommonConstants.RDBMS_SRC_SYSTEM_COL,
                        src_system_val=data_parameters[CommonConstants.RDBMS_SRC_SYSTEM_COL],
                        database_name_col=CommonConstants.RDBMS_DATABASE_NAME_COL,
                        database_name_val=data_parameters[CommonConstants.RDBMS_DATABASE_NAME_COL],
                        object_name_col=CommonConstants.RDBMS_OBJECT_NAME_COL,
                        object_name_val=data_parameters[CommonConstants.RDBMS_OBJECT_NAME_COL],
                        status_col=CommonConstants.RDBMS_STATUS_COL,
                        status_val=CommonConstants.RDBMS_STATUS_COMPLETE_KEY,
                        target_location_col=CommonConstants.RDBMS_TARGET_LOCATION_COL,
                        query_col=CommonConstants.RDBMS_QUERY_COL,
                        query_val=query)
                elif data_parameters[CommonConstants.RDBMS_QUERY_FILE_NAME_COL] != "" and data_parameters[
                    CommonConstants.RDBMS_QUERY_FILE_NAME_COL] is not None:
                    query_res = utility_obj.file_reader(CommonConstants.AIRFLOW_CODE_PATH,
                                                             data_parameters[CommonConstants.RDBMS_QUERY_FILE_NAME_COL])
                    if query_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                        raise Exception(str(query_res))
                    query = query_res[CommonConstants.RESULT_KEY]

                    checking_query = "select count(*) as row_count, {target_location_col} from {db}.{log_table_name} where " \
                                     "{src_platform_col} = '{src_platform_val}' and {src_system_col} = '{src_system_val}' " \
                                     "and {database_name_col} = '{database_name_val}' and {object_name_col} = " \
                                     "'{object_name_val}' and {status_col} = '{status_val}' and  " \
                                     "{query_col} = '{query_val}'".format(
                        db=self.mysql_database,
                        log_table_name=CommonConstants.RDBMS_LOG_TABLE_NAME,
                        src_platform_col=CommonConstants.RDBMS_SOURCE_PLATFORM_COL,
                        src_platform_val=data_parameters[CommonConstants.RDBMS_SOURCE_PLATFORM_COL],
                        src_system_col=CommonConstants.RDBMS_SRC_SYSTEM_COL,
                        src_system_val=data_parameters[CommonConstants.RDBMS_SRC_SYSTEM_COL],
                        database_name_col=CommonConstants.RDBMS_DATABASE_NAME_COL,
                        database_name_val=data_parameters[CommonConstants.RDBMS_DATABASE_NAME_COL],
                        object_name_col=CommonConstants.RDBMS_OBJECT_NAME_COL,
                        object_name_val=data_parameters[CommonConstants.RDBMS_OBJECT_NAME_COL],
                        status_col=CommonConstants.RDBMS_STATUS_COL,
                        status_val=CommonConstants.RDBMS_STATUS_COMPLETE_KEY,
                        target_location_col=CommonConstants.RDBMS_TARGET_LOCATION_COL,
                        query_col=CommonConstants.RDBMS_QUERY_COL,
                        query_val=query)
                else:
                    checking_query = "select count(*) as row_count, {target_location_col} from {db}.{log_table_name} where " \
                                     "{src_platform_col} = '{src_platform_val}' and {src_system_col} = '{src_system_val}' " \
                                     "and {database_name_col} = '{database_name_val}' and {object_name_col} = " \
                                     "'{object_name_val}' and {status_col} = '{status_val}'".format(
                        db=self.mysql_database,
                        log_table_name=CommonConstants.RDBMS_LOG_TABLE_NAME,
                        src_platform_col=CommonConstants.RDBMS_SOURCE_PLATFORM_COL,
                        src_platform_val=data_parameters[CommonConstants.RDBMS_SOURCE_PLATFORM_COL],
                        src_system_col=CommonConstants.RDBMS_SRC_SYSTEM_COL,
                        src_system_val=data_parameters[CommonConstants.RDBMS_SRC_SYSTEM_COL],
                        database_name_col=CommonConstants.RDBMS_DATABASE_NAME_COL,
                        database_name_val=data_parameters[CommonConstants.RDBMS_DATABASE_NAME_COL],
                        object_name_col=CommonConstants.RDBMS_OBJECT_NAME_COL,
                        object_name_val=data_parameters[CommonConstants.RDBMS_OBJECT_NAME_COL],
                        status_col=CommonConstants.RDBMS_STATUS_COL,
                        status_val=CommonConstants.RDBMS_STATUS_COMPLETE_KEY,
                        target_location_col=CommonConstants.RDBMS_TARGET_LOCATION_COL)
                checking_query_res = utility_obj.mysql_utilty(mysql_host=self.mysql_host, mysql_username=self.mysql_username,
                                                      mysql_password=self.mysql_password,
                                                      mysql_database=self.mysql_database,
                                                      query=checking_query, dict_flag="Y", insert_update_key="N")
                if checking_query_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                    raise Exception(checking_query_res)
                checking_row_count = checking_query_res[CommonConstants.RESULT_KEY]
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                if int(checking_row_count['row_count']) > 0:
                    response[CommonConstants.RESULT_KEY] = False
                    logger.info("The data for source_platform '{source_platform}', src_system '{src_system}',"
                                " database '{database}', table '{table}' and query '{query}'"
                                "has been already copied to s3 location '{location}'".format(
                        source_platform=data_parameters[CommonConstants.RDBMS_SOURCE_PLATFORM_COL],
                        src_system=data_parameters[CommonConstants.RDBMS_SRC_SYSTEM_COL],
                        database=data_parameters[CommonConstants.RDBMS_DATABASE_NAME_COL],
                        table=data_parameters[CommonConstants.RDBMS_OBJECT_NAME_COL],
                        query=query,
                        location=checking_row_count[CommonConstants.RDBMS_TARGET_LOCATION_COL]))
                else:
                    response[CommonConstants.RESULT_KEY] = True
            elif load_type == CommonConstants.RDBMS_INCREMENTAL_KEY.lower() or load_type == CommonConstants.RDBMS_DELTA_KEY.lower():

                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = True
            return response

        except Exception as e:
            error = "Error occurred while checking if the data has processed or not. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            # logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "check_data_processed"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            return response

    def main(self, src_system, database_name, table_name, no_of_threads):
        """

        :param src_system:
        :param database_name:
        :param table_name:
        :param no_of_threads:
        :return:
        """
        response = dict()
        start_time = str(datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
        spark = self.create_spark_session()
        if spark[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
            raise Exception(str(spark))
        else:
            spark_session = spark[CommonConstants.RESULT_KEY]
        logger.info("The spark session has been created at time {0}".format(datetime.datetime.utcnow()))
        try:
            query = "select * from {db}.{control_table}".format(
                db=self.mysql_database,
                control_table=CommonConstants.RDBMS_CONTROL_TABLE
            )
            query_to_append = ""
            if src_system is not None:
                src_system_list = src_system.split(",")
                if len(src_system_list) > 1:
                    query = "select * from {db}.{control_table} where src_system in ({src_system})"
                    join_src_system = "'" + "','".join(src_system_list) + "'"
                    query_to_append = query.format(
                        db=self.mysql_database,
                        control_table=CommonConstants.RDBMS_CONTROL_TABLE,
                        src_system=join_src_system.lower())
                else:
                    query_to_append = query + " where src_system = '{0}'".format(src_system.lower())
            if "where" in query_to_append:
                query_to_append = query_to_append + " and lower(active_flag)='{0}'".format(CommonConstants.RDBMS_ACTIVE_FLAG.lower())
            else:
                query_to_append = query_to_append + " where lower(active_flag)='{0}'".format(CommonConstants.RDBMS_ACTIVE_FLAG.lower())
            if database_name is not None:
                database_list = str(database_name).split(",")
                if len(database_list) > 1:
                    raise Exception("Please provide single database name")
                if "where" in query_to_append:
                    query_to_append = query_to_append + " and database_name='{0}'".format(database_name)
                else:
                    query_to_append = query_to_append + " where database_name='{0}'".format(database_name)
            if table_name is not None:
                table_list = str(table_name).split(",")
                if len(table_list) > 1:
                    raise Exception("Please provide single table name")
                if "where" in query_to_append:
                    query_to_append = query_to_append + " and object_name='{0}'".format(table_name)
                else:
                    query_to_append = query_to_append + " where object_name='{0}'".format(table_name)
            if no_of_threads is None:
                no_of_threads = CommonConstants.RDBMS_THREADS

            if query_to_append != "":
                query = query_to_append

            copy_obj = RDBMSCopyUtility(cycle_id=self.cycle_id,
                                        spark_session=spark_session)
            final_list_res = utility_obj.mysql_utilty(mysql_host=self.mysql_host, mysql_username=self.mysql_username,
                                                  mysql_password=self.mysql_password,
                                                  mysql_database=self.mysql_database,
                                                  query=query, dict_flag="N", insert_update_key="N")
            if final_list_res[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception(final_list_res)
            full_list = final_list_res[CommonConstants.RESULT_KEY]
            final_list = full_list 

            logger.info("The final list is: {0}".format(final_list))
            if final_list:
                pool = ThreadPool(no_of_threads)
                results = pool.map(copy_obj.read_from_database, final_list)
                pool.close()
                pool.join()

        except Exception as e:
            error = "Failed while copying the files from RDBMS to S3'. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.METHOD_KEY] = "main"
            response[CommonConstants.RESULT_KEY] = "Error"
            response[CommonConstants.ERROR_KEY] = error
            raise Exception
        finally:
            spark_session.stop()
            logger.info("The spark session has been stopped at time {0}".format(datetime.datetime.utcnow()))


if __name__ == '__main__':
    try:
        PARSER = argparse.ArgumentParser(description="RDBMS EXTRACTOR")
        PARSER.add_argument("-sc", "--src_system",
                            help="Data Source system is required to know from which data source we have to extract data",
                            required=False)
        PARSER.add_argument("-db", "--database", help="Database name is required", required=False)
        PARSER.add_argument("-tbl", "--table", help="Table name is required", required=False)
        PARSER.add_argument("-th", "--threads", help="No of threads to run", required=False)
        ARGS = vars(PARSER.parse_args())
        try:
            if ARGS['src_system'] is not None:
                src_system = ARGS['src_system']
            else:
                src_system = None
            if ARGS['database'] is not None:
                database_name = ARGS['database']
            else:
                database_name = None
            if ARGS['table'] is not None:
                table_name = ARGS['table']
            else:
                table_name = None
            if ARGS['threads'] is not None:
                no_of_threads = int(ARGS['threads'])
            else:
                no_of_threads = None
        except Exception as e:
            logger.exception("provide the proper args")
            logger.exception(e)
            raise Exception

        obj = RDBMSCopyWrapper()
        obj.main(src_system=src_system, database_name=database_name, table_name=table_name, no_of_threads=no_of_threads)
    except Exception as e:
        logger.exception("Error occured in main method")
        logger.exception(e)
        raise Exception



