#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

"""
Doc_Type        :   Tech Products
Tech Description:   This utility is used to perform load from s3 to RedShift
Pre_requisites  :   This utility requires RedShiftUtilityConstants.py which contains the constants and
                    DatabaseUtility.py to access the database
Inputs          :   environment, cdl_run_identifier, cdl_frequency, service_name, business_unit_code, db_host, db_port,
                    db_name, db_username, db_password, access_key, secret_key
Ouputs          :   Dictionary containing the status, result output and error output (if any)
Example         :
                    Note(s)-
                    1. Access key and secret key will not be used to perform loads when IAM roles have been provided
                    2. This utility cannot be triggered stand-alone
Config_file     :   NA
"""

# The usage sting to be displayed to the user for the utility
USAGE_STRING = """
SYNOPSIS
    python RedshiftUtility.py -f/--conf_file_path <conf_file_path> -c/--conf <conf>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string

        Note: Either 'conf_file_path' or 'conf' should be provided.

"""

# Module level imports
import traceback
import os
import psycopg2
import datetime
import getopt
import sys
import json
import configparser

# Custom module imports
import RedShiftUtilityConstants
import logging
# from LogSetup import get_logger
from DatabaseUtility import DatabaseUtility
# from datetime import datetime


# Module level constants
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"

EMPTY = ""
WITHDRAW_QUERY = "DELETE FROM " + RedShiftUtilityConstants.TABLE_NAME_PARAMETER + " WHERE " + \
                 RedShiftUtilityConstants.PRIMARY_KEY_PARAMETER + "= '" + \
                 RedShiftUtilityConstants.PRIMARY_KEY_VALUE_PARAMETER + "'"
SET_TIMEOUT_QUERY = "set statement_timeout to " + RedShiftUtilityConstants.TIMEOUT_PARAMETER
BEGIN_TRANSACTION_QUERY = "BEGIN TRANSACTION"
COMMIT_TRANSACTION_QUERY = "COMMIT TRANSACTION"
END_TRANSACTION_QUERY = "END TRANSACTION"
VACUUM_QUERY = "VACUUM " + RedShiftUtilityConstants.TABLE_NAME_PARAMETER + " TO " + \
               RedShiftUtilityConstants.VACUUM_THRESHOLD_PARAMETER + " PERCENT"

# logger = get_logger()

file_hosted_path = os.path.abspath(os.path.dirname(sys.argv[0]))
config = configparser.RawConfigParser()
config.read(os.path.join(file_hosted_path, "RedshiftLoadUtility.conf"))
log_file = config.get('LOG', 'path').format(datetime.datetime.now().strftime("%Y%m%d"))
log_format = config.get('LOG', 'formatter')

logger = logging.getLogger('RedshiftUtility')
logger.setLevel(logging.DEBUG)
hndlr = logging.FileHandler(os.path.join(file_hosted_path, log_file))
formatter = logging.Formatter(log_format)
hndlr.setFormatter(formatter)
logger.addHandler(hndlr)

FAILED_STATUS = {RedShiftUtilityConstants.STATUS_KEY: RedShiftUtilityConstants.STATUS_FAILED,
                 RedShiftUtilityConstants.ERROR_KEY: "",
                 RedShiftUtilityConstants.RESULT_KEY: ""}
SUCCESS_STATUS = {RedShiftUtilityConstants.STATUS_KEY: RedShiftUtilityConstants.STATUS_SUCCESS,
                  RedShiftUtilityConstants.ERROR_KEY: "",
                  RedShiftUtilityConstants.RESULT_KEY: {}}

ROW_COUNT_QUERY = "SELECT COUNT(*) FROM " + RedShiftUtilityConstants.SCHEMA_PARAMETER + "." + \
                  RedShiftUtilityConstants.TABLE_NAME_PARAMETER

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


class RedShiftUtility(object):
    """
    This class contains all the functions related to loading data from S3 to RedShift
    """

    def __init__(self):
        self.primary_key = None
        self.primary_key_value = None
        self.port = None
        self.host = None
        self.user = None
        self.password = None
        self.db = None
        self.iam_role = None
        self.operation = None
        self.schema = None
        self.table_format = None
        self.table_delimiter = None
        self.source_s3_location = None
        self.withdraw_flag = None
        self.timeout = None
        self.vacuum_threshold = None
        self.target_table = None
        self.db_utility_object = DatabaseUtility()
        self.retry_limit = int(RedShiftUtilityConstants.DEFAULT_RETRY_VALUE)

    @staticmethod
    def close_connection(redshift_connection):
        """
        Purpose     :   This function will close the Redshift connection
        Input       :   redshift_connection
        Output      :   None
        """
        try:
            status_message = "Starting function to close the connection"
            logger.info(status_message)
            if not redshift_connection:
                status_message = "No Redshift connection provided for close operation"
                logger.error(status_message)
                raise Exception(status_message)
            redshift_connection.close()
            status_message = "Connection closed successfully"
            logger.info(status_message)
        except Exception as err:
            status_message = "Error closing Redshift DB connection " + str(err) + str(traceback.format_exc())
            logger.error(status_message)
            raise err

    def get_row_count(self, target_table_name_with_schema, redshift_connection, db_host, db_username, db_password,
                      db_port,
                      db_name):
        """
        Purpose     :   This function will return the number of entries (rows) in the given table
        Input       :   target_table_name, redshift_connection, db_host, db_username, db_password, db_port, db_name
        Output      :   Row count (integer value)
        """
        try:
            return_status = {}
            count_query = "SELECT COUNT(*) FROM " + target_table_name_with_schema
            count_result_json = self._execute_query(count_query, redshift_connection, db_host, db_username, db_password,
                                                    db_port, db_name)
            logger.info("Count result json for table " + str(target_table_name_with_schema) + " is " +
                        str(count_result_json))
            if count_result_json[RedShiftUtilityConstants.STATUS_KEY] != RedShiftUtilityConstants.STATUS_FAILED:
                return_status[RedShiftUtilityConstants.STATUS_KEY] = RedShiftUtilityConstants.STATUS_SUCCESS
                return_status[RedShiftUtilityConstants.RESULT_KEY] = \
                    int(count_result_json[RedShiftUtilityConstants.RESULT_KEY][0][RedShiftUtilityConstants.COUNT_KEY])
                return_status[RedShiftUtilityConstants.ERROR_KEY] = ''
            else:
                return_status[RedShiftUtilityConstants.STATUS_KEY] = RedShiftUtilityConstants.STATUS_FAILED
                return_status[RedShiftUtilityConstants.RESULT_KEY] = 0
                return_status[RedShiftUtilityConstants.ERROR_KEY] = count_result_json[
                    RedShiftUtilityConstants.ERROR_KEY]

            return return_status
        except Exception as err:
            status_message = "Error getting row count " + str(err) + str(traceback.format_exc())
            logger.error(status_message)
            raise err

    @staticmethod
    def replace_query_parameters(query, param_values):
        """
        Purpose     :   This function will replace the parameters from the queries
        Input       :   Query, param_values(json)
        Output      :   Query with parameters replaced with their actual values
        """
        try:
            status_message = "Starting replacing parameters from query"
            logger.info(status_message)
            status_message = "Query obtained: " + query
            logger.debug(status_message)
            logger.debug(str(param_values))
            for param in param_values.keys():
                status_message = "Replacing parameter: " + param + " with " + str(param_values[param])
                logger.debug(status_message)
                if param in query:
                    query = query.replace(param, param_values[param])
            if "$$" in query:
                status_message = "Unable to replace all parameters"
                logger.error(status_message)
                raise Exception(status_message)
            return True, query
        except Exception as err:
            status_message = "Error replacing parameter values " + str(err) + str(traceback.format_exc())
            logger.error(status_message)
            raise err

    def set_time_out(self, redshift_connection, db_host, db_username, db_password, db_port, db_name, time_out):
        """
        Purpose     :   This function will set the timeout for all RedShift operations
        Input       :   redshift_connection, db_host, db_username, db_password, db_port, db_name, time_out
        Output      :   Boolean value signifying whether the timeout was set and corresponding status message
        """
        try:
            # Setting Timeout for Redshift Query
            RedShiftUtilityConstants.PARAMS_DICT[RedShiftUtilityConstants.TIMEOUT_PARAMETER] = time_out
            status_message = "Setting the timeout for all operations as " + time_out + "(ms)"
            logger.debug(status_message)
            replacement_status, timeout_query = self.replace_query_parameters(
                query=SET_TIMEOUT_QUERY, param_values=RedShiftUtilityConstants.PARAMS_DICT)
            if not replacement_status:
                return False, timeout_query
            timeout_query_status = self._execute_query(query=timeout_query, redshift_connection=redshift_connection,
                                                       db_host=db_host, db_username=db_username,
                                                       db_password=db_password,
                                                       db_port=db_port, db_name=db_name)
            if timeout_query_status[RedShiftUtilityConstants.STATUS_KEY] == RedShiftUtilityConstants.STATUS_FAILED:
                status_message = "Failed to set timeout for queries. ERROR - " + \
                                 str(timeout_query_status[RedShiftUtilityConstants.ERROR_KEY])
                logger.error(status_message)
                return False, status_message
            status_message = "Timeout has been set successfully"
            return True, status_message
        except Exception as err:
            status_message = "Error setting timeout for redshift command " + str(err) + str(traceback.format_exc())
            logger.error(status_message)
            raise err

    def execute_redshift_operation(self, input_json):
        status_message = "Starting function to execute redshift copy command "
        logger.info(status_message)
        try:
            # ##########################Validate json and set class variables###########################################
            self.validate_redshift_conn_details(input_json)
            self.validate_redshift_operation_details(input_json)

            # ####################################Create Redshift connection########################################## #
            redshift_conn = self.create_connection(self.host, self.port, self.user, self.password, self.db)
            if not redshift_conn:
                status_message = "Could not obtain redshift connection object"
                logger.error(status_message)
                raise Exception(status_message)

            if str(self.operation).upper() == 'LOAD':
                rs_cpy_cmd = self.construct_redshift_copy_cmd()
                retry_limit = input_json[RedShiftUtilityConstants.REDSHIFT_RETRY_LIMIT_KEY]
                if not retry_limit:
                    retry_limit = self.retry_limit
                if rs_cpy_cmd:
                    return self._execute_copy_command(rs_cpy_cmd, redshift_conn, self.host,
                                                      self.user, self.password,
                                                      self.port, self.db, self.source_s3_location,
                                                      int(self.retry_limit), self.schema, self.target_table,
                                                      self.timeout)

                else:
                    status_message = "Error in generating copy command "
                    logger.error(status_message)
                    raise Exception(status_message)
            elif str(self.operation).upper() == 'WITHDRAW':
                if RedShiftUtilityConstants.PRIMARY_KEY in input_json:
                    self.primary_key = input_json[RedShiftUtilityConstants.PRIMARY_KEY]
                    if not self.primary_key:
                        status_message = RedShiftUtilityConstants.RS_PRIMARY_KEY_NOT_PROVIDED
                        logger.error(status_message)
                        raise Exception(status_message)
                else:
                    status_message = "Primary Key parameter is missing in input json"
                    logger.error(status_message)
                    raise Exception(status_message)

                if RedShiftUtilityConstants.PRIMARY_KEY_VALUE in input_json:
                    self.primary_key_value = input_json[RedShiftUtilityConstants.PRIMARY_KEY_VALUE]
                    if not self.primary_key_value:
                        status_message = RedShiftUtilityConstants.RS_PRIMARY_KEY_VALUE_NOT_PROVIDED
                        logger.error(status_message)
                        raise Exception(status_message)
                else:
                    status_message = "Primary Key Value parameter is missing in input json"
                    logger.error(status_message)
                    raise Exception(status_message)
                retry_limit = input_json[RedShiftUtilityConstants.REDSHIFT_RETRY_LIMIT_KEY]
                if not retry_limit:
                    retry_limit = self.retry_limit

                rs_withdraw_cmd = self.construct_withdraw_query()
                if rs_withdraw_cmd:
                    return self.trigger_withdraw(rs_withdraw_cmd, redshift_conn,
                                                 int(retry_limit), self.host, self.user,
                                                 self.password, self.port, self.db, self.schema, self.target_table,
                                                 self.timeout)
                else:
                    status_message = "Error in constructing withdraw query"
                    logger.error(status_message)
                    raise Exception(status_message)
            elif str(self.operation) == 'VACCUUM':
                self.trigger_red_shift_vacuum(redshift_conn, self.retry_limit, self.host, self.user,
                                              self.password, self.port, self.db, self.timeout)
            elif str(self.operation) == 'TRUNCATE':
                self.trigger_red_shift_truncate(redshift_conn, self.host, self.user, self.password, self.port, self.db,
                                                self.schema, self.target_table)
            return True
        except Exception as err:
            error = "Error in redshift operation in utility file - " + str(err) + str(traceback.format_exc())
            logger.error(error)
            FAILED_STATUS[RedShiftUtilityConstants.ERROR_KEY] = error
            raise err

    def validate_redshift_conn_details(self, input_json):
        """
        Purpose     :   This function will validate the connection details in JSON passed as input and
                        set up all the class variables
        Input       :   input_json
        Output      :
        """
        status_message = "Starting function to validate the JSON structure"
        logger.info(status_message)
        try:
            if RedShiftUtilityConstants.REDSHIFT_HOST_KEY in input_json:
                self.host = input_json[RedShiftUtilityConstants.REDSHIFT_HOST_KEY]
                if not self.host:
                    status_message = RedShiftUtilityConstants.RS_HOST_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_HOST_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)

            if RedShiftUtilityConstants.REDSHIFT_PORT_KEY in input_json:
                self.port = input_json[RedShiftUtilityConstants.REDSHIFT_PORT_KEY]
                if not self.port:
                    status_message = RedShiftUtilityConstants.RS_PORT_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_PORT_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)

            if RedShiftUtilityConstants.REDSHIFT_USER_KEY in input_json:
                self.user = input_json[RedShiftUtilityConstants.REDSHIFT_USER_KEY]
                if not self.user:
                    status_message = RedShiftUtilityConstants.RS_USER_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_USER_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)

            if RedShiftUtilityConstants.REDSHIFT_PASSWORD_KEY in input_json:
                self.password = input_json[RedShiftUtilityConstants.REDSHIFT_PASSWORD_KEY]
            else:
                try:
                    configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                                   CommonConstants.ENVIRONMENT_CONFIG_FILE))
                    secret_password = MySQLConnectionManager().get_secret(
                        configuration.get_configuration(
                            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "redshift_password_secret_name"]),
                        configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
                    self.password = secret_password['password']
                except:
                    status_message = RedShiftUtilityConstants.RS_PASSWORD_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)

            if RedShiftUtilityConstants.REDSHIFT_DB_KEY in input_json:
                self.db = input_json[RedShiftUtilityConstants.REDSHIFT_DB_KEY]
                if not self.db:
                    status_message = RedShiftUtilityConstants.RS_DB_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_DB_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)

            if RedShiftUtilityConstants.REDSHIFT_IAM_KEY in input_json:
                self.iam_role = input_json[RedShiftUtilityConstants.REDSHIFT_IAM_KEY]
                if not self.iam_role:
                    status_message = RedShiftUtilityConstants.RS_IAM_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_IAM_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)
        except Exception as ex:
            error = "Error while executing Red Shift Utility. ERROR: " + str(ex) + str(traceback.format_exc())
            logger.error(error)
            raise ex

    def validate_redshift_operation_details(self, input_json):
        """
        Purpose     :   This function will validate the connection details in JSON passed as input and
                        set up all the class variables
        Input       :   input_json
        Output      :
        """
        status_message = "Starting function to validate redshift operation details"
        logger.info(status_message)
        try:
            # ##############################Validation for operation type################################ #
            if RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY in input_json:
                self.operation = input_json[RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY]
                if not self.operation:
                    status_message = RedShiftUtilityConstants.RS_OPERATION_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = "Redshift operation is not provided"
                logger.error(status_message)
                raise Exception(status_message)
            if str(self.operation).upper() not in ['LOAD', 'WITHDRAW', 'VACCUUM', 'TRUNCATE']:
                status_message = RedShiftUtilityConstants.RS_INVALID_OPERATION
                logger.error(status_message)
                raise Exception(status_message)

            # #############################Validation for redshift Schema################################## #
            if RedShiftUtilityConstants.REDSHIFT_TARGET_SCHEMA_KEY in input_json:
                self.schema = input_json[RedShiftUtilityConstants.REDSHIFT_TARGET_SCHEMA_KEY]
                if not self.schema:
                    status_message = RedShiftUtilityConstants.RS_SCHEMA_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_SCHEMA_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)

            # #####################Validation for redshift target table################################## #
            if RedShiftUtilityConstants.REDSHIFT_TARGET_TABLE_KEY in input_json:
                self.target_table = input_json[RedShiftUtilityConstants.REDSHIFT_TARGET_TABLE_KEY]
                if not self.target_table:
                    status_message = RedShiftUtilityConstants.RS_TARGET_TBL_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_TARGET_TBL_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)

            if str(self.operation).upper() in ['LOAD']:
                if RedShiftUtilityConstants.REDSHIFT_TABLE_FORMAT_KEY in input_json:
                    self.table_format = input_json[RedShiftUtilityConstants.REDSHIFT_TABLE_FORMAT_KEY]
                    if not self.table_format:
                        status_message = RedShiftUtilityConstants.RS_TBL_FORMAT_NOT_PROVIDED
                        logger.error(status_message)
                        raise Exception(status_message)
                else:
                    status_message = RedShiftUtilityConstants.RS_TBL_FORMAT_KEY_NOT_IN_DICT
                    logger.error(status_message)
                    raise Exception(status_message)

                if RedShiftUtilityConstants.REDSHIFT_TABLE_DELIMITER_KEY in input_json:
                    self.table_delimiter = input_json[RedShiftUtilityConstants.REDSHIFT_TABLE_DELIMITER_KEY]
                    if not self.table_delimiter:
                        status_message = RedShiftUtilityConstants.RS_TBL_DELIMITER_NOT_PROVIDED
                        logger.error(status_message)
                        raise Exception(status_message)
                else:
                    status_message = RedShiftUtilityConstants.RS_TBL_DELIMITER_KEY_NOT_IN_DICT
                    logger.error(status_message)
                    raise Exception(status_message)

            if str(self.operation).upper() in ['LOAD']:
                if RedShiftUtilityConstants.REDSHIFT_WITHDRAW_FLAG_KEY in input_json:
                    self.withdraw_flag = input_json[RedShiftUtilityConstants.REDSHIFT_WITHDRAW_FLAG_KEY]
                    if not self.withdraw_flag:
                        self.withdraw_flag = 'N'
                else:
                    self.withdraw_flag = 'N'

                if RedShiftUtilityConstants.REDSHIFT_SOURCE_S3_KEY in input_json:
                    self.source_s3_location = input_json[RedShiftUtilityConstants.REDSHIFT_SOURCE_S3_KEY]
                    if not self.source_s3_location:
                        status_message = RedShiftUtilityConstants.RS_SOURCE_S3_NOT_PROVIDED
                        logger.error(status_message)
                        raise Exception(status_message)
                else:
                    status_message = RedShiftUtilityConstants.RS_SOURCE_S3_KEY_NOT_IN_DICT
                    logger.error(status_message)
                    raise Exception(status_message)

            if RedShiftUtilityConstants.REDSHIFT_TIMEOUT_KEY in input_json:
                self.timeout = input_json[RedShiftUtilityConstants.REDSHIFT_TIMEOUT_KEY]
                if not self.timeout:
                    status_message = RedShiftUtilityConstants.RS_TIMEOUT_NOT_PROVIDED
                    logger.error(status_message)
                    raise Exception(status_message)
            else:
                status_message = RedShiftUtilityConstants.RS_TIMEOUT_KEY_NOT_IN_DICT
                logger.error(status_message)
                raise Exception(status_message)

            if str(self.operation).upper() in ['LOAD', 'WITHDRAW']:
                if RedShiftUtilityConstants.REDSHIFT_RETRY_LIMIT_KEY in input_json:
                    self.retry_limit = input_json[RedShiftUtilityConstants.REDSHIFT_RETRY_LIMIT_KEY]
                    if not self.retry_limit:
                        status_message = RedShiftUtilityConstants.RS_RETRY_LIMIT_NOT_PROVIDED
                        logger.error(status_message)
                        raise Exception(status_message)
                else:
                    status_message = RedShiftUtilityConstants.RS_RETRY_LIMIT_KEY_NOT_IN_DICT
                    logger.error(status_message)
                    raise Exception(status_message)

            if str(self.operation).upper() in ['VACCUUM']:
                if RedShiftUtilityConstants.VACUUM_THRESHOLD_KEY in input_json:
                    self.vacuum_threshold = input_json[RedShiftUtilityConstants.VACUUM_THRESHOLD_KEY]
                    if not self.vacuum_threshold:
                        status_message = RedShiftUtilityConstants.RS_VACCUUM_THRESHOLD_NOT_PROVIDED
                        logger.error(status_message)
                        raise Exception(status_message)
                else:
                    status_message = RedShiftUtilityConstants.RS_VACCUUM_THRESHOLD_KEY_NOT_IN_DICT
                    logger.error(status_message)
                    raise Exception(status_message)
        except Exception as ex:
            error = "Error while executing validation of redshift operation details" \
                    ". ERROR: " + str(ex) + str(traceback.format_exc())
            logger.error(error)
            raise ex

    def construct_redshift_copy_cmd(self):
        status_message = "Starting function to construct redshift copy command "
        logger.info(status_message)
        try:
            table_complete_name = str(self.schema).__add__(".").__add__(self.target_table)
            file_load_query = "COPY " + str(table_complete_name) + " from '" + str(self.source_s3_location) + "' "
            if self.iam_role is not None:
                status_message = "Authenticating using IAM roles"
                logger.debug(status_message)
                file_load_query = file_load_query + RedShiftUtilityConstants.IAM_ROLE_KEY + " '" + str(self.iam_role) +\
                                                                                            "' "

            else:
                status_message = "IAM ROLE is needed for redshift copy operation"
                logger.debug(status_message)
                raise Exception(status_message)

            # file_load_query = file_load_query + RedShiftUtilityConstants.DELIMITER_KEY + " '" +
            #  str(self.table_delimiter) + "' "

            file_load_query = file_load_query + RedShiftUtilityConstants.FORMAT_KEY_CMD + " " + str(self.table_format)

            # append parameters for ignoring headers and blank lines to the command
            # file_load_query = file_load_query + " " + RedShiftUtilityConstants.IGNORE_BLANK_LINES_PARAMETER
            # if self.skip_header_count is None:
            # self.skip_header_count = 0
            # file_load_query = file_load_query + " " + RedShiftUtilityConstants.IGNORE_HEADER_PARAMETER + " as " + \
            # str(self.skip_header_count)
            status_message = "Query to load data has been obtained - " + str(file_load_query)
            logger.debug(status_message)
            return file_load_query
        except Exception as err:
            error = "Error - " + str(err) + str(traceback.format_exc())
            logger.error(error)
            raise err

    @staticmethod
    def create_connection(host, port, username, password, db_name):
        """
        Purpose     :   This function is used to create a connection
        Input       :   host, port, username, password, db_name
        Output      :   None
        """
        try:
            status_message = "Starting function to create the connection to Redshift DB"
            logger.info(status_message)
            redshift_connection = psycopg2.connect(host=host, port=port, user=username, password=password,
                                                   dbname=db_name)
            status_message = "Connection to the database has been created"
            logger.info(status_message)
            return redshift_connection
        except Exception as err:
            error = "Error - " + str(err) + str(traceback.format_exc())
            logger.error(error)
            raise err

    def _execute_query(self, query, redshift_connection, db_host, db_username, db_password, db_port, db_name,
                       auto_commit=True):
        """
        Purpose     :   This function will execute the query passed as input
        Input       :   query, redshift_connection, db_host, db_username, db_password, db_port, db_name, auto_commit
        Output      :   Execution status of the query
        """
        logger.debug("Executing query - " + str(query))
        return self.db_utility_object.execute(
            query, conn=redshift_connection, host=db_host, username=db_username, password=db_password, port=db_port,
            database_name=db_name, auto_commit=auto_commit)

    def _execute_copy_command(self, copy_command, redshift_connection, db_host, db_username, db_password, db_port,
                              db_name, source_s3_location, retry_limit, schema, table, timeout):
        """
        Purpose     :   This function will execute and check if load onto redshift has been performed successfully or
                        not
        Input       :   copy_command, redshift_connection, db_host, db_username, db_password, db_port, db_name,
                        source_s3_location, retry_limit
        Output      :   Boolean value indicating whether load was performed successfully and status message for load
        """

        try:
            initial_row_count = 0
            final_row_count = 0
            status_message = "Starting function to execute the redshift load command"
            logger.info(status_message)
            retry_count = 0
            if not schema or not table:
                status_message = "Missing schema or table name"
                logger.error(status_message)
                raise Exception(status_message)

            complete_tbl_name = str(schema).__add__('.'+table)
            return_status = self.get_row_count(complete_tbl_name, redshift_connection, db_host, db_username,
                                               db_password, db_port,
                                               db_name)
            logger.info(str(return_status))
            initial_row_count = return_status[RedShiftUtilityConstants.RESULT_KEY]
            logger.debug("Retry Limit provided for Load operation is "+str(retry_limit))
            if not timeout:
                status_message = "Timeout parameter is not provided in environment json file"
                logger.debug(status_message)
                raise Exception(status_message)
            time_out_status, status_message = self.set_time_out(
                redshift_connection, db_host, db_username, db_password, db_port, db_name, str(timeout))
            if not time_out_status:
                raise Exception(status_message)
            while retry_count < retry_limit:
                retry_count += 1
                status_message = "Trying to load data to redshift. Attempt: " + str(retry_count)
                logger.debug(status_message)
                redshift_load_status = self._execute_query(copy_command, redshift_connection, db_host, db_username,
                                                           db_password, db_port, db_name)
                if redshift_load_status[RedShiftUtilityConstants.STATUS_KEY] == RedShiftUtilityConstants.STATUS_FAILED:
                    status_message = "Failed to load data to redshift. Error: " + \
                                     redshift_load_status[RedShiftUtilityConstants.ERROR_KEY]
                    logger.error(status_message)
                    if retry_count >= retry_limit:
                        status_message = "Retry limit exceeded. " + status_message
                        logger.debug(status_message)
                        raise Exception(status_message)
                    else:
                        continue
                status_message = "Redshift load has been completed. Checking if load was correct."
                return_status = self.get_row_count(complete_tbl_name, redshift_connection, db_host, db_username,
                                                   db_password, db_port,
                                                   db_name)
                final_row_count = return_status[RedShiftUtilityConstants.RESULT_KEY]
                logger.debug(status_message)
                query = "SELECT QUERY, TRIM(FILENAME), CURTIME, STATUS FROM STL_LOAD_COMMITS WHERE FILENAME LIKE '" + \
                        source_s3_location + "%' ORDER BY CURTIME DESC LIMIT 1"
                redshift_commit_status = self._execute_query(query, redshift_connection, db_host, db_username,
                                                             db_password,
                                                             db_port, db_name)
                if redshift_commit_status['status'] != RedShiftUtilityConstants.STATUS_FAILED:
                    if redshift_commit_status['result'][0]['status'] \
                            == str(1):
                        status_message = "Redshift load has been completed and committed successfully"
                        logger.debug(status_message)
                        return final_row_count - initial_row_count
                    else:
                        status_message = "Load has been completed. Unable to check if load was committed successfully."
                        logger.debug(status_message)
                        return final_row_count - initial_row_count
                else:
                    status_message = "Error executing query to check the commit status of the load"
                    logger.error(status_message)
        except Exception as ex:
            error = "Error while executing Red Shift Utility. ERROR: " + str(ex) + str(traceback.format_exc())
            logger.error(error)
            raise ex

    def construct_withdraw_query(self):
        status_message = "Starting function to construct redshift copy command "
        logger.info(status_message)
        param_values_dict = {}
        try:
            redshift_withdraw_query = WITHDRAW_QUERY
            param_values_dict[RedShiftUtilityConstants.TABLE_NAME_PARAMETER] = str(self.schema) + '.' + \
                                                                                                  str(self.target_table)
            param_values_dict[RedShiftUtilityConstants.PRIMARY_KEY_PARAMETER] = self.primary_key
            param_values_dict[RedShiftUtilityConstants.PRIMARY_KEY_VALUE_PARAMETER] = self.primary_key_value
            replacement_status, withdraw_query = self.replace_query_parameters(query=redshift_withdraw_query,
                                                                               param_values=param_values_dict)
            logger.info("Withdraw query --> " + str(withdraw_query))
            return withdraw_query
        except Exception as err:
            error = "Error - " + str(err) + str(traceback.format_exc())
            logger.error(error)
            raise err

    def trigger_withdraw(self, query, redshift_connection, retry_limit, db_host, db_username, db_password, db_port,
                         db_name, schema, table, timeout, auto_commit=True):
        """
        Purpose     :   This function will trigger the data withdrawal from RedShift
        Input       :   query, redshift_connection, retry_limit, db_host, db_username, db_password, db_port, db_name
                        auto_commit (boolean)
        Output      :   boolean value signifying whether the withdraw was successful or not and corresponding
                        status message
        """
        try:
            logger.debug("Setting timeout for withdraw operation")
            if not timeout:
                status_message = "Timeout parameter is not provided in environment json file"
                logger.debug(status_message)
                raise Exception(status_message)
            time_out_status, status_message = self.set_time_out(
                redshift_connection, db_host, db_username, db_password, db_port, db_name, str(timeout))
            if not time_out_status:
                raise Exception(status_message)
            logger.debug("Timeout is set for withdraw operation : "+str(timeout))
            status_message = "Starting function to withdraw the data " + query
            logger.info(status_message)
            retry_count = 0
            initial_row_count = 0
            final_row_count = 0
            # trigger withdraw and use retries if required
            complete_tbl_name = str(schema).__add__('.'+table)
            return_status = self.get_row_count(complete_tbl_name, redshift_connection, db_host, db_username,
                                               db_password, db_port,
                                               db_name)
            initial_row_count = return_status[RedShiftUtilityConstants.RESULT_KEY]
            while retry_count <= retry_limit:
                retry_count += 1
                status_message = "Attempt: " + str(retry_count)
                logger.debug(status_message)
                load_query_status = self._execute_query(query=query, redshift_connection=redshift_connection,
                                                        db_host=db_host, db_username=db_username,
                                                        db_password=db_password,
                                                        db_port=db_port, db_name=db_name, auto_commit=auto_commit)
                if load_query_status[RedShiftUtilityConstants.STATUS_KEY] == RedShiftUtilityConstants.STATUS_FAILED:
                    # timeout error has been defined in the constants as a specific response which is received when
                    # the query gets timedout
                    if load_query_status[RedShiftUtilityConstants.ERROR_KEY].strip().lower() == \
                            RedShiftUtilityConstants.TIME_OUT_ERROR:
                        status_message = "Error - Query failed due to timeout"
                        logger.error(status_message)
                        raise Exception(status_message)

                    if retry_count >= retry_limit:
                        status_message = "Retry limit exceeded. ERROR - " + status_message
                        logger.debug(status_message)
                        raise Exception(status_message)
                else:
                    status_message = "Data withdrawal has been completed successfully"
                    return_status = self.get_row_count(complete_tbl_name, redshift_connection, db_host, db_username,
                                                       db_password, db_port,
                                                       db_name)
                    final_row_count = return_status[RedShiftUtilityConstants.RESULT_KEY]
                    return initial_row_count - final_row_count
        except Exception as err:
            status_message = "Error in redshift withdrawal utility"
            error = str(status_message) + str(traceback.format_exc())
            logger.error(error)
            raise err

    def trigger_red_shift_vacuum(self, redshift_connection, retry_limit, db_host, db_username, db_password, db_port,
                                 db_name, timeout, auto_commit=True):
        """
        Purpose     :   This function will be accessed by the RedShiftWrapper to perform vacuum operation on
                        RedShift tables
        Input       :   vacuum_configurations (JSON)
        Output      :   returns JSON depending on whether vacuum was performed successfully or not
        """

        redshift_connection = None
        param_dict_values = {}
        try:
            # create redshift connection
            redshift_connection = self.create_connection(
                host=db_host, port=db_port, username=db_username, password=db_password, db_name=db_name)
            # set time out for the redshift task
            time_out_status, status_message = self.set_time_out(
                redshift_connection, db_host, db_username, db_password, db_port, db_name, str(timeout))
            if not time_out_status:
                raise Exception(status_message)
            logger.debug(status_message)
            param_dict_values[RedShiftUtilityConstants.TABLE_NAME_PARAMETER] = str(self.schema).__add__('.'). \
                __add__(self.target_table)
            param_dict_values[RedShiftUtilityConstants.VACUUM_THRESHOLD_PARAMETER] = str(self.vacuum_threshold)
            vacuum_query_status, vacuum_query = self.replace_query_parameters(
                VACUUM_QUERY, param_dict_values)
            status_message = "Starting vacuum operation"
            logger.info(status_message)
            vacuum_status = self._execute_query(
                query=vacuum_query, redshift_connection=redshift_connection, db_host=db_host, db_username=db_username,
                db_password=db_password, db_port=db_port, db_name=db_name, auto_commit=True)
            if vacuum_status[RedShiftUtilityConstants.STATUS_KEY] == RedShiftUtilityConstants.STATUS_FAILED:
                status_message = "Failed to execute the vacuum operation. Error - " + \
                                 str(vacuum_status[RedShiftUtilityConstants.ERROR_KEY])
                logger.error(status_message)
                raise Exception(status_message)
            status_message = "Vacuum operation completed successfully"
            logger.debug(status_message)
            end_time_stamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            result_json = {
                RedShiftUtilityConstants.MESSAGE_KEY: status_message,
                RedShiftUtilityConstants.END_TIME_STAMP_KEY: end_time_stamp
            }
            SUCCESS_STATUS[RedShiftUtilityConstants.RESULT_KEY] = result_json
            self.close_connection(redshift_connection=redshift_connection)
            return SUCCESS_STATUS

        except Exception as err:
            end_time_stamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            result_json = {
                RedShiftUtilityConstants.END_TIME_STAMP_KEY: end_time_stamp
            }
            FAILED_STATUS[RedShiftUtilityConstants.RESULT_KEY] = result_json
            self.close_connection(redshift_connection=redshift_connection)
            FAILED_STATUS[RedShiftUtilityConstants.ERROR_KEY] = str(err)
            return FAILED_STATUS

    def trigger_red_shift_truncate(self, redshift_connection, db_host, db_username, db_password, db_port,
                                   db_name, schema, table_name):
        """
        Purpose     :   This function will be accessed by the RedShiftWrapper to perform vacuum operation on
                        RedShift tables
        Input       :   vacuum_configurations (JSON)
        Output      :   returns JSON depending on whether vacuum was performed successfully or not
        """
        try:
            status_message = "Starting truncate operation"
            logger.info(status_message)
            truncate_query = '''truncate {}.{}'''.format(schema, table_name)
            truncate_status = self._execute_query(
                query=truncate_query, redshift_connection=redshift_connection, db_host=db_host,
                db_username=db_username,
                db_password=db_password, db_port=db_port, db_name=db_name, auto_commit=True)
            if truncate_status[RedShiftUtilityConstants.STATUS_KEY] == RedShiftUtilityConstants.STATUS_FAILED:
                status_message = "Failed to execute the truncate operation. Error - " + \
                                 str(truncate_status[RedShiftUtilityConstants.ERROR_KEY])
                logger.error(status_message)
                raise Exception(status_message)
            status_message = "Truncate operation completed successfully"
            logger.debug(status_message)
            end_time_stamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            result_json = {
                RedShiftUtilityConstants.MESSAGE_KEY: status_message,
                RedShiftUtilityConstants.END_TIME_STAMP_KEY: end_time_stamp
            }
            SUCCESS_STATUS[RedShiftUtilityConstants.RESULT_KEY] = result_json
            self.close_connection(redshift_connection=redshift_connection)
            return SUCCESS_STATUS

        except Exception as err:
            end_time_stamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            result_json = {
                RedShiftUtilityConstants.END_TIME_STAMP_KEY: end_time_stamp
            }
            FAILED_STATUS[RedShiftUtilityConstants.RESULT_KEY] = result_json
            self.close_connection(redshift_connection=redshift_connection)
            FAILED_STATUS[RedShiftUtilityConstants.ERROR_KEY] = str(err)
            return FAILED_STATUS


# Print the usage for the Database Utility
def usage(status=1):
    sys.stdout.write(USAGE_STRING)
    sys.exit(status)

if __name__ == '__main__':
    # Setup basic logger info
    logger.basicConfig(level=logger.DEBUG)

    conf_file_path = None
    conf = None
    opts = None
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "f:c",
            ["conf_file_path=", "conf=",
             "help"])
    except Exception as e:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: " + str(e)}))
        usage(1)

    # Parse the input arguments
    for option, arg in opts:
        if option in ("-h", "--help"):
            usage(1)
        elif option in ("-f", "--conf_file_path"):
            conf_file_path = arg
        elif option in ("-c", "--conf"):
            conf = arg

    # Check for all the mandatory arguments
    if conf_file_path is None and conf is None:
        sys.stderr.write(json.dumps({
            STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: Either JSON configuration file path "
                                                  "or JSON configuration string should be provided\n"}))
        usage(1)

    try:
        # Parse the configuration
        if conf_file_path:
            with open(conf_file_path) as conf_file:
                rs_conf = json.load(conf_file)
        else:
            rs_conf = json.loads(conf)

    except Exception as e:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError while parsing configuration."
                                                                           " ERROR: " + str(e)}))
        sys.exit(1)

    # Instantiate and call the Database utility
    rs_utility = RedShiftUtility()
    rs_utility.execute_redshift_operation(rs_conf)
