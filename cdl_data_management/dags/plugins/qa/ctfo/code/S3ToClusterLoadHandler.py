#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import os
import traceback
import json
import argparse
import textwrap
import sys
from datetime import datetime
import queue
import boto
from LogSetup import logger
from threading import Thread
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from FileTransferUtility import FileTransferUtility
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager
sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "S3ToClusterLoadHandler"
PROCESS_NAME = "Copy Files from S3 to HDFS"

"""
Module Name         :   S3ToClusterLoadHandler
Purpose             :   This module will read parameters provided and will trigger FileTransferUtility
                        parsing and validating the required parameters
Input Parameters    :   Required Parameters: source_location, target_location or json_path
Output Value        :   Return Dictionary with status(Failed/Success)
Pre-requisites      :   Provide Mandatory Parameters either provide source & target location or JSON Payload
Last changed on     :   8th May 2018
Last changed by     :   Pushpendra Singh
Reason for change   :   S3 to HDFS Copy Module development
"""


class S3ToClusterLoadHandler(object):
    """
    This class Wrapper for copying files/directory from S3 to Hdfs Location
    """

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.env_configs = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.cluster_mode = self.env_configs.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "cluster_mode"])
        self.audit_db = self.env_configs.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.hadoop_configs = self.env_configs.get_configuration([CommonConstants.HADOOP_OPTIONS_KEY])
        self.s3_distcp_configs = self.env_configs.get_configuration([CommonConstants.S3_DISTCP_OPTIONS_KEY])
        self.mysql_connector = MySQLConnectionManager()
        self.json_flag = None
        self.bucket_name = ""
        self.kerberos_keytab_location = None
        self.kerberos_principal = None
        self.kerberos_enabled = None
        self.aws_access_key = None
        self.aws_secret_key = None
        self.s3_conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


    def s3_to_hdfs_copy(self, input_dict, json_flag):
        """
        Purpose   :   This method is used to call data copy utility and also calls other validation and initilization methods before calling utility
        Input     :   Input dictionary and Json flag which specifies if the input_dict to be parsed from JSON payload
        Output    :   Returns dictionary with status(Failed/Success
        """
        if json_flag is True:
            self.json_flag = True
        else:
            self.json_flag = False
        self.bucket_name = ""
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        try:
            status_message = "Executing s3_to_hdfs_copy function with json_flag : " + str(json_flag)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if self.json_flag is True:

                status_message = "Executing s3_to_hdfs_copy function with json payload as input"
                logger.debug(status_message, extra=self.execution_context.get_context())

                self.kerberos_keytab_location = input_dict["kerberos_keytab_location"]
                self.kerberos_principal = input_dict["kerberos_principal"]

                if self.kerberos_keytab_location is not None or self.kerberos_principal is not None:
                    self.kerberos_enabled = "Y"
                else:
                    self.kerberos_enabled = "N"

                if self.kerberos_enabled == "Y":
                    if self.kerberos_keytab_location is None or self.kerberos_keytab_location == "":
                        raise Exception("Keytab Location Is Not Provided")
                    if self.kerberos_principal is None or self.kerberos_principal == "":
                        raise Exception("Principal Is Not Provided")
                    command = "kinit" + " " + str(self.kerberos_principal) + " " + "-k -t" + " " + str(self.kerberos_keytab_location)
                    execution_status = CommonUtils().execute_shell_command(command)
                    if execution_status != True:
                        raise Exception("Failed To Kinit with provided kerberos details")
                else:
                    self.kerberos_keytab_location = None
                    self.kerberos_principal = None

                self.aws_access_key = input_dict["aws_access_key"] if input_dict[
                                                                               "aws_access_key"] is not None else None
                self.aws_secret_key = input_dict["aws_secret_key"] if input_dict[
                                                                               "aws_secret_key"] is not None else None

                status_message = "Starting validate_S3_parameters"
                logger.debug(status_message, extra=self.execution_context.get_context())

                if not self.validate_S3_parameters():
                    status_message = "validate_S3_parameters Failed."
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise Exception

                data_copy_dict = input_dict["data_copy_details"]
                batch_keys = list(data_copy_dict.keys())
                que = queue.Queue()
                failed_flag = False
                for batch_id in batch_keys:
                    threads = []
                    if "parallel_tasks" in data_copy_dict[batch_id]:
                        thread_limit = int(data_copy_dict[batch_id]["parallel_tasks"])
                    else:
                        thread_limit = 1

                    status_message = "For Batch : " + str(batch_id) + " Parallel Thread Limit Is : " + str(
                        thread_limit)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    for process in data_copy_dict[batch_id]:
                        if process != "parallel_tasks":
                            process_name = str(batch_id)+" : "+str(process)
                            source_location = data_copy_dict[batch_id][str(process)]["source_location"]
                            target_location = data_copy_dict[batch_id][str(process)]["target_location"]
                            data_movement_type = data_copy_dict[batch_id][str(process)]["data_movement_type"]

                            if "source_pattern" in data_copy_dict[batch_id][str(process)]:
                                source_pattern = str(data_copy_dict[batch_id][str(process)]["source_pattern"])
                            else:
                                source_pattern = None

                            input_signature = data_copy_dict[batch_id][str(process)]

                            t = Thread(target=lambda q, process_name, source_location, target_location,
                                                       data_movement_type, source_pattern, input_dict, input_signature: q.put(self.call_file_transfer(process_name, source_location, target_location,
                                                       data_movement_type, source_pattern, input_dict, input_signature)), args=(que,process_name, source_location, target_location,
                                                       data_movement_type, source_pattern, input_dict, input_signature))

                            threads.append(t)

                    thread_chunks = [threads[x:x + thread_limit] for x in range(0, len(threads), thread_limit)]
                    for thread in thread_chunks:
                        for t in thread:
                            try:
                                t.start()
                            except Exception as exception:
                                logger.error("Error in Thread Chunks", extra=self.execution_context.get_context())
                                logger.error(str(exception), extra=self.execution_context.get_context())
                                raise exception
                        for t in thread:
                            try:
                                t.join()
                                while not que.empty():
                                    result = que.get()
                                    if result[CommonConstants.STATUS_KEY] ==  CommonConstants.STATUS_FAILED:
                                        failed_flag = True
                            except Exception as exception:
                                logger.error("Error in Thread Chunks join()", extra=self.execution_context.get_context())
                                logger.error(str(exception), extra=self.execution_context.get_context())
                                raise exception
                if failed_flag:
                    result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
                else:
                    result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}

                return result_dictionary

            else:
                process_name = input_dict["process_name"]

                source_pattern = str(input_dict["source_pattern"]) if input_dict["source_pattern"] is not None else None
                self.kerberos_keytab_location = input_dict["kerberos_keytab_location"]
                self.kerberos_principal = input_dict["kerberos_principal"]

                if self.kerberos_keytab_location is not None or self.kerberos_principal is not None:
                    self.kerberos_enabled = "Y"
                else:
                    self.kerberos_enabled = "N"

                if self.kerberos_enabled == "Y":
                    if self.kerberos_keytab_location is None or self.kerberos_keytab_location == "":
                        raise Exception("Keytab Location Is Not Provided")
                    if self.kerberos_principal is None or self.kerberos_principal == "":
                        raise Exception("Principal Is Not Provided")
                    command = "kinit" + " " + str(self.kerberos_principal) + " " + "-k -t" + " " + str(
                        self.kerberos_keytab_location)
                    execution_status = CommonUtils().execute_shell_command(command)
                    if execution_status != True:
                        raise Exception("Failed To Kinit with provided kerberos details")
                else:
                    self.kerberos_keytab_location = None
                    self.kerberos_principal = None

                self.aws_access_key = input_dict["aws_access_key"] if input_dict[
                                                                               "aws_access_key"] is not None else None
                self.aws_secret_key = input_dict["aws_secret_key"] if input_dict[
                                                                               "aws_secret_key"] is not None else None

                status_message = "Starting validate_S3_parameters"
                logger.debug(status_message, extra=self.execution_context.get_context())

                if not self.validate_S3_parameters():
                    status_message = "validate_S3_parameters Failed."
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise Exception

                data_movement_type = input_dict["data_movement_type"]
                source_location = input_dict["source_location"]
                target_location = input_dict["target_location"]

                input_signature = input_dict.copy()
                input_signature["aws_access_key"] = "**restricted**"
                input_signature["aws_secret_key"] = "**restricted**"

                result_dictionary = self.call_file_transfer(process_name, source_location, target_location, data_movement_type, source_pattern, input_dict, input_signature)

            return result_dictionary
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc()) + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
            return result_dictionary

    def call_file_transfer(self, process_name, source_location, target_location, data_movement_type, source_pattern, input_dict, input_signature):
        log_id = -1
        try:
            source_location, target_location = self.initialize_arguments(source_location=source_location,
                                                                         target_location=target_location,
                                                                         input_dict=input_dict)
            copy_start_time = str(datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip())
            insert_query_string = "insert into {db_name}.{log_copy_dtl_table}(process_name,source_location,target_location,input_signature,copy_status,copy_start_time) values('{process_name}','{source_location}','{target_location}','{input_signature}','{copy_status}','{copy_start_time}')".format(
                db_name=self.audit_db, log_copy_dtl_table=CommonConstants.DATA_COPY_LOG_TABLE,
                process_name=process_name, source_location=source_location,
                target_location=target_location, copy_status=CommonConstants.STATUS_RUNNING,
                input_signature=json.dumps(input_signature)[:5000], copy_start_time=copy_start_time)
            log_id = self.mysql_connector.execute_query_mysql(insert_query_string)

            FileTransferUtility().transfer_files(
                source_location=source_location, target_location=target_location,
                data_movement_type=data_movement_type,
                aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                hadoop_configs=self.hadoop_configs, s3_distcp_configs=self.s3_distcp_configs,
                cluster_mode=self.cluster_mode, source_pattern=source_pattern)

            copy_end_time = str(datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip())
            update_query_string = "update {db_name}.{log_copy_dtl_table} set copy_status='{copy_status}' , copy_end_time='{copy_end_time}' where log_id={log_id}".format(
                db_name=self.audit_db, log_copy_dtl_table=CommonConstants.DATA_COPY_LOG_TABLE,
                copy_status=CommonConstants.STATUS_SUCCEEDED, copy_end_time=copy_end_time, log_id=log_id)
            self.mysql_connector.execute_query_mysql(update_query_string)

            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary
        except Exception as exception:
            status_message = "In Exception Block For call_file_transfer with exception: " +str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
            status_message =\
                """
                Possible Reasons For Exception - 
                1) Source location is incorrect
                2) Access to S3 from cluster is restricted
                3) Access or Secret key if provided are incorrect
                4) Kerberos authentication might be required
                """
            logger.error(status_message, extra=self.execution_context.get_context())

            if log_id != -1:
                copy_end_time = str(datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip())
                update_query_string = "update {db_name}.{log_copy_dtl_table} set copy_status='{copy_status}' , copy_end_time='{copy_end_time}' where log_id={log_id}".format(
                    db_name=self.audit_db, log_copy_dtl_table=CommonConstants.DATA_COPY_LOG_TABLE,
                    copy_status=CommonConstants.STATUS_FAILED, copy_end_time=copy_end_time, log_id=log_id)
                self.mysql_connector.execute_query_mysql(update_query_string)

            return result_dictionary
        finally:
            status_message = "In Finally Block For call_file_transfer Handler copy"
            logger.debug(status_message, extra=self.execution_context.get_context())


    def initialize_arguments(self, source_location=None, target_location=None, input_dict=None):
        """
        Purpose   :   This method is used to initialize all the necessary arguments .
        Input     :   Input dictionary containing all the necessary arguments.
        Output    :   Returns True if all the variables successfully initialized.
        """
        method_name = ""
        try:
            method_name = sys._getframe().f_code.co_name
            status_message = "Starting execution of the method %s of %s" % (str(method_name), str(MODULE_NAME))
            logger.debug(status_message, extra=self.execution_context.get_context())

            if not source_location:
                status_message = "Source Location not specified in the json/passed dictionary."
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception

            if source_location.startswith('s3://'):
                source_location = source_location.replace('s3://', 's3a://')

            if not source_location.startswith('s3a://'):
                raise Exception("Source Location Should Be S3 Path and should start with s3a://")


            if not target_location:
                status_message = "Target Location not specified in the json/passed dictionary."
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception

            if not target_location.endswith('/'):
                target_location += '/'

            if target_location.startswith('s3a://') or target_location.startswith('s3://'):
                raise Exception("Target Location Can Not Be S3 path")

            source_location = CommonUtils().replace_key(path=source_location)
            target_location = CommonUtils().replace_key(path=target_location)

            source_location = source_location.replace('$$batch_id', input_dict["batch_id"])
            source_location = source_location.replace('$$file_id', input_dict["file_id"])
            source_location = source_location.replace('$$cycle_id', input_dict["cycle_id"])
            source_location = source_location.replace('$$data_date', input_dict["data_date"])

            return source_location,target_location
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


    def create_s3_connection(self, aws_access_key=None, aws_secret_key=None, key_encoding=None):
        """
        Purpose   :   This method is used to create a s3 connection using aws access key and secret keys.
        Input     :   Aws Access and Secret key, key encoding flag : True (Keys are encoded using base64)
        Output    :   Returns s3 connection object if connection successful else None .
        """

        try:
            status_message = "Started Function to create s3 connection."
            logger.info(status_message, extra=self.execution_context.get_context())

            access_key = aws_access_key if aws_access_key else self.aws_access_key
            secret_key = aws_secret_key if aws_secret_key else self.aws_secret_key


            if access_key is None and secret_key is None:
                status_message = "Access key or secret key not provided,considering connection with role"
                logger.debug(status_message, extra=self.execution_context.get_context())
                s3_conn = boto.connect_s3(host='s3.amazonaws.com')
            else:
                s3_conn = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)

            return s3_conn
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def check_s3_connection(self, s3_connection_object):
        method_name = ""
        try:
            method_name = method_name = sys._getframe().f_code.co_name
            status_message = "starting %s function of the %s" % (str(method_name), str(MODULE_NAME))
            logger.debug(status_message, extra=self.execution_context.get_context())

            if not isinstance(s3_connection_object, boto.s3.connection.S3Connection):
                status_message = "The connection object passed is not a boto s3 connection object."
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception

            status_message = "The passed s3 connection object to %s is valid." % (str(method_name))
            logger.info(status_message, extra=self.execution_context.get_context())

            return True
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(extra=self.execution_context.get_context())
            raise exception


    def validate_S3_parameters(self, aws_access_key=None, aws_secret_key=None):
        """
        Purpose   :   This method is used to validate the necessary parameters :
                      1.Aws access key
                      2.Aws secret key
        Input     :   Aws Access and Secret key
        Output    :   Returns True if all the parameters are validated.
        """
        method_name = ""
        try:
            method_name = sys._getframe().f_code.co_name
            status_message = "Starting the %s function of the %s" % (str(method_name), str(MODULE_NAME))
            logger.info(status_message, extra=self.execution_context.get_context())

            access_key = aws_access_key if aws_access_key else self.aws_access_key
            secret_key = aws_secret_key if aws_secret_key else self.aws_secret_key

            #this key will be used in later enhancement to descrypt keys
            key_encoding = "N"

            if access_key is None and secret_key is None:
                status_message = "Both Access & Secret Key Is None"
                logger.debug(status_message, extra=self.execution_context.get_context())
                self.aws_access_key = None
                self.aws_secret_key = None
            elif access_key == "" and secret_key == "":
                status_message = "Both Access & Secret Key Is Blank"
                logger.debug(status_message, extra=self.execution_context.get_context())
                self.aws_access_key = None
                self.aws_secret_key = None
            elif access_key is None and secret_key == "":
                status_message = "Access key is None & Secret Key Is Blank"
                logger.debug(status_message, extra=self.execution_context.get_context())
                self.aws_access_key = None
                self.aws_secret_key = None
            elif access_key is "" and secret_key is None:
                status_message = "Access is Blank & Secret Key Is None"
                logger.debug(status_message, extra=self.execution_context.get_context())
                self.aws_access_key = None
                self.aws_secret_key = None
            elif access_key is not None and secret_key is not None:
                status_message = "Both Access & Secret Key Provided"
                logger.debug(status_message, extra=self.execution_context.get_context())
            else:
                raise Exception("Both Access & Secret Key Should Be Input")


            status_message = "Calling function to create s3 connection."
            logger.debug(status_message, extra=self.execution_context.get_context())

            s3_conn = self.create_s3_connection(aws_access_key=access_key, aws_secret_key=secret_key,
                                                key_encoding=key_encoding)
            if not s3_conn:
                status_message = "s3 Connection Failed."
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception

            if not self.check_s3_connection(s3_conn):
                status_message = "s3 Connection Object is not right.! Unable to connect to s3."
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception

            status_message = "Successfully Connected to s3."
            logger.info(status_message, extra=self.execution_context.get_context())

            self.s3_conn = s3_conn

            status_message = "Source location validation successful"
            logger.info(status_message, extra=self.execution_context.get_context())

            status_message = "All the parameters successfully validated."
            logger.info(status_message, extra=self.execution_context.get_context())

            return True

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception



    def main(self, input_dict, json_flag):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for S3 To HDFS Copy"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.s3_to_hdfs_copy(input_dict, json_flag)
            status_message = "Completing the main function for S3 to HDFS Copy"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


def check_mandatory_parameters_exists(input_dict):
    """
    Purpose   :   This method is used to check if all the necessary parameters exist in the passed input dictionary .
    Input     :   Input dictionary containing all the necessary arguments.
    Output    :   Returns True if all the variables exist in the passed input dictionary.
    """
    try:
        input_dictionary_keys = list(input_dict.keys())

        flag = True
        if CommonConstants.SOURCE_LOCATION_KEY in input_dictionary_keys:
            if not input_dict[CommonConstants.SOURCE_LOCATION_KEY]:
                flag = False
        else:
            flag = False

        if not flag:
            status_message = "The Source location does not exist in the specified parameters. " + \
                             "Kindly make sure you provide the source location."
            print(status_message)
            exit()

        if CommonConstants.TARGET_LOCATION_KEY in input_dictionary_keys:
            if not input_dict[CommonConstants.TARGET_LOCATION_KEY]:
                flag = False
        else:
            flag = False

        if not flag:
            status_message = "The Target location does not exist in the specified parameters. " \
                             "Kindly make sure you provide the target location."
            print(status_message)
            exit()

        return flag

    except Exception as exception:
        error = "Error in check_mandatory_parameters_exists due to " + str(exception)
        print (error)
        raise exception

def get_commandline_arguments():
    """
    Purpose   :   This method is used to get all the commandline arguments .
    Input     :   Console Input
    Output    :   Dictionary of all the command line arguments.
    """
    parser = argparse.ArgumentParser(prog=MODULE_NAME, usage='%(prog)s [command line parameters]',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=textwrap.dedent('''
    command line parameters :

    -s , --source_location : s3 Source Location from where files are to be copied to hdfs
    -t, --target_location: HDFS target location where files are to be copied.

    OR

    using the option -j : specify the json file path containing the input parameters stored in a json file.

    command : python3 S3ToHdfsLoadHandler.py -s <source_location> -t <target_location>

    The list of command line parameters valid for %(prog)s module.'''),
                                     epilog="The module will exit now!!")

    #List of Mandatory Parameters either provide source,target or provide json_path
    parser.add_argument('-s', '--source_location', required=False,
                        help='Specify the source location using -s/--source_location')
    parser.add_argument('-t', '--target_location', required=False,
                        help='Specify the target location using -t/--target_location')

    parser.add_argument('-j', '--json_path', required=False,
                        help="Specify the json file path containing all the parameters.")


    #Optional Parameters
    #aws access key
    parser.add_argument('-ak', '--aws_access_key', default=None, required=False,
                        help='Specify the access key : -ak/--aws_access_key_id')
    #aws secret key
    parser.add_argument('-sk', '--aws_secret_key', default=None, required=False,
                        help='Specify the secret key : -sk/--aws_secret_access_key')

    #kerberos key tab path
    parser.add_argument('-kt', '--kerberos_keytab_location', required=False, default=None,
                        help="Specify the keytab file path if kerberos enabled")
    # kerberos principal
    parser.add_argument('-kp', '--kerberos_principal', required=False, default=None,
                        help="Specify the principal name if kerberos enabled")

    #data movement type - This is used to create distcp overwrite or append mode
    parser.add_argument('-m', '--data_movement_type', required=False, default=None, choices=['overwrite', 'append'],
                        help="Specify the data movement type(append/overwrite")

    # data movement type - This is used to create distcp overwrite or append mode
    parser.add_argument('-sp', '--source_pattern', required=False, default=None,
                        help="A regular expression that filters the copy operation to a subset of the data at source")

    # need to provide this when utiltiy is wrapped within the process else default name will be data_movement_process(this is used for logging in RDS tabeles)
    parser.add_argument('-p', '--process_name', required=False, default='data_movement_process',
                        help='Specify the process name using -p/--data_movement_process')

    #replacable parameters in source location
    parser.add_argument('-b', '--batch_id', required=False, default="",
                        help="Specify the batch id to replace in sorurce location")
    parser.add_argument('-f', '--file_id', required=False, default="",
                        help="Specify the batch id to replace in sorurce location")
    parser.add_argument('-c', '--cycle_id', required=False, default="",
                        help="Specify the cycle id to replace in sorurce location")
    parser.add_argument('-d', '--data_date', required=False, default="",
                        help="Specify the data date to replace in sorurce location")

    cmd_arguments = vars(parser.parse_args())
    return cmd_arguments


if __name__ == '__main__':
    try:
        commandline_arguments = get_commandline_arguments()
        json_flag = False
        if commandline_arguments.get(CommonConstants.JSON_PATH_KEY) is None:
            if check_mandatory_parameters_exists(commandline_arguments):
                s3_hdfs_load_handler = S3ToClusterLoadHandler()
                result_dict = s3_hdfs_load_handler.main(input_dict=commandline_arguments, json_flag=json_flag)
            else:
                raise Exception("One or more mandatory parameters are missing.")
        else:
            json_flag = True
            path = commandline_arguments.get(CommonConstants.JSON_PATH_KEY)
            if not os.path.exists(path):
                message = "The json path " + str(path) + " does not exist!!"
                raise Exception(message)
            json_dict = json.load(open(path))

            final_dictionary = json_dict.copy()

            final_dictionary["batch_id"] = commandline_arguments.get("batch_id")
            final_dictionary["file_id"] = commandline_arguments.get("file_id")
            final_dictionary["cycle_id"] = commandline_arguments.get("cycle_id")
            final_dictionary["data_date"] = commandline_arguments.get("data_date")

            final_dictionary["aws_access_key"] = commandline_arguments.get("aws_access_key")
            final_dictionary["aws_secret_key"] = commandline_arguments.get("aws_secret_key")
            final_dictionary["kerberos_keytab_location"] = commandline_arguments.get("kerberos_keytab_location")
            final_dictionary["kerberos_principal"] = commandline_arguments.get("kerberos_principal")

            message = "The loaded json contents : " + str(json_dict)
            s3_hdfs_load_handler = S3ToClusterLoadHandler()
            result_dict = s3_hdfs_load_handler.main(input_dict=final_dictionary, json_flag=json_flag)
        STATUS_MSG = "\nCompleted execution for S3 to HDFS Utility with status " + json.dumps(result_dict) + "\n"
        sys.stdout.write(STATUS_MSG)
        if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
            raise Exception("Overall Status For Data Transfer Is Failed, Please Check Detailed Logs")
    except Exception as exception:
        message = "Error occurred in main Error : %s\nTraceback : %s" % (str(exception), str(traceback.format_exc()))
        raise exception
