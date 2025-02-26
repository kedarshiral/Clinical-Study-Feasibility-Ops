#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

"""
Doc_Type            : Tech Products
Tech Description    : This utility is used for copying/moving files from source location to destination location.
                      This utility is used by modules like File Ingestion etc.
Pre_requisites      : This utility needs MoveCopyConstants.py which contains the constants used by the utility
                      required by JobExecutorUtility.py
Inputs              : action, source_path, source_path_type, target_path, sse_flag(Optional)
Outputs             : Dictionary containing the status, result output and error output (if any)
Example             : {
                        "action":"copy",
                        "source_path":"s3://bucket/cdl/source/staging/product_master/product_master/source.txt",
                        "source_path_type":"s3",
                        "target_path":"cdl/source/staging/product_master/product_master/target.txt",
                        "sse_flag": True
                      }
                      Command to Execute - python MoveCopyUtility.py --json_file_path <input_dictionary_path>
Config_file         : NA
"""

# Library and external modules declaration
import logging
import traceback
import boto
import os
#import awscli.clidriver
from CommonUtils import CommonUtils

# Custom modules declaration
import MoveCopyConstants

class MoveCopy(object):
    """
    Class contains all the functions related to MoveCopyUtility
    """
    def __init__(self):
        pass

    def _check_if_file_or_folder(self, bucket_name, path, acc_key=None, sec_key=None):
        """
        Purpose   :   Checks whether provided S3 path is a file or a folder
        Input     :   access_key, secret_key, bucket_name, path
        Output    :   Boolean value -  if path is file, it returns true else returns false
        """
        path = path.strip()
        s3 = boto.connect_s3(aws_access_key_id=acc_key, aws_secret_access_key=sec_key)
        bucket = s3.get_bucket(bucket_name)
        key = bucket.list(path)
        if path.endswith("/"):
            path = path[0:-1]
        for k in key:
            if k.name == path:
                status_message = "Path is a file - " + path + " in the bucket - " + bucket_name
                logging.debug(status_message)
                return True
            elif k.name.startswith(path + "/"):
                status_message = "Path is a folder - " + path + " in the bucket - " + bucket_name
                logging.debug(status_message)
                return False
        status_message = "Path does not exist - " + path + " in the bucket - " + bucket_name
        logging.debug(status_message)
        raise Exception(status_message)

    def _execute_aws_cli_command(self, action, source_path_type, source_path, target_path, sse_flag):
        """
        Purpose     :   To execute AWS CLI command for move/copy
        Input       :   Action, source path type, source path, target path, sse_flag
        Output      :   Boolean value depending on whether files were moved/copied successfully
        """
        status_message = "Starting method to execute aws cli command for move/copy"
        logging.info(status_message)
        # change source and target path type to s3 from any of its other implementations for AWS CLI
        if source_path_type == MoveCopyConstants.PATH_S3_KEY:
            source_path = source_path[0:2] + source_path[source_path.index("://"):]
        target_path = target_path[0:2] + target_path[target_path.index("://"):]
        # create command based on action - move/copy
        if action == MoveCopyConstants.ACTION_COPY_KEY:
            command_action = "cp"
        else:
            command_action = "mv"
        if source_path_type == MoveCopyConstants.PATH_S3_KEY:
            # Get bucket name from source path
            bucket_name = source_path[source_path.index(":") + 3:source_path.index("/", source_path.index(":") + 3)].\
                strip()
            source_path_prefix = source_path[source_path.index("//") + 2:]
            source_path_prefix = source_path_prefix[source_path_prefix.index("/") + 1:]
            path_is_file = self._check_if_file_or_folder(bucket_name=bucket_name, path=source_path_prefix)
            # if path is folder then move/copy all files
            if not path_is_file:
                append_recursive = " --" + MoveCopyConstants.RECURSIVE_KEY
            else:
                append_recursive = ""
        else:
            # check if the source path exists
            if os.path.exists(source_path):
                # if path is folder then move/copy all files
                if os.path.isdir(source_path):
                    append_recursive = " --" + MoveCopyConstants.RECURSIVE_KEY
                else:
                    append_recursive = ""
            else:
                status_message = "Source path does not exist"
                raise Exception(status_message)
        # if server side encryption has to be used
        if sse_flag:
            append_sse = " --" + MoveCopyConstants.SSE_KEY
        else:
            append_sse = ""
        command = """aws {s3} {action} {source_path} {target_path}{sse}{recursive_flag}""".\
            format(s3=MoveCopyConstants.PATH_S3_KEY, action=command_action, source_path=source_path,
                   target_path=target_path, sse=append_sse, recursive_flag=append_recursive)
        logging.debug("Beginning execution of command - " + command)
        #cli_execution_status = awscli.clidriver.create_clidriver().main(command.split())
        CommonUtils(self.execution_context).execute_shell_command(command)
        status_message = "Command executed"
        logging.debug(status_message)
        status_message = "Returning execution status - " + str(cli_execution_status)
        logging.info(status_message)
        if cli_execution_status == 0:
            return True
        else:
            status_message = "Unable to complete the move/copy job"
            logging.debug(status_message)
            status_message = "Move/copy job failed"
            raise Exception(status_message)

    def _validate_cli_parameters(self, action_input, source_path_type, source_path, target_path, sse_key):
        """
        Purpose     :   To validate the input parameters when move or copy is to be done using AWS CLI
        Input       :   Action, source path type, source path, target path, sse_key
        Output      :   True if all parameters are validated successfully, else false
        """
        if action_input.lower() is None or action_input.lower() not in \
                [MoveCopyConstants.ACTION_COPY_KEY, MoveCopyConstants.ACTION_MOVE_KEY]:
            status_message = "No proper action found"
            raise Exception(status_message)
        if source_path is None or source_path == "":
            status_message = "No proper source path found"
            raise Exception(status_message)
        if target_path is None or target_path == "":
            status_message = "No proper target path found"
            raise Exception(status_message)
        if source_path_type is None or source_path_type == "" or source_path_type not in \
                [MoveCopyConstants.PATH_S3_KEY, MoveCopyConstants.PATH_MOVEIT_KEY]:
            status_message = "Source path type is mandatory and only 'moveit' or 's3' is allowed"
            raise Exception(status_message)
        if source_path_type is MoveCopyConstants.PATH_S3_KEY and not \
                source_path.startswith(MoveCopyConstants.PATH_S3_KEY):
            status_message = "Source path must start with s3 if source path type is s3"
            raise Exception(status_message)
        if source_path_type is MoveCopyConstants.PATH_MOVEIT_KEY and \
                source_path_type.startswith(MoveCopyConstants.PATH_S3_KEY):
            status_message = "Source path cannot start with s3 if source path type is moveIt"
            raise Exception(status_message)
        if sse_key and type(sse_key) != bool:
            status_message = "sse_key must be a boolean value only"
            raise Exception(status_message)
        status_message = "Input parameters validated successfully"
        logging.debug(status_message)
        return True

    def s3_move_copy(self, action, source_path_type, source_path, target_path, sse_flag=True):
        """
        Purpose     :   This function will move/copy files from moveit/s3 to s3
        Input       :   Action, source path type, source path, target path, sse flag
        Output      :   JSON containing the status of the move/copy job
        """
        status_message = "Starting method to move/copy from s3 to s3"
        logging.info(status_message)
        return_status = {}
        try:
            if self._validate_cli_parameters(action.lower(), source_path_type=source_path_type.lower(),
                                             source_path=source_path, target_path=target_path, sse_key=sse_flag):
                status_message = "Source path - " + str(source_path)
                logging.debug(status_message)
                status_message = "Target path - " + str(target_path)
                logging.debug(status_message)
                # Call function to create the aws cli command for move/copy and execute it
                execution_status = self._execute_aws_cli_command(action.lower(), source_path_type.lower(), source_path,
                                                                 target_path, sse_flag)
                # Checking the response received
                if execution_status:
                    # File has been moved/copied successfully
                    status_message = "Move/copy job was completed successfully"
                    logging.debug(status_message)
                    return_status[MoveCopyConstants.STATUS_KEY] = MoveCopyConstants.STATUS_SUCCESS
                    return_status[MoveCopyConstants.RESULT_KEY] = status_message
                    return_status[MoveCopyConstants.ERROR_KEY] = ""
                    return return_status

        except Exception as e:
            error = " Error - " + str(e) + str(traceback.format_exc())
            return_status[MoveCopyConstants.STATUS_KEY] = MoveCopyConstants.STATUS_FAILED
            return_status[MoveCopyConstants.ERROR_KEY] = str(e)
            logging.error(error)
            return return_status

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Python utility to move/copy file from source to destination")
#     parser.add_argument("--" + MoveCopyConstants.JSON_FILE_PATH_KEY,
#                         help="json file path containing file arguments", required=True)
#     args = vars(parser.parse_args())
#     file_path = args[MoveCopyConstants.JSON_FILE_PATH_KEY]
#     # open json file and read key values
#     with open(file_path) as file_object:
#         data = json.load(file_object)
#         action_i = data[MoveCopyConstants.ACTION_KEY].strip() if MoveCopyConstants.ACTION_KEY in data else None
#         source_path_type_i = data[MoveCopyConstants.SOURCE_PATH_TYPE_KEY].strip() \
#             if MoveCopyConstants.SOURCE_PATH_TYPE_KEY in data else None
#         source_path_i = data[MoveCopyConstants.SOURCE_PATH_KEY].strip() \
#             if MoveCopyConstants.SOURCE_PATH_KEY in data else None
#         target_path_i = data[MoveCopyConstants.TARGET_PATH_KEY] if MoveCopyConstants.TARGET_PATH_KEY in data else None
#
#     mc = MoveCopy()
#     # if source path type is s3 or moveit`
#     if source_path_type_i.lower() == MoveCopyConstants.PATH_MOVEIT_KEY or \
#             source_path_type_i.lower() == MoveCopyConstants.PATH_S3_KEY:
#         print mc.s3_move_copy(action_i, source_path_type_i, source_path_i, target_path_i, sse_flag=True)
