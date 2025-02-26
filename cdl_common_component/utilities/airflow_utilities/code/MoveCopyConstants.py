#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
"""
Doc_Type            : Tech
Tech Description    : This module is used for initializing all the constant parameters which MoveCopy
                      utility can refer
Pre_requisites      : NA
Inputs              : NA
Outputs             : NA
Example             : NA
Config_file         : NA

"""

# Constants representing the status keys
STATUS_FINISHED = "FINISHED"
STATUS_FAILED = "FAILED"
STATUS_SUCCESS = "SUCCESS"
STATUS_ERROR = "ERROR"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"
ACTION_MOVE_KEY = "move"
ACTION_COPY_KEY = "copy"
PATH_S3_KEY = "s3"
PATH_MOVEIT_KEY = "moveit"
PATH_AWS_KEY = "aws"
JSON_FILE_PATH_KEY = "json_file_path"
ACTION_KEY = "action"
SOURCE_PATH_KEY = "source_path"
SOURCE_PATH_TYPE_KEY = "source_path_type"
TARGET_PATH_KEY = "target_path"
SSE_KEY = "sse"
RECURSIVE_KEY = "recursive"