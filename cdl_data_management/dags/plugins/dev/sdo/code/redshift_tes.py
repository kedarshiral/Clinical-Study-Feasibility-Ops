
import os
from ConfigUtility import JsonConfigUtility
import socket
import json
import copy
from EmrClusterLaunchWrapper import EmrClusterLaunchWrapper
from ImportExportUtility import ImportExportUtility
from MySQLConnectionManager import MySQLConnectionManager
from Get_EMR_Host_IP import Get_EMR_Host_IP
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from CommonUtils import CommonUtils
from PopulateDependency import PopulateDependency
import logging
import traceback
import subprocess
from NotificationUtility import NotificationUtility
import random
import os
import sys
import datetime as dt
import time
from urllib.parse import urlparse
import boto3
import ast
import base64
from ExecuteSSHCommand import ExecuteSSHCommand
from DatabaseUtility import DatabaseUtility
import RedShiftUtilityConstants
from RedshiftUtility import RedShiftUtility

conf_file_path = "redshiftUtilityInputConfig.json"
with open(conf_file_path) as data_file:
    file_str = data_file.read()
configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE))
env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"]).lower()
file_str = file_str.replace("$$env", env)
conf_json = json.loads(file_str)
table_paths = conf_json['table_paths']
redshift_utility_obj = RedShiftUtility()
for redshift_table_name in table_paths:
    conf_json[RedShiftUtilityConstants.REDSHIFT_TARGET_TABLE_KEY] = redshift_table_name
    conf_json[RedShiftUtilityConstants.REDSHIFT_SOURCE_S3_KEY] = table_paths[redshift_table_name]
    # truncate
    conf_json[RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY] = "TRUNCATE"
    logging.info("{} operation to be executed".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY))
    operation_status = redshift_utility_obj.execute_redshift_operation(conf_json)
    if not operation_status:
        raise Exception("Redshift operation failed. Input JSON provided: {}".format(str(conf_json)))
    logging.info("{} operation executed on {}".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY,
                                                      redshift_table_name))
    # load
    conf_json[RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY] = "LOAD"
    logging.info("{} operation to be executed".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY))
    operation_status = redshift_utility_obj.execute_redshift_operation(conf_json)
    if not operation_status:
        raise Exception("Redshift operation failed. Input JSON provided: {}".format(str(conf_json)))
    logging.info("{} operation executed on {}".format(RedShiftUtilityConstants.REDSHIFT_OPERATION_KEY,
                                                      redshift_table_name))

