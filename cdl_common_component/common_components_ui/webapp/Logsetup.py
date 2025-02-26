#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

##################################Module Information##############################
#   Module Name         : logsetup
#   Purpose             : Setup the environment and assign value to all the variables required for logging
#   Pre-requisites      : Config variables should be present in radar config file
#   Last changed on     : 1st September 2014
#   Last changed by     : Shreekant Agrawal
#   Reason for change   : Commented
##################################################################################

#Library and external modules declaration
import logging
import logging.handlers
#import logstash
import inspect
import socket
from datetime import datetime
from ConfigUtility import ConfigUtility

ENVIRONMENT_CONFIG_FILE = "commoncomponent.conf"
MODULE_NAME = "logsetup"
TIME_FORMAT= str(datetime.now().strftime("%Y%m%d_%H_%M_%S"))

configuration = ConfigUtility(ENVIRONMENT_CONFIG_FILE)
LOG_PATH="Logpath"
app_master_ip = configuration.get_configuration(MODULE_NAME, "app_master_ip")
amgen_unix_ip = configuration.get_configuration(MODULE_NAME, "")
logstash_port = configuration.get_configuration(MODULE_NAME, "logstash_port")
logstash_schema_version = configuration.get_configuration(MODULE_NAME, "schema_version")
debug_flag = configuration.get_configuration(MODULE_NAME, "debug_flag")
log_path = configuration.get_configuration(LOG_PATH, "log_path")

hostname = socket.gethostname()
ip_address = hostname.replace('ip-', '').replace('-', '.')

#if (app_master_ip in ip_address) or (amgen_unix_ip in ip_address):
#    logstash_host_name = configuration.get_configuration(MODULE_NAME, "logstash_public_ip")
#else:
#    logstash_host_name = configuration.get_configuration(MODULE_NAME, "logstash_private_ip")

logger = logging.getLogger(__name__)
if debug_flag == 'Y':
    # Set logging level accross the logger. Set to INFO in production
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
# create formatter
#formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s')
formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s - %(process_name)s - %(current_module)s - '
                              '%(function_start_time)s - %(function_name)s - %(process_identifier)s - '
                              '%(parent_function_name)s - %(function_status)s - %(traceback)s')

# LogstashLog Handler
#logstash_handler = logstash.LogstashHandler(logstash_host_name, int(logstash_port), version=int(logstash_schema_version))

#Local File Handler
#create file handler which logs even debug messages
file_name=TIME_FORMAT+__name__
file_handler = logging.FileHandler(log_path+file_name + '_file.log')

file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# create console handler with debug level
# This should be changed to ERROR in production
console_handler = logging.StreamHandler()
if debug_flag == 'Y':
    console_handler.setLevel(logging.DEBUG)
else:
    console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(file_handler)

logger.addHandler(console_handler)
#logger.addHandler(logstash_handler)


