"""Module for MySQL Connection Management"""
#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

######################################################Module Information###########################
#   Module Name         :   MySQLConnectionManager
#   Purpose             :   Enable MySQL connectivity. Enable other python modules to trigger
#                           queries on the configured
#                           MySQL instance
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate and object of this class and call the class functions
#   Predecessor module  :   This module is  a generic module
#   Successor module    :   NA
#   Pre-requisites      :   MySQL server should be up and running on the configured MySQL server
#   Last changed on     :   17 March 2019
#   Last changed by     :   Sushant Choudhary
#   Reason for change   :   Added method to get RDS credentials using Secrets Manager
###################################################################################################

# Library and external modules declaration
import os
import sys
import logging
import traceback
import base64
import mysql.connector
import boto3
import json
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
import CommonConstants as CommonConstants
import time

sys.path.insert(0, os.getcwd())
#sys.path.insert(1, CommonConstants.AIRFLOW_CODE_PATH.__add__('/'))
# Define all module level constants here
MODULE_NAME = "MySQLConnectionManager"

##################class MySQLConnectionManager######################
# class contains all the functions related to MySQLConnectionManager
####################################################################
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


class MySQLConnectionManager(object):
    """Class for MySQL Connection Management"""
    # Instantiate an object of JsonConfigUtility class to load environment parameters json file
    def __init__(self, parent_execution_context=None):

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context

        self.files = []
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"current_module": MODULE_NAME})
        if(os.path.exists(CommonConstants.AIRFLOW_CODE_PATH)):
            self.configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE)
        else:
            self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.connection = None

    # logger.info(self.configuration[CommonConstants.ENVIRONMENT_CONFIG_FILE])

    # This acts as the de-constructor for the objects of this class
    def __exit__(self, type, value, traceback):
        for file in self.files:
            os.unlink(file)

    ##################################################Execute my-sql query#########################
    # Purpose            :   Execute a query in MySQL
    # Input              :   Query String
    # Output             :   Tuple of tuples in case of a select query, None otherwise
    ###############################################################################################

    def execute_query_mysql(self, query, get_single_result=None):
        """ Module for executing queries"""
        mysql_connection = None
        try:
            status_message = "Starting function to execute a MySQLquery : "+str(query)
            logger.info(status_message, extra=self.execution_context.get_context())
            if query:
                query = query.strip()
            mysql_connection = self.get_my_sql_connection()
            cursor = mysql_connection.cursor(dictionary=True)
            status_message = "Created connection to MySQL"
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Executing query on MySQL"
            logger.info(status_message, extra=self.execution_context.get_context())
            if query.lower().startswith("select"):
                cursor.execute(query)
                if get_single_result:
                    result = cursor.fetchone()
                else:
                    result = cursor.fetchall()
            elif query.lower().startswith("insert into"):
                result = cursor.execute(query)
                #if result == 1:
                result = cursor.lastrowid
                cursor.execute("commit")
            else:
                cursor.execute(query)
                cursor.execute("commit")
                result = ""
            status_message = "Executed query on MySQL with result : " + str(result)
            logger.info(status_message, extra=self.execution_context.get_context())
            return result
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            raise exception

    def get_my_sql_connection(self):
        """Module for getting mySQL connection"""

        try:
            status_message = "Starting function to fetch my-sql connection"
            logger.info(status_message, extra=self.execution_context.get_context())

            # Preparing MySQL connection using the connection parameters provided in environment
            # parameters json file
            host = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                         "mysql_host"])
            logger.info("MySQL Host: "+ str(host))
            user = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                         "mysql_username"])
            logger.info("MySQL User: " + str(user))
            secret_password = self.get_secret(
                self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "rds_password_secret_name"]),
                self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
            password = secret_password['password']
            # password = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_password"])
            port = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                         "mysql_port"])

            db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                       "mysql_db"])
            if len(host) == 0 or len(user) == 0 or len(password) == 0 or len(db) == 0 \
                    or len(port) == 0:
                status_message = "Invalid configurations for " + MODULE_NAME + " in " + \
                                 CommonConstants.ENVIRONMENT_CONFIG_FILE
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception
            mysql_connection = mysql.connector.connect(host=host, user=user, passwd=password, database=db,
                                               port=int(port))
            status_message = "Connection to MySQL successful"
            logger.info(status_message, extra=self.execution_context.get_context())
            return mysql_connection
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            raise exception

    def decrypt_value(self, encoded_string):
        """
            Purpose :   This method decrypts encoded string
            Input   :   encoded value
            Output  :   decrypted value
        """
        try:
            status_message = "Started decoding for value:" + encoded_string
            logger.info(status_message, extra=self.execution_context.get_context())
            decoded_string = base64.b64decode(encoded_string)
            status_message = "Completed decoding value:" + str(encoded_string)
            logger.info(status_message, extra=self.execution_context.get_context())
            return decoded_string
        except Exception as exception:
            status_message = "Error occured in decrypting value:" + str(encoded_string)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_secret(self, secret_name, region_name):
        """
        Purpose: This fetches the secret details and decrypts them.
        Input: secret_name is the secret to be retrieved from Secrets Manager.
               region_name is the region which the secret is stored.
        Output: JSON with decoded credentials from Secrets Manager.
        """
        MAX_RETRY_COUNT = 5

        while True:
            try:
                print("++ Fetching secrets from secrets manager ++")
                #Create a Secrets Manager client
                if CommonConstants.SECRET_MANAGER_FLAG == 'Y':
                    session = boto3.session.Session()
                    client = session.client(
                        service_name='secretsmanager',
                        region_name=region_name
                    )
                    logger.info("Fetching the details for the secret name %s", secret_name,
                                extra=self.execution_context.get_context())
                    get_secret_value_response = client.get_secret_value(
                        SecretId=secret_name
                    )
                    logger.info(
                        "Fetched the Encrypted Secret from Secrets Manager for %s", secret_name,
                        extra=self.execution_context.get_context())

                    if 'SecretString' in get_secret_value_response:
                        secret = get_secret_value_response['SecretString']
                        logger.info("Decrypted the Secret", extra=self.execution_context.get_context())
                    else:
                        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                        logger.info("Decrypted the Secret", extra=self.execution_context.get_context())

                else:
                    password = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                                           , "mysql_password"])

                    password = base64.b64decode(password).decode()
                    secret = json.dumps({"password": password})

                return json.loads(secret)

            except Exception as error:
                MAX_RETRY_COUNT -= 1
                if MAX_RETRY_COUNT != 0:
                    time.sleep(5)
                    continue
                raise Exception("Unable to fetch secrets.")


