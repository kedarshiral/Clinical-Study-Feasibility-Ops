"""Module for RDS Backup Creation"""
#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

######################################################Module Information###########################
#   Module Name         :   MysqlBackup
#   Purpose             :   Backup RDS database and store it in S3.
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Run the python file
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Pre-requisites      :   MySQL server should be up and running on the configured MySQL server
#   Last changed on     :   17 January, 2019
#   Last changed by     :   Abhishek Kurup
#   Reason for change   :   Integrated into CC environment
###################################################################################################

import os
import subprocess
from datetime import datetime, timedelta
import re
import boto3
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager


class MysqlBackup(object):
    """
    Mysql Backup
    """

    def __init__(self):
        self.configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.s3 = boto3.client('s3')
        self.execution_context = ExecutionContext()
        self.time_stamp = datetime.today().strftime('%Y%m%d%H%M%S')
        self.database = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.file_save = "{}_{}.sql".format(self.database, self.time_stamp)

        self.user_name = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_username"])
        secret_password = MySQLConnectionManager().get_secret(
            self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "rds_password_secret_name"]),
            self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
        self.password = secret_password['password']

        self.host = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_host"])

        self.s3_path_mysql = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.HOUSEKEEPING_PARAMS_KEY, "SERVER",
             "s3_path_mysql"])

        self.s3_bucket = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                               CommonConstants.HOUSEKEEPING_PARAMS_KEY, "SERVER",
                                                          "s3_bucket"])

        self.year = datetime.today().strftime('%Y')
        self.month = datetime.today().strftime('%B')
        self.day = datetime.today().strftime('%d')



    def system_command(self, command):
        """
        This function is used to run Shell command in python
        :param command: command to be executed
        :return: output
        """
        logger.debug("System command initialized", extra=self.execution_context.get_context())
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        (shell_output, err) = p.communicate()
        p_status = p.wait()
        if p_status == 0:
            return shell_output
        else:
            logger.error("System command Error", extra=self.execution_context.get_context())
            raise Exception

    def s3_upload_file(self, file_save):
        """
        This function is used to upload file to s3 location
        :param file_save: File name to be saved in destination location
        :return:
        """
        logger.debug("Uploading %s to %s", self.file_save, self.s3_bucket + '/' +
                     self.s3_path_mysql + "/" + file_save,
                     extra=self.execution_context.get_context())
        self.s3.upload_file(self.file_save, self.s3_bucket, self.s3_path_mysql + "/" + file_save)
        logger.debug("File uploaded to S3. Now removing the local copy.", extra=self.execution_context.get_context())
        os.remove(self.file_save)
        logger.debug("Local copy removed.", extra=self.execution_context.get_context())

def main():
    """
    Purpose: The function is used to save a Database dump to S3.
    Steps: 1) Create a RDS dump on local.
           2) Save the dump on S3.
           3) Delete the local copy.
    :return:
    """
    obj = MysqlBackup()
    logger.info("Starting RDS MySQL backup.", extra=obj.execution_context.get_context())
    command = "mysqldump --user={} --password={} --host {} {} --single-transaction --quick --lock-tables=false> {}". \
        format(obj.user_name, obj.password, obj.host, obj.database, obj.file_save)
    obj.system_command(command)
    obj.s3_upload_file(obj.file_save)
    logger.info("Backup of RDS MySQL completed!", extra=obj.execution_context.get_context())


if __name__ == '__main__':
    main()

