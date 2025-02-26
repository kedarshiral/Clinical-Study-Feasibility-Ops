#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import os
import traceback
import argparse
import textwrap
import sys

from PySparkUtility import PySparkUtility
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from CommonUtils import CommonUtils

sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "HiveToHDFSHandler"
PROCESS_NAME = "Copy Files from Hive Tables to HDFS with repartition"

"""
Module Name         :   HiveToHDFSHandler
Purpose             :   Repartition and copy data to HDFS
Input Parameters    :   Parameters: hive_table_name, target_hdfs_location and no_of_partitions
Output Value        :   Return Dictionary with status(Failed/Success)
Last changed on     :   3rd Jan 2019
Last changed by     :   Alageshan M
Reason for change   :   Hive(HDFS) to HDFS Copy Module with repartition -- development
"""


class HiveToHDFSHandler(object):
    """
    This class is Wrapper for copying Files from Hive Tables(HDFS) to HDFS with repartition
    """

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.env_configs = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.env_configs.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.hadoop_configs = self.env_configs.get_configuration([CommonConstants.HADOOP_OPTIONS_KEY])
        self.s3_distcp_configs = self.env_configs.get_configuration([CommonConstants.S3_DISTCP_OPTIONS_KEY])
        self.s3_end_point = self.env_configs.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region_endpoint"])

    def copy_data(self, hive_table_name, target_location=None):
        self.spark = PySparkUtility().get_spark_context(hive_table_name)
        try:
            # get source from fetch_hive_table_location and target we fetched from dataset information table
            source_location = str(self.fetch_hive_table_location(hive_table_name))

            no_of_repartition = CommonUtils().get_staging_partition(source_location)

            logger.info("Repartition value: " + str(no_of_repartition),
                        extra=self.execution_context.get_context())

            logger.info("Fetching partitions form the table:" + hive_table_name,
                        extra=self.execution_context.get_context())
            dynamic_partitions = self.get_dynamic_partitions(hive_table_name)
            if len(dynamic_partitions) > 0:
                no_of_repartition = no_of_repartition / len(dynamic_partitions)
                if int(no_of_repartition) < 2:
                    no_of_repartition = 2
                logger.info("Starting to write data to HDFS ",
                            extra=self.execution_context.get_context())
                df = self.spark.sql("select * from ".__add__(hive_table_name)).repartition(
                        int(no_of_repartition))
                logger.info("Writing to target " + str(target_location) +
                            "with partitions: " + str(dynamic_partitions),
                            extra=self.execution_context.get_context())
                df.write.mode('overwrite').partitionBy(*dynamic_partitions).save(target_location)
                status_message = "Data copied to HDFS"
                logger.info(status_message,
                            extra=self.execution_context.get_context())
            else:
                if int(no_of_repartition) < 2:
                    no_of_repartition = 2
                df = self.spark.sql("select * from ".__add__(hive_table_name)).repartition(
                    int(no_of_repartition))
                logger.info("Writing to target " + str(target_location),
                            extra=self.execution_context.get_context())
                df.write.mode('overwrite').save(target_location)
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            status_message = "Failed to copy data to HDFS"
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
        finally:
            self.spark.stop()
            logger.info("spark job is stopped"
                        , extra=self.execution_context.get_context())

    def get_dynamic_partitions(self, hive_table_name):
        try:
            partitions = self.spark.sql("show partitions ".__add__(hive_table_name)).collect()  # returns a list
            logger.info("Partitions obtained from Hive table: " + str(partitions),
                        extra=self.execution_context.get_context())
            dynamic_partitions = []
            if partitions is not None:
                for value in partitions:
                    # value will be in row format
                    partitions_row = value.partition  # reading the values from the row format
                    logger.info("partition row: " + str(partitions_row),
                                extra=self.execution_context.get_context())
                    custom_partitions = partitions_row.split("/")

                    for custom_partitions_value in custom_partitions:
                        i = custom_partitions_value.split("=")
                        if i[0] not in dynamic_partitions:
                            dynamic_partitions.append(i[0])
            logger.info("Obtained custom partitions are: " + str(dynamic_partitions),
                            extra=self.execution_context.get_context())
            return dynamic_partitions
        except Exception as exception:
            error = "ERROR in get_dynamic_partitions, ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": traceback.format_exc()})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def fetch_hive_table_location(self, table_name=None):
        """
        Purpose   :   This method is used to fetch the hive table location
        Input     :   hive table name  (Mandatory)
        Output    :   Returns hive table location
        """
        try:
            status_message = "Executing fetch_hive_table_location function for table_name : " + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if table_name is None or table_name == "":
                raise Exception("Table Name Can Not be None")

            if "." not in table_name:
                table_name = "default." + str(table_name)

            result_set = CommonUtils().execute_hive_query("DESCRIBE FORMATTED " + table_name)
            for result in result_set:
                if (str(list(result)[0]).strip()) == "Location:":
                    location = "/" + (str(str(
                        list(result)[1]).strip().split(":", -1)[2].split("/", 1)[1]).strip()).strip(
                        '/')
                    return location
            else:
                raise Exception("Improper result obtained by describe formatted query")

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            status_message = 'Failed to get the table Location' + error
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
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

   -st, --source_table : Source Hive table name

   -tp, --target_path : Target HDFS path


    command :

    python HiveToHDFSHandler.py -st <Source Hive table name> -tp <Target HDFS path> -p <No of target partitions>

    The list of command line parameters valid for %(prog)s module.'''), epilog="The module will exit now!!")

    parser.add_argument('-st', '--source_table', required=True,
                        help='Specify the source hive table name to be written in hdfs using -st/--source_table')

    parser.add_argument('-tp', '--target_path', required=False,
                        help='Specify the target hdfs path using -tp/--target_path')


    cmd_arguments = vars(parser.parse_args())
    return cmd_arguments


if __name__ == '__main__':
    try:
        commandline_arguments = get_commandline_arguments()
        obj = HiveToHDFSHandler()
        obj.copy_data(commandline_arguments["source_table"], commandline_arguments["target_path"])
    except Exception as e:
        raise Exception("Exception in HiveToHDFSHandler" + str(e) + str(traceback.format_exc()))