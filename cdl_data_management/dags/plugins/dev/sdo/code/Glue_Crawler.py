#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Glue_Crawler
#  Purpose             :   This module will trigger the Glue Crawlers from CLI.
#  Input Parameters    :   Crawler_name
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   25th Jan 2019
#  Last changed by     :   Alageshan M
#  Reason for change   :   Added function to delete all tables of the database associated with crawler
# ######################################################################################################################

import boto3
import os
import sys
import time
import logging
import CommonConstants
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
import argparse
import textwrap
import traceback
sys.path.insert(0, os.getcwd())



MODULE_NAME = "Glue_Crawler"
PROCESS_NAME = "Glue_Crawler"
# Create global variables
EXPRESSION = "*dummy*"
# Given below variable are used to create dummy files and folders
DUMMY_PATH = "z_dummy/dummy.txt"
DUMMMY_FILE_PATH = "dummy.txt"

# sample date to write in a file
SAMPLE_DATA = """{
  "a": "1",
  "b": "2",
  "c": "3"
}"""


logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
hndlr.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)



class Glue_Crawler(object):
    """This class will trigger the input GLUE crawler"""

    def __init__(self, execution_context=None):
        if execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = execution_context
        self.s3_object = boto3.client("s3")
        configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        region_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"])
        self.glue_client = boto3.client(CommonConstants.GLUE, region_name=region_name)

    def create_common_prefix_list(self, bucket_name, depth, input_pre_list, output_pre_list):
        try:

            if not input_pre_list:
                logger.info("input pre list is empty", extra=self.execution_context.get_context())
                return output_pre_list
            temp_output_prefix_list = []
            for prefix in input_pre_list:
                output_pre_list.append(prefix)
                resp = self.s3_object.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
                commonprefix_list = resp.get("CommonPrefixes")
                if commonprefix_list is not None:
                    for out_prefix_dict in commonprefix_list:
                        pre = out_prefix_dict.get("Prefix")
                        if pre.count('/') < depth:
                            temp_output_prefix_list.append(pre)
            self.create_common_prefix_list(bucket_name, depth, temp_output_prefix_list, output_pre_list)
            logger.info(output_pre_list, extra=self.execution_context.get_context())
            return output_pre_list
        except Exception as e:
            logger.error("Failed to create Common Prefix", extra=self.execution_context.get_context())
            raise e

    # Create list of directory only. Not used currently
    def create_dir_list(self, key_list):
        dir_list = []
        try:
            for key in key_list:
                if str(key).endswith("/"):
                    dir_list.append(key)
            logger.info(dir_list, extra=self.execution_context.get_context())
            return dir_list
        except Exception as e:
            raise e

    # Function to create list of new keys to be created in S3
    def create_dummy_dir_list(self, dir_list):
        output_list = []
        try:
            for dir_item in dir_list:
                key_file_name1 = os.path.join(dir_item, DUMMMY_FILE_PATH)
                key_file_name2 = os.path.join(dir_item, DUMMY_PATH)
                output_list.append(key_file_name1)
                output_list.append(key_file_name2)
            logger.info(output_list, extra=self.execution_context.get_context())
            return output_list
        except Exception as e:
            raise e

    # Start Crawler Run
    def run_crawler(self):
        logger.info("crawler starting", extra=self.execution_context.get_context())
        try:
            self.glue_client.start_crawler(Name=crawler_name)
        except self.glue_client.exceptions.CrawlerRunningException:
            logger.error('already running', extra=self.execution_context.get_context())
        logger.info("Crawler started", extra=self.execution_context.get_context())

    # monitor Crawler status
    def get_crawler(self, crawler_name):
        try:
            response = self.glue_client.get_crawler(Name=crawler_name)
            crawler = response.get("Crawler")
            logger.info(crawler, extra=self.execution_context.get_context())
            return (crawler)
        except Exception as e:
            logger.error("Crawler not Found", extra=self.execution_context.get_context())
            raise e

    # monitor Crawler status
    def check_status(self):
        status = CommonConstants.RUNNING_STATE
        try:
            while status == CommonConstants.RUNNING_STATE:
                time.sleep(20)
                response = self.glue_client.get_crawler(Name=crawler_name)
                status = response.get("Crawler").get("State")
                logger.info(status, extra=self.execution_context.get_context())
        except Exception as e:
            if "Throttling" in e:
                self.check_status()
            else:
                raise e

    # created dummy directories
    def create_dummy_dir(self, BucketName, dir_list):
        try:

            for dir_item in dir_list:
                self.s3_object.put_object(Body=SAMPLE_DATA, Bucket=BucketName, Key=dir_item)
        except Exception as e:
            logger.error("Failed to create Dummy Directories", extra=self.execution_context.get_context())
            raise e

    # Delete the dummy keys after completion of cralwer run
    def delete_dummy_dir_and_files(self, BucketName, dir_list):
        try:

            for key_item in dir_list:
                if "dummy" in key_item:
                    s3_re = boto3.resource("s3")
                    s3_re.Object(BucketName, key_item).delete()
                    logger.info("Dummy files are deleted", extra=self.execution_context.get_context())
        except Exception as e:
            logger.error("Failed to delete Dummy Directories", extra=self.execution_context.get_context())
            raise e

    # Fetch list of table in database based upon pattern
    def get_table_list(self, database_name, pattern=None):
        try:
            response = {'NextToken': ''}
            table_list = []
            while 'NextToken' in response:
                if pattern is not None:
                    response = self.glue_client.get_tables(DatabaseName=database_name, Expression=pattern,
                                                           NextToken=response['NextToken'])
                else:
                    response = self.glue_client.get_tables(DatabaseName=database_name, NextToken=response['NextToken'])
                item_list = response.get("TableList")

                if item_list:
                    item_list = response.get("TableList")
                    for item in item_list:
                        table_name = item.get("Name")
                        table_list.append(table_name)
            logger.info(table_list, extra=self.execution_context.get_context())
            return table_list
        except Exception as e:
            logger.error("Failed to get tables list", extra=self.execution_context.get_context())
            raise e

    # delete table fetched
    def delete_tables(self, database_name, table_list):
        try:

            for table in table_list:
                response = self.glue_client.delete_table(DatabaseName=database_name, Name=table)
                logger.info(response, extra=self.execution_context.get_context())
        except Exception as e:
            logger.error("Failed to drop tables", extra=self.execution_context.get_context())
            raise e

    def get_database_name_for_crawler(self, crawler_name_input):
        try:
            response = self.glue_client.get_crawler(Name=crawler_name_input)
            return response['Crawler']['DatabaseName']
        except Exception as e:
            logger.error("Database not Found", extra=self.execution_context.get_context())
            raise e

    def update_table_schema(self, db_name, table_name):

        table_meta = self.glue_client.get_table(
            DatabaseName=db_name,
            Name=table_name
        )
        logger.info(table_meta)
        column_list = table_meta.get("Table").get("StorageDescriptor").get("Columns")
        partition_list = table_meta.get("Table").get("PartitionKeys")
        logger.info(column_list)
        logger.info(partition_list)

        column_list_updated = []
        duplicate_col_list = []
        for item in column_list:
            delete_flag = 0
            col_name = item.get("Name").encode('ascii', 'ignore')
            logger.info(col_name)
            col_type = item.get("Type").encode('ascii', 'ignore')
            logger.info(col_type)
            for part_item in partition_list:
                part_name = part_item.get("Name")
                part_type = part_item.get("Type")
                if part_name == str(CommonConstants.COLUMN_PREFIX + str(col_name)):
                    delete_flag = 1
                    duplicate_col_list.append(col_name)
            if not delete_flag:
                column_list_updated.append(item)

        table_meta_new = table_meta.copy()
        table_meta_new["Table"]["StorageDescriptor"]["Columns"] = column_list_updated
        table_meta_new.get("Table").pop('UpdateTime', None)
        table_meta_new.get("Table").pop('CreateTime', None)
        table_meta_new.get("Table").pop('CreatedBy', None)

        self.glue_client.update_table(DatabaseName=db_name, TableInput=table_meta_new.get("Table"))

    def get_table_list_from_database(self, db_name=None):
        try:
            if db_name is not None:
                response_database = self.glue_client.get_tables(DatabaseName=db_name)
                if response_database is not None:
                    table_list = response_database['TableList']
                    for tableDict in table_list:
                        table_name = tableDict['Name']
                        logger.info(table_name, extra=self.execution_context.get_context())
                        self.update_table_schema(db_name=db_name, table_name=table_name)

                else:
                    raise Exception
        except Exception as exception:
            raise exception

    def call_glue_crawler(self, crawler_name=None, remove_duplicates=None):
        try:
            crawler = self.get_crawler(crawler_name)
            logger.info(crawler, extra=self.execution_context.get_context())
            crawler_s3_path = crawler.get("Targets").get("S3Targets")[0].get("Path")
            logger.info(crawler_s3_path, extra=self.execution_context.get_context())
            path_split = crawler_s3_path.split(os.sep)
            logger.info(path_split, extra=self.execution_context.get_context())
            depth = len(path_split) - 2
            bucket_name = path_split[2]
            logger.info(bucket_name, extra=self.execution_context.get_context())
            prefix_list = [i for j, i in enumerate(path_split) if j not in [0, 1, 2]]
            input_prefix = '/'.join(prefix_list)
            input_prefix_list = [input_prefix + "/"]
            output_pre_list = self.create_common_prefix_list(bucket_name, depth, input_prefix_list, [])
            logger.info("output_pre_list - " + str(output_pre_list), extra=self.execution_context.get_context())
            dir_list_to_create = self.create_dir_list(output_pre_list)
            logger.info("dir_list_to_create - " + str(dir_list_to_create))
            final_dir_list = self.create_dummy_dir_list(dir_list_to_create)
            logger.info("final_dir_list - " + str(final_dir_list), extra=self.execution_context.get_context())
            self.create_dummy_dir(bucket_name, final_dir_list)
            self.run_crawler()
            self.check_status()
            self.delete_dummy_dir_and_files(bucket_name, final_dir_list)
            database_name = self.get_database_name_for_crawler(crawler_name)
            logger.info("database_name - " + database_name, extra=self.execution_context.get_context())
            table_list = self.get_table_list(database_name, EXPRESSION)
            logger.info("deleting dummy tables - " + str(table_list), extra=self.execution_context.get_context())
            if remove_duplicates is not None and remove_duplicates.lower() == 'yes':
                self.delete_tables(database_name, table_list)
                logger.info("removing the duplicate columns ")
                self.get_table_list_from_database(db_name=database_name)
                logger.info("duplicate columns are removed")
            else:
                logger.info("duplicate columns are not removed")
        except Exception as e:
            logger.error("Crawler Failed", extra=self.execution_context.get_context())
            raise e

    def delete_crawler_tables(self, crawler_name):
        try:
            database_name = self.get_database_name_for_crawler(crawler_name)
            table_list = self.get_table_list(database_name)
            self.delete_tables(database_name, table_list)
        except Exception as exception:
            self.execution_context.set_context({"traceback": str(traceback.format_exc())})
            logger.error(str(exception), extra=self.execution_context.get_context())
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

    -c, --crawler_name : Crawler Name

    OR

    -c, --crawler_name : Crawler Name
    -rd, --remove_duplicates : Duplicate Columns
    -dt --drop_tables_flag : drop tables of the database associated with the crawler




    command : python Glue_Crawler.py -c <crawler_name>
              python Glue_Crawler.py -c <crawler_name> -rd <yes>


    The list of command line parameters valid for %(prog)s module.'''),
                                     epilog="The module will exit now!!")

    # List of Mandatory Parameters either provide source,target or provide json_path
    parser.add_argument('-c', '--crawler_name', required=True,
                        help='Specify the crawler name using -c/--crawler_name')

    parser.add_argument('-rd', '--remove_duplicates', required=False,
                        help='Specify whether you want to remove duplicate columns or not using -rd/--remove_duplicates')

    parser.add_argument('-dt', '--drop_tables_flag', required=False,
                        help='Specify Y to drop the tables before running the crawler')

    cmd_arguments = vars(parser.parse_args())
    if cmd_arguments['drop_tables_flag'] is None:
        cmd_arguments['drop_tables_flag'] = "Y"
    return cmd_arguments


if __name__ == '__main__':
    #  Main program execution
    commandline_arguments = get_commandline_arguments()
    if commandline_arguments is not None:
        glue_crawler = Glue_Crawler()
        crawler_name = commandline_arguments['crawler_name']
        remove_duplicates = commandline_arguments['remove_duplicates']
        drop_tables_flag = commandline_arguments['drop_tables_flag']
        if str(drop_tables_flag).lower() == "y":
            glue_crawler.delete_crawler_tables(crawler_name)
        glue_crawler.call_glue_crawler(crawler_name=crawler_name,
                                       remove_duplicates=remove_duplicates)
