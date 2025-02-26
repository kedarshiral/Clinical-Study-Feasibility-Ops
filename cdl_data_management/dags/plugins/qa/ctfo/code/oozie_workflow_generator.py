#!/usr/bin/python

# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

######################################################Module Information################################################
#   Module Name         :   oozie_workflow_generator.py
#   Purpose             :   Generate dynamic oozie workflows per file present on given location. 
#                           Used in case of EMR wherein there is need to run specific workflows on specific EMR instance 
#   Input Parameters    :   Taked process_name as input and Creates map(process_name, oozie_workflow_to_be_created) from 
#			    table snfidev_bdp.process_oozie_workflow_map for supplied process_name
#   Output              :   Generated oozie workflows per input file present in location
#   Execution Steps     :   
#   Predecessor module  :   This module is  a generic module 
#   Successor module    :   NA
#   Pre-requisites      :   MySQL server should be up and running on the configured MySQL server
#   Last changed on     :   22 December 2017
#   Last changed by     :   Kaushal Pandya
#   Reason for change   :   First Version
########################################################################################################################

import sys
import getopt
import boto
import re
from boto.s3.connection import S3Connection
from MySQLConnectionManager import MySQLConnectionManager

from Pattern_Validator import PatternValidator

########################################################################################################################
# main() function                                                                                                      #
########################################################################################################################

if __name__ == "__main__":

    ################################################################################################################
    # Creating Empty Variables                                                                                    #
    ################################################################################################################
    process_name = ''

    ################################################################################################################
    # Parsing Command Line Arguments                                                                               #
    ################################################################################################################
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:p:")
    except getopt.GetoptError:
        print "Invalid Argument"
        usage
        sys.exit(1)

    for opt, arg in opts:
        if opt == "h":
            usage
        elif opt == "-p":
            process_name = arg
            print "Process_name: ", process_name

    ################################################################################################################
    # Reading MySql snfiprod_bdp.process_oozie_workflow_map for the process_name to get active workflow list        #
    ################################################################################################################
    query = "SELECT * FROM snfiprod.process_oozie_workflow_map WHERE process_name='" + process_name + "' and active_flag = 'Y'"
    print "Generated Query : ", query
    workflow_list = MySQLConnectionManager().execute_query_mysql(query, False)
    print "Workflow List Retrieved: ", workflow_list

    ################################################################################################################
    # For each active workflow, generate the oozie-workflow-xml                                                    #
    ################################################################################################################
    for workflow in workflow_list:
        print "Workflow Name: ", workflow["workflow_name"]

        ########################################################################################################
        # Reading File source location from dataset_information table                                          #
        ########################################################################################################
        query = "SELECT dataset_file_source_location, dataset_file_name_pattern from snfiprod.dataset_information WHERE dataset_id = " + str(
            workflow["dataset_id"]) + ";"
        file_source_location = MySQLConnectionManager().execute_query_mysql(query, False)
        print "File Source Location Retrived: ", file_source_location[0]['file_source_location']

        ########################################################################################################
        # Creating list of files present in the source location, matching file pattern of dataset_id           #
        ########################################################################################################
        aws_access_key = ""
        aws_secrete_key = ""
        aws_region = "us-east-1"

        print "Listing Files present in location: ", file_source_location
        bucket_name = ""
        conn = boto.s3.connect_to_region(aws_region, aws_access_key_id=aws_access_key,
                                         aws_secret_access_key=aws_secrete_key, is_secure=True,
                                         calling_format=boto.s3.connection.OrdinaryCallingFormat())
        # if not str(file_source_location["file_source_location"]):
        if file_source_location[0]['file_source_location'] is not None:
            s3_tmp_path = str(file_source_location[0]['file_source_location']).replace("s3://", "").replace("s3n://",
                                                                                                            "").replace(
                "s3a://", "")
            dirs = s3_tmp_path.split('/')
            bucket_name = dirs[0]
            print "Bucket Name: ", bucket_name
            bucket = conn.get_bucket(bucket_name)
            s3_folder_path = s3_tmp_path.lstrip(bucket_name).lstrip('/')
            print "S3 Folder Path: ", s3_folder_path

            # s3_folder_list = bucket.list(s3_folder_path, prefix='Customer_Affiliation_F_')
            s3_folder_list = bucket.list(s3_folder_path)
            print "S3 Folder List: ", s3_folder_list

            print "Starting processing of files present in location: ", file_source_location[0]['file_source_location']
            for files in s3_folder_list:
                #				print "File in Folder: ", files.name
                pattern = re.compile("(\w+.txt$)")

                # Skip Dirs, process only file entries
                if not str(files.name).endswith('/'):
                    # Extracting File Name from file absolute/full path
                    file_name = pattern.findall(str(files.name))[0]
                    #					print "Only file name: ", file_name

                    # Perform file Pattern validation
                    if PatternValidator().patter_validator(file_name, file_source_location[0]['file_name_pattern']):
                        print "File Name Matches Pattern: ", file_source_location[0]['file_name_pattern']
                        print "Generating Oozie workflow for the file: ", file_name

        else:
            print "S3 Source location is not provided, Skipping this workflow creation."


