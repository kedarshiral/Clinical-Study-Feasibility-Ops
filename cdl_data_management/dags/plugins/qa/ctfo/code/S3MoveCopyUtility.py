#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'ZS ASSOCIATES'

# ####################################################Module Information################################################
#  Module Name         :   S3MoveCopyUtility
#  Purpose             :   This module will perform file copy from S3 to S3/HDFS
#  Input Parameters    :
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   19th November 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   File Check Utility development
# ######################################################################################################################

# All the package imports
import traceback
#import pydoop.hdfs
import os
import sys

sys.path.insert(0, os.getcwd())
from ExecutionContext import ExecutionContext
from LogSetup import logger
from PySparkUtility import PySparkUtility
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

# All the global constants declarations
MODULE_NAME = 'S3Utility'
PROCESS_NAME = 'File Copy from S3'
CSV_FORMAT = "com.databricks.spark.csv"
SPARK_JOB_SUCCESS_DIR_NAME = '_SUCCESS'
DEFAULT_MULTIPART_SIZE_MB = 15


# This class contains all the necessary functions for the copy of files from S3 to S3/HDFS
class S3MoveCopyUtility(object):
    # initialization of class S3ToS3LoadUtility
    def __init__(self, execution_context=None):
        method_name = ""
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "cluster_mode"])
        try:
            if not execution_context:
                self.execution_context = ExecutionContext()
                self.execution_context.set_context({"module_name": MODULE_NAME})
            else:
                self.execution_context = execution_context
                self.execution_context.set_context({"module_name": MODULE_NAME})

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    # ############################################ S3 copy to S3 using spark ###########################################
    # Purpose   :   This method is used to copy files from S3 to S3 using spark
    # Input     :   S3 file path
    # Output    :   NA
    # ##################################################################################################################
    def copy_from_s3_to_s3_with_spark(self, s3_file_path=None, target_s3_path=None, file_partition_count=None):
        spark_context = None
        try:
            status_message = "Starting s3 file copy module from " + s3_file_path
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get spark and sql context
            spark_context = PySparkUtility().get_spark_context()
            spark_sql_context = PySparkUtility().get_sql_context(spark_context)
            status_message = 'Fetched spark sql context'
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Set the number of partitions for data frame
            if file_partition_count is None:
                file_partition_count = 1
            self.execution_context.set_context({"Partition count": file_partition_count})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Write file to target hdfs location
            status_message = 'Starting file copy to HDFS'
            s3_file_df = spark_sql_context.read.format("com.databricks.spark.csv").load(
                s3_file_path).repartition(file_partition_count)
            s3_file_df.write.format(CSV_FORMAT).save(target_s3_path)
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Logging completion of file copy
            status_message = 'Completed file copy to hdfs path'
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
        finally:
            # Stop the spark context
            status_message = "Closing spark context"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if spark_context:
                spark_context.stop()
            status_message = "Successfully closed spark context"
            logger.debug(status_message, extra=self.execution_context.get_context())

    # ########################################### S3 copy to S3 with s3cmd command #####################################
    # Purpose   :   This method is used to copy files from S3 to S3 using spark
    # Input     :   S3 file path
    # Output    :   NA
    ###################################################################################################################
    def copy_s3_to_s3_with_s3cmd(self, s3_source_location=None, s3_target_location=None, multipart_size_mb=None):
        try:
            status_message = "Starting s3 file copy module from " + s3_source_location
            logger.debug(status_message, extra=self.execution_context.get_context())

            if self.cluster_mode != 'EMR':
                # Fetch s3 connection details for current user
                aws_s3_cred_params = CommonUtils(self.execution_context).fetch_s3_conn_details_for_user()
                s3_access_key = aws_s3_cred_params.get_configuration(CommonConstants.S3_ACCESS_KEY)
                s3_secret_key = aws_s3_cred_params.get_configuration(CommonConstants.S3_SECRET_KEY)

                # Take multipart size as default value, which is 15 MB
                if multipart_size_mb is None:
                    multipart_size_mb = DEFAULT_MULTIPART_SIZE_MB

                # Constructing s3cmd for file copy
                s3copy_cmd = str('s3cmd ').__add__(' --access-key ').__add__(s3_access_key).__add__(
                    ' --secret-key ').__add__(
                    s3_secret_key).__add__(
                    ' --server-side-encryption ').__add__('AES256').__add__(' --multipart-chunk-size-mb ').__add__(
                    str(50)).__add__(
                    ' cp "').__add__(s3_source_location).__add__('" "').__add__(s3_target_location).__add__('"')
            else:
                s3copy_cmd = str('aws s3 cp "').__add__(s3_source_location).__add__('" "').__add__(s3_target_location).__add__('"')

            status_message = 'Constructed s3 copy command for execution : ' + str(s3copy_cmd)
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Execute s3 cmd shell command for file copy
            CommonUtils(self.execution_context).execute_shell_command(s3copy_cmd)
            status_message = 'Executed s3 copy command successfully '
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
    def copy_s3_to_s3_recursive_with_s3cmd(self, s3_source_location=None, s3_target_location=None, multipart_size_mb=None):
        try:
            status_message = "Starting s3 file copy module from " + s3_source_location
            logger.debug(status_message, extra=self.execution_context.get_context())

            if self.cluster_mode != 'EMR':
                # Fetch s3 connection details for current user
                aws_s3_cred_params = CommonUtils(self.execution_context).fetch_s3_conn_details_for_user()
                s3_access_key = aws_s3_cred_params.get_configuration(CommonConstants.S3_ACCESS_KEY)
                s3_secret_key = aws_s3_cred_params.get_configuration(CommonConstants.S3_SECRET_KEY)

                # Take multipart size as default value, which is 15 MB
                if multipart_size_mb is None:
                    multipart_size_mb = DEFAULT_MULTIPART_SIZE_MB

                # Constructing s3cmd for file copy
                s3copy_cmd = str('s3cmd ').__add__(' --access-key ').__add__(s3_access_key).__add__(
                    ' --secret-key ').__add__(
                    s3_secret_key).__add__(
                    ' --server-side-encryption ').__add__('AES256').__add__(' --multipart-chunk-size-mb ').__add__(
                    str(50)).__add__(
                    ' cp --recursive "').__add__(s3_source_location).__add__('" "').__add__(s3_target_location).__add__('"')
            else:
                s3copy_cmd = str('aws s3 cp --recursive "').__add__(s3_source_location).__add__('" "').__add__(s3_target_location).__add__('"')

            status_message = 'Constructed s3 copy command for execution : ' + str(s3copy_cmd)
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Execute s3 cmd shell command for file copy
            CommonUtils(self.execution_context).execute_shell_command(s3copy_cmd)
            status_message = 'Executed s3 copy command successfully '
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
    # #################################### S3 copy to HDFS with pyspark ########################################
    # Purpose   :   This method is used to copy files from S3 to HDFS using spark
    # Input     :   S3 source path, HDFS target path
    # Output    :   NA
    ###################################################################################################################
    def copy_from_s3_to_hdfs(self, s3_file_path=None, target_hdfs_path=None, file_partition_count=None):
        spark_context = None
        try:
            status_message = "Starting s3 file copy module from " + s3_file_path
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get spark and sql context
            spark_context = PySparkUtility().get_spark_context()
            spark_sql_context = PySparkUtility().get_sql_context(spark_context)
            status_message = 'Fetched spark sql context'
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Set the number of partitions for data frame
            if file_partition_count is None:
                file_partition_count = 1
            self.execution_context.set_context({"Partition count": file_partition_count})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Write file to target hdfs location
            status_message = 'Starting file copy to HDFS'
            s3_file_df = spark_sql_context.read.format("com.databricks.spark.csv").load(
                s3_file_path).repartition(file_partition_count)
            s3_file_df.write.format(CSV_FORMAT).save(target_hdfs_path)
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Delete spark job success folder from target location
            hdfs_copy_success_dir = os.path.join(target_hdfs_path.rstrip("/"), SPARK_JOB_SUCCESS_DIR_NAME)
            #pydoop.hdfs.rmr(hdfs_copy_success_dir)
            status_message = 'Deleted spark job success folder ' + hdfs_copy_success_dir
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Logging completion of file copy
            status_message = 'Completed file copy to hdfs path'
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
        finally:
            # Stop the spark context
            status_message = "Closing spark context"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if spark_context:
                spark_context.stop()
            status_message = "Successfully closed spark context"
            logger.debug(status_message, extra=self.execution_context.get_context())

    # ########################################### S3 copy to S3 with s3cmd command in recursive #######################
    # Purpose   :   This method is used to copy files from S3 to S3 using aws s3
    # Input     :   location , target, multipart_size
    # Output    :   NA
    ###################################################################################################################
    def copy_s3_to_s3_with_aws_s3_in_recursive(self, s3_source_location=None, s3_target_location=None,
                                               multipart_size_mb=None):
        try:
            status_message = "Starting s3 file copy module from " + s3_source_location
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Take multipart size as default value, which is 15 MB
            if multipart_size_mb is None:
                multipart_size_mb = DEFAULT_MULTIPART_SIZE_MB

            # Constructing s3cmd for file copy - .add('--recursive ') was added for arching files
            s3copy_cmd = str('aws s3 cp ').__add__('--recursive "').__add__(s3_source_location).__add__('" "').__add__(
                s3_target_location).__add__('"')
            status_message = 'Constructed s3 copy command for execution'
            self.execution_context.set_context({'s3_copy_command': s3copy_cmd})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Execute s3 cmd shell command for file copy
            CommonUtils(self.execution_context).execute_shell_command(s3copy_cmd)
            status_message = 'Executed s3 copy command successfully '
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
