#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module for S3 Copy Utility
"""
__author__ = 'ZS ASSOCIATES'

# All the package imports
import traceback
import os
import pydoop.hdfs as hdfs
from ExecutionContext import ExecutionContext
from LogSetup import logger
from PySparkUtility import PySparkUtility


# All the global constants declarations
MODULE_NAME = 'S3ToHdfsLoadUtility'
PROCESS_NAME = 'File Copy from S3 to HDFS'
CSV_FORMAT = "com.databricks.spark.csv"
SPARK_JOB_SUCCESS_DIR_NAME = '_SUCCESS'



# This class contains all the necessary functions for the S3ToHdfsLoadUtility
class S3ToHdfsLoadUtility(object):
    """
    Class for S3 to HDFS Load Utility
    """
    # initialization of class S3ToHdfsLoadUtility
    def __init__(self, execution_context=None):
        #method_name = ""  ->Unused variable
        try:
            if not execution_context:
                self.execution_context = ExecutionContext()
                self.execution_context.set_context({"MODULE_NAME": MODULE_NAME,
                                                    "PROCESS_NAME": PROCESS_NAME})
            else:
                self.execution_context = execution_context
                self.execution_context.set_context({"MODULE_NAME": MODULE_NAME})

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exc_error:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exc_error

    # ############################################ S3 copy to HDFS_arguments ######################
    # Purpose   :   This method is used to copy files from S3 to HDFS using spark
    # Input     :   S3 file path
    # Output    :   NA
    # #############################################################################################
    def copy_from_s3_to_hdfs(self, s3_file_path=None, target_hdfs_path=None,
                             file_partition_count=None):
        """
        Method to copy from S3 to HDFS
        :param self:
        :param s3_file_path:
        :param target_hdfs_path:
        :param file_partition_count:
        :return:
        """
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
            hdfs_copy_success_dir = os.path.join(target_hdfs_path.rstrip("/"),
                                                 SPARK_JOB_SUCCESS_DIR_NAME)
            status = "delete path",hdfs_copy_success_dir
            logger.info(status,extra=self.execution_context.get_context())
            hdfs.rmr(hdfs_copy_success_dir)
            status_message = 'Deleted spark job success folder ' + hdfs_copy_success_dir
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Logging completion of file copy
            status_message = 'Completed file copy to hdfs path'
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exc_error:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exc_error
        finally:
            # Stop the spark context
            status_message = "Closing spark context"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if spark_context:
                spark_context.stop()
            status_message = "Successfully closed spark context"
            logger.debug(status_message, extra=self.execution_context.get_context())

