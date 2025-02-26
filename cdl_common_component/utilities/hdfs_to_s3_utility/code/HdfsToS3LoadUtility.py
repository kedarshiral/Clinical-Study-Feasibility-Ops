"""This module is the loading Utility for Hdfs to S3."""
#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'ZS ASSOCIATES'

# All the package imports
import traceback
import os
import sys
from ExecutionContext import ExecutionContext
from LogSetup import logger
from PySparkUtility import PySparkUtility
sys.path.insert(0, os.getcwd())


# All the global constants declarations
MODULE_NAME = 'HdfsToS3LoadUtility'
PROCESS_NAME = 'File Copy from HDFS to S3'
READ_FORMAT = "parquet"
WRITE_FORMAT = "parquet"


# This class contains all the necessary functions for the S3ToHdfsLoadUtility
class HdfsToS3LoadUtility(object):
    """This class contains all the necessary functions for the S3ToHdfsLoadUtility"""

    # initialization of class S3ToHdfsLoadUtility
    def __init__(self, execution_context=None):
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

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    # ############################################ HDFS copy to S3_arguments #######################
    # Purpose   :   This method is used to copy files from HDFS to S3 using spark
    # Input     :   HDFS file path
    # Output    :   NA
    # ##############################################################################################
    def copy_from_hdfs_to_s3(self, hdfs_file_path=None, target_s3_path=None, write_mode=None,
                             spark_context=None):
        """This method is used to copy files from HDFS to S3 using spark."""
        try:
            status_message = "Starting s3 file copy module from " + hdfs_file_path
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get spark and sql context
            if spark_context is None:
                spark_context = PySparkUtility().get_spark_context()
                status_message = 'Fetched spark sql context'
                logger.info(status_message, extra=self.execution_context.get_context())

            # Set the number of partitions for data frame
            #if file_partition_count is None:
            #    file_partition_count = 1
            #self.execution_context.set_context({"Partition count": file_partition_count})
            #.debug(status_message, extra=self.execution_context.get_context())

            # Write file to target s3 location
            status_message = 'Starting file copy to S3'
            hdfs_file_df = spark_context.read.format(READ_FORMAT).load(hdfs_file_path)
            hdfs_file_df.write.mode(write_mode).format(WRITE_FORMAT).save(target_s3_path)
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Logging completion of file copy
            status_message = 'Completed file copy to s3 path'+str(target_s3_path)
            logger.info(status_message, extra=self.execution_context.get_context())

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e



