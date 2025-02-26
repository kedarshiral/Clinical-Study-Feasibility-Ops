#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ############################################## Module Information ####################################################
#   Module Name         : PySparkUtility
#   Purpose             : This class is used for initialization of spark environment
#   Input Parameters    :
#   Output Value        : spark context, sql context,
#  Dependencies         :
#  Predecessor Module   : None
#  Successor Module     : None
#  Pre-requisites       : All the dependent libraries should be present in same environment from where the module would
#                         execute.
#   Last changed on     : 23rd November 2017
#   Last changed by     : Amal Kurup
#   Reason for change   :
# ######################################################################################################################

# Library and external modules declaration
import os
import sys

sys.path.insert(0, os.getcwd())
import traceback
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from ConfigUtility import JsonConfigUtility
from pyspark.sql import functions as F

MODULE_NAME = "PySparkUtility"


class PySparkUtility(object):
    # Initialization instance of class
    def __init__(self, parent_execution_context=None):
        # Parent execution context can be passed to the utility, in case its not; then instantiate the execution context
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context

        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)

    # ######################################### Initialize spark context ##############################################
    # Purpose   :   This method is used for initialization of spark SQL context
    # Input     :   NA
    # Output    :   Returns sql context object
    # ##################################################################################################################
    def get_spark_context(self, module_name=None, hadoop_conf_dict=None,spline_enable_flag=None):
        spark_context = None
        status_message = None
        spline_enable_flag = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "spline_enable_flag"])
        cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "cluster_mode"])
        try:
            status_message = "Starting to get fetch spark context"
            logger.info(status_message, extra=self.execution_context.get_context())
            os.environ[CommonConstants.HADOOP_CONF_DIR] = CommonConstants.HADOOP_CONF_PATH
            os.environ[CommonConstants.HADOOP_HOME_DIR] = CommonConstants.HADOOP_HOME_PATH

            if spline_enable_flag == 'Y':
                sc = SparkContext(appName=MODULE_NAME)
                # sc = spark_session.sparkContext
                jvm = sc._jvm
                sb = jvm.org.apache.spark.sql.SparkSession.builder()
                ss = sb.getOrCreate()
                demo = jvm.com.spline.Demo
                ssl = demo.setSparkListener(ss)
                spark_with_listener = SparkSession(sc, ssl)
                status_message = "Completed function to fetch spark context with spline enabled"
                logger.info(status_message, extra=self.execution_context.get_context())
                spark_with_listener = self.set_s3_connection_for_spark(spark_with_listener,hadoop_conf_dict)
                return spark_with_listener

            if module_name is None:
                spark_session = SparkSession.builder.appName(MODULE_NAME).enableHiveSupport().getOrCreate()
            else:
                spark_session = SparkSession.builder.appName(module_name).enableHiveSupport().getOrCreate()

            spark_session = self.set_s3_connection_for_spark(spark_session,hadoop_conf_dict)
            status_message = "Completed function to fetch spark context"
            logger.info(status_message, extra=self.execution_context.get_context())
            return spark_session
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    # #########################################Initialize sqlcontext #################################################
    # Purpose   :   This method is used for initialization of spark SQL context
    # Input     :   NA
    # Output    :   Returns sql context object
    # #################################################################################################################
    def get_sql_context(self, spark_context=None):
        sql_context = None
        status_message = None
        try:
            status_message = "Starting to get fetch sql context"
            logger.info(status_message, extra=self.execution_context.get_context())
            if spark_context is None:
                spark_context = self.get_spark_context()
            sql_context = SQLContext(spark_context)
            status_message = "Completed function to fetch sql context"
            logger.info(status_message, extra=self.execution_context.get_context())
            return sql_context
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    # #########################################Setting S3 connection details in spark context#########################
    # Purpose   :   This method is used for setting s3 connection details in spark context
    # Input     :   NA
    # Output    :   Returns spark context object
    # #################################################################################################################
    def set_s3_connection_for_spark(self, spark_context=None,hadoop_conf_dict=None):
        sql_context = None
        status_message = None
        cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "cluster_mode"])
        try:
            status_message = "Starting to set s3 connection details in spark context"
            logger.info(status_message, extra=self.execution_context.get_context())
            if spark_context is None:
                spark_context = self.get_spark_context()

            # Get the s3 credentials as per the service accounts
            if cluster_mode != 'EMR':
                pass
                #Non-EMR is not supported anymore
                #s3_cred_params = CommonUtils().fetch_s3_conn_details_for_user()
                ## Set the s3 connection details in hadoop configurations
                #hadoop_conf = spark_context._jsc.hadoopConfiguration()
                #hadoop_conf.set(CommonConstants.HADOOP_FS_REGION_ENDPOINT_PROPERTY,
                #                s3_cred_params.get_configuration([CommonConstants.S3_REGION_ENDPOINT]))
                #hadoop_conf.set(CommonConstants.HADOOP_FS_S3A_IMPL_PROPERTY, CommonConstants.S3A_IMPL)
                #hadoop_conf.set(CommonConstants.MAPREDUCE_OUTPUTCOMMITTER_ALGO_VERSION_PROPERTY,
                #                CommonConstants.MAPREDUCE_OUTPUTCOMMITTER_ALGO_VERSION)
                #hadoop_conf.set(CommonConstants.SPARK_SPECULATION_PROPERTY,
                #                CommonConstants.SPARK_SPECULATION)

            else:
                hadoop_conf = spark_context._jsc.hadoopConfiguration()
                hadoop_conf.set(CommonConstants.HADOOP_FS_REGION_ENDPOINT_PROPERTY,
                                self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                      CommonConstants.S3_REGION_ENDPOINT]))
                status_message = "HADOOP_CONF properties to be set : ", hadoop_conf_dict
                logger.info(status_message, extra=self.execution_context.get_context())
                if hadoop_conf_dict is not None:
                    for prop_name, prop_value in hadoop_conf_dict.items():
                        hadoop_conf.set(prop_name, prop_value)

            status_message = "Completed function to set s3 connection details in spark context"
            logger.info(status_message, extra=self.execution_context.get_context())
            return spark_context
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    def get_file_record_count(self, file=None, header=None):
        spark_context = None
        status_message = None
        try:
            status_message = "Starting to fetch record count for: " + str(file)
            # logger.debug(status_message, extra=self.execution_context.get_context())

            # Get spark and sql context

            spark_context = self.get_spark_context(module_name="CSV_record_count")
            status_message = 'Fetched spark sql context'
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Load file to dataframe and get record count
            status_message = 'Loading file to dataframe'
            if header is not None and header == 'Y' or header == 'V':
                if CommonConstants.MULTILINE_READ_FLAG == "Y":
                    source_df = spark_context.read.format("csv").option("multiline","true").option("header", "true").load(file)

                else:
                    source_df = spark_context.read.format("csv").option("header", "true").load(file)

            else:
                if CommonConstants.MULTILINE_READ_FLAG == "Y":
                    source_df = spark_context.read.format("csv").option("multiline","true").load(file)

                else:
                    source_df = spark_context.read.format("csv").load(file)

            file_record_count = source_df.groupBy([CommonConstants.PARTITION_FILE_ID]).count()
            return file_record_count
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    def get_parquet_record_count(self, file=None):
        spark_context = None
        status_message = None
        try:
            status_message = "Starting to fetch record count for: " + str(file)
            # logger.debug(status_message, extra=self.execution_context.get_context())

            # Get spark and sql context
            spark_context = self.get_spark_context(module_name="Parquet_record_count")
            status_message = 'Fetched spark sql context'
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Load file to dataframe and get record count
            status_message = 'Loading file to dataframe'
            source_df = spark_context.read.format("parquet").load(file)
            file_record_count = source_df.groupBy([CommonConstants.PARTITION_FILE_ID]).count()
            return file_record_count
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    def get_excel_record_count(self, file=None):
        spark_context = None
        status_message = None
        try:
            status_message = "Starting to fetch record count for: " + str(file)
            # logger.debug(status_message, extra=self.execution_context.get_context())

            # Get spark and sql context
            spark_context = self.get_spark_context(module_name="Excel_record_count")
            status_message = 'Fetched spark sql context'
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = file.split("/")[-1].replace(CommonConstants.PARTITION_BATCH_ID+"=", '')
            file_id_tuples = CommonUtils().get_file_ids_by_batch_id(CommonConstants.AUDIT_DB_NAME, batch_id)

            status_message = 'Loading file to dataframe'
            logger.info(status_message, extra=self.execution_context.get_context())

            source_df = None

            for file_id_tuple in file_id_tuples:
                file_id = file_id_tuple["file_id"]
                file_name = CommonUtils().get_file_name_from_file_id(file_id)
                file_name = file_name.split("/")[-1]
                file_name_path = str(file) + "/" + CommonConstants.PARTITION_FILE_ID + "=" + str(file_id) + "/" + str(file_name)
                source_df_temp = spark_context.read.format("com.crealytics.spark.excel").\
                    option("useHeader", "true").load(file_name_path)
                source_df = source_df_temp.withColumn(CommonConstants.PARTITION_FILE_ID, F.lit(file_id))

            file_record_count = source_df.groupBy([CommonConstants.PARTITION_FILE_ID]).count()
            return file_record_count
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e