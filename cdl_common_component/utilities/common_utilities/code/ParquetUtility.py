
#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ############################################## Module Information ####################################################
#   Module Name         : ParquetUtility
#   Purpose             : This class is used for parquet conversion using spark
#   Input Parameters    :
#   Output Value        :
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
import re

sys.path.insert(0, os.getcwd())
import traceback
from LogSetup import logger
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from PySparkUtility import PySparkUtility
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame

MODULE_NAME = "ParquetUtility"
SPARK_JOB_SUCCESS_DIR_NAME = '_SUCCESS'


class ParquetUtility(object):
    # Initialization instance of class
    def __init__(self, parent_execution_context=None):
        # Parent execution context can be passed to the utility, in case its not; then instantiate the execution context
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    # ######################################### Initialize spark context ##############################################
    # Purpose   :   This method is used for initialization of spark SQL context
    # Input     :   NA
    # Output    :   Returns sql context object
    # ##################################################################################################################
    def convertToParquet(self, source=None, target=None, exclude_row_id=None, dataset_id=None, delimiter=None,
                         header_flag=None, file_format=None):
        status_message = None
        spark_context = None
        try:
            status_message = "Starting parquet conversion module for source: " + source
            logger.info(status_message, extra=self.execution_context.get_context())
            module_name_for_spark = str(
                MODULE_NAME + "_dataset_" + dataset_id)
            spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,
                                                                                     CommonConstants.HADOOP_CONF_PROPERTY_DICT)
            status_message = 'Fetched spark sql context successful'
            logger.info(status_message, extra=self.execution_context.get_context())

            no_of_re_partitions = CommonUtils().get_dynamic_repartition_value(source)
            status_message = "no. of repartition:" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())

            file_schema = self.get_header(dataset_id,header_flag,pre_landing_location=source,spark_context=spark_context)
            if header_flag == CommonConstants.HEADER_FLAG_PRESENT:

                status_message = "Header is present in dataset:" + str(dataset_id)
                logger.info(status_message, extra=self.execution_context.get_context())

                if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                    status_message = "Reading parquet file as input"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    source_df = spark_context.read.format("parquet").load(source).repartition(no_of_re_partitions)
                    source_df = source_df.select([col(c).cast("string") for c in source_df.columns])

                elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                    batch_id = source.split("/")[-1].replace(CommonConstants.PARTITION_BATCH_ID+"=", '')
                    file_id_tuples = CommonUtils().get_file_ids_by_batch_id(CommonConstants.AUDIT_DB_NAME, batch_id)
                    file_name_path = ""
                    file_id = ""
                    source_df_list=[]
                    for file_id_tuple in file_id_tuples:
                        file_id = file_id_tuple["file_id"]
                        file_name = CommonUtils().get_file_name_from_file_id(file_id)
                        file_name = file_name.split("/")[-1]
                        file_name_path = str(source) + "/" + CommonConstants.PARTITION_FILE_ID + "=" + str(file_id) + "/" + str(file_name)
                        source_df_temp = spark_context.read.format('com.crealytics.spark.excel').option\
                        ('header', 'true').schema(file_schema).load(file_name_path).repartition(no_of_re_partitions)
                    # source_df = spark_context.read.format('com.crealytics.spark.excel').option \
                    #     ('header', 'true').option("sheetName", "Sheet1").schema(file_schema).load(file_name_path).repartition(no_of_re_partitions)
                        source_df_temp = source_df_temp.withColumn(CommonConstants.PARTITION_FILE_ID, F.lit(file_id))
                        source_df_list.append(source_df_temp)
                    source_df = reduce(DataFrame.unionAll, source_df_list)

                else:
                    if CommonConstants.MULTILINE_READ_FLAG == "Y":
                        source_df = spark_context.read.format("csv").option("multiline", "true").\
                            option("header","true").option("delimiter", delimiter).option("escape", "\"").\
                            schema(file_schema).load(source).repartition(no_of_re_partitions)

                    else:
                        source_df = spark_context.read.format("csv").option("header", "true").\
                            option("delimiter", delimiter).option("escape", "\"").schema(
                            file_schema).load(source).repartition(no_of_re_partitions)

                status_message = "Schema added to file for dataset:" + str(dataset_id)
                logger.info(status_message, extra=self.execution_context.get_context())

            elif header_flag == CommonConstants.HEADER_FLAG_ABSENT:

                status_message = "Header is absent in dataset:" + str(dataset_id)
                logger.info(status_message, extra=self.execution_context.get_context())

                if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                        logger.debug("Reading parquet file as input")
                        source_df = spark_context.read.format("parquet").load(source).repartition(no_of_re_partitions)
                        source_df = source_df.select([col(c).cast("string") for c in source_df.columns])

                elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                    batch_id = source.split("/")[-1].replace(CommonConstants.PARTITION_BATCH_ID+"=", '')
                    file_id_tuples = CommonUtils().get_file_ids_by_batch_id(CommonConstants.AUDIT_DB_NAME, batch_id)
                    file_name_path = ""
                    file_id = ""
                    source_df_list=[]
                    for file_id_tuple in file_id_tuples:
                        file_id = file_id_tuple["file_id"]
                        file_name = CommonUtils().get_file_name_from_file_id(file_id)
                        file_name = file_name.split("/")[-1]
                        file_name_path = str(source) + "/" + CommonConstants.PARTITION_FILE_ID + "=" + str(file_id) + "/" + str(file_name)
                        source_df_temp = spark_context.read.format('com.crealytics.spark.excel').option\
                        ('header', 'true').schema(file_schema).load(file_name_path).repartition(no_of_re_partitions)
                    # source_df = spark_context.read.format('com.crealytics.spark.excel').option \
                    #     ('header', 'true').option("sheetName", "Sheet1").schema(file_schema).load(file_name_path).repartition(no_of_re_partitions)
                        source_df_temp = source_df_temp.withColumn(CommonConstants.PARTITION_FILE_ID, F.lit(file_id))
                        source_df_list.append(source_df_temp)
                    source_df = reduce(DataFrame.unionAll, source_df_list)

                else:
                    if CommonConstants.MULTILINE_READ_FLAG == "Y":
                        source_df = spark_context.read.format("csv").option("multiline", "true").option("header",
                                                                                                        "false").option(
                            "delimiter",
                            delimiter).schema(
                            file_schema).load(source).repartition(no_of_re_partitions)

                    else:
                        source_df = spark_context.read.format("csv").option("header", "false").option("delimiter",
                                                                                                      delimiter).schema(
                            file_schema).load(source).repartition(no_of_re_partitions)

                status_message = "Schema added to file for dataset:" + str(dataset_id)
                logger.info(status_message, extra=self.execution_context.get_context())

            elif header_flag == CommonConstants.VARIABLE_FILE_FORMAT_TYPE_KEY:

                status_message = "Header is set to V in dataset master:" + str(dataset_id)
                logger.info(status_message, extra=self.execution_context.get_context())

                if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                        logger.debug("Reading parquet file as input")
                        source_df = spark_context.read.format("parquet").load(source).repartition(no_of_re_partitions)
                        source_df = source_df.select([col(c).cast("string") for c in source_df.columns])

                elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                    batch_id = source.split("/")[-1].replace(CommonConstants.PARTITION_BATCH_ID+"=", '')
                    file_id_tuples = CommonUtils().get_file_ids_by_batch_id(CommonConstants.AUDIT_DB_NAME, batch_id)
                    file_name_path = ""
                    file_id = ""
                    source_df_list=[]
                    for file_id_tuple in file_id_tuples:
                        file_id = file_id_tuple["file_id"]
                        file_name = CommonUtils().get_file_name_from_file_id(file_id)
                        file_name = file_name.split("/")[-1]
                        file_name_path = str(source) + "/" + CommonConstants.PARTITION_FILE_ID + "=" + str(file_id) + "/" + str(file_name)
                        source_df_temp = spark_context.read.format('com.crealytics.spark.excel').option\
                        ('header', 'true').schema(file_schema).load(file_name_path).repartition(no_of_re_partitions)
                    # source_df = spark_context.read.format('com.crealytics.spark.excel').option \
                    #     ('header', 'true').option("sheetName", "Sheet1").schema(file_schema).load(file_name_path).repartition(no_of_re_partitions)
                        source_df_temp = source_df_temp.withColumn(CommonConstants.PARTITION_FILE_ID, F.lit(file_id))
                        source_df_list.append(source_df_temp)
                    source_df = reduce(DataFrame.unionAll, source_df_list)

                else:
                    if CommonConstants.MULTILINE_READ_FLAG == "Y":
                        source_df = spark_context.read.format("csv").option("multiline", "true").option("header",
                                                                                                        "true").option(
                            "delimiter",
                            delimiter).option("escape","\"").schema(
                            file_schema).load(source).repartition(no_of_re_partitions)

                    else:
                        source_df = spark_context.read.format("csv").option("header", "true").option("delimiter",
                                                                                                     delimiter).option("escape","\"").schema(
                            file_schema).load(source).repartition(no_of_re_partitions)

                status_message = "Pre landing schema added to file for dataset:" + str(dataset_id)
                logger.info(status_message, extra=self.execution_context.get_context())
                temp_parquet_target_lcation= target + "_temp_parquet"
                source_df.write.mode("overwrite").format("parquet").partitionBy(CommonConstants.PARTITION_FILE_ID).save(
                    temp_parquet_target_lcation)
                status_message = "Parquet Data frame written at landing location"
                logger.info(status_message, extra=self.execution_context.get_context())
                column_metadata_schema = self.get_header(dataset_id= dataset_id,header_flag=CommonConstants.HEADER_FLAG_PRESENT)
                source_df = spark_context.read.format("parquet").option("header", "true").option("delimiter",delimiter).\
                    schema(column_metadata_schema).load(temp_parquet_target_lcation)
                source_df.show(10, False)
                source_df1= source_df
                source_df1.write.mode("overwrite").format("parquet").partitionBy(CommonConstants.PARTITION_FILE_ID).save(
                    target)

            else:
                status_message = "Inappropriate header flag value"
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            if exclude_row_id.lower() == CommonConstants.ROW_ID_EXCLUSION_IND_YES.lower():
                status_message = "Not Appending ROW ID to the file since ROW_ID_EXCLUSION_FLAG is set"
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                status_message = "Appending ROW ID to the file since ROW_ID_EXCLUSION_FLAG is NOT set"
                source_df = source_df.withColumn("row_id", monotonically_increasing_id())

            source_df.write.mode("overwrite").format("parquet").partitionBy(CommonConstants.PARTITION_FILE_ID).save(
                target)
            logger.info(status_message, extra=self.execution_context.get_context())
            # Logging completion of file copy
            status_message = 'Completed file copy to path :' + target
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + " ERROR MESSAGE: " + \
                    str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e
        finally:
            status_message = "In Finally block for Parquet Utility"
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "In Finally block for Parquet Utility Noe Closing Spark Context"
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Removing Temporary directory created for Parquet Dataframe"
            logger.info(status_message, extra=self.execution_context.get_context())
            hadoop_rm_command = "hadoop fs -rm -r -f " + target + "_temp_parquet"
            remove_command_result = CommonUtils().execute_shell_command(hadoop_rm_command)
            if remove_command_result:
                status_message = " Successfully removed temporary Parquet directory"
                logger.info(status_message, extra=self.execution_context.get_context())
            if spark_context:
                spark_context.stop()
            status_message = "Spark Context Closed In Parquet Utility Finally Block"
            logger.info(status_message, extra=self.execution_context.get_context())

    def get_header(self, dataset_id,header_flag,pre_landing_location = None,spark_context=None):
        if header_flag == CommonConstants.VARIABLE_FILE_FORMAT_TYPE_KEY:
            status_message = "Getting schema from pre-landing file path as " \
                             "header flag is set to V"
            logger.info(status_message, extra=self.execution_context.get_context())
            pre_landing_df = spark_context.read.format("csv").\
                option("header", "true").\
                option("delimiter",delimiter)\
                .load(pre_landing_location)
            pre_landing_df_columns_list = pre_landing_df.columns
            pre_landing_df_columns_list_upper_case = [x.upper() for x in pre_landing_df_columns_list]
            pre_landing_df_replace_spaces = [x.replace(" ", "_") for x in pre_landing_df_columns_list_upper_case]
            pre_landing_df_special_characters_removed = [
                re.sub("[^A-Za-z0-9_ ]+", '', x) for x in
                pre_landing_df_replace_spaces]
            column_names_list = pre_landing_df_special_characters_removed

        elif header_flag == CommonConstants.HEADER_FLAG_PRESENT or header_flag == CommonConstants.HEADER_FLAG_ABSENT:
            status_message = "Starting to prepare query for fetching column names for dataset id:" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            fetch_columns_query = "Select " + CommonConstants.COLUMN_NAME + " from " + self.audit_db + "." + \
                                  CommonConstants.COLUMN_METADATA_TABLE + " where dataset_id='" + str(
                dataset_id) + "' order by " + CommonConstants.COLUMN_SEQUENCE_NUMBER
            column_name_resultset = MySQLConnectionManager().execute_query_mysql(fetch_columns_query, False)
            status_message = "Columns fetched", column_name_resultset
            logger.info(status_message, extra=self.execution_context.get_context())
            column_names_list = []
            for column_result in column_name_resultset:
                column_names_list.append(column_result['column_name'])
        schema = StructType([
            StructField(column_name, StringType(), True) for (column_name) in column_names_list
        ])
        status_message = "Schema constructed", schema
        logger.info(status_message, extra=self.execution_context.get_context())
        return schema

if __name__ == '__main__':
    source = sys.argv[1]
    target = sys.argv[2]
    exclude_row_id = sys.argv[3]
    dataset_id = sys.argv[4]
    delimiter = sys.argv[5]
    header_flag = sys.argv[6]
    file_format = sys.argv[7]
    ParquetUtility().convertToParquet(source, target, exclude_row_id, dataset_id, delimiter, header_flag, file_format)
