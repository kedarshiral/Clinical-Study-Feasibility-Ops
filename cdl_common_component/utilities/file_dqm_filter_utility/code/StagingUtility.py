
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Staging Utility
#  Purpose             :   This module will perform the filtering of critical dqm errors from landing records
#  Input Parameters    :   landing file location, dqm error file location, post dqm output file location
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   14th December 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   Staging Utility Development
# ######################################################################################################################

# Library and external modules declaration
import traceback
import os
import sys

sys.path.insert(0, os.getcwd())
from ExecutionContext import ExecutionContext
from LogSetup import logger
from PySparkUtility import PySparkUtility
from CommonUtils import CommonUtils
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql.functions import col, upper, lower
import re
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager

# all module level constants are defined here
MODULE_NAME = "Staging"
CSV_FORMAT = "com.databricks.spark.csv"


class StagingUtility:
    # Default constructor
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def perform_dqm_filter(self, landing_file_path=None, dqm_error_file_path=None):
        status_message = ""
        spark_context = None
        try:
            status_message = "Starting staging process to filter out DQM records"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())
            module_name_for_spark = str(
                MODULE_NAME)
            spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,
                                                                                     CommonConstants.HADOOP_CONF_PROPERTY_DICT)

            landing_df = spark_context.read.format("parquet").option("inferSchema", "true"). \
                option("header", "true").option("delimiter", "|").load(landing_file_path)
            error_location_empty_flag = CommonUtils().check_dqm_error_location(dqm_error_file_path)
            if error_location_empty_flag:
                return landing_df
            else:
                dqm_error_df = spark_context.read.format("parquet").option("inferSchema", "true"). \
                    option("header", "true").option("delimiter", "|").load(dqm_error_file_path)
                cond = col('criticality') == "C"

                dqm_file_error_df = dqm_error_df.where(cond)
                if dqm_file_error_df.rdd.isEmpty():
                    return landing_df
                col_a = str("a." + CommonConstants.PARTITION_FILE_ID)
                col_b = str("b." + CommonConstants.PARTITION_FILE_ID)
                dqm_filtered_data_df = landing_df.alias('a').join(dqm_file_error_df.alias('b'),
                                                                  [(col(col_a) == col(col_b)),
                                                                   (col('a.row_id') == col('b.row_id'))],
                                                                  how="left_outer").select("a.*",
                                                                                           "b.criticality").filter(
                    col("criticality").isNull()).drop('criticality')
            return dqm_filtered_data_df
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"function_status": 'FAILED', "traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    def perform_data_stitching_type_1(self, unique_column_names, source_location, current_df, batch_id=None,
                                      dataset_id=None):
        """
            Purpose :   This method performs data stitching of Type 1
            Input   :   Stitch Key Columns, Source Location, Current Df , Batch Id
            Output  :   Stitched DF
        """
        status_message = ""
        try:
            status_message = "Starting to execute perfor data stitching based on column name list:" + str(
                unique_column_names)
            logger.debug(status_message, extra=self.execution_context.get_context())
            module_name_for_spark = str(MODULE_NAME + "stitching")
            spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,
                                                                                     CommonConstants.HADOOP_CONF_PROPERTY_DICT)
            status_message = "Source location for data stitching is:" + source_location
            logger.info(status_message, extra=self.execution_context.get_context())
            history_df = spark_context.read.format("parquet").load(source_location)
            unique_column_list = unique_column_names.split(",")
            pk_unique_column_list = ["pk_" + s for s in unique_column_list]
            unique_column_dict = dict(zip(unique_column_list, pk_unique_column_list))
            for column, pk_column in unique_column_dict.items():
                current_df = current_df.withColumn(pk_column,
                                                   when(col(column).isNull(), lit("0")).otherwise(col(column)))
                current_df = current_df.fillna({pk_column: 0})
                history_df = history_df.withColumn(pk_column,
                                                   when(col(column).isNull(), lit("0")).otherwise(col(column)))
                history_df = history_df.fillna({pk_column: 0})
            # Prepare dataframe of new/updated records
            updated_record_df = current_df.join(history_df, [lower(trim(history_df[f])) == lower(trim(current_df[s])) for (f, s) in
                                                             zip(pk_unique_column_list, pk_unique_column_list)],
                                                "left_outer").select(current_df["*"])
            final_where_condition = ""
            new_unique_column_list = [None] * len(pk_unique_column_list)
            for index in range(0, len(pk_unique_column_list)):
                # Renaming unique columns for join condition
                new_unique_column_list[index] = pk_unique_column_list[index] + "_tmp"
                # Renaming columns associated with current dataframe
                current_df = current_df.withColumnRenamed(pk_unique_column_list[index],
                                                          pk_unique_column_list[index] + "_tmp")
                # creating where condition with renamed columns
                initial_where_condition = pk_unique_column_list[index] + "_tmp" + " is null"
                if index != (len(pk_unique_column_list) - 1):
                    initial_where_condition = initial_where_condition + " and "
                final_where_condition = final_where_condition + initial_where_condition

            status_message = "Final where condition:" + str(final_where_condition)
            logger.info(status_message, extra=self.execution_context.get_context())
            # Prepare dataframe of history record not present in current(delta)
            new_record_df = history_df.alias('a').join(current_df.alias('b'),
                                                       [lower(trim(history_df[f])) == lower(trim(current_df[s])) for (f, s) in
                                                        zip(pk_unique_column_list, new_unique_column_list)],
                                                       how='left_outer').where(final_where_condition).select(
                history_df["*"])
            print(updated_record_df.printSchema())
            print(new_record_df.printSchema())
            new_record_df1 = new_record_df.drop('pt_batch_id')
            print(new_record_df1.printSchema())
            stitched_df = updated_record_df.union(new_record_df1)
            stitched_df = stitched_df.distinct()

            for pk_column in pk_unique_column_list:
                stitched_df = stitched_df.drop(pk_column)

            status_message = "Completed executing data stitching based on column name list:" + str(
                unique_column_names)
            logger.debug(status_message, extra=self.execution_context.get_context())
            # file_id_list = CommonUtils().get_list_of_file_id_by_batch(batch_id)
            batch_id_list = CommonUtils().get_list_of_batch_id_by_dataset(dataset_id)
            if len(batch_id_list) > 1:
                raise Exception("One run should have only one batch ID")
            batch_id = batch_id_list[0]['batch_id']
            stitched_df = stitched_df.drop(CommonConstants.PARTITION_BATCH_ID)

            return stitched_df
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"function_status": 'FAILED', "traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

