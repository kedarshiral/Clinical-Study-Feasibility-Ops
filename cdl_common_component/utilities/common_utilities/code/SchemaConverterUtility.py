"""
Module for Schema Converter Utility
"""
__author__ = 'ZS Associates'

# ####################################################Module Information###########################
#  Module Name         : Schema Converter Utility
#  Purpose             : This module will perform the schema conversion of file
#  Input Parameters    : file location and schema dictionary
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     : 29/05/2018
#  Last changed by     : Amal Kurup
#  Reason for change   : Casting string data to timestamp in pyspark, using input date format in
#                        column metadata table
# #################################################################################################

# Library and external modules declaration
import traceback
import os
import sys
from pyspark.sql.functions import *
from LogSetup import logger
from ExecutionContext import ExecutionContext
sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "Schema Coversion"
CSV_FORMAT = "com.databricks.spark.csv"


class SchemaConverterUtility:
    # Default constructor
    """
    Class for Schema Converter Utility
    """
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def perform_schema_conversion(self, d_f=None, column_metadata=None):
        """
        Method to perform schema conversion
        :param d_f:
        :param column_metadata:
        :return:
        """
        status_message = ""
        #spark_context = None
        try:
            status_message = "Starting staging process to filter out DQM records"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if not d_f:
                raise Exception("Dataframe for schema conversion is empty")

            if not column_metadata:
                raise Exception("Column metadata info is missing")

            for x_iterator in column_metadata:
                column_name = x_iterator['column_name']
                column_type = x_iterator['column_data_type']
                if column_type == "timestamp":
                    tmstmp_format = x_iterator['column_date_format']
                    if not tmstmp_format:
                        raise Exception("Timestamp format is not provided for column " +
                                        str(column_name))
                    d_f = d_f.withColumn(column_name, to_timestamp(d_f[column_name],
                                                                   str(tmstmp_format)).
                                         cast('timestamp'))
                else:
                    d_f = d_f.withColumn(column_name, d_f[column_name].cast(column_type))

            return d_f
        except Exception as err:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"function_status": 'FAILED', "traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise err
