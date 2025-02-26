#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import re
import traceback
from functools import reduce
from pyspark.sql.functions import *
from ExecutionContext import ExecutionContext
from LogSetup import logger
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from ConfigUtility import JsonConfigUtility
from pyspark.sql.types import *
from datetime import datetime


# all module level constants are defined here
MODULE_NAME = "PRE-DQM Utility"
"""
Module Name         :   Pre DQM Utility
Purpose             :   This module will perform the Pre DQM Standardization on columns.
Input Parameters    :   upper_conversion_list, lower_conversion_list,
                        blank_converion_list, left_padding_list, right_padding_list,
                        trim_list, sub_string_list, replace_string_list,
                        zip_extractor_list, pre_dqm_location,
                        file_id, batch_id, spark_context
Output Value        :   returns the status SUCCESS or FAILURE
Pre-requisites      :   landing locaion & pre dqm locations should be present
Last changed on     :   30th May 2018
Last changed by     :   Pushpendra Singh
Reason for change   :   Pre - DQM Bug Fixing
"""


class PreDQMStandardizationUtility:
    """
    This class is utility to apply pre-dqm standardization
    """

    def __init__(self, process_name=None, workflow_name=None, process_id=None, workflow_id=None, dataset_id=None,
                 batch_id=None, execution_context=None):
        self.execution_context = ExecutionContext()
        # self.execution_context.set_context({"dataset_id": DATASET_ID})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        if execution_context is None:
            self.execution_context.set_context({"module_name": MODULE_NAME})
        else:
            self.execution_context = execution_context

        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.process_name = process_name
        self.workflow_name = workflow_name
        self.process_id = process_id
        self.workflow_id = workflow_id
        self.dataset_id = dataset_id
        self.batch_id = batch_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def perform_pre_dqm_standardization(self, file_df=None, upper_conversion_list=None, lower_conversion_list=None,
                                        blank_converion_list=None, left_padding_list=None, right_padding_list=None,
                                        trim_list=None, sub_string_list=None, replace_string_list=None,
                                        zip_extractor_list=None,date_conversion_list=None ,pre_dqm_location=None
                                        , batch_id=None, spark_context=None):

        """
        Purpose   :   This method is used to perform standardization & write backk the dataframe to pre-dqm location
        Input     :   upper_conversion_list, lower_conversion_list,
                      blank_converion_list, left_padding_list, right_padding_list,
                      trim_list, sub_string_list, replace_string_list,
                      zip_extractor_list, pre_dqm_location,
                      batch_id, spark_context
        Output    :   Returns status(True/Exception)
        """

        status_message = ""
        try:
            status_message = "Starting function to perform Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.info(status_message, extra=self.execution_context.get_context())
            file_df.persist()

            status_message = "Starting Blank  Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if blank_converion_list is not None:
                for blank_conversion in blank_converion_list:
                    c_name = blank_conversion
                    c_name = re.sub('\s+', ' ', c_name).strip()
                    function_name = 'blank_conversion'
                    function_params = 'No Params'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }
                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name, when((col(c_name) != '') & (col(c_name) != ' '),
                                                               col(c_name)).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)

                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Trim Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if trim_list is not None:

                for trim_c_name in trim_list:
                    c_name = trim_c_name
                    function_name = 'trim'
                    function_params = "No Params"
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }

                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name, when(col(c_name) != "", trim(col(c_name))).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)
                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Upper Case Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if upper_conversion_list is not None:
                for upper_c_name in upper_conversion_list:
                    c_name = upper_c_name
                    function_name = 'type_conversion'
                    function_params = '{"type":"Upper"}'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }
                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name,
                                                     when(col(c_name) != "", upper(col(c_name))).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)

                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Lower Case Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if lower_conversion_list is not None:
                for lower_c_name in lower_conversion_list:
                    c_name = lower_c_name
                    function_name = 'type_conversion'
                    function_params = '{"type":"Lower"}'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }
                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name,
                                                     when(col(c_name) != "", lower(col(c_name))).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)
                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Left Padding Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

## Date Conversion Utility

            if date_conversion_list is not None:
                for format_date in date_conversion_list:
                    c_name = list(format_date.keys())[0]
                    function_name = 'date_conversion'
                    source_format = format_date[c_name]['src']
                    target_format = format_date[c_name]['tgt']
                    function_params = '{"src":"' + source_format + '","tgt":"' + target_format + '"}'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }
                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        func = udf(lambda x: None if x == None or x == '' else datetime.strptime(x, source_format), DateType())
                        file_df = file_df.withColumn(c_name,when(length(trim((col(c_name))))==4,to_date(col(c_name),"yyyy")).when(length(trim((col(c_name))))==7,to_date(col(c_name),"yyyy-MM")).otherwise(col(c_name)))
                        file_df = file_df.withColumn(c_name, date_format(func(col(c_name)), target_format))

                        CommonUtils().update_pre_dqm_success_status(log_info)

                    except Exception as exception:
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception


            status_message = "Starting Date Conversion Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if left_padding_list is not None:
                for left_padding in left_padding_list:
                    c_name = list(left_padding.keys())[0]
                    function_name = 'left_padding'

                    pad_val = left_padding[c_name]['padding_value']
                    pad_len = int(left_padding[c_name]['length'])
                    function_params = '{"padding_value":"' + pad_val + '","length":"' + str(pad_len) + '"}'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }

                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name, when(col(c_name) != "",
                                                                  lpad(col(c_name), pad_len, pad_val)).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)
                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Right Padding Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if right_padding_list is not None:
                for right_padding in right_padding_list:
                    c_name = list(right_padding.keys())[0]
                    function_name = 'right_padding'
                    pad_val = right_padding[c_name]['padding_value']
                    pad_len = int(right_padding[c_name]['length'])
                    function_params = '{"padding_value":"' + pad_val + '","length":"' + str(pad_len) + '"}'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }

                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name, when(col(c_name) != "",
                                                                  rpad(col(c_name), pad_len, pad_val)).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)
                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Substring Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if sub_string_list is not None:
                for sub_str in sub_string_list:
                    function_name = 'sub_string'
                    c_name = list(sub_str.keys())[0]

                    start_index = int(sub_str[c_name]['start_index'])
                    sub_string_length = int(sub_str[c_name]['length'])

                    function_params = '{"start_index":"' + str(start_index) + '","length":"' + str(sub_string_length) + '"}'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }
                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name, when(col(c_name) != "", substring(col(c_name), start_index,
                                                                                               sub_string_length)).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)
                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Replace String Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if replace_string_list is not None:
                for replace_str in replace_string_list:
                    c_name = list(replace_str.keys())[0]
                    to_replace = replace_str[c_name]['to_replace']
                    value = replace_str[c_name]['value']
                    function_name = 'replace'
                    function_params = '{"to_replace":"' + to_replace + '","length":"' + value + '"}'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }

                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name, when(col(c_name) != "",
                                                                  regexp_replace(col(c_name), to_replace,
                                                                                 value)).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)
                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting Zip Extractor Pre-DQM Standardization"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if zip_extractor_list is not None:
                for zip_extractor in zip_extractor_list:
                    c_name = zip_extractor
                    cond_1 = col(c_name) != ""
                    cond_2 = col(c_name).contains("-")
                    cond_1_2 = cond_1 & cond_2
                    function_name = 'zip_extractor'
                    function_params = 'No Params'
                    log_info = {
                        CommonConstants.PROCESS_NAME: self.process_name,
                        CommonConstants.DATASET_ID: self.dataset_id,
                        CommonConstants.BATCH_ID: self.batch_id,
                        'workflow_name': self.workflow_name,
                        CommonConstants.WORKFLOW_ID: self.workflow_id,
                        'process_id': self.process_id,
                        'c_name': c_name,
                        'function_name': function_name,
                        'function_params': function_params
                    }

                    try:
                        CommonUtils().insert_pre_dqm_status(log_info)
                        file_df = file_df.withColumn(c_name,
                                                     when(cond_1_2, split(col(c_name), "-").getItem(0)).when(~(cond_2),
                                                        col(c_name)).otherwise(None))
                        CommonUtils().update_pre_dqm_success_status(log_info)
                    except Exception as exception:
                        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
                        CommonUtils().update_pre_dqm_failure_status(log_info)
                        raise exception

            status_message = "Starting To Write Pre-DQM Standardization Results to Pre-DQM Location"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            if pre_dqm_location:
                file_df.write.mode('overwrite').format('parquet').partitionBy(CommonConstants.PARTITION_FILE_ID).save(pre_dqm_location)
            else:
                raise Exception

            status_message = "Pre-DQM Standardization Results written at Pre-DQM Location"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())

            return True
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") \
                    + " ERROR MESSAGE: " + \
                    str(traceback.format_exc() + str(exception))
            logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())

            self.execution_context.set_context({"function_status": 'FAILED', "traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception
        finally:
            status_message = "In Pre-DQm Standardization Utility Finally Block"
            logger.debug(status_message, extra=self.execution_context.get_context())
