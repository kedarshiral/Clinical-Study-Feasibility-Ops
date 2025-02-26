
#!/usr/bin/python
# -*- coding: utf-8 -*
__author__ = "ZS Associates"
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_cc/browse ) & all static servers.

import re
import os
import copy
import subprocess
import traceback
from datetime import datetime
import SparkStrings
import MCEConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MCEJobExecutor import MCEJobExecutor

MODULE_NAME = "CcMetricEngine"


class CcMetricEngine(object):

    def __init__(self, execution_context=None):
        # initializing variables
        self.dataframe_list = list()
        self.aggregated_dataframe_list = dict()
        self.process_id = None
        self.master_ip_address = None
        self.cycle_id = None
        self.data_date = None
        self.table = None
        self.metric_content = None
        self.metric_column_list = list()
        self.ts = datetime.now().strftime(MCEConstants.FILE_DATETIME_FORMAT)
        try:
            if not execution_context:
                self.EXECUTION_CONTEXT = ExecutionContext()
                self.EXECUTION_CONTEXT.set_context({"module_name": MODULE_NAME})
            else:
                self.EXECUTION_CONTEXT = execution_context
                self.EXECUTION_CONTEXT.set_context({"module_name": MODULE_NAME})
        except Exception as exception:
            error_msg = "ERROR in " + self.EXECUTION_CONTEXT.get_context_param("module_name") + \
                        " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.EXECUTION_CONTEXT.set_context({"traceback": error_msg})
            logger.error(error_msg, extra=self.EXECUTION_CONTEXT.get_context())
            raise exception

    def _replace_params(self, raw_string, params):
        """
        Purpose   :   This function is used to replace variable starting & ending with '$$' in any raw string
        Input     :   raw_string and variable dictionary
        Output    :   Return execution status (Success/Failed) and replaced string
        """
        try:
            status_message = "Start of replace params method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # Sort the dictionary keys in ascending order
            dict_keys = params.keys()

            # Iterate through the dictionary containing parameters for replacements and replace all the variables in the
            # raw string with their values
            for key in dict_keys:
                raw_string = raw_string.replace("$$" + str(key) + "$$", str(params[key]))
            if raw_string.find("$$") > -1:
                for key in dict_keys:
                    raw_string = raw_string.replace("$$" + str(key) + "$$", str(params[key]))
            status_message = "Replaced string:  " + raw_string
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "End of replace params method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: raw_string}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: "",
                    MCEConstants.ERROR_KEY: exception}

    def _generate_spark_code_to_union_dataframes(self):
        """
        Purpose   :   This function is used to execute union and aggregation operation on all the dataframes generated
                      for standard & custom metrics
        Input     :   NA
        Output    :   Spark Code String
        """
        try:
            status_message = "Start of _generate_spark_code_to_union_dataframes method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # extracting spark command string to arrange columns in dataframes
            add_extra_columns_in_dataframes_string = self.metric_content[MCEConstants.SPARK_STR_KEY][
                MCEConstants.SELECT_COLUMN_IN_DATAFRAMES_STRING]

            spark_string_to_get_existing_columns_in_df = ""
            # spark command to iterate over column list
            spark_string_for_loop = """\nfor column in column_lookup_list:"""
            spark_command_for_if_condition = ""
            spark_string_to_select_columns_from_df = ""
            loop_counter = 0
            df_string = ""

            # string to union all standard dfs
            union_all = """\nunionDF =  """

            # iterating over dataframe list
            for df_name in self.dataframe_list:
                spark_string_to_get_existing_columns_in_df += """\n{dataframe}_columns = {dataframe}.columns""".format(
                    dataframe=df_name)
                # adding column as None in dataframes if not present already
                spark_command_for_if_condition += """\n  if column not in {dataframe}_columns:
                                                     \n    {dataframe} = {dataframe}.withColumn(column, lit(None))
                                                  """.format(dataframe=df_name)

                spark_string_to_select_columns_from_df += """\n{dataframe} = {dataframe}.select(select_columns)
                                                        """.format(dataframe=df_name)
                if loop_counter == 0:
                    df_string = df_name
                else:
                    # unionizing all dataframes
                    df_string += ".unionAll({dataframe})".format(dataframe=df_name)
                    loop_counter += 1
                loop_counter += 1
            # concat all the string generated separately
            add_extra_columns_in_dataframes_string += \
                spark_string_to_get_existing_columns_in_df + spark_string_for_loop + spark_command_for_if_condition + \
                spark_string_to_select_columns_from_df

            union_dataframe_str = union_all + df_string

            # registering the union dataframe as a temp table
            register_table = self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.REGISTER_DF_AS_TABLE]

            # aggregation of final dataframe excluding the null values after the union all operation
            final_aggregation_string = self.metric_content[MCEConstants.SPARK_STR_KEY][
                MCEConstants.FINAL_AGGREGATED_DATAFRAME]

            spark_code = \
                add_extra_columns_in_dataframes_string + union_dataframe_str + register_table + final_aggregation_string


            # creating replace params dictionary
            replace_params = {MCEConstants.METRIC_COLUMN_LIST_KEY: self.metric_column_list,
                              MCEConstants.TARGET_TABLE_KEY: self.table,
                              MCEConstants.DATAFRAME_LIST_KEY: self.dataframe_list}

            # calling replace params
            spark_code_replace_params_response = self._replace_params(spark_code, replace_params)

            if spark_code_replace_params_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                status_message = "Error in replace param method: " + str(spark_code_replace_params_response[
                                                                             MCEConstants.ERROR_KEY])
                raise Exception(status_message)

            # checking if replaced string still have $$
            if spark_code_replace_params_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                status_message = "Spark string didn't get replaced : " + str(spark_code_replace_params_response[
                                                                                 MCEConstants.RESULT_KEY])
                raise Exception(status_message)
            logger.debug(spark_code_replace_params_response[MCEConstants.RESULT_KEY],
                         extra=self.EXECUTION_CONTEXT.get_context())


            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: spark_code_replace_params_response[MCEConstants.RESULT_KEY]}

        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}

    def _generate_aggregated_spark_code_to_union_dataframes(self):
        """
        Purpose   :   This function is used to execute union and aggregation operation on all the dataframes generated
                      for standard & custom metrics
        Input     :   NA
        Output    :   Spark Code String
        """
        try:
            status_message = "Start of _generate_spark_code_to_union_dataframes method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            ###########For aggregated templates

            # extracting spark command string to arrange columns in dataframes
            add_extra_columns_in_dataframes_string = self.metric_content[MCEConstants.SPARK_STR_KEY][
                MCEConstants.SELECT_COLUMN_IN_TEMP_DATAFRAMES_STRING]

            spark_string_to_get_existing_columns_in_df = ""
            # spark command to iterate over column list
            spark_string_for_loop = """\nfor column in column_lookup_list:"""
            spark_command_for_if_condition = ""
            spark_string_to_select_columns_from_df = ""
            loop_counter = 0
            df_string = ""

            # string to join all standard dfs
            join_all = """\njoinDF =  """

            # iterating over dataframe list
            temp_counter = 0
            for df_name,join_columns in sorted(self.aggregated_dataframe_list.items(), reverse= True):
                if temp_counter == 0:
                    temp_counter += 1
                else:
                    spark_string_to_get_existing_columns_in_df += """\n{dataframe}_columns = {dataframe}.columns""".format(
                    dataframe=df_name)
                    # adding column as None in dataframes if not present already
                    spark_command_for_if_condition += """\n  if column not in {dataframe}_columns:
                                                     \n    {dataframe} = {dataframe}.withColumn(column, lit(None))
                                                  """.format(dataframe=df_name)
                    temp_counter += 1


                if loop_counter == 0:
                    df_string = target_df = df_name
                else:
                    # unionizing all dataframes
                    df_string += ".join({dataframe}, [{join_columns}] , how='left')".format(dataframe=df_name, join_columns=join_columns)
                    loop_counter += 1
                loop_counter += 1
            # concat all the string generated separately
            add_extra_columns_in_dataframes_string += \
                spark_string_to_get_existing_columns_in_df

            union_dataframe_str = join_all + df_string

            # registering the union dataframe as a temp table
            register_table = self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.REGISTER_TEMPDF_AS_TABLE]

            # aggregation of final dataframe excluding the null values after the union all operation
            final_aggregation_string = self.metric_content[MCEConstants.SPARK_STR_KEY][
                MCEConstants.FINAL_TEMP_AGGREGATED_DATAFRAME]

            aggregated_spark_code = add_extra_columns_in_dataframes_string + union_dataframe_str + register_table + final_aggregation_string

            # creating replace params dictionary
            replace_params = {MCEConstants.METRIC_COLUMN_LIST_KEY: self.metric_column_list,
                              MCEConstants.TARGET_TABLE_KEY: self.table,
                              MCEConstants.DATAFRAME_LIST_KEY: self.dataframe_list}

            spark_code_replace_params_response = self._replace_params(aggregated_spark_code, replace_params)

            if spark_code_replace_params_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                status_message = "Error in replace param method: " + str(spark_code_replace_params_response[
                                                                             MCEConstants.ERROR_KEY])
                raise Exception(status_message)

            # checking if replaced string still have $$
            if spark_code_replace_params_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                status_message = "Spark string didn't get replaced : " + str(spark_code_replace_params_response[
                                                                                 MCEConstants.RESULT_KEY])
                raise Exception(status_message)
            logger.debug(spark_code_replace_params_response[MCEConstants.RESULT_KEY],
                         extra=self.EXECUTION_CONTEXT.get_context())

            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: spark_code_replace_params_response[MCEConstants.RESULT_KEY]}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}


    def _standard_metric_generation(self, standard_checks_list):
        """
        Purpose   :   This function is used to execute standard metric
        Input     :   standard_checks_list
        Output    :   Return execution status (Success/Failed)
        """
        try:
            status_message = "Start of _standard_metric_generation method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # defining variables
            count = 0
            base_str = ""
            start_str = ""
            spark_str = ""
            template_params = {}

            # iterating over each standard metric
            for metric in standard_checks_list:
                # extracting source table name
                source_table = metric.get(MCEConstants.SOURCE_TABLE_KEY)

                # extracting optional filter

                optional_filters = metric.get(MCEConstants.OPTIONAL_FILTER_KEY)

                # extracting metric column name
                metric_id = metric.get(MCEConstants.METRIC_COLUMN_NAME)

                # extract template name
                template = metric[MCEConstants.METRIC_TEMPLATE_NAME_KEY]

                # extract template parameters
                template_params = copy.deepcopy(metric[MCEConstants.TEMPLATE_PARAM_KEY])

                # extract metric type
                metric_type = metric[MCEConstants.METRIC_TYPE_KEY]

                # extract metric data type
                metric_data_type = metric[MCEConstants.METRIC_DATA_TYPE_KEY]

                status_message = "Metric type: " + str(metric_type)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Metric info:" + str(metric)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template name:" + template
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template params:" + str(template_params)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Updating templates_params with necessary keys"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                # adding environment key & column name key in metric info dictionary
                template_params.update({MCEConstants.COLUMN_NAME_KEY: metric_id,
                                        MCEConstants.PARAMETERS_KEY: metric[MCEConstants.TEMPLATE_PARAM_KEY],
                                        MCEConstants.SOURCE_TABLE_KEY: source_table,
                                        MCEConstants.OPTIONAL_FILTER_KEY : optional_filters,
                                        MCEConstants.CYCLE_ID: self.cycle_id,
                                        MCEConstants.METRIC_TYPE_KEY: metric_type,
                                        MCEConstants.METRIC_DATA_TYPE_KEY: metric_data_type,
                                        MCEConstants.DATA_DATE: self.data_date.strip(),
                                        MCEConstants.METRIC_COLUMN_LIST_KEY: self.metric_column_list
                                        }
                                       )

                # checking if it is first iteration then add spark string to create df on input table
                # add import strings also
                if count == 0:
                    start_str = self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.IMPORT_STR_KEY] + \
                                self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.REGISTER_DF_KEY]

                # checking if it is active metric
                if str(metric[MCEConstants.ACTIVE_INDICATOR_KEY]) == "Y":
                    # fetching template path for standard metric
                    template_path = os.path.join(os.getcwd(), MCEConstants.STANDARD_TEMPLATE_FOLDER_NAME_KEY,
                                                 template + ".py")
                    status_message = "Template path: " + str(template_path)
                    logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                    # reading template content for standard metric
                    fp = open(template_path).read()

                    # checking if optional filters are given
                    # if given: then add spark command to filter data
                    if metric.get(MCEConstants.OPTIONAL_FILTER_KEY) is not None and metric.get(
                            MCEConstants.OPTIONAL_FILTER_KEY).strip(" ") != "":
                        filter_data_condition = self.metric_content[MCEConstants.SPARK_STR_KEY][
                            MCEConstants.FILTER_CONDITION_KEY]
                        fp = fp + filter_data_condition

                    # calling replace method to replace variables in metric template
                    replace_param_response = self._replace_params(fp, template_params)

                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced : " + str(replace_param_response[
                                                                                         MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                    start_str = start_str + "\n" + str(replace_param_response[MCEConstants.RESULT_KEY])

                    # calling replace params to replace variables in generate_withcolumn string
                    replace_param_response = self._replace_params(str(self.metric_content[MCEConstants.SPARK_STR_KEY][
                                                                          MCEConstants.GENERATE_WITHCOLUMN_STR_KEY]
                                                                      ),
                                                                  template_params
                                                                  )

                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                        MCEConstants.RESULT_KEY])
                        raise Exception(status_message)
                else:
                    # when metric is inactive
                    # then add metric column having null values
                    # calling replace params to replace variables in generate_withcolumn string
                    replace_param_response = \
                        self._replace_params(str(self.metric_content[MCEConstants.SPARK_STR_KEY]
                                                 [MCEConstants.GENERATE_WITHCOLUMN_FOR_INACTIVE_METRICS_STR]),
                                             template_params
                                             )

                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                        MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                # creating consolidated pyspark string
                base_str += replace_param_response[MCEConstants.RESULT_KEY]
                if count == len(standard_checks_list) - 1:
                    if base_str.strip(" ") != "":
                        spark_str = spark_str + start_str + str(self.metric_content[MCEConstants.SPARK_STR_KEY]
                                                                [MCEConstants.CREATE_NEW_DF_STR_KEY]) + base_str
                count += 1
                # for loop ends here

            replace_param_response = self._replace_params(spark_str,
                                                          template_params
                                                          )

            if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                status_message = "Error in replace param method: " + str(replace_param_response[
                                                                             MCEConstants.ERROR_KEY])
                raise Exception(status_message)

            # checking if replaced string still have $$
            if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                MCEConstants.RESULT_KEY])
                raise Exception(status_message)
            logger.debug(replace_param_response[MCEConstants.RESULT_KEY], extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "End of _standard_metric_generation method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # return standard_check_execution_response
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: replace_param_response[MCEConstants.RESULT_KEY]
                    }

        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}

    def _custom_metric_execution(self, custom_metric_list):
        """
        Purpose   :   This function is used to execute custom metric
        Input     :   custom_metric_list
        Output    :   Custom template code string
        """
        try:
            status_message = "Start of _custom_metric_execution method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            custom_template = ""

            # iterating over each custom metric
            for metric in custom_metric_list:
                # extract metric column name
                metric_id = metric.get(MCEConstants.METRIC_COLUMN_NAME)

                # extract template name
                template = metric.get(MCEConstants.METRIC_TEMPLATE_NAME_KEY)

                # extract template parameters
                template_params = copy.deepcopy(metric.get(MCEConstants.TEMPLATE_PARAM_KEY))
                template_params.update({MCEConstants.COLUMN_NAME_KEY: metric_id,
                                        MCEConstants.CYCLE_ID: self.cycle_id,
                                        MCEConstants.DATA_DATE: self.data_date.strip(),
                                        MCEConstants.TARGET_TABLE_KEY: self.table
                                        }
                                       )

                # extract metric type
                metric_type = metric[MCEConstants.METRIC_TYPE_KEY]

                status_message = "Metric type: " + str(metric_type)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Metric info:" + str(metric)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template name:" + template
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template params:" + str(template_params)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                # checking if it is active metric
                if str(metric[MCEConstants.ACTIVE_INDICATOR_KEY]) == "Y":
                    # fetching custom template path
                    template_path = os.path.join(os.getcwd(), MCEConstants.CUSTOM_TEMPLATE_FOLDER_NAME_KEY,
                                                 template + ".py"
                                                 )
                    status_message = "Custom Template path: " + str(template_path)
                    logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                    # reading custom template
                    fp = open(template_path).read()

                    # creating dataframe name for custom metrics
                    dataframe_name = MCEConstants.CUSTOM_KEY + "_" + metric_id + "_df"
                    # appending to dataframe list for each created dataframe
                    self.dataframe_list.append(dataframe_name)
                    # updating template parameters for dataframe name key
                    template_params.update({MCEConstants.DATAFRAME_NAME_KEY: dataframe_name})

                    custom_template = custom_template + "\n" + fp

                    replace_param_response = self._replace_params(custom_template, template_params)

                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                        MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                    custom_template = replace_param_response[MCEConstants.RESULT_KEY]
                else:
                    # when metric is inactive
                    # then add metric column having null values

                    # creating dataframe name for custom metrics
                    dataframe_name = MCEConstants.CUSTOM_KEY + "_" + metric_id + "_df"
                    # appending to dataframe list for each created dataframe
                    self.dataframe_list.append(dataframe_name)
                    # updating template parameters for dataframe name key
                    template_params.update({MCEConstants.DATAFRAME_NAME_KEY: dataframe_name})

                    # calling replace params to replace variables in generate_withcolumn string
                    replace_param_response = \
                        self._replace_params(str(self.metric_content[MCEConstants.SPARK_STR_KEY][
                                                     MCEConstants.INACTIVE_CUSTOM_METRIC_COMMAND_KEY]
                                                 ),
                                             template_params
                                             )
                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                        MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                    inactive_custom_template = replace_param_response[MCEConstants.RESULT_KEY]

                    custom_template = custom_template + inactive_custom_template

                logger.debug(custom_template, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Dataframe list for custom metrics: " + str(self.dataframe_list)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "End of _custom_metric_execution method"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: custom_template}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}

    def _aggregated_standard_metric_generation(self, aggregated_standard_metric_list):
        """
        Purpose   :   This function is used to execute standard metric
        Input     :   standard_checks_list
        Output    :   Return execution status (Success/Failed)
        """
        try:
            status_message = "Start of _aggregated_standard_metric_generation method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # defining variables
            count = 0
            base_str = ""
            start_str = ""
            spark_str = ""
            template_params = {}
            aggregated_standard_template = ""

            # iterating over each standard metric
            for metric in aggregated_standard_metric_list:
                aggregated_standard_template = ""
                # extracting source table name
                source_table = metric.get(MCEConstants.SOURCE_TABLE_KEY)

                optional_filters = metric.get(MCEConstants.OPTIONAL_FILTER_KEY)

                # extracting metric column name
                metric_id = metric.get(MCEConstants.METRIC_COLUMN_NAME)

                # extract template name
                template = metric[MCEConstants.METRIC_TEMPLATE_NAME_KEY]

                # extract template parameters
                template_params = copy.deepcopy(metric[MCEConstants.TEMPLATE_PARAM_KEY])

                # extract metric type
                metric_type = metric[MCEConstants.METRIC_TYPE_KEY]

                # extract metric data type
                metric_data_type = metric[MCEConstants.METRIC_DATA_TYPE_KEY]

                # # extract metric optional filters
                # optional_filters = metric[MCEConstants.OPTIONAL_FILTER_KEY]

                status_message = "Metric type: " + str(metric_type)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Metric info:" + str(metric)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template name:" + template
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template params:" + str(template_params)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template params:" + str(optional_filters)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Updating templates_params with necessary keys"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                # adding environment key & column name key in metric info dictionary
                template_params.update({MCEConstants.COLUMN_NAME_KEY: metric_id,
                                        MCEConstants.PARAMETERS_KEY: metric[MCEConstants.TEMPLATE_PARAM_KEY],
                                        MCEConstants.SOURCE_TABLE_KEY: source_table,
                                        MCEConstants.CYCLE_ID: self.cycle_id,
                                        MCEConstants.METRIC_TYPE_KEY: metric_type,
                                        MCEConstants.METRIC_DATA_TYPE_KEY: metric_data_type,
                                        MCEConstants.DATA_DATE: self.data_date.strip(),
                                        MCEConstants.METRIC_COLUMN_LIST_KEY: self.metric_column_list,
                                        MCEConstants.OPTIONAL_FILTER_KEY: optional_filters
                                        }
                                       )

                # checking if it is first iteration then add spark string to create df on input table
                # add import strings also
                if count == 0:
                    start_str = self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.IMPORT_STR_KEY] + \
                                self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.REGISTER_TARGET_DF_KEY]

                # checking if it is active metric
                if str(metric[MCEConstants.ACTIVE_INDICATOR_KEY]) == "Y":
                    # fetching template path for standard metric
                    template_path = os.path.join(os.getcwd(), MCEConstants.AGGREGATED_STANDARD_TEMPLATE_FOLDER_NAME_KEY,
                                                 template + ".py")
                    status_message = "Template path: " + str(template_path)
                    logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                    # reading template content for standard metric
                    fp = open(template_path).read()

                    # checking if optional filters are given
                    # if given: then add spark command to filter data
                    # if metric.get(MCEConstants.OPTIONAL_FILTER_KEY) is not None and metric.get(
                    #         MCEConstants.OPTIONAL_FILTER_KEY).strip(" ") != "":
                    #     filter_data_condition = self.metric_content[MCEConstants.SPARK_STR_KEY][
                    #         MCEConstants.AGGREGATED_FILTER_CONDITION_KEY]
                    #     fp = fp + filter_data_condition

                    # creating dataframe name for custom metrics
                    dataframe_name = MCEConstants.AGGREGATED_STANDARD_KEY + "_" + metric_id + "_df"
                    join_columns = template_params["aggregation_on"]
                    temp_join_columns = join_columns.split(',')
                    join_columns = ""
                    for i in temp_join_columns:
                        join_columns = join_columns + "'"+i+"',"
                    join_columns = join_columns.rstrip(',')

                    # appending to dataframe list for each created dataframe
                    self.aggregated_dataframe_list.update({dataframe_name : join_columns})
                    # updating template parameters for dataframe name key
                    template_params.update({MCEConstants.DATAFRAME_NAME_KEY: dataframe_name})

                    aggregated_standard_template = aggregated_standard_template + "\n" + fp

                    replace_param_response = self._replace_params(aggregated_standard_template, template_params)


                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced : " + str(replace_param_response[
                                                                                         MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                    start_str = start_str + "\n" + str(replace_param_response[MCEConstants.RESULT_KEY])

                else:
                    # when metric is inactive
                    # then add metric column having null values
                    # calling replace params to replace variables in generate_withcolumn string
                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                        MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                # creating consolidated pyspark string
                base_str += replace_param_response[MCEConstants.RESULT_KEY]
                if count == len(aggregated_standard_metric_list) - 1:
                    if base_str.strip(" ") != "":
                        spark_str = spark_str + start_str
                count += 1
                # for loop ends here

            replace_param_response = self._replace_params(spark_str,
                                                          template_params
                                                          )

            if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                status_message = "Error in replace param method: " + str(replace_param_response[
                                                                             MCEConstants.ERROR_KEY])
                raise Exception(status_message)

            # checking if replaced string still have $$
            if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                MCEConstants.RESULT_KEY])
                raise Exception(status_message)
            logger.debug(replace_param_response[MCEConstants.RESULT_KEY], extra=self.EXECUTION_CONTEXT.get_context())

            logger.debug(aggregated_standard_template, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "Dataframe list for aggregated standard metrics: " + str(self.aggregated_dataframe_list)
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())


            status_message = "End of _aggregated_standard_metric_generation method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # return standard_check_execution_response
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: replace_param_response[MCEConstants.RESULT_KEY]
                    }

        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}


    def _aggregated_custom_metric_generation(self, aggregated_custom_metric_list):
        """
        Purpose   :   This function is used to execute standard metric
        Input     :   standard_checks_list
        Output    :   Return execution status (Success/Failed)
        """
        try:
            status_message = "Start of _aggregated_custom_metric_generation method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # defining variables
            count = 0
            base_str = ""
            start_str = ""
            spark_str = ""
            template_params = {}
            aggregated_custom_template = ""

            # iterating over each standard metric
            for metric in aggregated_custom_metric_list:
                aggregated_custom_template = ""
                # extracting source table name
                source_table = metric.get(MCEConstants.SOURCE_TABLE_KEY)

                # extracting optional filter
                optional_filters = metric.get(MCEConstants.OPTIONAL_FILTER_KEY)

                # extracting metric column name
                metric_id = metric.get(MCEConstants.METRIC_COLUMN_NAME)

                # extract template name
                template = metric[MCEConstants.METRIC_TEMPLATE_NAME_KEY]

                # extract template parameters
                template_params = copy.deepcopy(metric[MCEConstants.TEMPLATE_PARAM_KEY])

                # extract metric type
                metric_type = metric[MCEConstants.METRIC_TYPE_KEY]

                # extract metric data type
                metric_data_type = metric[MCEConstants.METRIC_DATA_TYPE_KEY]

                status_message = "Metric type: " + str(metric_type)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Metric info:" + str(metric)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template name:" + template
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Template params:" + str(template_params)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                status_message = "Updating templates_params with necessary keys"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                # adding environment key & column name key in metric info dictionary
                template_params.update({MCEConstants.COLUMN_NAME_KEY: metric_id,
                                        MCEConstants.PARAMETERS_KEY: metric[MCEConstants.TEMPLATE_PARAM_KEY],
                                        MCEConstants.SOURCE_TABLE_KEY: source_table,
                                        MCEConstants.OPTIONAL_FILTER_KEY: optional_filters,
                                        MCEConstants.CYCLE_ID: self.cycle_id,
                                        MCEConstants.METRIC_TYPE_KEY: metric_type,
                                        MCEConstants.METRIC_DATA_TYPE_KEY: metric_data_type,
                                        MCEConstants.DATA_DATE: self.data_date.strip(),
                                        MCEConstants.METRIC_COLUMN_LIST_KEY: self.metric_column_list
                                        }
                                       )

                # checking if it is first iteration then add spark string to create df on input table
                # add import strings also
                if count == 0:
                    start_str = self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.IMPORT_STR_KEY] + \
                                self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.REGISTER_TARGET_DF_KEY]

                # checking if it is active metric
                if str(metric[MCEConstants.ACTIVE_INDICATOR_KEY]) == "Y":
                    # fetching template path for standard metric
                    template_path = os.path.join(os.getcwd(), MCEConstants.AGGREGATED_CUSTOM_TEMPLATE_FOLDER_NAME_KEY,
                                                 template + ".py")
                    status_message = "Template path: " + str(template_path)
                    logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                    # reading template content for standard metric
                    fp = open(template_path).read()

                    # checking if optional filters are given
                    # if given: then add spark command to filter data
                    #if metric.get(MCEConstants.OPTIONAL_FILTER_KEY) is not None and metric.get(
                    #        MCEConstants.OPTIONAL_FILTER_KEY).strip(" ") != "":
                    #    filter_data_condition = self.metric_content[MCEConstants.SPARK_STR_KEY][
                    #        MCEConstants.AGGREGATED_FILTER_CONDITION_KEY]
                    #    fp = fp + filter_data_condition

                    # creating dataframe name for custom metrics
                    dataframe_name = MCEConstants.AGGREGATED_CUSTOM_KEY + "_" + metric_id + "_df"
                    join_columns = template_params["aggregation_on"]
                    temp_join_columns = join_columns.split(',')
                    join_columns = ""
                    for i in temp_join_columns:
                        join_columns = join_columns + "'"+i+"',"
                    join_columns = join_columns.rstrip(',')

                    # appending to dataframe list for each created dataframe
                    self.aggregated_dataframe_list.update({dataframe_name : join_columns})
                    # updating template parameters for dataframe name key
                    template_params.update({MCEConstants.DATAFRAME_NAME_KEY: dataframe_name})

                    aggregated_custom_template = aggregated_custom_template + "\n" + fp

                    replace_param_response = self._replace_params(aggregated_custom_template, template_params)


                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced : " + str(replace_param_response[
                                                                                         MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                    start_str = start_str + "\n" + str(replace_param_response[MCEConstants.RESULT_KEY])

                else:
                    # when metric is inactive
                    # then add metric column having null values
                    # calling replace params to replace variables in generate_withcolumn string
                    if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                        status_message = "Error in replace param method: " + str(replace_param_response[
                                                                                     MCEConstants.ERROR_KEY])
                        raise Exception(status_message)

                    # checking if replaced string still have $$
                    if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                        status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                        MCEConstants.RESULT_KEY])
                        raise Exception(status_message)

                # creating consolidated pyspark string
                base_str += replace_param_response[MCEConstants.RESULT_KEY]
                if count == len(aggregated_custom_metric_list) - 1:
                    if base_str.strip(" ") != "":
                        spark_str = spark_str + start_str
                count += 1
                # for loop ends here

            replace_param_response = self._replace_params(spark_str,
                                                          template_params
                                                          )

            if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                status_message = "Error in replace param method: " + str(replace_param_response[
                                                                             MCEConstants.ERROR_KEY])
                raise Exception(status_message)

            # checking if replaced string still have $$
            if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                status_message = "Spark string didn't get replaced: " + str(replace_param_response[
                                                                                MCEConstants.RESULT_KEY])
                raise Exception(status_message)
            logger.debug(replace_param_response[MCEConstants.RESULT_KEY], extra=self.EXECUTION_CONTEXT.get_context())

            logger.debug(aggregated_custom_template, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "Dataframe list for aggregated custom metrics: " + str(self.aggregated_dataframe_list)
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())


            status_message = "End of _aggregated_custom_metric_generation method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # return standard_check_execution_response
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: replace_param_response[MCEConstants.RESULT_KEY]
                    }

        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}



    def _insert_into_target_table(self, alter_table_schema_dictionary):
        """
        Purpose   :   This function is used to insert data of metric execution into target tables
        Input     :   alter_table_schema_dictionary
        Output    :   Code string to insert the dataframe into target table
        """
        try:
            status_message = "Start of _insert_into_target_table method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # Adding spark code for registering df as temp table
            status_message = "Adding spark code for registering df as temp table"
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            spark_str = self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.REGISTER_AGGREGATED_DF_AS_TABLE]
            # creating replace params
            template_params = {MCEConstants.METRIC_COLUMN_LIST_KEY: self.metric_column_list,
                               MCEConstants.ALTER_TABLE_SCHEMA_KEY: alter_table_schema_dictionary,
                               MCEConstants.TARGET_TABLE_KEY: self.table,
                               MCEConstants.CYCLE_ID: self.cycle_id,
                               MCEConstants.DATA_DATE: self.data_date.strip()
                               }
            # spark command string for alter table command
            spark_str += self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.ALTER_TABLE_STRING_KEY]

            # Adding spark code for inserting into target table
            status_message = "Adding spark code for inserting into target table"
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            spark_str += self.metric_content[MCEConstants.SPARK_STR_KEY][MCEConstants.INSERT_INTO_TABLE_KEY]

            # calling replace params to replace variables in select_columns pyspark string
            replace_param_response = self._replace_params(spark_str, template_params)

            if replace_param_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                status_message = "Error in replace param method: " + str(replace_param_response[
                                                                             MCEConstants.ERROR_KEY])
                raise Exception(status_message)

            # checking if replaced string still have $$
            if replace_param_response[MCEConstants.RESULT_KEY].find("$$") > -1:
                status_message = "Query string didn't get replaced"
                raise Exception(status_message)
            spark_str = replace_param_response[MCEConstants.RESULT_KEY]
            logger.debug(spark_str, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "End of _insert_into_target_table method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # return insert_command_execution_response
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: replace_param_response[MCEConstants.RESULT_KEY]}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}

    def _execute_shell_command(self, command):
        """
        Purpose: This method executes shell command
        Input: Command
        Output: Command execution status
        """
        try:
            status_message = "Started executing shell command " + command
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            # executing shell command
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            # fetch output
            if standard_output:
                standard_output_lines = standard_output.splitlines()
                for line in standard_output_lines:
                    logger.debug(line, extra=self.EXECUTION_CONTEXT.get_context())
            # fetch error if any
            if standard_error:
                standard_error_lines = standard_error.splitlines()
                for line in standard_error_lines:
                    logger.debug(line, extra=self.EXECUTION_CONTEXT.get_context())
            # checking exit status code
            if command_output.returncode == 0:
                return True
            else:
                status_message = "Error occurred while executing command on shell :" + command
                raise Exception(status_message)
        except Exception as exception:
            status_message = "Shell command execution error: %s" % exception
            logger.error(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception(status_message)

    def _execute_standard_and_custom_metrics(self, metrics):
        """
        Purpose   :   This function is used to generate pyspark code for metric rules
        Input     :   List of metrics
        Output    :   Return execution status (Success/Failed)
        """
        try:
            status_message = "Start of _execute_standard_and_custom_metrics method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # defining variables
            standard_template = ""
            custom_template = ""
            aggregated_standard_template = ""
            aggregated_custom_template = ""
            standard_metric_list = list()
            custom_metric_list = list()
            aggregated_standard_metric_list = list()
            aggregated_custom_metric_list = list()
            alter_table_schema_dictionary = dict()

            self.aggregated_dataframe_list.update({'target_df' : 'NULL'})

            # iterating over each active metric and on the basis of type of metric(standard or custom) it will generate
            # different list of metrics
            for metric in metrics:
                status_message = "Metric id for metric rule to be executed: " + \
                                 str(metric.get(MCEConstants.METRIC_COLUMN_NAME.strip(" ")))
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                # extract the metric column name
                metric_id = str(metric.get(MCEConstants.METRIC_COLUMN_NAME.strip(" ")))
                # apeend the metric column names to a list
                self.metric_column_list.append(metric_id)
                # extracting metric type
                metric_type = metric.get(MCEConstants.METRIC_TYPE_KEY)
                # checking if metric type is standard
                if str(metric_type).lower().strip(" ") == MCEConstants.STANDARD_KEY.lower():
                    # append metric to standard metric list
                    standard_metric_list.append(metric)
                # checking if metric type is custom
                elif str(metric_type).lower().strip(" ") == MCEConstants.CUSTOM_KEY.lower():
                    # append metric to custom metric list
                    custom_metric_list.append(metric)
                # checking if metric type is aggregated standard
                elif str(metric_type).lower().strip(" ") == MCEConstants.AGGREGATED_STANDARD_KEY.lower():
                    # append metric to custom metric list
                    aggregated_standard_metric_list.append(metric)
                # checking if metric type is aggregated standard
                elif str(metric_type).lower().strip(" ") == MCEConstants.AGGREGATED_CUSTOM_KEY.lower():
                    # append metric to custom metric list
                    aggregated_custom_metric_list.append(metric)
                # otherwise it is invalid metric type
                else:
                    status_message = "Invalid type of metric: " + str(metric_type)
                    raise Exception(status_message)

                # fetch spark command string to alter target table
                alter_table_schema_dictionary[metric_id] = metric.get(MCEConstants.METRIC_DATA_TYPE_KEY)

            status_message = "Standard metric list : " + str(standard_metric_list)
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "Custom metric list: " + str(custom_metric_list)
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "Aggregated Standard metric list " + str(aggregated_standard_metric_list)
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "Aggregated Custom metric list " + str(aggregated_custom_metric_list)
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # checking if standard metric list is non-empty
            if standard_metric_list:
                # creating dataframe name to be replaced in spark strings
                dataframe_name = MCEConstants.STANDARD_KEY + "_df"

                # creating list of all the dataframes created during metric execution
                # these df will be union and aggregation will be applied on that
                self.dataframe_list.append(dataframe_name)
                status_message = "Dataframe list: " + str(self.dataframe_list)
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                # calling _standard_metric_generation method to execute standard metrics
                status_message = "Calling _standard_metric_generation method"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                standard_template_generation_response = self._standard_metric_generation(standard_metric_list)
                # checking execution status of _standard_metric_generation
                if standard_template_generation_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                    status_message = "Error in _standard_check_generation method: " + str(
                        standard_template_generation_response[MCEConstants.ERROR_KEY])
                    raise Exception(status_message)

                standard_template = standard_template_generation_response[MCEConstants.RESULT_KEY]

            # checking if custom metric list is non-empty
            if custom_metric_list:
                status_message = "Calling _custom_metric_execution method"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                custom_template_generation_response = self._custom_metric_execution(custom_metric_list)

                # checking execution status of _custom_metric_execution
                if custom_template_generation_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                    status_message = "Error in _custom_metric_execution method: " + str(
                        custom_template_generation_response[MCEConstants.ERROR_KEY])
                    raise Exception(status_message)

                custom_template = custom_template_generation_response[MCEConstants.RESULT_KEY]

            # checking if aggregated standard metric list is non-empty
            if aggregated_standard_metric_list:
                # creating list of all the dataframes created during metric execution
                # these df will be union and aggregation will be applied on that
                status_message = "Calling _aggregated_standard_metric_generation method"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                aggregated_standard_template_generation_response = self._aggregated_standard_metric_generation(aggregated_standard_metric_list)

                # checking execution status of _custom_metric_execution
                if aggregated_standard_template_generation_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                    status_message = "Error in _aggregated_standard_metric_generation method: " + str(
                        aggregated_standard_template_generation_response[MCEConstants.ERROR_KEY])
                    raise Exception(status_message)

                aggregated_standard_template = aggregated_standard_template_generation_response[MCEConstants.RESULT_KEY]

            # checking if aggregated custom metric list is non-empty
            if aggregated_custom_metric_list:
                # creating list of all the dataframes created during metric execution
                # these df will be union and aggregation will be applied on that
                status_message = "Calling _aggregated_custom_metric_generation method"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                aggregated_custom_template_generation_response = self._aggregated_custom_metric_generation(aggregated_custom_metric_list)

                # checking execution status of _custom_metric_execution
                if aggregated_custom_template_generation_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                    status_message = "Error in _aggregated_custom_metric_generation method: " + str(
                        aggregated_custom_template_generation_response[MCEConstants.ERROR_KEY])
                    raise Exception(status_message)

                aggregated_custom_template = aggregated_custom_template_generation_response[MCEConstants.RESULT_KEY]

            # calling method which will union all the generated dataframes so far
            status_message = "Calling _generate_spark_code_to_union_dataframes method"
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            if standard_template == "" and custom_template == "" :
                union_pyspark_code = ""
            else :

                union_pyspark_code_generation_response = self._generate_spark_code_to_union_dataframes()

                # checking execution status of _generate_spark_code_to_union_dataframes
                if union_pyspark_code_generation_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                    status_message = "Error in _generate_spark_code_to_union_dataframes method: " + \
                                     str(union_pyspark_code_generation_response[MCEConstants.ERROR_KEY])
                    raise Exception(status_message)

                union_pyspark_code = union_pyspark_code_generation_response[MCEConstants.RESULT_KEY]


            if aggregated_standard_template == "" and aggregated_custom_template == "" :
                join_pyspark_code = ""
            else :
                join_pyspark_code_generation_response = self._generate_aggregated_spark_code_to_union_dataframes()

                # checking execution status of _generate_spark_code_to_union_dataframes
                if join_pyspark_code_generation_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                    status_message = "Error in _generate_spark_code_to_union_dataframes method: " + \
                                     str(join_pyspark_code_generation_response[MCEConstants.ERROR_KEY])
                    raise Exception(status_message)

                join_pyspark_code = join_pyspark_code_generation_response[MCEConstants.RESULT_KEY]

            # calling method to insert data into target table
            status_message = "Calling _insert_into_target_table method"
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            target_table_insertion_response = self._insert_into_target_table(alter_table_schema_dictionary)
            # checking execution status of _insert_into_target_table method
            if target_table_insertion_response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                status_message = "Error in _insert_into_target_table method: " + \
                                 str(target_table_insertion_response[MCEConstants.ERROR_KEY])
                raise Exception(status_message)

            status_message = "End of _execute_standard_and_custom_metrics method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            target_table_insertion_code = target_table_insertion_response[MCEConstants.RESULT_KEY]

            aggregated_spark_code = \
                standard_template + "\n" + custom_template + "\n" + aggregated_standard_template + "\n" + aggregated_custom_template + "\n" +\
                union_pyspark_code + "\n" + join_pyspark_code + "\n" + "\n" + target_table_insertion_code

            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: aggregated_spark_code}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(exception))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: "",
                    MCEConstants.ERROR_KEY: exception}

    def execute_rules(self, table, metrics, process_id, master_ip_address, cycle_id, data_date):
        """
        Purpose   :   This function is main method of engine, it will call all methods one by one to generate spark
                      code & execute it with the help of Job Executor
        Input     :   table, metrics, process_id, master_ip_address, cycle_id, data_date
        Output    :   Return execution status (Success/Failed)
        """
        try:
            status_message = "Starting execute_rules method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # assigning values to self parameter so that it can be used everywhere
            self.process_id = process_id
            self.table = table
            self.master_ip_address = master_ip_address
            self.cycle_id = cycle_id
            self.data_date = data_date

            # extracting spark command strings from SparkStrings.py file
            self.metric_content = SparkStrings.TEMPLATE

            # calling the function which will return the aggregated spark code
            spark_code_generation_response = self._execute_standard_and_custom_metrics(metrics=metrics)

            spark_code = spark_code_generation_response[MCEConstants.RESULT_KEY]

            # fetching the path where the spark code will be written to a python file
            file_name = self.table + self.ts + ".py"
            spark_aggregated_code_path = os.path.join(os.getcwd(), MCEConstants.AGGREGATED_SPARK_CODE_FOLDER,
                                                      file_name)

            # writing the aggregated code to a file
            status_message = "Writing the aggregated code to a file"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            with open(spark_aggregated_code_path, "w") as f:
                f.write(spark_code)

            spark_submit_command = MCEConstants.SPARK_SUBMIT_STR +  spark_aggregated_code_path

            status_message = "Submitting the command to job executor"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            try:
                response = MCEJobExecutor().execute_command(spark_submit_command, self.master_ip_address)

                status_message = "Final output: " + str(response)
                logger.debug(str(status_message))
                print("not in if and else")
                if response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_FAILED:
                    print("----------->Failed spark job")
                    return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED,
                            MCEConstants.ERROR_KEY: "Failed spark job"}

                if response[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_SUCCESS:
                    print("-------------->spark job executed successfully")
                    return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                            MCEConstants.RESULT_KEY: "spark job executed successfully"}
            except:
                status_message = "ERROR - " + str(traceback.format_exc())
                logger.info(status_message)
                print("-------->raising exception")
                raise Exception(status_message)
        except Exception as err:
            error = " ERROR MESSAGE: " + str(traceback.format_exc(err))
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())

            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: False,
                    MCEConstants.ERROR_KEY: err}


# if __name__ == '__main__':
#     out = CcMetricEngine().execute_rules("mce.sales_data_target", [
#         {
#             "process_id": 1,
#             "metric_column_name": "jan_feb",
#             "metric_data_type": "double",
#             "metric_description": "calculate total",
#             "metric_type": "standard",
#             "metric_template_name": "summation",
#             "template_parameters": {
#                 "sum_value_1": "january",
#                 "sum_value_2": "february"
#             },
#             "source_table": "mce.sales_data",
#             "target_table": "mce.sales_data_target",
#             "optional_filters": "",
#             "table_type": "mce.sales_data_target",
#             "active_indicator": "Y"
#         },
#         {
#             "process_id": 1,
#             "metric_column_name": "april_may",
#             "metric_data_type": "double",
#             "metric_description": "calculate total",
#             "metric_type": "standard",
#             "metric_template_name": "summation",
#             "template_parameters": {
#                 "sum_value_1": "april",
#                 "sum_value_2": "may"
#             },
#             "source_table": "mce.sales_data",
#             "target_table": "mce.sales_data_target",
#             "optional_filters": "",
#             "table_type": "mce.sales_data_target",
#             "active_indicator": "Y"
#         },
#         {
#             "process_id": 1,
#             "metric_column_name": "feb_march",
#             "metric_data_type": "double",
#             "metric_description": "calculate change",
#             "metric_type": "custom",
#             "metric_template_name": "custom_change_feb_march",
#             "template_parameters": {},
#             "source_table": "mce.sales_data",
#             "target_table": "mce.sales_data_target",
#             "optional_filters": "",
#             "table_type": "mce.sales_data_target",
#             "active_indicator": "Y"
#         },
#         {
#             "process_id": 1,
#             "metric_column_name": "march_april",
#             "metric_data_type": "double",
#             "metric_description": "calculate change",
#             "metric_type": "custom",
#             "metric_template_name": "custom_change_march_april",
#             "template_parameters": {},
#             "source_table": "mce.sales_data",
#             "target_table": "mce.sales_data_target",
#             "optional_filters": "",
#             "table_type": "mce.sales_data_target",
#             "active_indicator": "Y"
#         }
#     ], 1, "10.228.32.229", "20190410074355", "2018-06-15")
#
#     print(out['result'])


