
#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_cc/browse ) & all static servers.

"""
Doc_Type            : Metric Calculation Engine Constants
Tech Description    : This class contains all the constants for Metrics Engine
"""

import CommonConstants

# Status values
MODULE_NAME = 'mce'
STATUS_KEY = "status"
MCE_STATUS_KEY = "mce_status"
STATUS_SUCCESS = "SUCCESS"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_IN_PROGRESS = "IN PROGRESS"
STATUS_FAILED = "FAILED"
RESULT_KEY = "result"
TEMP_RESULT_KEY="temp_result"
ERROR_KEY = "error"
IN_PROGRESS_KEY = "In-Progress"

# keys used in metric input configuration
PROCESS_ID = "process_id"
TEMPLATE_PARAM_KEY = "template_parameters"
COLUMN_NAME_KEY = "column_name"
SOURCE_TABLE_KEY = "source_table"
TARGET_TABLE_KEY = "target_table"
METRIC_COLUMN_NAME = "metric_column_name"
METRIC_DATA_TYPE_KEY = "metric_data_type"
METRIC_DESCRIPTION_KEY = "metric_description"
METRIC_TEMPLATE_NAME_KEY = "metric_template_name"
METRIC_TYPE_KEY = "metric_type"
OPTIONAL_FILTER_KEY = "optional_filters"
ACTIVE_INDICATOR_KEY = "active_flag"
SPARK_CONFIG_PARAMETER = "spark_config_parameters"
TABLE_TYPE_KEY = "table_type"
CYCLE_ID = "cycle_id"
DATA_DATE = "data_date"

# MySQL Connection Manager
ENVIRONMENT_CONFIG_FILE = "environment_params.json"
ENVIRONMENT_PARAMS_KEY = "EnvironmentParams"

# logging db details
INSERT_KEY = "insert"
UPDATE_KEY = "update"
SELECT_CYCLE_ID = "select_cycle_id"
SELECT_DATA_DATE = "select_data_date"
LOGGING_CONNECTION_DETAIL_KEY = "logging_connection_details"
HOST_KEY = "host"
PORT_KEY = "port"
DATABASE_NAME_KEY = "database_name"
ENGINE_LOGGING_TABLE_KEY = "log_mce_dtl"
LOG_CYCLE_DTL = "log_cycle_dtl"
CYCLE_STATUS = "cycle_status"

# cluster related keys
CREATE_KEY = "create"
DELETE_KEY = "delete"
CLUSTER_ID_KEY = "cluster_id"
CREATED_CLUSTER_ID = "clusterId"
CLUSTER_NAME_KEY = "cluster_name"
CONTEXT_ID_KEY = "context_id"
CLUSTER_TEMPLATE_KEY = "cluster_templates"
DEFAULT_CLUSTER_TEMPLATE_KEY = "default_cluster_template"
BASE_URL_KEY = "base_url"
END_POINT_KEY = "endpoint"
REGION_KEY = "region"
PAYLOAD_KEY = "payload"
CLUSTER_TYPE_KEY = "cluster_type"
MEDIUM_CLUSTER_TEMPLATE = "medium_cluster_template"
PRIVATE_IP_ADDRESS = "PrivateIpAddress"
SPARK_SUBMIT_STR = "sudo /usr/lib/spark/bin/spark-submit "
PEM_FILE_LOCATION = "/home/svc-rwe-user-dev/key-pair/aws-a0036-use1-00-d-kpr-shrd-chb-emr03.pem"
SCP_COMMAND = "scp -o StrictHostKeyChecking=no -i "
CLUSTER_USERNAME = "hadoop"
CLUSTER_TEMP_PATH = "/tmp/"
CLUSTER_SSH_PORT = "22"

# keys used in metric rules configuration
PARAMETERS_KEY = "parameters"
COPY_KEY = "copy"
PYTHON_KEY = "python"

# keys used in spark template
SPARK_TEMPLATE_KEY = "spark_template"
REGISTER_DF_KEY = "register_df"
REGISTER_TARGET_DF_KEY = "register_target_df"
REGISTER_DF_AS_TABLE = "register_df_as_table_str"
REGISTER_TEMPDF_AS_TABLE = "register_tempdf_as_table_str"
REGISTER_AGGREGATED_TEMPDF_AS_TABLE = "register_aggregated_tempdf_as_table_str"
REGISTER_AGGREGATED_DF_AS_TABLE = "register_aggregated_df_as_table_str"
FINAL_AGGREGATED_DATAFRAME = "aggregated_dataframe_str"
FINAL_TEMP_AGGREGATED_DATAFRAME = "temp_aggregated_dataframe_str"
GENERATE_WITHCOLUMN_STR_KEY = "generate_withColumn_str"
GENERATE_WITHCOLUMN_FOR_INACTIVE_METRICS_STR = "generate_withColumn_for_inactive_metric_str"
CREATE_NEW_DF_STR_KEY = "create_new_df_str"
GET_DATAFRAMES_COLUMNS = "get_columns_string"
SELECT_COLUMN_LIST_KEY = "select_columns_str"
SELECT_COLUMN_IN_DATAFRAMES_STRING = "select_all_columns_in_dataframe_string"
SELECT_COLUMN_IN_TEMP_DATAFRAMES_STRING = "select_all_columns_in_tempdataframe_string"
ADD_EXTRA_COLUMNS = "add_extra_columns"
ALTER_TABLE_STRING_KEY = "alter_table_string"
METRIC_COLUMN_LIST_KEY = "metric_column_list"
ALTER_TABLE_SCHEMA_KEY = "alter_table_schema"
WITHCOLUMN_GEO_AVG_STR_KEY = "with_column_for_geo_avg"
WITHCOLUMN_GEO_SKIP_STR_KEY = "with_column_for_geo_skip"
WITHCOLUMN_GEO_COPY_STR_KEY = "with_column_for_geo_copy"
CREATE_JOINED_DF_KEY = "create_joined_df"
FINAL_REPORTING_DATA_STR_KEY = "final_reporting_data_str"
GET_TARGET_BASE_COLUMNS_STR_KEY = "string_to_get_base_column_of_target_table"
BASE_COLUMNS_KEY = "base_columns"
IMPORT_STR_KEY = "import_str"
METRIC_NAME_KEY = "metric_name"
STANDARD_KEY = "standard"
CUSTOM_KEY = "custom"
AGGREGATED_STANDARD_KEY = "aggregated_standard"
AGGREGATED_CUSTOM_KEY = "aggregated_custom"
DATAFRAME_NAME_KEY = "dataframe_name"
DATAFRAME_LIST_KEY = "dataframe_list"
AGGREGATED_DATAFRAME_LIST_KEY = "aggregated_dataframe_list"
SPARK_STR_KEY = "spark_string"
FILTER_CONDITION_KEY = "filter_condition"
AGGREGATED_FILTER_CONDITION_KEY = "aggregated_filter_condition"
METRIC_TEMPLATE_KEY = "metric_template"
INSERT_INTO_TABLE_KEY = "insert_into_target_table_str"
COLUMN_LIST_KEY = "column_list"
PARTITION_BY_KEY = "partition_by"
METRIC_OUTPUT = "metric_output"
BU_KEY = "business_unit_code"
METRIC_FOLDER_NAME_KEY = "metrics"
STANDARD_TEMPLATE_FOLDER_NAME_KEY = CommonConstants.EMR_CODE_PATH + \
    "/configs/job_executor/standard_template"
CUSTOM_TEMPLATE_FOLDER_NAME_KEY = CommonConstants.EMR_CODE_PATH + \
    "/configs/job_executor/custom_template"
AGGREGATED_STANDARD_TEMPLATE_FOLDER_NAME_KEY = CommonConstants.EMR_CODE_PATH + \
    "/configs/job_executor/aggregated_standard_template"
AGGREGATED_CUSTOM_TEMPLATE_FOLDER_NAME_KEY = CommonConstants.EMR_CODE_PATH + \
    "/configs/job_executor/aggregated_custom_template"
AGGREGATED_SPARK_CODE_FOLDER = CommonConstants.EMR_CODE_PATH + \
    "/configs/job_executor/aggregated_spark_code"
INACTIVE_CUSTOM_METRIC_COMMAND_KEY = CommonConstants.EMR_CODE_PATH + \
    "/configs/job_executor/inactive_custom_metric"

# RDS details
METRICS_RULES_TABLE = "ctl_metric_config"

# Miscellaneous
REGION_NAME = "us-east-1"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_DATETIME_FORMAT = "_%Y_%m_%d_%H_%M_%S"
