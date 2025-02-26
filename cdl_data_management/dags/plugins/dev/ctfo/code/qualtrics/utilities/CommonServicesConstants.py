
#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
"""
Doc_Type            : Tech
Tech Description    : This module is used for initializing all the constant parameters
                      which different modules can refer
Pre_requisites      : NA
Inputs              : NAf
Outputs             : NA
Example             : NA
Config_file         : NA

"""
import os
import sys
import json

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "utilities/"))
sys.path.insert(0, UTILITIES_DIR_PATH)

ENV_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/environment_config.json"))
env_file = open(ENV_CONFIG_PATH, 'r').read()
environment_config = json.loads(env_file)

bucket_name = environment_config["bucket_name"]
environment = environment_config["environment"]

# Constants check key for invalid input
INVALID_INPUT = ["", " ", "null", "Null", "NULL", None, "NONE", "None", "none", "FALSE", "False",
                 "false", False]

# Constants representing the status keys

USER_ID = "user_id"
STATUS_KEY = "status"
ARCHIVE = "archive"
SCENARIO_DETAILS = "scenario_details"
ACTIVE = "ACTIVE"
JSON_FORMAT_PATH = "/clinical_design_center/webservices/cdl_ctfo/json_format"
STATUS_SUCCESS = "SUCCEEDED"
STATUS_FAILED = "FAILED"
STATUS_COMPLETED = "COMPLETED"
BODY_CONTENT = "bodyContent"
HEADER_CONTENT = "headerContent"
MESSAGE = "message"
SCHEMA = "schema"
SCHEMA_REPORTING = "schema_reporting"

#search service constants:
SEARCH_FIELD = 'search_field'
SEARCH_VALUE = 'search_value'
SEARCH_QUERY = "search_query"
SCENARIO_ID_KEY = "scenario_id"
SCENARIO_NAME = "scenario_name"
PROTOCOL_ID = "protocol_id"
THEME_ID = "theme_id"

#share service
SHARE_SCENARIO_DETAILS = "share_scenario_details"

SCENARIO_ID_KEY = "scenario_id"
SCENARIO_NAME = "scenario_name"
PROTOCOL_ID = "protocol_id"
THEME_ID = "theme_id"
USER_ID = "user_id"
ACTION = "action"
STATUS_SUCCESS = "SUCCESS"
MESSAGE = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"
OUTPUT_KEY = "output"
CTFO_SERVICES = "ctfo_services"
RDS_QUERIES = "rds_queries"
ARCHIVE_KEY = "archive"
DELETE_KEY = "delete"
CREATED_BY = "created_by"
JOB_ID_KEY = "job_id"

SCENARIO_HOUSEKEEPING = "scenario_housekeeping"
DISPLAY_FILTER = "display_filter"
fetch_filters = "fetch_filters"

GENERIC_CONFIGS = "generic_cofigs"
AUTHORIZATION_KEY = "Authorization"
BEARER_KEY = "Bearer"
ACCESS_TOKEN_KEY = "access_token"
ROLE_KEY = "role"
TOKEN_TYPE_KEY = "token_type"
EXPIRES_IN_KEY = "expires_in"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
SCOPE_KEY = "scope"
CLIENT_ID_KEY = "client_id"
NON_CFRG_USER_LIST_NAME = "NON_CFRG_USER"
NON_CFRG_USER_LIST_DESCRIPTION = ""
SUCCESSFUL_RESPONSE_MANDATORY_KEYS = [ACCESS_TOKEN_KEY, TOKEN_TYPE_KEY, EXPIRES_IN_KEY, SCOPE_KEY, CLIENT_ID_KEY]
PING_AUTHENTICATION_KEY = "ping_authentication"
PING_AUTHENTICATION_FLAG_KEY = "ping_authentication_flag"
PING_AUTHENTICATION_HOST_KEY = "ping_authentication_host"
PING_TOKEN_VALIDATION_USERNAME = "ping_token_validation_username"
PING_TOKEN_VALIDATION_PASSWORD = "ping_token_validation_password"
TOKEN_EXPIRED_KEY = "token expired"
ERROR_DESCRIPTION_KEY = "error_description"
UPDATE_RANKED_LIST = "update_ranked_site"
MESSAGE_KEY = "message"
VALIDATION_TABLE = "log_user_site_list_save_smry"
VALIDATION_TABLE_INVESTIGATOR = "log_user_investigator_list_save_smry"
FEEDBACK_FORM_TABLE = "log_user_feedback_smry"
USER_NAME_KEY = "user_name"
DEFAULT_AUTOCOMMIT_ENABLED = True
LIMIT = 100
REGION_NAME = 'us-east-1'
HOST_IP = "host_ip"
HOST_PORT = "host_port"
DB_HOST = 'db_host'
DB_USERNAME = 'db_username'
DB_PASSWORD = 'db_password'
DB_PORT = 'db_port'
DB_NAME = 'db_name'
IS_ALIVE_KEY = 'is_alive'
GRANT_TYPE_KEY = "grant_type"
TABLE_KEY = "table"
QUERY_KEY = "query"
RECORD_COUNT_TYPE_KEY = "record_count_type"
RECORD_COUNT_KEY = "record_count"
MANDATORY_HEADERS = [AUTHORIZATION_KEY]
LIMIT_KEY = "limit"
OFFSET_KEY = "offset"
SORT_ON_KEY = "sort_on"
SORT_ORDER_KEY = "sort_order"
SEARCH_FLAG_KEY = "search_flag"
SEARCH_PARAMETER_KEY = "search_parameter"
ORDER_BY_KEY = "order_by"
COMMENT_SCENARIO = "comment_scenario"
COMMENT_KEY = "comment"
ORDER_BY_FIELD_KEY = "order_by_field"
DISEASE_KEY = "disease"
PHASE_KEY = "trial_phase"
CASCADING_KEY = "cascading"
NCT_ID_KEY = "nct_id"
STUDY_NAME_KEY = "study_name"
THERAPEUTIC_AREA_KEY = "therapeutic_area"
INDICATION_KEY = "indication"
SCENARIO_FILTER_DETAILS = "scenario_filter_details"
NCT_TRIAL_ID_KEY = "nct_trial_id"
TRIAL_PHASE_KEY = "trial_phase"
DELEGATED_ROLE_KEY = "delegated_role"
COMMENT_KEY = "comment"
LOGIN_DETAILS = "login_details"
CREATE_SCENARIO = "create_scenario"
CREATE_FILTER = "create_filter"
COMMENT_SCENARIO = "comment_scenario"
USERNAME_KEY = "user_name"
PRIORITY_PREFERENCE = "priority_preference"
CATEGORY = "category"
BATCH_ID = "batch_id"
SITE_LIST_TYPE = "site_list_type"
FINAL_LIST_ID = "final_list_id"
SITE_LIST_ID = "site_list_id"
OPTIMIZE_SITE_ID = "optimize_site_id"
RESULT_TYPE = "result_type"
OPTIMIZED_VIEW_FLAG = "optimized_view_flag"
VERSION = "version"
ACTIVE_FLAG = "active_flag"
FILTERS_AND_CONSTRAINTS = "filters_and_constraints"
REGION_CONFIGURATION = "region_configuration"
COUNTRY_CONFIGURATION = "country_configuration"
UPDATED_BY = "updated_by"
S3_TARGET_FILE = "s3_target_file"

CTFO_SITE_ID = "ctfo_site_id"
SITE_NAME = "site_nm"
SITE_COUNTRY = "site_country"
TYPE_KEY = "type"
POST_OPTIMIZATION_ACTUAL_TABLE = "f_rpt_scenario_site_ranking_optimized"
POST_OPTIMIZATION_TEMP_TABLE = "f_rpt_scenario_site_ranking_optimized_temp"

# Country_Ranking Service Constants
COUNTRY_LIST_ID = "country_list_id"
COUNTRY_RANKING_ACTUAL_TABLE = "f_rpt_scenario_country_ranking"
COUNTRY_RANKING_TEMP_TABLE = "f_rpt_scenario_country_ranking_temp"
WEIGHTAGE_ACTUAL_TABLE = "f_rpt_scenario_country_metric_weight_dtl"
WEIGHTAGE_TEMP_TABLE = "f_rpt_scenario_country_metric_weight_dtl_temp"
WEIGHTAGE_KEY = "weightage"
COLOR_ADD_ON_TABLE = "f_rpt_scenario_add_on_attributes_dtl"
COLOR_KEY = "color"
COUNTRY_LIST_KEY = "country_list"
MAX_KEY = "max"
COUNTRY_RANKING_DETAILS = "country_ranking_details"
COUNTRY_RANK_QUERY = "country_rank_query"
INSERT_WEIGHTAGE_QUERY = "insert_weightage_query"
GET_COLOR_QUERY = "get_color_query"
DELETE_COLOR_QUERY = "delete_color_query"
COUNTRY_RANK_DISPLAY_QUERY = "country_rank_display_query"
WEIGHTAGE_DISPLAY_QUERY = "weightage_display_query"
COLOR_DISPLAY_QUERY = "color_display_query"
GET_COUNTRY_LIST_ID_QUERY = "get_country_list_id_query"
COUNTRY_RANK_SAVE_QUERY = "country_rank_save_query"
WEIGHTAGE_SAVE_QUERY = "weightage_save_query"
GET_VERSION_QUERY = "get_version_query"
UPDATE_ACTIVE_FLAG = "update_active_flag"
UPDATE_VERSION_QUERY = "update_version_query"
UPDATE_WEIGHTAGE_ACTIVE_FLAG = "update_weightage_active_flag"
UPDATE_WEIGHTAGE_VERSION_QUERY = "update_weightage_version_query"
INSERT_COLOR_QUERY = "insert_color_query"
SUMMARY_CRITERIA_JOIN_QUERY = "summary_criteria_join_query"
SUMMARY_TABLE_NAME = "f_rpt_scenario_stdy_sumry"
CRITERIA_TABLE_NAME = "f_rpt_scenario_stdy_criteria"

# Country Ranking Map View
FILTERS_SITE_RANKING_TABLE = "f_rpt_filters_site_ranking"
ST_STDY_DTLS_SITE_RNKG_TABLE = "f_rpt_site_study_details_site_ranking"
COUNTRY_LIST_TYPE = "country_list_type"
COUNTRY_RANKING_MAP_VIEW_DETAILS = "country_ranking_map_view_details"
COUNTRY_RANKING_MAP_COUNT_QUERY = "country_ranking_map_count_query"
COUNTRY_RANKING_FETCH_QUERY = "country_ranking_fetch_query"
MAP_COUNT = "map_count"
COUNTRY_RANKING_TOTAL_TRIAL_COUNT_QUERY = "country_ranking_total_trial_count_query"
FILTER_TABLE_NAME = "f_rpt_filters"
STUDY_DETAILS_TABLE_NAME = "f_rpt_site_study_details"
NUMBER_OF_TRIALS = "number_of_trials"
# COUNTRY_RANKING_ACTUAL_TABLE = "f_rpt_scenario_country_ranking_sid"
# COUNTRY_RANKING_TEMP_TABLE = "f_rpt_scenario_country_ranking_sid_temp"
# COUNTRY_LIST_ID = "country_list_id"
# SUMMARY_CRITERIA_JOIN_QUERY = "summary_criteria_join_query"

# Scenario Housekeeping
VALIDATE_USER_QUERY = "validate_user_query"
UPDATE_STATUS_QUERY = "update_status_query"

# Site Details
INVESTIGATOR_LIST_ID = "investigator_list_id"
SITE_DETAIL_QUERIES = "site_detail_queries"
GET_INVESTIGATOR_LIST_ID_QUERY = "get_investigator_list_id_query"
INVESTIGATOR_RANKING_ACTUAL_TABLE = "f_rpt_scenario_investigator_ranking"
INVESTIGATOR_RANKING_TEMP_TABLE = "f_rpt_scenario_investigator_ranking_temp"
FETCH_AFFILIATED_INVESTIGATORS_DETAILS = "fetch_affiliated_investigators_details"

# pi-ranking-map-view
# INVESTIGATOR_LIST_ID = "investigator_list_id"
INVESTIGATOR_LIST_TYPE = "investigator_list_type"
# INVESTIGATOR_RANKING_TEMP_TABLE = ".f_rpt_scenario_investigator_ranking_temp"
# INVESTIGATOR_RANKING_ACTUAL_TABLE = ".f_rpt_scenario_investigator_ranking_20256"
INVESTIGATOR_SITE_STUDY_DETAILS_TABLE =\
    "f_rpt_investigator_site_study_details"

# compare-scenarios
STUDY_CRITERIA_TABLE = "f_rpt_scenario_stdy_criteria"
SITE_RANK_TABLE = "f_rpt_scenario_site_ranking"
SITE_OPTIMIZED_TABLE = "f_rpt_scenario_site_ranking_optimized"
COUNTRY_RANK_TABLE = "f_rpt_scenario_country_ranking"
PI_RANK_TABLE = "f_rpt_scenario_investigator_ranking"
PI_OPTIMIZED_TABLE = "f_rpt_scenario_investigator_ranking_optimized"
FILTER_CONSTRAINTS_TABLE = "f_rpt_filter_constraints_optimizer_dtl"
SUMMARY_TABLE = "f_rpt_scenario_stdy_sumry"
SITE_WEIGHTS_TABLE = "f_rpt_scenario_metric_weight_dtl"
REGION_OPTIMIZER_TABLE = "f_rpt_region_optimizer_dtl"
COUNTRY_OPTIMIZER_TABLE = "f_rpt_country_optimizer_dtl"
SITE_FINAL_TABLE = "f_rpt_scenario_site_ranking_final"

# Export Service
# SUMMARY_TABLE = "f_rpt_scenario_stdy_sumry"
# STUDY_CRITERIA_TABLE = "f_rpt_scenario_stdy_criteria"
# SITE_RANK_TABLE = "f_rpt_scenario_site_ranking"
# SITE_WEIGHTS_TABLE = "f_rpt_scenario_metric_weight_dtl"
# FILTER_CONSTRAINTS_TABLE = "f_rpt_filter_constraints_optimizer_dtl"
# REGION_OPTIMIZER_TABLE = "f_rpt_region_optimizer_dtl"
# COUNTRY_OPTIMIZER_TABLE = "f_rpt_country_optimizer_dtl"
# SITE_OPTIMIZED_TABLE = "f_rpt_scenario_site_ranking_optimized"
TEMP_FILES_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files"
TEMP_SITE_RANK_FILE_NAME = "temp_ranking.xlsm"
TEMP_SITE_RANK_OPTIMIZED_FILE_NAME = "temp_optimization.xlsm"
FINAL_RANK_LIST_FILE_NAME = "site_rank_list.xlsm"
PROJECT_ENROLLMENT_DETAILS_FILE_NAME = "project_enrollment.xlsx"
FINAL_RANK_OPTIMIZED_LIST_FILE_NAME = "site_optimized_list.xlsm"
SITE_ZIP_FILE_NAME = "site_zip.zip"
FINAL_SITE_LIST_FILE_NAME = "site_final_list.xlsm"
TEMP_SITE_FINAL_FILE_NAME = "temp_site_final.xlsm"

# Tables Name
TEMP_RANK_TABLE = "f_rpt_scenario_site_ranking_temp"
RANK_TABLE = "f_rpt_scenario_site_ranking"
TEMP_WEIGHT_TABLE = "f_rpt_scenario_metric_weight_dtl_temp"
WEIGHT_TABLE = "f_rpt_scenario_metric_weight_dtl"

REGION_OPTIMIZER_DTL = "f_rpt_region_optimizer_dtl"
COUNTRY_OPTIMIZER_DTL = "f_rpt_country_optimizer_dtl"
FILTER_CONST_DTL = "f_rpt_filter_constraints_optimizer_dtl"

TEMP_RANK_INVESTIGATOR_TABLE = "f_rpt_scenario_investigator_ranking_temp"
ACTUAL_RANK_INVESTIGATOR_TABLE = "f_rpt_scenario_investigator_ranking"

#Optimize_Config_Path
#optimize_code_path = "/clinical_design_center/webservices/cdl_ctfo/microservices/optimization_model/"

optimize_code_path = "/clinical_design_center/data_science/cdl_ctfo/optimization_model/"

app_usage_local_path = "/clinical_design_center/webservices/cdl_ctfo/{file_name}"
#app_usage_local_path = "/app/clinical_design_center_admin_console/webservices/cdl_ctfo/{file_name}"

PATIENT_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/PatientExportTemplate.xlsm"
#EXPORT_PATH = "s3:///clinical-data-lake/dev/applications/export_service/{file_name}"
EXPORT_PATH = "s3://"+bucket_name+"/clinical-data-lake/"+environment+"/applications/export_service/{file_name}"
PATIENT_TEMPLATE_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/PatientExportTemplate.xlsm"

ALL_FILTER_VALUE = ["all"]

COUNTRY_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/CountryExportTemplate.xlsm"
COUNTRY_TEMPLATE_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/CountryExportTemplate.xlsm"

# drug landscape

DRUG_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/DrugExportTemplate.xlsm"
DRUG_TEMPLATE_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/DrugExportTemplate.xlsm"

# trial landscape

TRIAL_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/TrialExportTemplate.xlsm"
TRIAL_TEMPLATE_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/TrialExportTemplate.xlsm"

#trial universe and benchmark

TRIAL_UNIVERSE_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/TrialUniverseTemplate.xlsm"
TRIAL_UNIVERSE_TEMPLATE_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/TrialUniverseTemplate.xlsm"

# trial benchmark data
TRIAL_BENCHMARK_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/TrialBenchmarkTemplate.xlsm"
TRIAL_BENCHMARK_TEMPLATE_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/TrialBenchmarkTemplate.xlsm"

# Qualtrics

SURVEY_CREATION_STATUS = "200 - OK"
count_val = '3'
survey_status = 'go_live'
KEY_COUNTRY_OUTREACH = 'country_outreach'
KEY_SITE_OUTREACH = 'site_outreach'
KEY_VIEW = "View"
KEY_GENERATE = "Generate"
SURVEY_COMPLETED_PERCENT = 100.0

# project enrollment
PROJECT_ENROLLMENT_DETAILS = "project_enrollment_screen"
UPDATE_ENROLLMENT_ACTIVE_FLAG = "update_enrollment_active_flag"
#EXPORT_ENROLLMENT_S3_PATH = "s3:///temp/clinical-data-lake/applications/export_service/project_enrollment"
EXPORT_ENROLLMENT_S3_PATH = "s3://"+bucket_name+"/temp/clinical-data-lake/applications/export_service/project_enrollment"
EXPORT_ENROLLMENT_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/"

SEND_COUNTRY_REMINDER_SURVEY_SUBJECT = "Country Outreach Survey Reminder"
SEND_SITE_REMINDER_SURVEY_SUBJECT = "Site Outreach Survey Reminder"
COUNTRY_REMINDER_EMAIL_TYPE = "reminder_mail"

KEY_INDIVIDUAL_COUNTRY_HEADS_DETAILS_MSG = "No data in get_individual_country_heads_details_output"
KEY_COUNTRY_HEAD_DETAILS_MSG = "No data in get_country_heads_details_output"

VIEW_SURVEY_RESPONSE_LINK ="https://zsassociates.co1.qualtrics.com/WRQualtricsControlPanel/Report.php?R={responseID}&NoStatsTables=1&SID={surveyID}&ResponseSummary=True"

STATUS_CREATED = "created"
FALSE_FLAG = False

# Metric data upload
TEMP_EC2_DIRECTORY = "/clinical_design_center/webservices/cdl_ctfo/temp_files/metric_data/user_id/scenario_id/"
TEMP_EC2_CSV_DIRECTORY = "/clinical_design_center/webservices/cdl_ctfo/temp_files/metric_data/user_id/scenario_id/CSV/"
COUNTRY_META_FILE = 'country_metric_data'
SITE_META_FILE = 'site_metric_data'
KEY_VALIDATION_STATUS = 'validated'
KEY_COUNTRY = 'country'
KEY_SITE = 'site'

# admin console
CREATE_TYPE = "create_type"
USER_ACCESS_ID = "user_access_id"
USER_GROUP = "user_group"
FILTERS = "filters"
LAST_LOGIN_FROM = "last_login_from"
LAST_LOGIN_TO = "last_login_to"
COUNTRY_KEY = "country"
VIEW_SCENARIO_SEARCH = "view_scenario_search"

site_cols =['Q2_1', 'Q2_4', 'Q2_5', 'Q2_6', 'Q2_7', 'Q2_8', 'Q2_9', 'Q2_10', 'Q2_11', 'Q2_12']
site_cols_non_ecda =['QID2_1', 'QID2_4', 'QID2_5', 'QID2_6', 'QID2_7', 'QID2_8', 'QID2_9', 'QID2_10', 'QID2_11', 'QID2_12']

site_pi_speciality_cols =['Q7_1','Q7_2']
site_pi_speciality_cols_non_ecda =['QID7_1','QID7_2']

country_cols =['QID58_10','QID58_11','QID58_12','QID58_13','QID58_14','QID58_15','QID58_16','QID58_17','QID58_18']

ADMIN_CONSOLE_EXPORT_LOCAL_PATH = "/clinical_design_center/webservices/cdl_ctfo/"
ADMIN_CONSOLE_EXPORT_LOCAL_PATH_TEMPLATE_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/AdminConsoleExportTemplate.xlsm"

# Protocol Document
UPLOAD_TEMP_PATH = "/clinical_design_center/webservices/cdl_ctfo/temp_files/"
#PROTOCOL_DOC_S3_PATH = "s3:///temp/clinical-data-lake/applications/ProtocolDocuments/"
PROTOCOL_DOC_S3_PATH = "s3://"+bucket_name+"/temp/clinical-data-lake/applications/ProtocolDocuments/"
ISALIVE = "is_alive"

#Optimization:

UPLOAD_OPT_PATH = "s3://"+bucket_name+"/clinical-data-lake/"+environment+"/applications/export_service/"

#user roles:
superuser = "superuser"
gfl = "gfl"
mda = "mda"

# USER ACTIVITY BACKUP
user_activity_file_path = "/clinical_design_center/webservices/cdl_ctfo/"

# CSV upload for PE inserts:

PATIENTS_ENROLLED_COUNTRY_INSERT = "/clinical_design_center/webservices/cdl_ctfo/temp_files/INSERT_COUNTRY_PATIENTS_ENROLLED"
PATIENTS_ENROLLED_GLOBAL_INSERT = "/clinical_design_center/webservices/cdl_ctfo/temp_files/INSERT_GLOBAL_PATIENTS_ENROLLED"
PATIENTS_ENROLLED_REGION_INSERT = "/clinical_design_center/webservices/cdl_ctfo/temp_files/INSERT_REGION_PATIENTS_ENROLLED"
PATIENTS_ENROLLED_COUNTRY_LEVEL_INSERT = "/clinical_design_center/webservices/cdl_ctfo/temp_files/INSERT_COUNTRY_LEVEL_PATIENTS_ENROLLED"
SITES_ACTIVATED_STUDY_LEVEL_INSERT = "/clinical_design_center/webservices/cdl_ctfo/temp_files/INSERT_STUDY_SITES_ACTIVATED"
SITES_ACTIVATED_COUNTRY_LEVEL_INSERT = "/clinical_design_center/webservices/cdl_ctfo/temp_files/INSERT_COUNTRY_LEVEL_SITES_ACTIVATED"