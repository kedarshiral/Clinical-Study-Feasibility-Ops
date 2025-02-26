#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

######################################################Module Information################################################
#   Module Name         :   SurveyScheduledJobUtils
#   Purpose             :   Module to fetch survey statuses and response via API call to Qualtrics
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate the class and invoke functions using objects
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Last changed on     :   24th January 2021
#   Last changed by     :
#   Reason for change   :   Initial development
########################################################################################################################

import os
import copy
import sys
import traceback
import json
import base64
import re
import time
import statistics
import numpy as np
import argparse
import subprocess
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import SurveyScheduledJobQueries
from UpdateRecord import UpdateRecord

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "utilities/"))
sys.path.insert(0, UTILITIES_DIR_PATH)
import CommonServicesConstants as Constants
from DatabaseUtility import DatabaseUtility
from QualtricsUtility import QualtricsUtility
from LogSetup import get_logger

LOGGING = get_logger()
from utils import Utils


CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs"))
SURVEY_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/survey_config.json"))
APP_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/application_config.json"))
ENV_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/environment_config.json"))

UTILS_OBJ = Utils()

# response output
SURVEY_CREATION_STATUS = "200 - OK"

# schedule job log varables.
JOB_STATUS_IN_PROGRESS = "In-Progess"
JOB_STATUS_COMPLETE = "Completed"
JOB_STATUS_FAILED = "Failed"

JOB_DTL_LEVEL_SURVEY = "Survey"
JOB_DTL_LEVEL_CO = "Country"
JOB_DTL_LEVEL_SITE = "Site"

# Survey Types
SITE_OUTREACH = "site_outreach"
COUNTRY_OUTREACH = "country_outreach"

# Qualtrics survey status
SURVEY_ACTIVE_STATUS = "Active"
SURVEY_NOT_ACTIVE_STATUS = "Inactive"

# survey status
SURVEY_STATUS_GOLIVE = "go_live"
SURVEY_STATUS_INITIATED = "initiated"

# Qualtrics response API call variables
SURVEY_RESPONSE_FORMAT = "json"
INPROGRES_FLAG_TRUE = "true"
INPROGRES_FLAG_FLASE = "false"

MC_QUESTION_TYPE = "MC"
TE_QUESTION_TYPE = "TE"

# tables to update via UpdateRecord
SURVEY_DTL = "f_rpt_user_scenario_survey_dtl"
SURVEY_COUNTRY_DTL = "f_rpt_user_scenario_survey_country_dtl"
SURVEY_SITE_DTL = "f_rpt_user_scenario_survey_site_investigator_dtl"
schema_name = ""


class SurveyScheduledJobUtils:

    def __init__(self, caller, log_params=None):
        print("*****in init - " + str(log_params))
        self.caller = caller
        LOGGING.info("The caller of this module is {}".format(self.caller))
        self.application_config_data = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(self.application_config_data)
        if UTILS_OBJ.db_connection_details is None:
            raise Exception('db_connection_details KEY not present')
        print(self.application_config_data)

        self.host_name = UTILS_OBJ.host
        LOGGING.info("Host name fetched = {}".format(str(self.host_name)))
        self.port_name = UTILS_OBJ.port
        LOGGING.info("Port name fetched = {}".format(str(self.port_name)))
        self.user_name = UTILS_OBJ.username
        LOGGING.info("User name fetched = {}".format(str(self.user_name)))
        self.password = UTILS_OBJ.password
        # self.password = base64.b64decode(self.password).decode("utf-8")
        self.schema_name = self.application_config_data["ctfo_services"]["db_connection_details"]["schema"]
        self.schema_reporting = self.application_config_data["ctfo_services"]["schema_reporting"]
        LOGGING.info("Schema name fetched = {}".format(str(self.schema_name)))
        self.database_name = UTILS_OBJ.database_name
        LOGGING.info("Database name fetched = {}".format(str(self.database_name)))

        survey_json_file = open(SURVEY_CONFIG_PATH, 'r').read()
        self.survey_config_data = json.loads(survey_json_file)
        self.language = self.survey_config_data["language"]
        # self.project_category = self.survey_config_data["survey_configuration"]["project_category"]
        # API token and group ID base 64 encoded
        self.api_token = self.survey_config_data["api_token"]
        self.api_token = base64.b64decode(self.api_token).decode("utf-8")
        self.data_center = self.survey_config_data["data_center"]

    def get_active_survey_ids(self, conn_var, extra_args):
        """
                   Purpose :   Fetch the active survey_id from  from survey_dtl table
                   Input   :
                           conn_var   -> connection details of the postgres instance
                           extra_args -> for logging purposes
                   Output  :
                           list of survey ids
        """
        try:

            query_to_get_active_surveyid = SurveyScheduledJobQueries.QUERY_TO_GET_SURVEYID
            query_to_get_active_surveyid = query_to_get_active_surveyid.format(schema_name=self.schema_name)

            LOGGING.info("query_to_get_active_surveyid query - {}".format(str(query_to_get_active_surveyid)))
            get_active_surveyid_response = DatabaseUtility().execute(
                query=query_to_get_active_surveyid, conn=conn_var, auto_commit=False)

            LOGGING.info("get_active_surveyid_response - {}".format(str(get_active_surveyid_response)))

            if get_active_surveyid_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to execute query_to_get_active_surveyid query- {}".format(
                        get_active_surveyid_response[Constants.ERROR_KEY]))
            active_surveyid_data = None
            LOGGING.info("Executed query to get_active_survey_id successfully")
            if len(get_active_surveyid_response[Constants.RESULT_KEY]) != 0:
                active_surveyid_data = get_active_surveyid_response[Constants.RESULT_KEY]
                LOGGING.info("Data fetched from get_active_survey_id - {}".format(
                    get_active_surveyid_response[Constants.RESULT_KEY])
                             )
            return active_surveyid_data
        except:
            raise Exception(traceback.format_exc())

    def get_survey_status(self, conn_var, surveyid_data, extra_args, log_dtl_update_params):
        """
                   Purpose :
                             Call the get_survey_metadata and get_survey_versions Qualtrics API;
                             Update the status of the survey "Go-Live" in the f_rpt_user_scenario_survey_dtl table and
                             log_dlt table
                   Input   :
                             conn_var -> PostgreSQL connection variable
                             surveyid_data ->
                             extra_argd ->
                             log_dtl_update_params ->

                   Output  :
                             executed_successfully -> flag; true/flase
        """
        try:
            LOGGING.info("Fetching the survey status for {}".format(str(surveyid_data)))

            survey_status = ""
            survey_last_activated = ""

            # parameter_dict = {
            #     "api_token": self.api_token,
            #     "data_center": self.data_center,
            #     "survey_id": surveyid_data["survey_id"]
            # }
            parameter_dict = {
                "api_token": base64.b64decode(surveyid_data["key"]).decode("utf-8"),
                "data_center": self.data_center,
                "survey_id": surveyid_data["survey_id"]
            }
            call_get_survey_metadata = QualtricsUtility().get_survey_metadata
            invoke_qualtrics_api_response = QualtricsUtility().invoke_qualtrics_api(call_get_survey_metadata,
                                                                                    parameter_dict, extra_args)
            LOGGING.info("call_get_survey_metadata response - {}".format(str(invoke_qualtrics_api_response)))

            if invoke_qualtrics_api_response["meta"]["httpStatus"] == SURVEY_CREATION_STATUS:
                LOGGING.info("metadata fetched successfully for survey_id {}".format(surveyid_data["survey_id"]))

                survey_status = invoke_qualtrics_api_response["result"]["SurveyStatus"]
                LOGGING.info("Qualtrics status for survey_id {} is {}".format(surveyid_data["survey_id"], survey_status)
                             )

                if survey_status == SURVEY_ACTIVE_STATUS:
                    call_get_survey_versions = QualtricsUtility().get_survey_versions
                    invoke_qualtrics_api_response = QualtricsUtility().invoke_qualtrics_api(call_get_survey_versions,
                                                                                            parameter_dict, extra_args)
                    LOGGING.info("call_get_survey_versions response - {}".format(str(invoke_qualtrics_api_response)))

                    if invoke_qualtrics_api_response["meta"]["httpStatus"] == SURVEY_CREATION_STATUS:
                        LOGGING.info(
                            "Survey versions fetched successfully for survey_id {}".format(surveyid_data["survey_id"]))
                        if invoke_qualtrics_api_response["result"]["elements"]:
                            for versions in invoke_qualtrics_api_response["result"]["elements"]:
                                if versions["metadata"]["published"]:
                                    survey_last_activated = versions["metadata"]["publishEvents"][0]["date"]
                                    survey_status = SURVEY_STATUS_GOLIVE
                        else:
                            survey_status = ""
                            survey_last_activated = ""

                    else:
                        raise Exception(
                            "Failed to get_survey_versions for Survey {} - {}".format(surveyid_data["survey_id"],
                                                                                      str(
                                                                                          invoke_qualtrics_api_response)))
                else:
                    survey_status = ""
                    survey_last_activated = ""

                LOGGING.info("Response from get_survey_status {} - {}".format(survey_status, survey_last_activated))

                # survey_status will be "" when: 1. an exception occurs in get_survey_status or Survey is not punlished yet
                if surveyid_data["survey_status"] != survey_status and len(survey_status) > 0:
                    current_timestamp = datetime.utcnow()
                    current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")
                    version = datetime.strftime(current_timestamp, "%Y%m%d%H%M%S")
                    update_flag = "Y"

                    if surveyid_data["survey_version"] == "" or surveyid_data["survey_version"] is None:

                        keys_to_be_updated = {
                            "survey_id": surveyid_data["survey_id"],
                            "survey_dtl_version": version,
                            "survey_version": "1",
                            "survey_published_date": survey_last_activated,
                            "survey_status": survey_status,
                            "last_updated_by": self.user_name,
                            "last_updated_time": current_time_string,
                            "is_active": True
                        }
                    else:
                        keys_to_be_updated = {
                            "survey_id": surveyid_data["survey_id"],
                            "survey_dtl_version": version,
                            "survey_version": str(int(surveyid_data["survey_version"]) + 1),
                            "survey_update_published_date": survey_last_activated,
                            "survey_status": survey_status,
                            "last_updated_by": self.user_name,
                            "last_updated_time": current_time_string,
                            "is_active": True
                        }

                    where_clause_json = {
                        "theme_id": surveyid_data["theme_id"],
                        "survey_id": surveyid_data["survey_id"],
                        "survey_version": surveyid_data["survey_version"],
                        "is_active": True
                    }
                    # add a try_except here
                    update_record_response = UpdateRecord().update_record(table_name=SURVEY_DTL,
                                                                          schema_name=self.schema_name,
                                                                          connection_object=conn_var,
                                                                          keys_to_be_updated=keys_to_be_updated,
                                                                          update_flag=update_flag,
                                                                          log_params=extra_args,
                                                                          update_where_clause_json=where_clause_json)

                return True
            else:
                raise Exception("Failed to get_survey_metadata for Survey {} - {}".format(surveyid_data["survey_id"],
                                                                                          str(
                                                                                              invoke_qualtrics_api_response)))
        except:
            log_dtl_update_params["end_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
            log_dtl_update_params["status"] = JOB_STATUS_FAILED
            self.update_log_dtl_record(conn_var, extra_args, log_dtl_update_params)
            LOGGING.error(
                "Error in get_survey_status method of SurveyScheduleJobUtilts : {}".format(traceback.format_exc()))

    def update_survey_dtl_responses(self, conn_var, surveyid_data, previous_job_details, extra_args,
                                    log_dtl_update_params):
        """
                Purpose :
                Input   :

                Output  :
        """
        try:
            LOGGING.info("Fetching the survey structure for survey_id  - {}".format(str(surveyid_data)))
            # parameter_dict = {
            #     "api_token": self.api_token,
            #     "data_center": self.data_center,
            #     "survey_id": surveyid_data["survey_id"]
            # }
            parameter_dict = {
                "api_token": base64.b64decode(surveyid_data["key"]).decode("utf-8"),
                "data_center": self.data_center,
                "survey_id": surveyid_data["survey_id"]
            }
            call_get_survey = QualtricsUtility().get_survey
            survey_structure = None
            invoke_qualtrics_api_response = QualtricsUtility().invoke_qualtrics_api(call_get_survey,
                                                                                    parameter_dict, extra_args)
            LOGGING.info("call_get_survey response - {}".format(str(invoke_qualtrics_api_response)))

            if invoke_qualtrics_api_response["meta"]["httpStatus"] == SURVEY_CREATION_STATUS:
                LOGGING.info("get_survey fetched successfully for survey_id {}".format(surveyid_data["survey_id"]))
                survey_structure = invoke_qualtrics_api_response
            else:
                raise Exception(
                    "Failed to call_get_survey for Survey - {}".format(str(invoke_qualtrics_api_response)))

            # calling the get_survey_response API for responses in progress
            if previous_job_details is not None:
                # parameter_dict = {
                #     "api_token": self.api_token,
                #     "data_center": self.data_center,
                #     "survey_id": surveyid_data["survey_id"],
                #     "file_format": SURVEY_RESPONSE_FORMAT,
                #     "inprogress_flag": INPROGRES_FLAG_TRUE,
                #     "start_time": str(
                #         datetime.strptime(previous_job_details["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                #             "%Y-%m-%dT%H:%M:%SZ")),
                #     "end_time": str(
                #         datetime.strptime(log_dtl_update_params["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                #             "%Y-%m-%dT%H:%M:%SZ")),
                # }
                parameter_dict = {
                    "api_token": base64.b64decode(surveyid_data["key"]).decode("utf-8"),
                    "data_center": self.data_center,
                    "survey_id": surveyid_data["survey_id"],
                    "file_format": SURVEY_RESPONSE_FORMAT,
                    "inprogress_flag": INPROGRES_FLAG_TRUE,
                    "start_time": str(
                        datetime.strptime(previous_job_details["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                            "%Y-%m-%dT%H:%M:%SZ")),
                    "end_time": str(
                        datetime.strptime(log_dtl_update_params["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                            "%Y-%m-%dT%H:%M:%SZ")),
                }
            else:
                # parameter_dict = {
                #     "api_token": self.api_token,
                #     "data_center": self.data_center,
                #     "survey_id": surveyid_data["survey_id"],
                #     "file_format": SURVEY_RESPONSE_FORMAT,
                #     "inprogress_flag": INPROGRES_FLAG_TRUE
                # }
                parameter_dict = {
                    "api_token": base64.b64decode(surveyid_data["key"]).decode("utf-8"),
                    "data_center": self.data_center,
                    "survey_id": surveyid_data["survey_id"],
                    "file_format": SURVEY_RESPONSE_FORMAT,
                    "inprogress_flag": INPROGRES_FLAG_TRUE
                }

            call_get_survey_response_inprogress = QualtricsUtility().get_survey_response
            survey_response_inprogress = None
            LOGGING.info(
                "call call_get_survey_response_inprogress from SurveyScheduledJob with parameter - {}" \
                    .format(str(parameter_dict)))

            invoke_qualtrics_api_response = QualtricsUtility().invoke_qualtrics_api(call_get_survey_response_inprogress,
                                                                                    parameter_dict, extra_args)
            LOGGING.info("call_get_survey_response_inprogress response - {}".format(str(invoke_qualtrics_api_response)))

            LOGGING.info("call_get_survey_response_inprogress fetched successfully for survey_id {}".format(
                surveyid_data["survey_id"]))
            survey_response_inprogress = invoke_qualtrics_api_response
            # sorting the responses in the order of endDate
            if survey_response_inprogress:
                survey_response_inprogress["responses"] = sorted(survey_response_inprogress["responses"],
                                                                 key=lambda x: x["values"]["endDate"])

            if previous_job_details is not None:
                # parameter_dict = {
                #     "api_token": self.api_token,
                #     "data_center": self.data_center,
                #     "survey_id": surveyid_data["survey_id"],
                #     "file_format": SURVEY_RESPONSE_FORMAT,
                #     "inprogress_flag": INPROGRES_FLAG_FLASE,
                #     "start_time": str(
                #         datetime.strptime(previous_job_details["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                #             "%Y-%m-%dT%H:%M:%SZ")),
                #     "end_time": str(
                #         datetime.strptime(log_dtl_update_params["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                #             "%Y-%m-%dT%H:%M:%SZ")),
                # }
                parameter_dict = {
                    "api_token": base64.b64decode(surveyid_data["key"]).decode("utf-8"),
                    "data_center": self.data_center,
                    "survey_id": surveyid_data["survey_id"],
                    "file_format": SURVEY_RESPONSE_FORMAT,
                    "inprogress_flag": INPROGRES_FLAG_FLASE,
                    "start_time": str(
                        datetime.strptime(previous_job_details["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                            "%Y-%m-%dT%H:%M:%SZ")),
                    "end_time": str(
                        datetime.strptime(log_dtl_update_params["start_time"], "%Y-%m-%d %H:%M:%S").strftime(
                            "%Y-%m-%dT%H:%M:%SZ")),
                }
            else:
                # parameter_dict = {
                #     "api_token": self.api_token,
                #     "data_center": self.data_center,
                #     "survey_id": surveyid_data["survey_id"],
                #     "file_format": SURVEY_RESPONSE_FORMAT,
                #     "inprogress_flag": INPROGRES_FLAG_FLASE
                # }
                parameter_dict = {
                    "api_token": base64.b64decode(surveyid_data["key"]).decode("utf-8"),
                    "data_center": self.data_center,
                    "survey_id": surveyid_data["survey_id"],
                    "file_format": SURVEY_RESPONSE_FORMAT,
                    "inprogress_flag": INPROGRES_FLAG_FLASE
                }

            survey_response_complete = None

            call_get_survey_response_complete = QualtricsUtility().get_survey_response

            LOGGING.debug(
                "call call_get_survey_response_complete from SurveySchduledJob with parameter - {}" \
                    .format(str(parameter_dict)),
                extra=extra_args)

            invoke_qualtrics_api_response = QualtricsUtility().invoke_qualtrics_api(
                call_get_survey_response_complete, parameter_dict, extra_args)

            LOGGING.debug(
                "call_get_survey_response_complete response - {}".format(str(invoke_qualtrics_api_response)),
                extra=extra_args)

            LOGGING.debug("call_get_survey_response_complete fetched successfully for survey_id {}".format(
                surveyid_data["survey_id"]), extra=extra_args)
            survey_response_complete = invoke_qualtrics_api_response

            # sorting the responses in the order of endDate
            if survey_response_complete:
                survey_response_complete["responses"] = sorted(survey_response_complete["responses"],
                                                               key=lambda x: x["values"]["endDate"])

            self.update_responses_complete(conn_var, surveyid_data, extra_args, survey_response_complete,
                                           survey_structure, log_dtl_update_params)

            self.update_responses_inprogress(conn_var, surveyid_data, extra_args, survey_response_inprogress,
                                             log_dtl_update_params)

            return True

        except:
            log_dtl_update_params["end_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
            log_dtl_update_params["status"] = JOB_STATUS_FAILED
            self.update_log_dtl_record(conn_var, extra_args, log_dtl_update_params)
            LOGGING.error(
                "Error in update_survey_country_dtl_responses method of SurveyScheduleJobUtilts : {}".format(
                    traceback.format_exc()))

    def update_responses_inprogress(self, conn_var, surveyid_data, extra_args, survey_response_inprogress,
                                    log_dtl_update_params):
        """
            Purpose :
            Input   :
            Output  :
        """
        try:
            LOGGING.info("calling  the update_responses_inprogress for survey_id  - {}".format(str(surveyid_data)))
            survey_response_type_gl = self.survey_config_data["survey_questionnaire"]["responses"]["survey_response_type_gl"]
            survey_response_type_anonymous = self.survey_config_data["survey_questionnaire"]["responses"]["survey_response_type_anonymous"]
            if len(survey_response_inprogress["responses"]) > 0:
                for responses in survey_response_inprogress["responses"]:
                    # skipping the non "gl" responses
                    if responses["values"]["distributionChannel"] not in [survey_response_type_gl,
                                                                          survey_response_type_anonymous]:
                        continue

                    if "ContactID" not in responses["values"].keys():
                        continue
                    response_id = progress = contact_id = None
                    response_id = responses["responseId"]
                    progress = responses["values"]["progress"]
                    contact_id = responses["values"]["ContactID"]

                    current_timestamp = datetime.utcnow()
                    current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")
                    version = datetime.strftime(current_timestamp, "%Y%m%d%H%M%S")

                    update_flag = "Y"
                    # check if survey_type = COUNTRY_OUTREACH then update country_dtl table else update investigator_dtl table
                    # update the response_id and progress in survey_country_dtl table

                    if surveyid_data["survey_type"] == COUNTRY_OUTREACH:

                        keys_to_be_updated = {
                            "survey_country_version": version,
                            "response_id": response_id,
                            "progress_percentage": progress,
                            "last_updated_by": self.user_name,
                            "last_updated_time": current_time_string,
                            "is_active": True
                        }

                        where_clause_json = {
                            "theme_id": surveyid_data["theme_id"],
                            "survey_id": surveyid_data["survey_id"],
                            "qualtrics_contact_id": contact_id,
                            "is_active": True
                        }

                        # read the existing record and update if the value of the columns in keys_to_be_updated changed
                        query_get_record_country_dtl = SurveyScheduledJobQueries.GET_RECORD_COUNTRY_DTL
                        query_get_record_country_dtl = query_get_record_country_dtl.format(schema_name=self.schema_name,
                                                                                           survey_id=surveyid_data[
                                                                                               "survey_id"],
                                                                                           theme_id=surveyid_data[
                                                                                               "theme_id"],
                                                                                           qualtrics_contact_id=contact_id)

                        LOGGING.debug("query_get_record_country_dtl query - {}".format(str(query_get_record_country_dtl)))

                        get_record_country_dtl_response = DatabaseUtility().execute(
                            query=query_get_record_country_dtl, conn=conn_var, auto_commit=False)

                        LOGGING.debug(
                            "get_record_country_dtl_response - {}".format(str(get_record_country_dtl_response)))
                        country_dtl_record = get_record_country_dtl_response[Constants.RESULT_KEY]
                        if len(country_dtl_record)==0:
                            LOGGING.debug("Skipping the survey - {} since no records found in country_dtl table".format(surveyid_data["survey_id"]))
                            continue
                        if country_dtl_record[0]["progress_percentage"] is None:
                            country_dtl_record[0]["progress_percentage"] = 0

                        log_dtl_update_params["country_name"] = country_dtl_record[0]["country_name"]
                        log_dtl_update_params["ctfo_site_id"] = ""
                        log_dtl_update_params["investigator_id"] = ""
                        if country_dtl_record[0]["response_id"] != response_id or \
                                int(float(country_dtl_record[0]["progress_percentage"])) != int(float(progress)):
                            update_record_response = UpdateRecord().update_record(SURVEY_COUNTRY_DTL, self.schema_name,
                                                                                  conn_var,
                                                                                  keys_to_be_updated, update_flag,
                                                                                  extra_args,
                                                                                  where_clause_json)

                    if surveyid_data["survey_type"] == SITE_OUTREACH:

                        keys_to_be_updated = {
                            "survey_site_investigator_version": version,
                            "response_id": response_id,
                            "progress_percentage": progress,
                            "last_updated_by": self.user_name,
                            "last_updated_time": current_time_string,
                            "is_active": True
                        }

                        where_clause_json = {
                            "theme_id": surveyid_data["theme_id"],
                            "survey_id": surveyid_data["survey_id"],
                            "qualtrics_contact_id": contact_id,
                            "is_active": True
                        }

                        # read the existing record and update if thevalue of the coloumns in keys_to_be_updated changed
                        query_get_record_investigator_dtl = SurveyScheduledJobQueries.GET_RECORD_INVESTIGATOR_DTL
                        query_get_record_investigator_dtl = query_get_record_investigator_dtl.format(
                            schema_name=self.schema_name,
                            survey_id=surveyid_data[
                                "survey_id"],
                            theme_id=surveyid_data[
                                "theme_id"],
                            qualtrics_contact_id=contact_id)

                        LOGGING.debug("query_get_record_investigator_dtl query - {}".format(str(query_get_record_investigator_dtl)))
                        get_record_investigator_dtl_response = DatabaseUtility().execute(
                            query=query_get_record_investigator_dtl, conn=conn_var, auto_commit=False)

                        LOGGING.debug(
                            "get_record_investigator_dtl_response - {}".format(
                                str(get_record_investigator_dtl_response)))
                        investigator_dtl_record = get_record_investigator_dtl_response[Constants.RESULT_KEY]
                        if len(investigator_dtl_record) == 0:
                            LOGGING.debug("Skipping the survey - {} since no records found in site_inv_table".format(
                                surveyid_data["survey_id"]))
                            continue
                        if investigator_dtl_record[0]["progress_percentage"] is None:
                            investigator_dtl_record[0]["progress_percentage"] = 0

                        log_dtl_update_params["country_name"] = ""
                        log_dtl_update_params["ctfo_site_id"] = investigator_dtl_record[0]["ctfo_site_id"]
                        log_dtl_update_params["investigator_id"] = investigator_dtl_record[0]["investigator_id"]

                        if investigator_dtl_record[0]["response_id"] != response_id or \
                                int(float(investigator_dtl_record[0]["progress_percentage"])) != int(float(progress)):
                            update_record_response = UpdateRecord().update_record(SURVEY_SITE_DTL, self.schema_name,
                                                                                  conn_var,
                                                                                  keys_to_be_updated, update_flag,
                                                                                  extra_args,
                                                                                  where_clause_json)


        except:
            LOGGING.error(
                "Error in update_responses_inprogress method of SurveyScheduleJobUtilts : {}".format(
                    traceback.format_exc()))
            raise Exception(
                "Error in update_responses_inprogress method of SurveyScheduleJobUtilts : {}".format(
                    traceback.format_exc())
            )

    def update_responses_complete(self, conn_var, surveyid_data, extra_args, survey_response_complete, survey_structure,
                                  log_dtl_update_params):
        """
            Purpose :
            Input   :
            Output  :
        """
        try:
            LOGGING.info("calling  the update_responses_complete for survey_id  - {}".format(str(surveyid_data)))
            # extract the survey questionID and question_text
            LOGGING.info("survey questionID and question_text for survey_id  - {}".format(str(survey_structure)))
            if len(survey_structure["result"]["Questions"]) == 0:
                log_dtl_update_params["country_name"] = ""
                log_dtl_update_params["ctfo_site_id"] = ""
                log_dtl_update_params["investigator_id"] = ""
                return
            survey_questions_json = survey_structure["result"]["Questions"]
            survey_questions_json_key = survey_structure["result"]["Questions"].keys()

            LOGGING.info("Question IDs for Country_Outreach Survey_id {} is {}".
                         format(surveyid_data["survey_id"], survey_questions_json_key))
            ques_param = {
                "protocol_fit": [],
                "others": []
            }

            country_outreach_questionnaire = self.survey_config_data["survey_questionnaire"][COUNTRY_OUTREACH]
            site_outreach_questionnaire = self.survey_config_data["survey_questionnaire"][SITE_OUTREACH]
            survey_responses = self.survey_config_data["survey_questionnaire"]["responses"]
            survey_response_type_gl = survey_responses["survey_response_type_gl"]
            survey_response_type_anonymous = survey_responses["survey_response_type_anonymous"]
            survey_response_yes = survey_responses["survey_response_yes"]
            survey_response_no = survey_responses["survey_response_no"]
            survey_response_yes_hi = survey_responses["survey_response_yes_hi"]
            survey_response_yes_mi = survey_responses["survey_response_yes_mi"]
            survey_response_high_medium = survey_responses["survey_response_high_medium"]
            date_format = "%Y/%m/%d"

            # add details comments here
            # check if the survey_type = "country_outreach:
            if surveyid_data["survey_type"] == COUNTRY_OUTREACH:
                for question_id in survey_questions_json_key:
                    if survey_questions_json[question_id]["QuestionType"] == MC_QUESTION_TYPE and \
                            (True in [bool(i["question_id"] == survey_questions_json[question_id]["QuestionID"]) for i in
                                      country_outreach_questionnaire["protocol_fit"].values()]):
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in country_outreach_questionnaire["protocol_fit"].items() if bool(
                                value["question_id"] == survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"]}
                        ques_param["protocol_fit"].append(question_details)

                    if (survey_questions_json[question_id]["QuestionType"] == TE_QUESTION_TYPE or
                        survey_questions_json[question_id]["QuestionType"] == "MC") and \
                            True in [bool(country_outreach_questionnaire[x]["question_id"] ==
                                          survey_questions_json[question_id]["QuestionID"]) for x in ["degree_of_burden_patient",
                                                                             "first_site_initiated_response",
                                                                             "fifty_percent_sites_initiated_response",
                                                                             "all_sites_initiated_response",
                                                                             "avg_time_for_site_initiation_response",
                                                                             "last_patient_in_response",
                                                                             "last_patient_visit",
                                                                             "first_patient_in_date",
                                                                             "recruitment_duration",
                                                                             "is_delay_expected",
                                                                             "concern_on_include_exclude_criteria",
                                                                             "regulatory_fit",
                                                                             "investigators_declining_operational_feasibility",
                                                                             "degree_of_compliance",
                                                                             "precision_sites", "covid_impact",
                                                                             "inv_interested_in_study", "dropout_rate",
                                                                             "screen_failure_rate",
                                                                             "pa_groups_involved",
                                                                             "operational_capacity",
                                                                             "no_of_sites",
                                                                             "no_of_patients", "participation_will"
                                                                             ]]:
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in country_outreach_questionnaire.items() if
                             key != "protocol_fit" and bool(value["question_id"] ==
                                                            survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"]}
                        ques_param["others"].append(question_details)

                    if (survey_questions_json[question_id]["QuestionType"] == TE_QUESTION_TYPE or
                        survey_questions_json[question_id]["QuestionType"] == "MC") and \
                            True in [bool(country_outreach_questionnaire[x]["question_id"] ==
                                          survey_questions_json[question_id]["QuestionID"]) for x in ["any_competing_ongoing_planned_trials_patients",
                                                                             "any_competing_ongoing_planned_trials_staff"]]:
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in country_outreach_questionnaire.items() if
                             key != "protocol_fit" and bool(value["question_id"] ==
                                                            survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"]}
                        ques_param["others"].append(question_details)

                    if (survey_questions_json[question_id]["QuestionType"]) == "Slider" and \
                            (True in [bool(country_outreach_questionnaire[x]["question_id"] ==
                                           survey_questions_json[question_id]["QuestionID"]) for x in ["dropout_rate", "screen_failure_rate"]]):
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in country_outreach_questionnaire.items() if
                             key != "protocol_fit" and bool(value["question_id"] ==
                                                            survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"],
                                            "Choices": survey_questions_json[question_id]["Choices"],
                                            "ChoiceOrder": survey_questions_json[question_id]["Choices"]}
                        ques_param["others"].append(question_details)

                    if (survey_questions_json[question_id]["QuestionType"]) == "Matrix" and \
                            (True in [bool(country_outreach_questionnaire[x]["question_id"] ==
                                           survey_questions_json[question_id]["QuestionID"]) for x in ["number_of_sites_contacted_diverse_backgrounds"]]):
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in country_outreach_questionnaire.items() if
                             key != "protocol_fit" and bool(value["question_id"] ==
                                                            survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"],
                                            "Choices": survey_questions_json[question_id]["Choices"],
                                            "ChoiceOrder": survey_questions_json[question_id]["Choices"]}
                        ques_param["others"].append(question_details)

                LOGGING.info("Questions for Country_Outreach Survey_id {} is {}".
                             format(surveyid_data["survey_id"], ques_param))

                if len(survey_response_complete["responses"]):
                    for responses in survey_response_complete["responses"]:
                        # skipping the non "gl" responses
                        if responses["values"]["distributionChannel"] not in [survey_response_type_gl,
                                                                              survey_response_type_anonymous]:
                            continue

                        if "ContactID" not in responses["values"].keys():
                            continue
                        survey_end_date = response_id = progress = contact_id = None
                        survey_end_date = responses["values"]["recordedDate"]
                        response_id = responses["responseId"]
                        progress = responses["values"]["progress"]
                        contact_id = responses["values"]["ContactID"]

                        current_timestamp = datetime.utcnow()
                        current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")
                        version = datetime.strftime(current_timestamp, "%Y%m%d%H%M%S")

                        for ques in ques_param["protocol_fit"]:
                            if ques["QuestionID"] in responses["labels"] and ques["QuestionType"] == MC_QUESTION_TYPE:
                                ques["Answer"] = responses["labels"][ques["QuestionID"]]
                            elif str(ques["QuestionID"] + "_TEXT") in responses["values"] and ques[
                                "QuestionType"] == "TE":
                                ques["Answer"] = responses["values"][ques["QuestionID"] + "_TEXT"]
                            else:
                                ques["Answer"] = None
                        for ques in ques_param["others"]:
                            if ques["QuestionID"] in responses["labels"] and ques["QuestionType"] == MC_QUESTION_TYPE:
                                ques["Answer"] = responses["labels"][ques["QuestionID"]]
                            elif str(ques["QuestionID"] + "_TEXT") in responses["values"] and ques[
                                "QuestionType"] == TE_QUESTION_TYPE \
                                    and str(responses["values"][ques["QuestionID"] + "_TEXT"]).strip() != "":
                                ques["Answer"] = responses["values"][ques["QuestionID"] + "_TEXT"]
                            elif ques["QuestionType"] == "Slider":
                                ch_num = str(list(ques["Choices"].keys())[0])
                                if str(ques["QuestionID"] +"_" + ch_num) in responses["values"] and \
                                        str(responses["values"][ques["QuestionID"] +"_" + ch_num]).strip() != "":
                                    ques["Answer"] = responses["values"][ques["QuestionID"] + "_" + ch_num]
                                else:
                                    ques["Answer"] = None
                            elif ques["QuestionType"] == "Matrix":
                                if ques["QuestionID"] == "QID1":
                                    answers_dict = {}
                                    for ch_key, ch_value in ques["Choices"].items():
                                        search_key = str(ques["QuestionID"] +"_" + str(ch_key) + "_1")
                                        if search_key in responses["values"] and str(responses["values"][search_key]).strip() != "":
                                            answers_dict[ch_value["Display"].replace(" ", "").lower()] = responses["values"][search_key]
                                        else:
                                            answers_dict[ch_value["Display"].replace(" ", "").lower()] = None
                                    ques["Answer"] = answers_dict
                            else:
                                ques["Answer"] = None

                        protocol_fit = None
                        protocol_fit_q1 = None
                        # TODO to be removed
                        protocol_fit_q2 = None
                        protocol_fit_q3 = None

                        patients_committed = None
                        number_of_sites_committed = None
                        timeline_fsi_date = None
                        timeline_fpi_date = None
                        timeline_lpi_date = None
                        timeline_lpv_date = None
                        timeline_50_si_date = None
                        timeline_all_si_date = None
                        avg_si_to_fp_in = None
                        recruitment_duration = None
                        concern_on_include_exclude_criteria = None
                        pa_groups_involved = None
                        any_competing_ongoing_planned_trials_patients = None
                        any_competing_ongoing_planned_trials_staff = None
                        screen_failure_rate = None
                        dropout_rate = None
                        operational_capacity = None
                        is_delay_expected = None
                        inv_interested_in_study = None
                        no_of_inv_declined_due_to_covid = None
                        covid_impact = None
                        exact_site_interested = None
                        degree_of_compliance = None
                        willingness = None

                        # protocol fit answer TODO
                        country_interested_in_study = None

                        ################################
                        degree_of_interest_scientific = None
                        degree_of_interest_medical = None
                        degree_of_burden_operation = None
                        #####################################
                        degree_of_burden_patient = None

                        sites_contacted_black_afro_american = None
                        sites_contacted_asian = None
                        sites_contacted_hispanic = None
                        sites_contacted_american_indian = None
                        sites_contacted_native_hawaiian = None
                        sites_contacted_other = None
                        for ques in ques_param["others"]:
                            if ques["QuestionID"] == "QID57":
                                willingness = ques.get("Answer", '')
                                if willingness is None:
                                    willingness = ''
                            if ques["QuestionID"] == "QID54":
                                patients_committed = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID53":
                                number_of_sites_committed = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID45":
                                if "Answer" in ques:
                                    try:
                                        timeline_fsi_date = str(datetime.strptime(ques["Answer"], date_format).date())
                                        print(100*'&')
                                    except Exception as e:
                                        print(str(e))
                                        timeline_fsi_date = None
                                        print(100 * '%')
                                else:
                                    timeline_fsi_date = ques["Answer"]
                            if ques["QuestionID"] == "QID51":
                                if "Answer" in ques:
                                    try:
                                        timeline_fpi_date = str(datetime.strptime(ques["Answer"], date_format).date())
                                        print(100 * '&')
                                    except Exception as e:
                                        print(str(e))
                                        timeline_fpi_date = None
                                        print(100 * '%')
                                else:
                                    timeline_fsi_date = ques["Answer"]
                            if ques["QuestionID"] == "QID49":
                                if "Answer" in ques:
                                    try:
                                        timeline_lpi_date = str(datetime.strptime(ques["Answer"], date_format).date())
                                        print(100 * '&')
                                    except Exception as e:
                                        print(str(e))
                                        timeline_lpi_date = None
                                        print(100 * '%')
                                else:
                                    timeline_lpi_date = ques["Answer"]
                            if ques["QuestionID"] == "QID50":
                                if "Answer" in ques:
                                    try:
                                        timeline_lpv_date = str(datetime.strptime(ques["Answer"], date_format).date())
                                    except:
                                        timeline_lpv_date = None
                                else:
                                    timeline_lpv_date = ques["Answer"]
                            if ques["QuestionID"] == "QID46":
                                if "Answer" in ques:
                                    try:
                                        timeline_50_si_date = str(datetime.strptime(ques["Answer"], date_format).date())
                                    except Exception as e:
                                        timeline_50_si_date = None
                                else:
                                    timeline_50_si_date = ques["Answer"]
                            if ques["QuestionID"] == "QID47":
                                if "Answer" in ques:
                                    try:
                                        timeline_all_si_date = str(datetime.strptime(ques["Answer"], date_format).date())
                                    except:
                                        timeline_all_si_date = None
                                else:
                                    timeline_all_si_date = ques["Answer"]
                            if ques["QuestionID"] == "QID48":
                                if "Answer" in ques:
                                    avg_si_to_fp_in = ques.get("Answer", None)
                                    if avg_si_to_fp_in is None:
                                        avg_si_to_fp_in = 0
                                    if not str(avg_si_to_fp_in).isdigit():
                                        avg_si_to_fp_in = 0
                                else:
                                    avg_si_to_fp_in = None
                            if ques["QuestionID"] == "QID52":
                                if "Answer" in ques:
                                    recruitment_duration = ques.get("Answer", None)
                                else:
                                    recruitment_duration = None
                            if ques["QuestionID"] == "QID19":
                                if "Answer" in ques:
                                    concern_on_include_exclude_criteria = ques.get("Answer", None)
                                else:
                                    concern_on_include_exclude_criteria = None
                            if ques["QuestionID"] == "QID37":
                                if "Answer" in ques:
                                    pa_groups_involved = ques.get("Answer", None)
                                else:
                                    pa_groups_involved = None
                            if ques["QuestionID"] == "QID32":
                                if "Answer" in ques:
                                    any_competing_ongoing_planned_trials_patients = ques.get("Answer", None)
                                else:
                                    any_competing_ongoing_planned_trials_patients = None
                            # if ques["QuestionID"] == "any_competing_ongoing_planned_trials_staff":
                            #     if "Answer" in ques:
                            #         any_competing_ongoing_planned_trials_staff = ques.get("Answer", None)
                            #     else:
                            #         any_competing_ongoing_planned_trials_staff = None
                            if ques["QuestionID"] == "QID21":
                                if "Answer" in ques:
                                    screen_failure_rate = ques.get("Answer", None)
                                else:
                                    screen_failure_rate = None
                            if ques["QuestionID"] == "QID23":
                                if "Answer" in ques:
                                    dropout_rate = ques.get("Answer", None)
                                else:
                                    dropout_rate = None
                            if ques["QuestionID"] == "QID25":
                                if "Answer" in ques:
                                    operational_capacity = ques.get("Answer", None)
                                else:
                                    operational_capacity = None
                            if ques["QuestionID"] == "QID15":
                                if "Answer" in ques:
                                    degree_of_compliance = ques.get("Answer", None)
                                else:
                                    degree_of_compliance = None
                            if ques["QuestionID"] == "QID28":
                                if "Answer" in ques:
                                    is_delay_expected = ques.get("Answer", None)
                                else:
                                    is_delay_expected = None
                            if ques["QuestionID"] == "QID11":
                                if "Answer" in ques:
                                    inv_interested_in_study = ques.get("Answer", None)
                                else:
                                    inv_interested_in_study = None
                            if ques["QuestionID"] == "QID6":
                                if "Answer" in ques:
                                    no_of_inv_declined_due_to_covid = ques.get("Answer", None)
                                else:
                                    no_of_inv_declined_due_to_covid = None
                            if ques["QuestionID"] == "QID4":
                                if "Answer" in ques:
                                    covid_impact = ques.get("Answer", None)
                                else:
                                    covid_impact = None
                            if ques["QuestionID"] == "QID2":
                                if "Answer" in ques:
                                    exact_site_interested = ques.get("Answer", None)
                                else:
                                    exact_site_interested = None
                            if ques["QuestionID"] == "QID1":
                                if "Answer" in ques:
                                    for key, value in ques["Answer"].items():
                                        if "black" in key:
                                            sites_contacted_black_afro_american = value
                                        if "hispanic" in key:
                                            sites_contacted_hispanic = value
                                        if "asian" in key:
                                            sites_contacted_asian = value
                                        if "americanindian" in key:
                                            sites_contacted_american_indian = value
                                        if "nativehawaiian" in key:
                                            sites_contacted_native_hawaiian = value
                                        if "other" in key:
                                            sites_contacted_other = value
                            if ques["QuestionID"] == "QID17":
                                degree_of_burden_patient = ques.get("Answer", None)

                        for ques in ques_param["protocol_fit"]:
                            if ques["QuestionID"] == "QID8":
                                protocol_fit_q1 = ques.get("Answer", None)
                        if protocol_fit_q1 is not None:
                            if str(protocol_fit_q1).casefold() == survey_response_no.casefold():
                                protocol_fit = survey_response_no
                            else:
                                protocol_fit = survey_response_yes
                        else:
                            protocol_fit = survey_response_no

                        LOGGING.info("Questions & Answers for Country_Outreach Survey_id {} is {}".
                                     format(surveyid_data["survey_id"], ques_param))

                        # update the country_dtl table

                        update_flag = "Y"
                        keys_to_be_updated = {
                            "survey_country_version": version,
                            "response_id": response_id,
                            "survey_completed_date": survey_end_date,
                            "progress_percentage": progress,
                            "feasibility_or_protocol_fit": protocol_fit,
                            "patients_committed": patients_committed,
                            "number_of_sites_committed": number_of_sites_committed,
                            "degree_of_compliance": degree_of_compliance,
                            "timeline_fsi_date": timeline_fsi_date,
                            "timeline_fpi_date": timeline_fpi_date,
                            "timeline_lpi_date": timeline_lpi_date,
                            "timeline_lpv_date": timeline_lpv_date,
                            "number_of_sites_contacted_black_afro_american": sites_contacted_black_afro_american,
                            "number_of_sites_contacted_asian": sites_contacted_asian,
                            "number_of_sites_contacted_hispanic": sites_contacted_hispanic,
                            "number_of_sites_contacted_american_indian_alaskan_native": sites_contacted_american_indian,
                            "number_of_sites_contacted_native_hawaiian_other_pacific": sites_contacted_native_hawaiian,
                            "number_of_sites_contacted_other": sites_contacted_other,
                            "exact_site_interested": exact_site_interested,
                            "covid_impact": covid_impact,
                            "no_of_inv_declined_due_to_covid": no_of_inv_declined_due_to_covid,
                            "country_interested_in_study": protocol_fit_q1,
                            "inv_interested_in_study": inv_interested_in_study,
                            "concern_on_include_exclude_criteria": concern_on_include_exclude_criteria,
                            "screen_failure_rate": screen_failure_rate,
                            "dropout_rate": dropout_rate,
                            "operational_capacity": operational_capacity,
                            "is_delay_expected": is_delay_expected,
                            "any_competing_ongoing_planned_trials_patients": any_competing_ongoing_planned_trials_patients,
                            "any_competing_ongoing_planned_trials_staff": any_competing_ongoing_planned_trials_staff,
                            "pa_groups_involved": pa_groups_involved,
                            "timeline_50_si_date": timeline_50_si_date,
                            "timeline_all_si_date": timeline_all_si_date,
                            "avg_si_to_fp_in": avg_si_to_fp_in,
                            "recruitment_duration": recruitment_duration,
                            "degree_of_interest_scientific": degree_of_interest_scientific,
                            "degree_of_interest_medical": degree_of_interest_medical,
                            "degree_of_burden_operation": degree_of_burden_operation,
                            "degree_of_burden_patient": degree_of_burden_patient,
                            "last_updated_by": self.user_name,
                            "last_updated_time": current_time_string,
                            "is_active": True
                        }
                        # read the existing record and update if thevalue of the coloumns in keys_to_be_updated changed
                        query_get_record_country_dtl = SurveyScheduledJobQueries.GET_RECORD_COUNTRY_DTL
                        query_get_record_country_dtl = query_get_record_country_dtl.format(schema_name=self.schema_name,
                                                                                           survey_id=surveyid_data[
                                                                                               "survey_id"],
                                                                                           theme_id=surveyid_data[
                                                                                               "theme_id"],
                                                                                           qualtrics_contact_id=contact_id)

                        LOGGING.debug(
                            "query_get_record_country_dtl query - {}".format(str(query_get_record_country_dtl)))

                        get_record_country_dtl_response = DatabaseUtility().execute(
                            query=query_get_record_country_dtl, conn=conn_var, auto_commit=False)

                        LOGGING.debug(
                            "get_record_country_dtl_response - {}".format(str(get_record_country_dtl_response)))
                        country_dtl_record = get_record_country_dtl_response[Constants.RESULT_KEY]
                        if len(country_dtl_record) == 0:
                            LOGGING.debug("Skipping the survey - {} since no records found in country_dtl table".format(
                                surveyid_data["survey_id"]))
                            continue
                        if country_dtl_record[0]["progress_percentage"] is None:
                            country_dtl_record[0]["progress_percentage"] = 0

                        log_dtl_update_params["country_name"] = country_dtl_record[0]["country_name"]
                        log_dtl_update_params["ctfo_site_id"] = ""
                        log_dtl_update_params["investigator_id"] = ""

                        response_link_shared = country_dtl_record[0]["response_link_shared"]
                        if response_link_shared is None:
                            response_link_shared = False
                        smtp_server = self.application_config_data["ctfo_services"]["smtp_details"]["smtp_server"]
                        smtp_port = self.application_config_data["ctfo_services"]["smtp_details"]["smtp_port"]
                        country_head_emails = []
                        country_head_email = country_dtl_record[0]["country_head_email"]
                        country_sec_mails = country_dtl_record[0]["country_head_email_secondary"]
                        country_head_emails.append(country_head_email)
                        country_head_email = ",".join(country_head_emails)
                        country_name = country_dtl_record[0]["country_name"]
                        salutation = country_dtl_record[0]["salutation"]
                        if salutation is None or salutation == "":
                            salutation = ""
                        else:
                            salutation = " " + salutation
                        cc_recipient_list = [self.survey_config_data["CC_DL_email"]]
                        if country_sec_mails is not None:
                            cc_recipient_list.extend(country_sec_mails.split(";"))
                        cc_recipient_mails = ",".join(cc_recipient_list)
                        recipient_personalized_link = Constants.VIEW_SURVEY_RESPONSE_LINK.format(responseID=response_id,
                                                                                                 surveyID=surveyid_data[
                                                                                                     "survey_id"])
                        country_head_name = country_dtl_record[0]["country_head_first_name"]
                        scenario_id = country_dtl_record[0]["scenario_id"]

                        # Fetch details from cohort summary table
                        cohort_details_query = SurveyScheduledJobQueries.COHORT_DETAILS_QUERY

                        cohort_details_query = cohort_details_query.format(schema_name=self.schema_name,
                                                                           scenario_id=scenario_id)
                        print(cohort_details_query)
                        cohort_details_query_result = DatabaseUtility().execute(query=cohort_details_query,
                                                                              conn=conn_var, auto_commit=False)
                        LOGGING.debug("scenario cohort summary details - {}".format(str(cohort_details_query_result)))

                        # Fetch theme_id and study description in smry table
                        scn_smry_details = SurveyScheduledJobQueries.GET_SMRY_DETAILS.format(
                            schema_name=self.schema_name,
                            scenario_id=scenario_id)
                        scn_smry_details_response = DatabaseUtility().execute(query=scn_smry_details,
                                                                              conn=conn_var, auto_commit=False)
                        LOGGING.debug("scn_smry_details_response - {}".format(str(scn_smry_details_response)))

                        lpi_date_from_smry = cohort_details_query_result[Constants.RESULT_KEY]
                        lpi_date_from_smry = lpi_date_from_smry[0]["last_patient_in"]

                        keys_to_be_updated["timeline_lpi_date"] = lpi_date_from_smry

                        if keys_to_be_updated["avg_si_to_fp_in"] is not None:
                            try:
                                if keys_to_be_updated["timeline_fsi_date"] is not None:
                                    keys_to_be_updated["timeline_fpi_date"] = (
                                            datetime.strptime(keys_to_be_updated["timeline_fsi_date"], "%Y-%m-%d") +
                                            timedelta(days=keys_to_be_updated["avg_si_to_fp_in"])).strftime("%Y-%m-%d")
                                else:
                                    keys_to_be_updated["timeline_fpi_date"] = None
                            except Exception as e:
                                print(str(e))
                                keys_to_be_updated["timeline_fpi_date"] = None
                                print(100 * '%')

                        # calculate enrollment duration
                        calc_enrollment_sur = None
                        if keys_to_be_updated["timeline_fpi_date"] is not None:
                            fpi_date = datetime.strptime(keys_to_be_updated["timeline_fpi_date"], "%Y-%m-%d")
                            lpi_date = datetime.strptime(keys_to_be_updated["timeline_lpi_date"], "%Y-%m-%d")
                            calc_enrollment_sur = round(float((lpi_date-fpi_date).days/30.4), 2)

                        keys_to_be_updated["recruitment_duration"] = calc_enrollment_sur

                        # Getting details for present active contact id
                        existing_enrollment_details_query = \
                            SurveyScheduledJobQueries.ENROLLMENT_DETAILS_QUERY_COUNTRY.format(schema_name=self.schema_name,
                                                                                      theme_id=surveyid_data["theme_id"],
                                                                                      survey_id=surveyid_data["survey_id"],
                                                                                      contact_id=contact_id)
                        print(existing_enrollment_details_query)
                        existing_enrollment_details_query_resp = DatabaseUtility().execute(query=existing_enrollment_details_query,
                                                                              conn=conn_var, auto_commit=False)
                        LOGGING.debug("Existing active record response - {}".format(str(existing_enrollment_details_query_resp)))

                        if existing_enrollment_details_query_resp[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                            LOGGING.debug("Issue running Existing Active record query")
                            raise Exception("Issue running Existing Active record query")

                        existing_enrollment_details_query_resp = existing_enrollment_details_query_resp[Constants.RESULT_KEY]
                        if len(existing_enrollment_details_query_resp) == 0:
                            LOGGING.debug("No result for active query")
                            raise Exception("No result for active query")
                        enrollment = existing_enrollment_details_query_resp[0]["include_in_trial"]
                        include_in_trial = "No"
                        if enrollment is None:
                            if "yes".lower() in willingness.lower():
                                include_in_trial = "Yes"
                            else:
                                include_in_trial = "No"
                        if enrollment is None or enrollment.strip() == "":
                            keys_to_be_updated["include_in_trial"] = include_in_trial
                            if include_in_trial == "No":
                                keys_to_be_updated["reason_for_inclusion_exclusion"] = "Country Declined"

                        where_clause_json = {
                            "theme_id": surveyid_data["theme_id"],
                            "survey_id": surveyid_data["survey_id"],
                            "qualtrics_contact_id": contact_id,
                            "is_active": True
                        }


                        if country_dtl_record[0]["response_id"] != response_id or \
                                int(float(country_dtl_record[0]["progress_percentage"])) != int(float(progress)):
                            update_record_response = UpdateRecord().update_record(SURVEY_COUNTRY_DTL, self.schema_name,
                                                                                  conn_var,
                                                                                  keys_to_be_updated, update_flag,
                                                                                  extra_args,
                                                                                  where_clause_json)
                            LOGGING.debug("Update Record Response : {}".format(update_record_response))

                        if int(progress) == 100 and (response_link_shared is False):
                            print("Inside Mail Sending Logic")
                            # update the flag in country survey table
                            update_response_link_shared = SurveyScheduledJobQueries. \
                                UPDATE_RESPONSE_LINK_SHARED_COUNTRY.format(schema_name=self.schema_name,
                                                                           survey_id=surveyid_data["survey_id"],
                                                                           theme_id=surveyid_data["theme_id"],
                                                                           qualtrics_contact_id=contact_id)
                            update_response = DatabaseUtility().execute(query=update_response_link_shared,
                                                                        conn=conn_var, auto_commit=True)
                            print("Updated Flag Response :", update_response)
                            mail_template_path = os.path.abspath(
                                os.path.join(SERVICE_DIRECTORY_PATH, self.survey_config_data[
                                    "email_template_path"][
                                    "send_country_survey_completion"]))
                            mail_template = open(mail_template_path, 'r').read()
                            print("Template Before Response :", mail_template)
                            mail_body = str()
                            mail_body = mail_template.replace('$$recipient_name$$', country_head_name). \
                                replace('$$country_name$$', country_head_name). \
                                replace('$$link$$', recipient_personalized_link).\
                                replace("$$salutation$$", salutation)
                            print("Template after formatting :", mail_template)

                            try:
                                if scn_smry_details_response[Constants.RESULT_KEY][0]['manual_mail_subject'] == "" \
                                        or scn_smry_details_response[Constants.RESULT_KEY][0]['manual_mail_subject'] is None:
                                    theme_id = scn_smry_details_response[Constants.RESULT_KEY][0]['therapeutic_area'].title()
                                else:
                                    theme_id = scn_smry_details_response[Constants.RESULT_KEY][0]['manual_mail_subject'].title()
                                if scenario_id in ["88664ef8", "b198adb1"]:
                                    theme_id = "Rare Disease"
                            except Exception as e:
                                LOGGING.debug("Exception while fetching theme_id")
                                theme_id = "NA"
                            study_code = scn_smry_details_response[Constants.RESULT_KEY][0]['study_code']
                            subject = 'Completion of zs’s Country Questionnaire for $$study_code$$ in $$theme_id$$'
                            subject = subject.replace('$$theme_id$$', theme_id).replace('$$study_code$$', study_code)
                            UTILS_OBJ.send_email(sender_email=self.survey_config_data["sender_name"],
                                                 recipient_list=country_head_email, cc_recipient_list=cc_recipient_mails,
                                                 bcc_recipient_list=None, subject=subject,
                                                 email_body=mail_body, smtp_server=smtp_server, smtp_port=smtp_port)

            # check if the survey_type = "site_outreach:
            if surveyid_data["survey_type"] == SITE_OUTREACH:
                for question_id in survey_questions_json_key:
                    if survey_questions_json[question_id]["QuestionType"] == MC_QUESTION_TYPE and \
                            (True in [bool(i["question_id"] == survey_questions_json[question_id]["QuestionID"]) for i in
                                site_outreach_questionnaire["protocol_fit"].values()]):
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in site_outreach_questionnaire["protocol_fit"].items() if bool(
                                value["question_id"] == survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"]}
                        ques_param["protocol_fit"].append(question_details)

                    if (survey_questions_json[question_id]["QuestionType"] == TE_QUESTION_TYPE or
                        survey_questions_json[question_id]["QuestionType"] == "MC") and \
                            True in [bool(site_outreach_questionnaire[x]["question_id"] ==
                                          survey_questions_json[question_id]["QuestionID"]) for x in ["patients_committed",
                                                                               "interested_in_study",
                                                                               "degree_of_interest_scientific",
                                                                               "degree_of_interest_medical",
                                                                               "degree_of_burden_operation",
                                                                               "degree_of_burden_patient",
                                                                               "covid_impact",
                                                                               "main_inv_medical_specialty",
                                                                               "exp_in_comparable_complex_study",
                                                                               "study_procedure_availability",
                                                                               "reason_not_interested_in_study",
                                                                               "competition_for_patient_recruitment",
                                                                               "patient_followed_last_year",
                                                                               "patients_allowed_on_criteria",
                                                                               "is_delay_expected",
                                                                               "female_participated_on_similar_study",
                                                                               "patients_above_64_participated_on_similar_study",
                                                                               "female_patient_followed_last_year",
                                                                               "patient_above_64_followed_last_year"]]:
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in site_outreach_questionnaire.items() if
                             key != "protocol_fit" and bool(value["question_id"] == survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"]}
                        ques_param["others"].append(question_details)

                    if (survey_questions_json[question_id]["QuestionType"]) == "Slider" and \
                            (True in [bool(site_outreach_questionnaire[x]["question_id"] ==
                                           survey_questions_json[question_id]["QuestionID"]) for x in ["dropout_rate", "screen_failure_rate"]]):
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in site_outreach_questionnaire.items() if
                             key != "protocol_fit" and bool(value["question_id"] ==
                                                            survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"],
                                            "Choices": survey_questions_json[question_id]["Choices"],
                                            "ChoiceOrder": survey_questions_json[question_id]["Choices"]}
                        ques_param["others"].append(question_details)

                    if (survey_questions_json[question_id]["QuestionType"]) == "Matrix" and \
                            (True in [bool(site_outreach_questionnaire[x]["question_id"] ==
                                           survey_questions_json[question_id]["QuestionID"]) for x in ["percentage_patients_enrolled",
                                                                               "count_of_patients_enrolled"]]):
                        question_details = {"QuestionID": question_id, "QuestionKey":
                            [key for (key, value) in site_outreach_questionnaire.items() if
                             key != "protocol_fit" and bool(value["question_id"] == survey_questions_json[question_id]["QuestionID"])][0],
                                            "QuestionType": survey_questions_json[question_id]["QuestionType"],
                                            "Choices": survey_questions_json[question_id]["Choices"],
                                            "ChoiceOrder": survey_questions_json[question_id]["Choices"]}
                        ques_param["others"].append(question_details)

                LOGGING.info("Questions for Site_Outreach Survey_id {} is {}".
                                 format(surveyid_data["survey_id"], ques_param))

                if len(survey_response_complete["responses"]):
                    for responses in survey_response_complete["responses"]:
                        # skipping the non "gl" responses
                        if responses["values"]["distributionChannel"] not in [survey_response_type_gl,
                                                                              survey_response_type_anonymous]:
                            continue

                        if "ContactID" not in responses["values"].keys():
                            continue
                        survey_end_date = response_id = progress = contact_id = None
                        survey_end_date = responses["values"]["recordedDate"]
                        response_id = responses["responseId"]
                        progress = responses["values"]["progress"]
                        contact_id = responses["values"]["ContactID"]

                        current_timestamp = datetime.utcnow()
                        current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")
                        version = datetime.strftime(current_timestamp, "%Y%m%d%H%M%S")

                        for ques in ques_param["protocol_fit"]:
                            if ques["QuestionID"] in responses["labels"] and ques[
                                "QuestionType"] == MC_QUESTION_TYPE:
                                ques["Answer"] = responses["labels"][ques["QuestionID"]]
                            elif str(ques["QuestionID"] + "_TEXT") in responses["values"] and ques[
                                "QuestionType"] == "TE":
                                ques["Answer"] = responses["values"][ques["QuestionID"] + "_TEXT"]
                            else:
                                ques["Answer"] = None
                        for ques in ques_param["others"]:
                            if ques["QuestionID"] in responses["labels"] and ques["QuestionType"] == MC_QUESTION_TYPE:
                                if ques["QuestionKey"] in ["reason_not_interested_in_study"]:
                                    answer = None
                                    if ques["QuestionID"] not in responses["labels"]:
                                        answer = None
                                    else:
                                        answer_list = responses["labels"][ques["QuestionID"]]
                                        if len(answer_list) == 0:
                                            answer = None
                                        else:
                                            answer = '#'.join(answer_list)
                                    ques["Answer"] = answer
                                else:
                                    ques["Answer"] = responses["labels"][ques["QuestionID"]]
                            elif ques["QuestionType"] == TE_QUESTION_TYPE:
                                if ques["QuestionKey"] in ["patients_committed"]:
                                    min = responses["values"].get(ques["QuestionID"] + "_4", "-")
                                    max = responses["values"].get(ques["QuestionID"] + "_5", "-")
                                    if min == "-" and max != "-":
                                        ques["Answer"] = str(max) + "-" + str(max)
                                    elif max == "-" and min != '-':
                                        ques["Answer"] = str(min) + "-" + str(min)
                                    elif min == "-" and max == "-":
                                        ques["Answer"] = "0-0"
                                    else:
                                        ques["Answer"] = min + "-" + max
                                elif str(ques["QuestionID"] + "_TEXT") in responses["values"] and \
                                        ques["QuestionType"] == TE_QUESTION_TYPE and \
                                        str(responses["values"][ques["QuestionID"] + "_TEXT"]).strip() != "":
                                    ques["Answer"] = responses["values"][ques["QuestionID"] + "_TEXT"]
                            elif ques["QuestionType"] == "Slider":
                                ch_num = str(list(ques["Choices"].keys())[0])
                                if str(ques["QuestionID"] +"_" + ch_num) in responses["values"] and \
                                        str(responses["values"][ques["QuestionID"] +"_" + ch_num]).strip() != "":
                                    ques["Answer"] = responses["values"][ques["QuestionID"] + "_" + ch_num]
                                else:
                                    ques["Answer"] = None
                            elif ques["QuestionType"] == "Matrix":
                                if ques["QuestionKey"] in ["count_of_patients_enrolled", "percentage_patients_enrolled"]:
                                    answers_dict = {}
                                    for ch_key, ch_value in ques["Choices"].items():
                                        search_key = str(ques["QuestionID"] +"_" + str(ch_key) + "_1")
                                        if search_key in responses["values"] and str(responses["values"][search_key]).strip() != "":
                                            answers_dict[ch_value["Display"].replace(" ", "").lower()] = responses["values"][search_key]
                                        else:
                                            answers_dict[ch_value["Display"].replace(" ", "").lower()] = None
                                    ques["Answer"] = answers_dict
                            else:
                                ques["Answer"] = None

                        protocol_fit = None
                        protocol_fit_q1 = None
                        protocol_fit_q2 = None
                        protocol_fit_q3 = None
                        protocol_fit_q4 = None
                        patients_committed = None
                        enrollment_duration_committed = None
                        timeline_fsi_date = None
                        timeline_fpi_date = None
                        timeline_lpi_date = None
                        timeline_lpv_date = None
                        degree_of_interest_scientific = None
                        degree_of_interest_medical = None
                        degree_of_burden_operation = None
                        degree_of_burden_patient = None

                        # new keys
                        interested_in_study = None
                        covid_impact = None
                        main_inv_medical_specialty = None
                        exp_in_comparable_complex_study = None
                        study_procedure_availability = None
                        competition_for_patient_recruitment = None
                        patient_followed_last_year = None
                        patients_allowed_on_criteria = None
                        screen_failure_rate = None
                        dropout_rate = None
                        is_delay_expected = None
                        female_participated_on_similar_study = None
                        patients_above_64_participated_on_similar_study = None
                        female_patient_followed_last_year = None
                        patient_above_64_followed_last_year = None

                        # matrix questions
                        percentage_patients_enrolled = None
                        count_of_patients_enrolled = None
                        count_white = None
                        count_black_afro_american = None
                        count_asian = None
                        count_hispanic = None
                        count_american_indian = None
                        count_native_hawaiian = None
                        count_contacted_other = None
                        percent_white = None
                        percent_black_afro_american = None
                        percent_asian = None
                        percent_hispanic = None
                        percent_american_indian = None
                        percent_native_hawaiian = None
                        percent_contacted_other = None
                        reason_not_interested = None

                        print(100000 * '*')
                        print(ques_param)


                        for ques in ques_param["others"]:
                            if ques["QuestionID"] == "QID28":
                                patients_committed = ques.get("Answer", None)
                            # if ques["QuestionKey"] == "enrollment_duration_committed":
                            #     enrollment_duration_committed = ques.get("Answer", None)
                            # if ques["QuestionKey"] == "timeline_fsi_date":
                            #     if "Answer" in ques:
                            #         try:
                            #             timeline_fsi_date = str(datetime.strptime(ques["Answer"], date_format).date())
                            #         except:
                            #             timeline_fsi_date = None
                            #     else:
                            #         timeline_fsi_date = ques["Answer"]
                            # if ques["QuestionKey"] == "timeline_fpi_date":
                            #     if "Answer" in ques:
                            #         try:
                            #             timeline_fsi_date = str(datetime.strptime(ques["Answer"], date_format).date())
                            #         except:
                            #             timeline_fsi_date = None
                            #     else:
                            #         timeline_fsi_date = ques["Answer"]
                            # if ques["QuestionKey"] == "timeline_lpi_date":
                            #     if "Answer" in ques:
                            #         try:
                            #             timeline_fsi_date = str(datetime.strptime(ques["Answer"], date_format).date())
                            #         except:
                            #             timeline_fsi_date = None
                            #     else:
                            #         timeline_fsi_date = ques["Answer"]
                            # if ques["QuestionKey"] == "timeline_lpv_date":
                            #     if "Answer" in ques:
                            #         try:
                            #             timeline_fsi_date = str(datetime.strptime(ques["Answer"], date_format).date())
                            #         except:
                            #             timeline_fsi_date = None
                            #     else:
                            #         timeline_fsi_date = ques["Answer"]
                            if ques["QuestionID"] == "QID8":
                                degree_of_interest_scientific = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID11":
                                degree_of_interest_medical = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID9":
                                degree_of_burden_operation = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID12":
                                degree_of_burden_patient = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID1":
                                if "Answer" in ques:
                                    interested_in_study = ques.get("Answer", None)
                                else:
                                    interested_in_study = None
                            if ques["QuestionID"] == "QID4":
                                if "Answer" in ques:
                                    covid_impact = ques.get("Answer", None)
                                else:
                                    covid_impact = None
                            if ques["QuestionID"] == "QID7":
                                if "Answer" in ques:
                                    main_inv_medical_specialty = ques.get("Answer", None)
                                else:
                                    main_inv_medical_specialty = None
                            if ques["QuestionID"] == "QID15":
                                if "Answer" in ques:
                                    exp_in_comparable_complex_study = ques.get("Answer", None)
                                else:
                                    exp_in_comparable_complex_study = None
                            if ques["QuestionID"] == "QID17":
                                if "Answer" in ques:
                                    study_procedure_availability = ques.get("Answer", None)
                                else:
                                    study_procedure_availability = None
                            if ques["QuestionID"] == "QID19":
                                if "Answer" in ques:
                                    competition_for_patient_recruitment = ques.get("Answer", None)
                                else:
                                    competition_for_patient_recruitment = None
                            if ques["QuestionID"] == "QID22":
                                if "Answer" in ques:
                                    patient_followed_last_year = ques.get("Answer", None)
                                else:
                                    patient_followed_last_year = None
                            if ques["QuestionID"] == "QID2":
                                if "Answer" in ques:
                                    reason_not_interested = ques.get("Answer", None)
                                else:
                                    reason_not_interested = None
                            if ques["QuestionID"] == "QID23":
                                if "Answer" in ques:
                                    patients_allowed_on_criteria = ques.get("Answer", None)
                                else:
                                    patients_allowed_on_criteria = None
                            if ques["QuestionID"] == "QID24":
                                if "Answer" in ques:
                                    screen_failure_rate = ques.get("Answer", None)
                                else:
                                    screen_failure_rate = None
                            if ques["QuestionID"] == "QID26":
                                if "Answer" in ques:
                                    dropout_rate = ques.get("Answer", None)
                                else:
                                    dropout_rate = None
                            if ques["QuestionID"] == "QID30":
                                if "Answer" in ques:
                                    is_delay_expected = ques.get("Answer", None)
                                else:
                                    is_delay_expected = None
                            if ques["QuestionID"] == "QID35":
                                if "Answer" in ques:
                                    patients_above_64_participated_on_similar_study = ques.get("Answer", None)
                                else:
                                    patients_above_64_participated_on_similar_study = None
                            if ques["QuestionID"] == "QID34":
                                if "Answer" in ques:
                                    female_participated_on_similar_study = ques.get("Answer", None)
                                else:
                                    female_participated_on_similar_study = None
                            if ques["QuestionID"] == "QID37":
                                if "Answer" in ques:
                                    female_patient_followed_last_year = ques.get("Answer", None)
                                else:
                                    female_patient_followed_last_year = None
                            if ques["QuestionID"] == "QID38":
                                if "Answer" in ques:
                                    patient_above_64_followed_last_year = ques.get("Answer", None)
                                else:
                                    patient_above_64_followed_last_year = None
                            if ques["QuestionID"] == "QID33":
                                if "Answer" in ques:
                                    for key, value in ques["Answer"].items():
                                        if "white" in  key:
                                            percent_white = value
                                        if "black" in key:
                                            percent_black_afro_american = value
                                        if "hispanic" in key:
                                            percent_hispanic = value
                                        if "asian" in key:
                                            percent_asian = value
                                        if "americanindian" in key:
                                            percent_american_indian = value
                                        if "hawai" in key:
                                            percent_native_hawaiian = value
                                        if "other" in key:
                                            percent_contacted_other = value
                            if ques["QuestionID"] == "QID36":
                                if "Answer" in ques:
                                    for key, value in ques["Answer"].items():
                                        if "white" in key:
                                            count_white = value
                                        if "black" in key:
                                            count_black_afro_american = value
                                        if "hispanic" in key:
                                            count_hispanic = value
                                        if "asian" in key:
                                            count_asian = value
                                        if "americanindian" in key:
                                            count_american_indian = value
                                        if "hawai" in key:
                                            count_native_hawaiian = value
                                        if "other" in key:
                                            count_contacted_other = value

                        for ques in ques_param["protocol_fit"]:
                            if ques["QuestionID"] == "QID17":
                                protocol_fit_q1 = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID1":
                                protocol_fit_q2 = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID8":
                                protocol_fit_q3 = ques.get("Answer", None)
                            if ques["QuestionID"] == "QID9":
                                protocol_fit_q4 = ques.get("Answer", None)

                        if str(protocol_fit_q1).casefold() == survey_response_no.casefold():
                            protocol_fit = survey_response_no
                        elif str(protocol_fit_q1).casefold() == survey_response_yes.casefold() and str(
                                protocol_fit_q2).casefold() == survey_response_no.casefold():
                            protocol_fit = survey_response_no
                        elif str(protocol_fit_q1).casefold() == survey_response_yes.casefold() and re.sub("\s*", "",
                                                                                                          str(
                                                                                                                  protocol_fit_q2)).casefold() == re.sub(
                                "\s*", "", survey_response_yes_hi).casefold():
                            protocol_fit = survey_response_yes
                        elif str(protocol_fit_q1).casefold() == survey_response_yes.casefold() and re.sub("\s*", "",
                                                                                                          str(
                                                                                                                  protocol_fit_q2)).casefold() == re.sub(
                                "\s*", "", survey_response_yes_mi).casefold() and \
                                (
                                        protocol_fit_q3 in survey_response_high_medium or protocol_fit_q4 in survey_response_high_medium):
                            protocol_fit = survey_response_yes
                        else:
                            protocol_fit = survey_response_no

                        LOGGING.info("Questions & Answers for Site_Outreach Survey_id {} is {}".
                                     format(surveyid_data["survey_id"], ques_param))

                        # update the site_dtl table
                        print(10000 * '!')
                        update_flag = "Y"
                        keys_to_be_updated = {
                            "survey_site_investigator_version": version,
                            "response_id": response_id,
                            "survey_completed_date": survey_end_date,
                            "progress_percentage": progress,
                            "feasibility_or_protocol_fit": protocol_fit,
                            "patients_committed": patients_committed,
                            "enrollment_duration_committed": enrollment_duration_committed,
                            "timeline_fsi_date": timeline_fsi_date,
                            "timeline_fpi_date": timeline_fpi_date,
                            "timeline_lpi_date": timeline_lpi_date,
                            "timeline_lpv_date": timeline_lpv_date,
                            "degree_of_interest_scientific": degree_of_interest_scientific,
                            "degree_of_interest_medical": degree_of_interest_medical,
                            "degree_of_burden_operation": degree_of_burden_operation,
                            "degree_of_burden_patient": degree_of_burden_patient,
                            "interested_in_study": interested_in_study,
                            "covid_impact": covid_impact,
                            "main_inv_medical_specialty": main_inv_medical_specialty,
                            "exp_in_comparable_complex_study": exp_in_comparable_complex_study,
                            "study_procedure_availability": study_procedure_availability,
                            "competition_for_patient_recruitment": competition_for_patient_recruitment,
                            "patient_followed_last_year": patient_followed_last_year,
                            "patients_allowed_on_criteria": patients_allowed_on_criteria,
                            "screen_failure_rate": screen_failure_rate,
                            "dropout_rate": dropout_rate,
                            "is_delay_expected": is_delay_expected,
                            "reason_for_not_interested": reason_not_interested,
                            "percentage_of_patients_contacted_white": percent_white,
                            "percentage_of_patients_contacted_black_afro_american": percent_black_afro_american,
                            "percentge_of_patients_contacted_asian": percent_asian,
                            "percentge_of_patients_contacted_hispanic": percent_hispanic,
                            "percentage_of_patients_contacted_american_indian_alaskan_native": percent_american_indian,
                            "percentge_of_patients_contacted_native_hawaiian_other_pacific": percent_native_hawaiian,
                            "percentge_of_patients_contacted_other": percent_contacted_other,
                            "female_participated_on_similar_study": female_participated_on_similar_study,
                            "patients_above_64_participated_on_similar_study": patients_above_64_participated_on_similar_study,
                            "number_of_patients_contacted_white": count_white,
                            "number_of_patients_contacted_black_afro_american": count_black_afro_american,
                            "number_of_patients_contacted_asian": count_asian,
                            "number_of_patients_contacted_hispanic": count_hispanic,
                            "number_of_patients_contacted_american_indian_alaskan_native": count_american_indian,
                            "number_of_patients_contacted_native_hawaiian_other_pacific": count_native_hawaiian,
                            "number_of_patients_contacted_other": count_contacted_other,
                            "female_patient_followed_last_year": female_patient_followed_last_year,
                            "patient_above_64_followed_last_year": patient_above_64_followed_last_year,
                            "last_updated_by": self.user_name,
                            "last_updated_time": current_time_string,
                            "is_active": True
                        }

                        # Getting details for present active contact id
                        existing_enrollment_details_query = \
                            SurveyScheduledJobQueries.ENROLLMENT_DETAILS_QUERY.format(schema_name=self.schema_name,
                                                                                      theme_id=surveyid_data[
                                                                                          "theme_id"],
                                                                                      survey_id=surveyid_data[
                                                                                          "survey_id"],
                                                                                      contact_id=contact_id)
                        LOGGING.debug(surveyid_data)
                        LOGGING.debug("Existing Data Query: {}".format(str(existing_enrollment_details_query)))
                        print(existing_enrollment_details_query)
                        existing_enrollment_details_query_resp = DatabaseUtility().execute(
                            query=existing_enrollment_details_query,
                            conn=conn_var, auto_commit=False)
                        LOGGING.debug(
                            "Existing active record response - {}".format(str(existing_enrollment_details_query_resp)))

                        if existing_enrollment_details_query_resp[
                            Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                            LOGGING.debug("Issue running Existing Active record query")
                            raise Exception("Issue running Existing Active record query")

                        existing_enrollment_details_query_resp = existing_enrollment_details_query_resp[Constants.RESULT_KEY]
                        if len(existing_enrollment_details_query_resp) == 0:
                            LOGGING.debug("No result for active query")
                            enrollment = None
                        else:
                            enrollment = existing_enrollment_details_query_resp[0]["include_in_trial"]
                        include_in_trial = None
                        if enrollment is None:
                            if interested_in_study is not None and interested_in_study.lower() != "No".lower():
                                include_in_trial = "Yes"
                            else:
                                include_in_trial = "No"

                        if enrollment is None or enrollment.strip() == "":
                            keys_to_be_updated["include_in_trial"] = include_in_trial
                            if include_in_trial == "No":
                                keys_to_be_updated["reason_for_inclusion_exclusion"] = "Site Declined"

                        where_clause_json = {
                            "theme_id": surveyid_data["theme_id"],
                            "survey_id": surveyid_data["survey_id"],
                            "qualtrics_contact_id": contact_id,
                            "is_active": True
                        }
                        # read the existing record and update if thevalue of the coloumns in keys_to_be_updated changed
                        query_get_record_investigator_dtl = SurveyScheduledJobQueries.GET_RECORD_INVESTIGATOR_DTL
                        query_get_record_investigator_dtl = query_get_record_investigator_dtl.format(
                            schema_name=self.schema_name,
                            survey_id=surveyid_data["survey_id"],
                            theme_id=surveyid_data["theme_id"],
                            qualtrics_contact_id=contact_id)

                        LOGGING.debug("query_get_record_investigator_dtl query - {}".format(
                            str(query_get_record_investigator_dtl)))

                        get_record_investigator_dtl_response = DatabaseUtility().execute(
                            query=query_get_record_investigator_dtl, conn=conn_var, auto_commit=False)

                        LOGGING.debug("get_record_investigator_dtl_response - {}".format(
                            str(get_record_investigator_dtl_response)))

                        investigator_dtl_record = get_record_investigator_dtl_response[Constants.RESULT_KEY]
                        if len(investigator_dtl_record) == 0:
                            LOGGING.debug("Skipping the survey - {} since no records found in site_inv_table".format(
                                surveyid_data["survey_id"]))
                            continue
                        if investigator_dtl_record[0]["progress_percentage"] is None:
                            investigator_dtl_record[0]["progress_percentage"] = 0

                        log_dtl_update_params["country_name"] = ""
                        log_dtl_update_params["ctfo_site_id"] = investigator_dtl_record[0]["ctfo_site_id"]
                        log_dtl_update_params["investigator_id"] = investigator_dtl_record[0]["investigator_id"]

                        if investigator_dtl_record[0]["response_id"] != response_id or \
                                investigator_dtl_record[0]["survey_completed_date"] != survey_end_date or \
                                int(float(investigator_dtl_record[0]["progress_percentage"])) != int(float(progress)):
                            if investigator_dtl_record[0]["survey_site_investigator_version"] == keys_to_be_updated["survey_site_investigator_version"]:
                                keys_to_be_updated["survey_site_investigator_version"] = str(int(keys_to_be_updated["survey_site_investigator_version"]) + 1)
                            update_record_response = UpdateRecord().update_record(SURVEY_SITE_DTL, self.schema_name,
                                                                                  conn_var,
                                                                                  keys_to_be_updated, update_flag,
                                                                                  extra_args,
                                                                                  where_clause_json)
                        response_link_shared = investigator_dtl_record[0]["response_link_shared"]
                        if response_link_shared is None:
                            response_link_shared = False
                        smtp_server = self.application_config_data["ctfo_services"]["smtp_details"]["smtp_server"]
                        smtp_port = self.application_config_data["ctfo_services"]["smtp_details"]["smtp_port"]
                        investigator_email = [investigator_dtl_record[0]["investigator_email"]]
                        investigator_email_secondary = investigator_dtl_record[0]["ctfo_inv_email_secondary"]
                        if investigator_email_secondary is not None:
                            investigator_email.extend(investigator_email_secondary.split(";"))
                        investigator_email = ",".join(investigator_email)
                        site_name = investigator_dtl_record[0]["site_nm"]
                        country_name = investigator_dtl_record[0]["site_country"]
                        recipient_personalized_link = Constants.VIEW_SURVEY_RESPONSE_LINK.format(
                            responseID=response_id,
                            surveyID=surveyid_data["survey_id"])
                        investigator_name = investigator_dtl_record[0]["investigator_last_name"]
                        scenario_id = investigator_dtl_record[0]["scenario_id"]
                        salutation = investigator_dtl_record[0]["salutation"]
                        if int(progress) == 100 and str(response_link_shared).lower() == "false":
                            # fetchhing theme_id
                            scn_smry_details = SurveyScheduledJobQueries.GET_SMRY_DETAILS.format(
                                schema_name=self.schema_name,
                                scenario_id=scenario_id)
                            scn_smry_details_response = DatabaseUtility().execute(query=scn_smry_details,
                                                                                  conn=conn_var, auto_commit=False)
                            LOGGING.debug("scn_smry_details_response - {}".format(str(scn_smry_details_response)))

                            study_code = scn_smry_details_response[Constants.RESULT_KEY][0]['study_code']

                            # get the country head email
                            country_details = SurveyScheduledJobQueries.GET_COUNTRY_HEAD_DETAILS.format(
                                schema_name=self.schema_name,
                                country_name=country_name,
                                scenario_id=scn_smry_details_response[Constants.RESULT_KEY][0]['scenario_id'],
                                survey_id=surveyid_data["survey_id"])
                            LOGGING.debug("get_country_head query - {}".format(str(country_details)))
                            country_details_response = DatabaseUtility().execute(query=country_details,
                                                                                 conn=conn_var, auto_commit=False)
                            LOGGING.debug("get_country_head result - {}".format(str(country_details_response)))
                            if len(country_details_response[Constants.RESULT_KEY]) != 0 and \
                                    country_details_response[Constants.RESULT_KEY][0]["country_head_name"] is not None\
                                    and country_details_response[Constants.RESULT_KEY][0]["country_head_email"] is not None:
                                #TODO uncomment this on prod
                                country_head_email = "lokesh.gk@zs.com"
                                # country_head_emails = [country_details_response['result'][0]["country_head_email"]]
                                # country_head_secondary_email = country_details_response['result'][0]["country_head_email_secondary"]
                                # if country_head_secondary_email is not None:
                                #     country_head_emails.extend(country_head_secondary_email.split(";"))
                                # country_head_emails.append(str(self.survey_config_data["CC_DL_email"]))
                                # country_head_email = ",".join(country_head_emails)
                                # TODO remove the bottom line
                                # country_head_email = country_details_response['result'][0]["country_head_email"] + "," + str(self.survey_config_data["CC_DL_email"])
                                try:
                                    country_head_name = country_details_response['result'][0]["country_head_name"]
                                except Exception as e:
                                    LOGGING.debug("Country head name is null")
                                    country_head_name = "zs's Representative"
                            else:
                                country_details = SurveyScheduledJobQueries.GET_REPORTING_COUNTRY_HEAD_DETAILS.format(
                                    schema_name=self.schema_reporting,
                                    country_name=country_name, study_code=study_code)
                                LOGGING.debug("get_country_head query - {}".format(str(country_details)))
                                country_details_response = DatabaseUtility().execute(query=country_details,
                                                                                     conn=conn_var, auto_commit=False)
                                LOGGING.debug("get_country_head result - {}".format(str(country_details_response)))
                                # TODO uncomment this on prod
                                country_head_email = "lokesh.gk@zs.com"
                                # country_head_email = country_details_response['result'][0]["country_head_email"] + "," + str(self.survey_config_data["CC_DL_email"])
                                try:
                                    country_head_name = country_details_response['result'][0]["country_head_name"]
                                except Exception as e:
                                    LOGGING.error("Country head name is null")
                                    country_head_name = "zs's Representative"
                            # update the flag in country survey table
                            update_response_link_shared = SurveyScheduledJobQueries. \
                                UPDATE_RESPONSE_LINK_SHARED_SITE.format(schema_name=self.schema_name,
                                                                        survey_id=surveyid_data["survey_id"],
                                                                        theme_id=surveyid_data["theme_id"],
                                                                        qualtrics_contact_id=contact_id)
                            update_response = DatabaseUtility().execute(query=update_response_link_shared,
                                                                        conn=conn_var, auto_commit=True)
                            LOGGING.debug("Update Response : {}".format(json.dumps(update_response)))
                            mail_template_path = os.path.abspath(
                                os.path.join(SERVICE_DIRECTORY_PATH, self.survey_config_data[
                                    "email_template_path"][
                                    "send_site_survey_completion"]))
                            mail_body = str()
                            mail_template = open(mail_template_path, 'r').read()
                            mail_body = mail_template.replace('$$recipient_name$$', investigator_name). \
                                replace('$$country_name$$', site_name). \
                                replace('$$link$$', recipient_personalized_link).\
                                replace('$$country_head_name$$', country_head_name).\
                                replace("$$salutation$$", salutation)
                            try:
                                if scn_smry_details_response[Constants.RESULT_KEY][0]['manual_mail_subject'] == "" \
                                        or scn_smry_details_response[Constants.RESULT_KEY][0]['manual_mail_subject'] is None:
                                    theme_id = scn_smry_details_response[Constants.RESULT_KEY][0]['therapeutic_area'].title()
                                else:
                                    theme_id = scn_smry_details_response[Constants.RESULT_KEY][0]['manual_mail_subject'].title()
                                if scenario_id in ["88664ef8", "b198adb1"]:
                                    theme_id = "Rare Disease"
                            except Exception as e:
                                LOGGING.debug("Exception while fetching theme_id")
                                theme_id = "NA"
                            study_code = scn_smry_details_response[Constants.RESULT_KEY][0]['study_code']
                            subject = 'Completion of zs’s Operational Feasibility Questionnaire for $$study_code$$ in $$theme_id$$'
                            subject = subject.replace('$$theme_id$$', theme_id).replace('$$study_code$$', study_code)
                            UTILS_OBJ.send_email(sender_email=self.survey_config_data["sender_name"],
                                                 recipient_list=investigator_email, cc_recipient_list=country_head_email,
                                                 subject=subject,
                                                 email_body=mail_body, smtp_server=smtp_server, smtp_port=smtp_port)

        except Exception as e:
            print(e)
            LOGGING.error(
                "Error in update_responses_complete method of SurveyScheduleJonUtilts : {}".format(
                    traceback.format_exc()))
            raise Exception(
                "Error in update_responses_complete method of SurveyScheduleJonUtilts : {}".format(
                    traceback.format_exc()))

    def create_log_smry_record(self, conn_var, extra_args, log_smry_params):
        """
            Purpose :    To create an entry in log_survey_response_data_sync_smry table for the current execution
            Input   :    conn_var -> connection details to connect to PostgreSQL instances
                         extra_args -> job_id and caller details
            Output  :
        """
        try:
            query_to_insert_log_smry = SurveyScheduledJobQueries.QUERY_INSERT_LOG_SMRY
            LOGGING.info(query_to_insert_log_smry)
            query_to_insert_log_smry = query_to_insert_log_smry.format(schema_name=self.schema_name,
                                                                       job_id=log_smry_params["job_id"],
                                                                       start_time=log_smry_params["start_time"],
                                                                       end_time=log_smry_params["end_time"],
                                                                       requested_by=log_smry_params["requested_by"]
                                                                       , status=log_smry_params["status"])
            LOGGING.info("query_to_insert_log_smry - {}".format(str(query_to_insert_log_smry)))
            insert_log_smry_response = DatabaseUtility().execute(
                query=query_to_insert_log_smry, conn=conn_var, auto_commit=False)
            LOGGING.info("insert_log_smry_response - {}".format(str(insert_log_smry_response)))

            if insert_log_smry_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to execute query_to_insert_log_smry query- {}".format(
                        insert_log_smry_response[Constants.ERROR_KEY]))  # have to verify this=

            LOGGING.info("Executed query to query_to_insert_job_smry successfully")

        except:
            raise Exception(traceback.format_exc())

    def create_log_dtl_record(self, conn_var, extra_args, log_dtl_params):
        # this function will update the log_survey_response_data_sync_dtl table
        """
            Purpose :    To create an entry in log_survey_response_data_sync_smry table for the current execution
            Input   :    conn_var -> connection details to connect to PostgreSQL instances
                         extra_args -> job_id and caller details
            Output  :
        """
        try:
            query_to_insert_log_dtl = SurveyScheduledJobQueries.QUERY_INSERT_LOG_DTL
            query_to_insert_log_dtl = query_to_insert_log_dtl.format(schema_name=self.schema_name,
                                                                     job_id=log_dtl_params["job_id"],
                                                                     survey_id=log_dtl_params["survey_id"],
                                                                     survey_type=log_dtl_params["survey_type"],
                                                                     level=log_dtl_params["level"],
                                                                     country_name=log_dtl_params["country_name"],
                                                                     ctfo_site_id=log_dtl_params["ctfo_site_id"],
                                                                     investigator_id=log_dtl_params["investigator_id"],
                                                                     start_time=log_dtl_params["start_time"],
                                                                     end_time=log_dtl_params["end_time"],
                                                                     requested_by=log_dtl_params["requested_by"],
                                                                     status=log_dtl_params["status"])
            LOGGING.info("query_to_insert_log_dtl - {}".format(query_to_insert_log_dtl))

            insert_log_smry_response = DatabaseUtility().execute(
                query=query_to_insert_log_dtl, conn=conn_var, auto_commit=False)
            LOGGING.info("insert_log_dtl_response - {}".format(str(insert_log_smry_response)))

            if insert_log_smry_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to execute query_to_insert_log_dtl query- {}".format(
                        insert_log_smry_response[Constants.ERROR_KEY]))

            LOGGING.info("Executed query to query_to_insert_job_dtl successfully")
        except:
            raise Exception(traceback.format_exc())

    def update_log_smry_record(self, conn_var, extra_args, log_smry_update_params):
        """
            Purpose :    To create an entry in log_survey_response_data_sync_smry table for the current execution
            Input   :    conn_var -> connection details to connect to PostgreSQL instances
                         extra_args -> job_id and caller details
            Output  :
        """

        try:
            query_to_update_log_smry = SurveyScheduledJobQueries.QUERY_UPDATE_LOG_SMRY
            query_to_update_log_smry = query_to_update_log_smry.format(schema_name=self.schema_name,
                                                                       end_time=log_smry_update_params["end_time"],
                                                                       status=log_smry_update_params["status"],
                                                                       job_id=log_smry_update_params["job_id"]
                                                                       )

            update_log_smry_response = DatabaseUtility().execute(query=query_to_update_log_smry, conn=conn_var,
                                                                 auto_commit=False)
            LOGGING.info("update_job_smry_response - {}".format(str(update_log_smry_response)))

            if update_log_smry_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to execute query_to_update_log_smry query- {}".format(
                        update_log_smry_response[Constants.ERROR_KEY]))

            LOGGING.info("Executed query to query_to_insert_job_dtl successfully")
        except:
            raise Exception(traceback.format_exc())

    def update_log_dtl_record(self, conn_var, extra_args, log_dtl_update_params):
        """
                    Purpose :    To create an entry in log_survey_response_data_sync_smry table for the current execution
                    Input   :    conn_var -> connection details to connect to PostgreSQL instances
                                 extra_args -> job_id and caller details
                    Output  :
                """
        try:
            query_to_update_log_dtl = SurveyScheduledJobQueries.QUERY_UPDATE_LOG_DTL
            query_to_update_log_dtl = query_to_update_log_dtl.format(schema_name=self.schema_name,
                                                                     end_time=log_dtl_update_params["end_time"],
                                                                     status=log_dtl_update_params["status"],
                                                                     job_id=log_dtl_update_params["job_id"],
                                                                     survey_id=log_dtl_update_params["survey_id"],
                                                                     survey_type=log_dtl_update_params["survey_type"],
                                                                     level=log_dtl_update_params["level"],
                                                                     country_name=log_dtl_update_params["country_name"],
                                                                     ctfo_site_id=log_dtl_update_params[
                                                                         "ctfo_site_id"],
                                                                     investigator_id=log_dtl_update_params[
                                                                         "investigator_id"]
                                                                     )

            insert_log_smry_response = DatabaseUtility().execute(query=query_to_update_log_dtl, conn=conn_var,
                                                                 auto_commit=False)
            LOGGING.info("update_log_dtl_response - {}".format(str(insert_log_smry_response)))

            if insert_log_smry_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to execute query_to_update_log_dtl query- {}".format(
                        insert_log_smry_response[Constants.ERROR_KEY]))

            LOGGING.info("Executed query to query_to_insert_job_dtl successfully")
        except:
            raise Exception(traceback.format_exc())

    def check_running_job(self, conn_var, extra_args):
        """
                   Purpose : To check if the previous job is running. If yes then exit the function
                   Input   : conn_var and extra_args
                   Output  : previous_job_details

        """
        try:
            LOGGING.info("calling  the check_running_job", extra=extra_args)

            previous_job_details = None

            query_to_check_active_job = SurveyScheduledJobQueries.QUERY_CHECK_ACTIVE_JOB
            query_to_check_active_job = query_to_check_active_job.format(schema_name=self.schema_name)

            check_active_job_response = DatabaseUtility().execute(
                query=query_to_check_active_job, conn=conn_var, auto_commit=False)
            LOGGING.debug("upadte_log_dtl_response - {}".format(str(check_active_job_response)))

            if check_active_job_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to execute query_to_check_active_job query - {}".format(
                        check_active_job_response[Constants.ERROR_KEY]))

            LOGGING.debug("Executed query to query_to_check_active_job successfully", extra=extra_args)

            if len(check_active_job_response[Constants.RESULT_KEY]) != 0:
                previous_job_details = check_active_job_response[Constants.RESULT_KEY]

            if previous_job_details is not None and previous_job_details[0]["status"] == JOB_STATUS_IN_PROGRESS:
                LOGGING.info("Previous job is still in progress. Previous job details - {}".format(previous_job_details)
                             , extra=extra_args)
                sys.exit(0)
            return previous_job_details[0]
        except SystemExit as e:
            sys.exit(e)
        except:
            LOGGING.error(
                "Error in check_running_job method of SurveyScheduleJobUtilts : {}".format(
                    traceback.format_exc()),
                extra=extra_args)

    def retry_failed_transactions(self, conn_var, extra_args, log_smry_params):
        """
                   Purpose : to re-try the failed transactions from the previous job run
                   Input   : conn_var and extra_args
                   Output  : none

        """
        try:
            LOGGING.info("calling  the retry_failed_transactions", extra=extra_args)
            failed_job_details = None

            query_to_get_previous_failed = SurveyScheduledJobQueries.GET_PREVIOUS_FAILED
            query_to_get_previous_failed = query_to_get_previous_failed.format(schema_name=self.schema_name)
            LOGGING.debug("query_to_get_previous_failed - {}".format(str(query_to_get_previous_failed)))

            get_previous_failed_response = DatabaseUtility().execute(
                query=query_to_get_previous_failed, conn=conn_var, auto_commit=False)
            LOGGING.debug("upadte_log_dtl_response - {}".format(str(get_previous_failed_response)))

            if get_previous_failed_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to execute query_to_check_active_job query - {}".format(
                        get_previous_failed_response[Constants.ERROR_KEY]))

            LOGGING.debug("Executed query to query_to_check_active_job successfully  - {}".format(
                str(get_previous_failed_response)), extra=extra_args)

            if len(get_previous_failed_response[Constants.RESULT_KEY]) != 0:
                failed_job_details = get_previous_failed_response[Constants.RESULT_KEY]
            else:
                return

            for i in range(len(failed_job_details)):
                # Any updated made here should be made in start_schedule_job also !!!

                query_to_get_failed_survey_dtl = SurveyScheduledJobQueries.GET_FAILED_SURVEY_DTL
                query_to_get_failed_survey_dtl = query_to_get_failed_survey_dtl.format(schema_name=self.schema_name,
                                                                                       survey_id=failed_job_details[i][
                                                                                           "survey_id"])
                get_failed_survey_dtl_response = DatabaseUtility().execute(
                    query=query_to_get_failed_survey_dtl, conn=conn_var, auto_commit=False)

                LOGGING.debug("get_failed_survey_dtl_response - {}".format(str(get_failed_survey_dtl_response)))

                failed_survey_dtl = get_failed_survey_dtl_response[Constants.RESULT_KEY]

                # initialise the log_dtl_params
                log_dtl_params = {}
                log_dtl_params["job_id"] = log_smry_params["job_id"]
                log_dtl_params["survey_id"] = ""
                log_dtl_params["survey_type"] = ""
                log_dtl_params["level"] = ""
                log_dtl_params["country_name"] = ""
                log_dtl_params["ctfo_site_id"] = ""
                log_dtl_params["investigator_id"] = ""
                log_dtl_params["start_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                log_dtl_params["end_time"] = "NULL"
                log_dtl_params["requested_by"] = log_smry_params["requested_by"]
                log_dtl_params["status"] = JOB_STATUS_IN_PROGRESS

                # Parameters to update the log_survey_response_data_sync_dtl
                log_dtl_update_params = {}
                log_dtl_update_params["job_id"] = log_smry_params["job_id"]
                log_dtl_update_params["start_time"] = log_dtl_params["start_time"]
                log_dtl_update_params["survey_id"] = ""
                log_dtl_update_params["survey_type"] = ""
                log_dtl_update_params["country_name"] = ""
                log_dtl_update_params["ctfo_site_id"] = ""
                log_dtl_update_params["investigator_id"] = ""
                log_dtl_update_params["level"] = ""
                log_dtl_update_params["end_time"] = ""
                log_dtl_update_params["status"] = ""

                if failed_job_details[i]["level"] == JOB_DTL_LEVEL_SURVEY:
                    current_timestamp = datetime.utcnow()
                    current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

                    log_dtl_params["start_time"] = current_time_string
                    log_dtl_params["survey_id"] = failed_survey_dtl[0]["survey_id"]
                    log_dtl_params["survey_type"] = failed_survey_dtl[0]["survey_type"]
                    log_dtl_params["level"] = JOB_DTL_LEVEL_SURVEY
                    self.create_log_dtl_record(conn_var, extra_args, log_dtl_params)

                    # parms for updating the log_dtl table
                    log_dtl_update_params["survey_id"] = failed_survey_dtl[0]["survey_id"]
                    log_dtl_update_params["survey_type"] = failed_survey_dtl[0]["survey_type"]
                    log_dtl_update_params["level"] = JOB_DTL_LEVEL_SURVEY

                    executed_successfully = self.get_survey_status(conn_var, failed_survey_dtl[0],
                                                                   extra_args, log_dtl_update_params)

                    if executed_successfully:
                        log_dtl_update_params["end_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                        log_dtl_update_params["status"] = JOB_STATUS_COMPLETE
                        self.update_log_dtl_record(conn_var, extra_args, log_dtl_update_params)

                else:
                    if failed_job_details[i]["level"] == JOB_DTL_LEVEL_SITE:
                        # call the query to table site_investigator_dtl and populate ctfo_site_id & investigator_id
                        current_timestamp = datetime.utcnow()
                        current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

                        log_dtl_params["start_time"] = current_time_string
                        log_dtl_params["survey_id"] = failed_survey_dtl[0]["survey_id"]
                        log_dtl_params["survey_type"] = failed_survey_dtl[0]["survey_type"]
                        log_dtl_params["level"] = JOB_DTL_LEVEL_SITE

                        self.create_log_dtl_record(conn_var, extra_args, log_dtl_params)

                        log_dtl_update_params["survey_id"] = failed_survey_dtl[0]["survey_id"]
                        log_dtl_update_params["survey_type"] = failed_survey_dtl[0]["survey_type"]
                        log_dtl_update_params["level"] = JOB_DTL_LEVEL_SITE

                    if failed_job_details[i]["level"] == JOB_DTL_LEVEL_CO:
                        current_timestamp = datetime.utcnow()
                        current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

                        log_dtl_params["start_time"] = current_time_string
                        log_dtl_params["survey_id"] = failed_survey_dtl[0]["survey_id"]
                        log_dtl_params["survey_type"] = failed_survey_dtl[0]["survey_type"]
                        log_dtl_params["level"] = JOB_DTL_LEVEL_CO

                        self.create_log_dtl_record(conn_var, extra_args, log_dtl_params)

                        log_dtl_update_params["survey_id"] = failed_survey_dtl[0]["survey_id"]
                        log_dtl_update_params["survey_type"] = failed_survey_dtl[0]["survey_type"]
                        log_dtl_update_params["level"] = JOB_DTL_LEVEL_CO

                    executed_successfully = self.update_survey_dtl_responses(conn_var, failed_survey_dtl[0],
                                                                             None, extra_args,
                                                                             log_dtl_update_params)

                    if executed_successfully:
                        log_dtl_update_params["end_time"] = datetime.strftime(datetime.utcnow(),
                                                                              "%Y-%m-%d %H:%M:%S")
                        log_dtl_update_params["status"] = JOB_STATUS_COMPLETE
                        self.update_log_dtl_record(conn_var, extra_args, log_dtl_update_params)

        except:
            LOGGING.error(
                "Error in retry_failed_transactions method of SurveyScheduleJobUtils : {}".format(
                    traceback.format_exc()),
                extra=extra_args)

    def start_schedule_job(self):
        """
                   Purpose :
                   Input   :

                   Output  :

        """
        # this function will update survey_status col and survey_published_date col of the table f_rpt_user_scenario_survey_dtl
        conn_var = None
        current_timestamp = datetime.utcnow()
        current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")
        version = datetime.strftime(current_timestamp, "%Y%m%d%H%M%S")
        job_id = version
        extra_args = {"job_id": job_id, "caller": self.caller, "current_time_string": current_time_string}
        try:
            LOGGING.info("Creating connection object ")
            connection_object = DatabaseUtility().create_and_get_connection(
                host=self.host_name, username=self.user_name, password=self.password, port=self.port_name,
                database_name=self.database_name,
                auto_commit=True)
            if connection_object[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception(
                    "Failed to create connection object. ERROR - {}".format(connection_object[Constants.ERROR_KEY]))
            LOGGING.info("Connection object created successfully")
            conn_var = connection_object[Constants.RESULT_KEY]

            # call the fucntion to check is the job is currently running, if yes then exit the program
            previous_job_details = self.check_running_job(conn_var, extra_args)
            print("**********************************Previous Job Details*********************************************")
            print(previous_job_details)

            # Parameters to add a record in log_survey_response_data_sync_smry and log_survey_response_data_sync_dtl
            log_smry_params = {}
            log_smry_params["job_id"] = job_id
            log_smry_params["start_time"] = current_time_string
            log_smry_params["end_time"] = "NULL"
            log_smry_params["requested_by"] = self.caller
            log_smry_params["status"] = JOB_STATUS_IN_PROGRESS

            # Paramenter to update the end_time and status in log_survey_response_data_sync_smry and log_survey_response_data_sync_dtl
            log_smry_update_params = {}
            log_smry_update_params["job_id"] = log_smry_params["job_id"]
            log_smry_update_params["end_time"] = ""
            log_smry_update_params["status"] = ""

            self.create_log_smry_record(conn_var, extra_args, log_smry_params)

            # Re-try previous failed transactions
            self.retry_failed_transactions(conn_var, extra_args, log_smry_params)

            # process surveys for the current run
            active_surveyid_data = self.get_active_survey_ids(conn_var, extra_args)
            # above function might throw an exception

            if active_surveyid_data != 0:

                for i in range(len(active_surveyid_data)):
                    if active_surveyid_data[i]["qualtrics_user"] is None:
                        continue
                    if len(active_surveyid_data[i]["survey_id"]) != 0:
                        # Any updates made here should be made inside retry_failed_transactions also !!!
                        # Parameters to add a record in log_survey_response_data_sync_dtl
                        log_dtl_params = {}
                        log_dtl_params["job_id"] = log_smry_params["job_id"]
                        log_dtl_params["survey_id"] = ""
                        log_dtl_params["survey_type"] = ""
                        log_dtl_params["level"] = ""
                        log_dtl_params["country_name"] = ""
                        log_dtl_params["ctfo_site_id"] = ""
                        log_dtl_params["investigator_id"] = ""
                        log_dtl_params["start_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                        log_dtl_params["end_time"] = "NULL"
                        log_dtl_params["requested_by"] = log_smry_params["requested_by"]
                        log_dtl_params["status"] = JOB_STATUS_IN_PROGRESS

                        # Parameters to update the log_survey_response_data_sync_dtl
                        log_dtl_update_params = {}
                        log_dtl_update_params["job_id"] = log_smry_params["job_id"]
                        log_dtl_update_params["start_time"] = log_dtl_params["start_time"]
                        log_dtl_update_params["survey_id"] = ""
                        log_dtl_update_params["survey_type"] = ""
                        log_dtl_update_params["country_name"] = ""
                        log_dtl_update_params["ctfo_site_id"] = ""
                        log_dtl_update_params["investigator_id"] = ""
                        log_dtl_update_params["level"] = ""
                        log_dtl_update_params["end_time"] = ""
                        log_dtl_update_params["status"] = ""

                        if active_surveyid_data[i]["survey_status"] == SURVEY_STATUS_INITIATED:
                            # call the function to populate the job_dlt table
                            ##### update the logging start time here
                            current_timestamp = datetime.utcnow()
                            current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

                            log_dtl_params["start_time"] = current_time_string
                            log_dtl_params["survey_id"] = active_surveyid_data[i]["survey_id"]
                            log_dtl_params["survey_type"] = active_surveyid_data[i]["survey_type"]
                            log_dtl_params["level"] = JOB_DTL_LEVEL_SURVEY
                            self.create_log_dtl_record(conn_var, extra_args, log_dtl_params)

                            # parms for updating the log_dtl table
                            log_dtl_update_params["survey_id"] = active_surveyid_data[i]["survey_id"]
                            log_dtl_update_params["survey_type"] = active_surveyid_data[i]["survey_type"]
                            log_dtl_update_params["level"] = JOB_DTL_LEVEL_SURVEY

                            executed_successfully = self.get_survey_status(conn_var, active_surveyid_data[i],
                                                                           extra_args, log_dtl_update_params)

                            if executed_successfully:
                                log_dtl_update_params["end_time"] = datetime.strftime(datetime.utcnow(),
                                                                                      "%Y-%m-%d %H:%M:%S")
                                log_dtl_update_params["status"] = JOB_STATUS_COMPLETE
                                self.update_log_dtl_record(conn_var, extra_args, log_dtl_update_params)

                        print("********************************************************************************")

                        # add survey publish date as well in the if condition
                        if active_surveyid_data[i]["survey_status"] == SURVEY_STATUS_GOLIVE or \
                                active_surveyid_data[i]["survey_published_date"] is not None:
                            # add the current timestamp
                            print("SURVEY PUBLISH DATE {}".format(active_surveyid_data[i]["survey_published_date"]))

                            if active_surveyid_data[i]["survey_type"] == SITE_OUTREACH:
                                current_timestamp = datetime.utcnow()
                                current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

                                log_dtl_params["start_time"] = current_time_string
                                log_dtl_params["survey_id"] = active_surveyid_data[i]["survey_id"]
                                log_dtl_params["survey_type"] = active_surveyid_data[i]["survey_type"]
                                log_dtl_params["level"] = JOB_DTL_LEVEL_SITE

                                self.create_log_dtl_record(conn_var, extra_args, log_dtl_params)

                                log_dtl_update_params["survey_id"] = active_surveyid_data[i]["survey_id"]
                                log_dtl_update_params["survey_type"] = active_surveyid_data[i]["survey_type"]
                                log_dtl_update_params["level"] = JOB_DTL_LEVEL_SITE

                            if active_surveyid_data[i]["survey_type"] == COUNTRY_OUTREACH:
                                current_timestamp = datetime.utcnow()
                                current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

                                log_dtl_params["start_time"] = current_time_string
                                log_dtl_params["survey_id"] = active_surveyid_data[i]["survey_id"]
                                log_dtl_params["survey_type"] = active_surveyid_data[i]["survey_type"]
                                log_dtl_params["level"] = JOB_DTL_LEVEL_CO

                                self.create_log_dtl_record(conn_var, extra_args, log_dtl_params)

                                log_dtl_update_params["survey_id"] = active_surveyid_data[i]["survey_id"]
                                log_dtl_update_params["survey_type"] = active_surveyid_data[i]["survey_type"]
                                log_dtl_update_params["level"] = JOB_DTL_LEVEL_CO

                            executed_successfully = self.update_survey_dtl_responses(conn_var, active_surveyid_data[i],
                                                                                     None, extra_args,
                                                                                     log_dtl_update_params)

                            if executed_successfully:
                                log_dtl_update_params["end_time"] = datetime.strftime(datetime.utcnow(),
                                                                                      "%Y-%m-%d %H:%M:%S")
                                log_dtl_update_params["status"] = JOB_STATUS_COMPLETE
                                self.update_log_dtl_record(conn_var, extra_args, log_dtl_update_params)

            else:
                LOGGING.debug("No Active survey_id id found")
                # update the log_smry with Completed status
                log_smry_update_params["end_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                log_smry_update_params["status"] = JOB_STATUS_COMPLETE
                self.update_log_smry_record(conn_var, extra_args, log_smry_update_params)

            # check if any log_dtl for the current job_id has failed
            query_to_get_failed_dtl_log = SurveyScheduledJobQueries.QUERY_GET_FAILED_DTL_LOG
            query_to_get_failed_dtl_log = query_to_get_failed_dtl_log.format(schema_name=self.schema_name,
                                                                             job_id=log_smry_params["job_id"]
                                                                             )

            failed_dtl_log_response = DatabaseUtility().execute(
                query=query_to_get_failed_dtl_log, conn=conn_var, auto_commit=False)
            LOGGING.info("update_job_smry_response - {}".format(str(failed_dtl_log_response)))

            if failed_dtl_log_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                LOGGING.info("Executed query to query_to_get_failed_dtl_log Un-successfully")

            LOGGING.info("Executed query to query_to_get_failed_dtl_log successfully")

            if len(failed_dtl_log_response[Constants.RESULT_KEY]) != 0:
                LOGGING.info(
                    "Data fetched from get_active_survey_id - {}".format(failed_dtl_log_response[Constants.RESULT_KEY])
                    )
                if int(failed_dtl_log_response[Constants.RESULT_KEY][0]["count"]) > 0:
                    log_smry_update_params["end_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                    log_smry_update_params["status"] = JOB_STATUS_FAILED
                    # send the email notification for failed records
                    # fetching the email notification prams
                    smtp_server = self.application_config_data["ctfo_services"]["smtp_details"]["smtp_server"]
                    smtp_port = self.application_config_data["ctfo_services"]["smtp_details"]["smtp_port"]
                    sender_email = self.survey_config_data["survey_job"]["sender_email"]
                    recipient_list = self.survey_config_data["survey_job"]["job_failure"]["recipient_list"]
                    cc_recipient_list = self.survey_config_data["survey_job"]["job_failure"]["cc_recipient_list"]
                    email_subject = self.survey_config_data["survey_job"]["job_failure"]["subject"]

                    email_parameters = {
                        "count": failed_dtl_log_response[Constants.RESULT_KEY][0]["count"],
                        "job_id": log_smry_params["job_id"],
                        "env": self.application_config_data["ctfo_services"]["environment"],
                        "schema_name": self.schema_name
                    }
                    email_subject = email_subject.replace("$$env$$", email_parameters["env"]). \
                        replace("$$timestamp$$", log_smry_params["start_time"])
                    template_path = os.path.join(SERVICE_DIRECTORY_PATH, self.survey_config_data["survey_job"]["job_failure"]["email_body"])
                    email_body_template = open(template_path, 'r').read()
                    email_body = email_body_template. \
                        replace("$$count$$", email_parameters["count"]). \
                        replace("$$job_id$$", email_parameters["job_id"]). \
                        replace("$$schema_name$$", email_parameters["schema_name"]). \
                        replace("$$env$$", email_parameters["env"])

                    LOGGING.info("Email Body --> %s", email_body)
                    recipient_list = ", ".join(recipient_list)
                    cc_recipient_list = ", ".join(cc_recipient_list)
                    UTILS_OBJ.send_email(sender_email=sender_email,
                                         recipient_list=recipient_list, cc_recipient_list=cc_recipient_list,
                                         subject=email_subject, email_body=email_body,
                                         smtp_server=smtp_server, smtp_port=smtp_port)

                    LOGGING.debug("Sent Email to Survey Team successfully", extra=extra_args)
                else:
                    log_smry_update_params["end_time"] = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                    log_smry_update_params["status"] = JOB_STATUS_COMPLETE

                self.update_log_smry_record(conn_var, extra_args, log_smry_update_params)
        except SystemExit as e:
            LOGGING.info("Previous job is still in progress", extra=extra_args)
        except:
            if conn_var:
                conn_var.rollback()
                log_smry_update_params = {"end_time": datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S"),
                                          "status": JOB_STATUS_FAILED, "job_id": job_id}
                self.update_log_smry_record(conn_var, extra_args, log_smry_update_params)
            LOGGING.error(
                "Error in start_schedule_job method of SurveyScheduleJonUtilts : {}".format(traceback.format_exc()))

        finally:
            if conn_var:
                conn_var.commit()
                conn_var.close()
