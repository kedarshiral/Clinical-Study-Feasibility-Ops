
import threading
import uuid
import json
from datetime import datetime
from functools import wraps
from flask import current_app, request, Response
from werkzeug.exceptions import HTTPException, InternalServerError
from utilities import CommonServicesConstants
from utilities.LogSetup import get_logger
from datetime import datetime

import os
import time
from utilities.DatabaseUtility import DatabaseUtility
from utilities.utils import Utils

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "utilities/"))

APP_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH,
                                               "../configs/application_config.json"))
ENV_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH,
                                               "../configs/environment_config.json"))

UTILS_OBJ = Utils()

LOG = get_logger()


def validate_scenario_access(func):
    @wraps(func)
    def validate_access(*args, **kwargs):
        database_utility_object = DatabaseUtility()

        app_config_dict = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(app_config_dict)

        if UTILS_OBJ.db_connection_details is None:
            raise Exception('db_connection_details KEY not present')

        if request.method == "POST":
            try:
                request_json = request.get_data()
                request_json = json.loads(request_json)
                
            except Exception as e:
                    LOG.info(str(e))
                    request_json = dict(request.form)

            client_role_output = UTILS_OBJ.get_client_role(request_json["user_id"])
            client_role = client_role_output["result"][0]["client_role"]

            if "scenario_id" in request_json:
                scenario_id = request_json["scenario_id"]
            else:
                scenario_id = ""

            query_nonparam = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["user_roles_queries"]["get_scenario_client_role"]

            query_nonparam_params = {"scenario_id":  scenario_id}

            LOG.info("query_nonparam : %s", UTILS_OBJ.get_replaced_query(query_nonparam, query_nonparam_params))

            query_nonparam_output = UTILS_OBJ.execute_query(query_nonparam, UTILS_OBJ.host,
                                                            UTILS_OBJ.port,
                                                            UTILS_OBJ.username,
                                                            UTILS_OBJ.password,
                                                            UTILS_OBJ.database_name,
                                                            query_nonparam_params)

            if len(query_nonparam_output["result"]) > 0:
                admin_role = CommonServicesConstants.admin_role
                if client_role.lower().strip() not in ( query_nonparam_output["result"][0]["client_role"].lower().strip(), admin_role):
                    return Response("Unauthorized access to the resource", 401)

        return func(*args, **kwargs)

    return validate_access


def updated_session_timestamp(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        database_utility_object = DatabaseUtility()

        app_config_dict = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(app_config_dict)

        if UTILS_OBJ.db_connection_details is None:
            raise Exception('db_connection_details KEY not present')

        query_nonparam = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["user_roles_queries"]["update_user_session_time"]

        # get session id from headers
        session_id = request.headers.get('session-id')

        if request.method == "POST":
            request_json = request.get_data()
            request_json = json.loads(request_json)

            if "user_id" in request_json:
                user_id = request_json["user_id"]
            elif "user_access_id" in request_json:
                user_id = request_json["user_access_id"]
            else:
                user_id = ""

        now = datetime.now()
        current_time_string = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")

        query_nonparam_params = {"user_id": user_id, "session_id": session_id, "updated_timestamp": current_time_string}

        LOG.info("query_nonparam : %s", UTILS_OBJ.get_replaced_query(query_nonparam, query_nonparam_params))

        UTILS_OBJ.execute_query(query_nonparam, UTILS_OBJ.host,
                                UTILS_OBJ.port,
                                UTILS_OBJ.username,
                                UTILS_OBJ.password,
                                UTILS_OBJ.database_name,
                                query_nonparam_params)

        if request.path not in ('/CTFO/get-notifications') and "scenario_id" in request_json:
            scenario_id = request_json["scenario_id"]
            update_scenario_timestamp_q = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["user_roles_queries"][
                "update_scenario_timestamp"]
            params = {"user_access_id": user_id, "scenario_id": scenario_id}
            LOG.info("update_scenario_timestamp_q : %s", UTILS_OBJ.get_replaced_query(update_scenario_timestamp_q, params))

            UTILS_OBJ.execute_query(update_scenario_timestamp_q, UTILS_OBJ.host,
                                    UTILS_OBJ.port,
                                    UTILS_OBJ.username,
                                    UTILS_OBJ.password,
                                    UTILS_OBJ.database_name,
                                    params)
        return func(*args, **kwargs)

    return _time_it


def updated_session_timestamp_form_data(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        database_utility_object = DatabaseUtility()

        app_config_dict = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(app_config_dict)
        LOG.info("updated_session_timestamp_form_data ")

        if UTILS_OBJ.db_connection_details is None:
            raise Exception('db_connection_details KEY not present')

        query_nonparam = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["user_roles_queries"]["update_user_session_time"]
        LOG.info(" query --> %s", query_nonparam)

        # get session id from headers
        session_id = request.headers.get('session-id')
        LOG.info(" session_id --> %s", session_id)
        user_id = ''

        if request.method == "POST":
            user_id = request.form.get('user_id')
            LOG.info(" user_id --> %s", user_id)


        now = datetime.now()
        current_time_string = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")

        query_nonparam_params = {"user_id": user_id, "session_id": session_id, "updated_timestamp": current_time_string}

        LOG.info("query_nonparam : %s", UTILS_OBJ.get_replaced_query(query_nonparam, query_nonparam_params))

        UTILS_OBJ.execute_query(query_nonparam, UTILS_OBJ.host,
                                UTILS_OBJ.port,
                                UTILS_OBJ.username,
                                UTILS_OBJ.password,
                                UTILS_OBJ.database_name,
                                query_nonparam_params)

        return func(*args, **kwargs)

    return _time_it


def async_api(wrapped_function):
    database_utility_object = DatabaseUtility()

    app_config_dict = UTILS_OBJ.get_application_config_json()
    UTILS_OBJ.initialize_variable(app_config_dict)

    if UTILS_OBJ.db_connection_details is None:
        raise Exception('db_connection_details KEY not present')

    query_nonparam = app_config_dict[CommonServicesConstants.CTFO_SERVICES][
        CommonServicesConstants.RDS_QUERIES]["async_queries"]["insert_async_log_query"]

    tasks = {}

    @wraps(wrapped_function)
    def new_function(*args, **kwargs):
        def task_call(flask_app, environ):
            # Create a request context similar to that of the original request
            # so that the task can have access to flask.g, flask.request, etc.

            LOG.info("Inside task_call function")
            LOG.info("Environ variable --> %s", str(environ))

            with flask_app.request_context(environ):
                try:
                    tasks[task_id]['return_value'] = wrapped_function(*args, **kwargs)
                    LOG.info("tasks[task_id]['return_value'] : %s", str(tasks[task_id]['return_value']))
                    LOG.info("tasks : %s", str(tasks))
                except HTTPException as e:
                    LOG.info("inside HTTPException : %s", str(e))
                    tasks[task_id]['return_value'] = current_app.handle_http_exception(e)
                except Exception as e:
                    # The function raised an exception, so we set a 500 error
                    LOG.info("inside general Exception : %s", str(e))
                    tasks[task_id]['return_value'] = InternalServerError()
                    if current_app.debug:
                        # We want to find out if something happened so reraise
                        raise
                finally:
                    # We record the time of the response, to help in garbage
                    # collecting old tasks
                    tasks[task_id]['completion_timestamp'] = datetime.timestamp(datetime.utcnow())

                    # close the database session (if any)

        if "key" not in kwargs:
            key = "other"
        else:
            key = kwargs['key']

        assessment_type = str()

        if key == "generate":
            request_url = request.url
            LOG.info("Environ variable request_environ--> %s", str(request.environ))
            LOG.info(request_url)

            if "sitetier" in request_url:
                assessment_type = "landscape" + "_" + "sitetier"
            elif "sitemetric" in request_url:
                assessment_type = "landscape" + "_" + "sitemetric"
            elif "refresh-survey-data" in request_url:
                assessment_type = "pe" + "_" + "refresh"
            elif "calculate-enrollment-results" in request_url:
                assessment_type = "pe" + "_" + "calculate"
            elif "clinicaltrialchart" in request_url:
                assessment_type = "clinical" + "_" + "trial"
            elif "trialtimeline" in request_url:
                assessment_type = "trial" + "_" + "timeline"
            elif "landscapecount" in request_url:
                assessment_type = "landscape" + "_" + "count"
            elif "trialdetails" in request_url:
                assessment_type = "trial" + "_" + "details"
            elif "countrylandscape-detail" in request_url:
                assessment_type = "country" + "_" + "landscape-detail"
            elif "invflyout-generate" in request_url:
                assessment_type = "invflyout" + "_" + "gen"
            elif "flyout-generate" in request_url:
                assessment_type = "flyout" + "_" + "generate"
            elif "highest-inv" in request_url:
                assessment_type = "highest" + "_" + "generate"
            elif "export" in request_url:
                assessment_type = "prioritization_export"
            elif "export-survey" in request_url:
                assessment_type = "outreach_export"
            elif "send-survey" in request_url:
                assessment_type = "send_survey"
            else:
                assessment_type = "scenario"
            LOG.info("assessment_type-------------> %s", assessment_type)
            # Assign an id to the asynchronous task
            now = datetime.now()
            current_time = now.strftime("%Y%m%d%H%M%S%f")
            task_id = str(uuid.uuid4().hex) + "_" + current_time
            LOG.info("task_id-------------> %s", task_id)

            if "sitetier" in request_url or "sitemetric" in request_url or "clinicaltrialchart" in request_url or "trialtimeline" in request_url or \
                    "landscapecount" in request_url or "trialdetails" in request_url or "countrylandscape-detail" in request_url :

                kwargs['landscape'] = str(task_id)
                scenario_task_id = kwargs["landscape"] + "_" + current_time
                kwargs['key'] = str(scenario_task_id)
            elif "calculate-enrollment-results" in request_url or "refresh-survey-data" in request_url or \
                    "invflyout-generate" in request_url or "flyout-generate" in request_url or "highest-inv" in request_url:
                scenario_task_id = kwargs["scenario"] + "_" + current_time
                LOG.info("scenario_task_id-------------> %s", scenario_task_id)
                kwargs['scenario'] = str(task_id)
            else:
                scenario_task_id = kwargs["scenario"] + "_" + current_time
                kwargs['key'] = str(scenario_task_id)
                kwargs['scenario'] = str(task_id)

            LOG.info(task_id)
            LOG.info(scenario_task_id)

            current_time_string = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")

            query_nonparam_params = {"task_id": task_id, "status": 'Not Started', "assessment_type": assessment_type,
                                     "created_time": current_time_string, "updated_time": current_time_string}

            LOG.info("Query to Execute : ", UTILS_OBJ.get_replaced_query(query_nonparam, query_nonparam_params))

            UTILS_OBJ.execute_query(query_nonparam, UTILS_OBJ.host,
                                    UTILS_OBJ.port,
                                    UTILS_OBJ.username,
                                    UTILS_OBJ.password,
                                    UTILS_OBJ.database_name,
                                    query_nonparam_params)

            select_async_log_query = app_config_dict[CommonServicesConstants.CTFO_SERVICES][
                CommonServicesConstants.RDS_QUERIES]["async_queries"]["select_async_log_query"]

            select_async_log_query_params = {"task_id":  task_id}

            LOG.info("select_async_log_query parametrized: %s",
                     UTILS_OBJ.get_replaced_query(select_async_log_query, select_async_log_query_params))

            counter = 0
            while counter < 3:

                # Record the task, and then launch it
                tasks[task_id] = {'task_thread': threading.Thread(
                    target=task_call, args=(current_app._get_current_object(),
                                            request.environ))}

                tasks[task_id]['task_thread'].start()

                time.sleep(3)
                select_async_log_query_output = UTILS_OBJ.execute_query(select_async_log_query, UTILS_OBJ.host,
                                                                        UTILS_OBJ.port,
                                                                        UTILS_OBJ.username,
                                                                        UTILS_OBJ.password,
                                                                        UTILS_OBJ.database_name,
                                                                        select_async_log_query_params)

                LOG.info("select_async_log_query_output : %s", select_async_log_query_output)

                if str(select_async_log_query_output["result"][0]["progress_status"]).lower() == "not started":
                    counter += 1

                else:
                    break

            LOG.info("task completed")

            # Return a 202 response, with a link that the client can use to
            # obtain task status

            return {"status": "SUCCESS", "message": {"task_id": str(task_id), "async_list_id": scenario_task_id}}
        else:
            result = wrapped_function(*args, **kwargs)
            LOG.info("result %s", result)
            return result

    return new_function


def flask_async_api(wrapped_function):
    tasks = {}

    @wraps(wrapped_function)
    def new_function(*args, **kwargs):
        def task_call(flask_app, environ):
            # Create a request context similar to that of the original request
            # so that the task can have access to flask.g, flask.request, etc.

            LOG.info("Inside task_call function")

            with flask_app.request_context(environ):
                try:
                    tasks[task_id]['return_value'] = wrapped_function(*args, **kwargs)
                except HTTPException as e:
                    tasks[task_id]['return_value'] = current_app.handle_http_exception(e)
                except Exception as e:
                    # The function raised an exception, so we set a 500 error
                    tasks[task_id]['return_value'] = InternalServerError()
                    if current_app.debug:
                        # We want to find out if something happened so reraise
                        raise
                finally:
                    # We record the time of the response, to help in garbage
                    # collecting old tasks
                    tasks[task_id]['completion_timestamp'] = datetime.timestamp(datetime.utcnow())
                    # close the database session (if any)

        if "key" not in kwargs:
            key = "other"
        else:
            key = kwargs['key']

        if key in ["drug", "global"]:

            task_id = uuid.uuid4().hex
            kwargs['task_id'] = str(task_id)
            # Record the task, and then launch it
            tasks[task_id] = {'task_thread': threading.Thread(
                target=task_call, args=(current_app._get_current_object(),
                                        request.environ))}

            tasks[task_id]['task_thread'].start()
            return {"status": "SUCCESS", "message": "Data insertion in progress"}


        else:
            result = wrapped_function(*args, **kwargs)
            LOG.info("result %s", result)
            return result

    return new_function


def validate_endpoint_access(accepted_role, endpoint=None):
    def validate_endpoint_access_inner(func):
        @wraps(func)
        def validate_access(*args, **kwargs):
            if request.method == "POST":
                try:
                    request_json = request.get_data()
                    request_json = json.loads(request_json)
                except Exception as e:
                    LOG.info(str(e))
                    request_json = dict(request.form)

            endpoint_access = validate_endpoint_condition(endpoint, request_json)
            LOG.info("endpoint access %s", endpoint_access)

            user_role = request_json.get('user_group', '')
            user = request_json.get("user_id", '')
            scenario_id = request_json.get("scenario_id", '')
            if not user_role:
                client_role_output = UTILS_OBJ.get_user_group(user)
                user_role = client_role_output["result"][0]["app_user_group"]
                if scenario_id != '' and "||" in user_role :
                    scenario_level_user_group = UTILS_OBJ.get_user_group_scenario(user, scenario_id)
                    user_role = scenario_level_user_group["result"][0]["user_group"]
                elif "||" in user_role:
                    user_role = 'gfl'
            if endpoint_access and user_role.lower() not in accepted_role:
                LOG.error("validate_endpoint_access failed :( %s ,%s,%s ,%s", request.path, user, user_role,
                          accepted_role)
                return {"msg": "Unauthorized access to the resource"}, 403
                    
            if (endpoint == '/create-scenario' or endpoint == '/save-trialuniverse' or
                endpoint == '/save-add-trial' ) and request_json['is_created_before_r2'] == 'True':
                LOG.error("validate_endpoint_access failed :( %s ,%s,%s ,%s", request.path, user, user_role,
                          accepted_role)
                return {"msg": "Unauthorized access to the resource"}, 403

            LOG.info("validate_endpoint_access success :) %s ,%s,%s ,%s", request.path, user, user_role, accepted_role)
            return func(*args, **kwargs)

        return validate_access

    return validate_endpoint_access_inner


def validate_endpoint_condition(endpoint, request_json):

    if endpoint == '/send-survey-reminder' and request_json['source'] == 'site_outreach' and \
            request_json['user_group'] == 'mda':
        return False
    if endpoint == '/send-survey' and request_json['source'] == 'site_outreach':
        return False

    return True


