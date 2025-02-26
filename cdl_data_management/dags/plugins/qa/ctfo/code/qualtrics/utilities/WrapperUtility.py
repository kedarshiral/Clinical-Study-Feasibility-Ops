import threading
import uuid
import json
from datetime import datetime
from functools import wraps
from flask import current_app, request
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

def updated_session_timestamp(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        database_utility_object = DatabaseUtility()

        app_config_dict = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(app_config_dict)

        if UTILS_OBJ.db_connection_details is None:
            raise Exception('db_connection_details KEY not present')

        query = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["user_roles_queries"]["update_user_session_time"]

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
                #raise Exception("user_id or user_access_id is not in input payload ")
                user_id = ""


        now = datetime.now()
        current_time_string = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")

        query_nonparam = query.replace("$$user_id$$", user_id). \
            replace("$$session_id$$", session_id). \
            replace("$$updated_timestamp$$", current_time_string)

        LOG.info("query_nonparam : %s", query_nonparam)

        UTILS_OBJ.execute_query(query_nonparam, UTILS_OBJ.host,
                                UTILS_OBJ.port,
                                UTILS_OBJ.username,
                                UTILS_OBJ.password,
                                UTILS_OBJ.database_name)

        return func(*args, **kwargs)

    return _time_it

def updated_session_timestamp_form_data(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        database_utility_object = DatabaseUtility()

        app_config_dict = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(app_config_dict)

        if UTILS_OBJ.db_connection_details is None:
            raise Exception('db_connection_details KEY not present')

        query = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["user_roles_queries"]["update_user_session_time"]

        # get session id from headers
        session_id = request.headers.get('session-id')
        user_id = ''

        if request.method == "POST":
            # request_json = request.get_data()
            # request_json = json.loads(request_json)
            #
            # if "user_id" in request_json:
            #     user_id = request_json["user_id"]
            # elif "user_access_id" in request_json:
            #     user_id = request_json["user_access_id"]
            # else:
            #     #raise Exception("user_id or user_access_id is not in input payload ")
            #     user_id = ""
            user_id = request.form.get('user_id')


        now = datetime.now()
        current_time_string = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")

        query_nonparam = query.replace("$$user_id$$", user_id). \
            replace("$$session_id$$", session_id). \
            replace("$$updated_timestamp$$", current_time_string)

        LOG.info("query_nonparam : %s", query_nonparam)

        UTILS_OBJ.execute_query(query_nonparam, UTILS_OBJ.host,
                                UTILS_OBJ.port,
                                UTILS_OBJ.username,
                                UTILS_OBJ.password,
                                UTILS_OBJ.database_name)

        return func(*args, **kwargs)

    return _time_it


def async_api(wrapped_function):
    database_utility_object = DatabaseUtility()

    app_config_dict = UTILS_OBJ.get_application_config_json()
    UTILS_OBJ.initialize_variable(app_config_dict)

    if UTILS_OBJ.db_connection_details is None:
        raise Exception('db_connection_details KEY not present')

    query = app_config_dict[CommonServicesConstants.CTFO_SERVICES][
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
                LOG.info("assessment_type-------------> %s", assessment_type)
            elif "highest-inv" in request_url:
                assessment_type = "highest" + "_" + "generate"
                LOG.info("assessment_type-------------> %s", assessment_type)

            else:
                assessment_type = "scenario"
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

            query_nonparam = query.replace("$$task_id$$", task_id). \
                replace("$$status$$", "Not Started"). \
                replace("$$assessment_type$$", assessment_type). \
                replace("$$created_time$$", current_time_string). \
                replace("$$updated_time$$", current_time_string)

            UTILS_OBJ.execute_query(query_nonparam, UTILS_OBJ.host,
                                    UTILS_OBJ.port,
                                    UTILS_OBJ.username,
                                    UTILS_OBJ.password,
                                    UTILS_OBJ.database_name)

            select_async_log_query = app_config_dict[CommonServicesConstants.CTFO_SERVICES][
                CommonServicesConstants.RDS_QUERIES]["async_queries"]["select_async_log_query"]

            select_async_log_query = select_async_log_query.replace("$$task_id$$", task_id)

            LOG.info("select_async_log_query parametrized: %s", select_async_log_query)

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
                                                                        UTILS_OBJ.database_name)

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









