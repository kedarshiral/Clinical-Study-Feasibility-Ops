import os
import sys
import ast
import json
import base64
import random
import string
import hashlib
import datetime
import boto3
import subprocess
import psycopg2
import traceback
import smtplib
import pandas as pd
import requests
import platform
import ssl
import glob
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
from email.headerregistry import Address
from email.utils import make_msgid
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from utilities_postgres import CommonServicesConstants
from utilities_postgres.DatabaseUtility import DatabaseUtility
import CommonConstants

# Getting the path of Utilities directory which are needed for rule engine application


# Get the custom logger instance for this utility
LOGGING = logging.getLogger('__name__')

APP_CONFIG_PATH = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "utilities_postgres/application_config.json")
ENV_CONFIG_PATH = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "utilities_postgres/environment_params.json")
#SURVEY_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/survey_config.json"))
#USER_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/user_config.json"))

EMPTY = ""
DEFAULT_POSTGRESQL_PORT = 5432
DEFAULT_AUTOCOMMIT_ENABLED = True


class Utils:
    def __init__(self):
        pass
        self.database_utility_object = DatabaseUtility()


    @staticmethod
    def get_user_details_dict():
        """
        Purpose     :   This function is used to fetch the user template configuration
        Input       :   NA
        Output      :   User template dict
        """

        status_message = "Fetching user configuration"
        LOGGING.info(status_message)

        json_file = open(USER_CONFIG_PATH, 'r').read()
        config_data = json.loads(json_file)
        user_config_str = json.dumps(config_data)

        user_config = ast.literal_eval(user_config_str)
        user_config["api_token"] = base64.b64decode(user_config["api_token"]).decode("utf-8")
        LOGGING.info("Finished Fetching user configuration")
        return user_config

    @staticmethod
    def get_application_config_json():
        """
        Purpose     :   This function is used to fetch the application configuration
        Input       :   NA
        Output      :   Application configuration dict
        """

        status_message = "Fetching Application configuration"
        LOGGING.info(status_message)

        json_file = open(APP_CONFIG_PATH, 'r').read()
        config_data = json.loads(json_file)
        application_config_str = json.dumps(config_data)
        env_file = open(ENV_CONFIG_PATH, 'r').read()
        environment_config = json.loads(env_file)

        for key in environment_config:
            application_config_str = application_config_str.replace("$$" + key + "$$", str(environment_config[key]))
        application_config = ast.literal_eval(application_config_str)
        # LOGGING.info("config_Data %s ", application_config_str)
        LOGGING.info("Finished Fetching Application configuration")
        return application_config


    @staticmethod
    def get_env_config_json():
        """
        Purpose     :   This function is used to fetch the application configuration
        Input       :   NA
        Output      :   Application configuration dict
        """
        status_message = "Fetching Application configuration"
        LOGGING.info(status_message)

        json_file = open(ENV_CONFIG_PATH, 'r').read()
        config_data = json.loads(json_file)
        application_config_str = json.dumps(config_data)
        application_config = ast.literal_eval(application_config_str)
        # LOGGING.info("config_Data %s ", application_config_str)
        LOGGING.info("Finished Fetching Application configuration")
        return application_config

    @staticmethod
    def get_survey_config_json():
        """
        Purpose     :   This function is used to fetch the application configuration
        Input       :   NA
        Output      :   Application configuration dict
        """

        status_message = "Fetching Survey configuration"
        LOGGING.info(status_message)

        json_file = open(SURVEY_CONFIG_PATH, 'r').read()
        config_data = json.loads(json_file)
        survey_config_str = json.dumps(config_data)

        survey_config = ast.literal_eval(survey_config_str)
        survey_config["api_token"] = base64.b64decode(survey_config["api_token"]).decode("utf-8")
        LOGGING.info("Finished Fetching Survey configuration")
        return survey_config

    def initialize_variable(self, app_config_dict):

        """
         Purpose     :   This function will be used to initialize variables for database
         Input       :   config dictionary
         Output      :   N/A
         """
        LOGGING.info("Initializing Variable function started")
        self.db_connection_details = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["db_connection_details"]

        if self.db_connection_details is None:
            raise Exception('db_connection_details key not present')

        self.host = self.db_connection_details.get("host", None)
        self.port = int(self.db_connection_details.get("port", None))
        self.username = self.db_connection_details.get("username", None)
        self.password = os.environ['DB_PASSWORD']
        self.database_name = self.db_connection_details.get("database_name", None)
        LOGGING.info("Variables initialized successfully")

    @staticmethod
    def get_secret(secret_name, region_name):
        """
        Purpose: This fetches the secret details and decrypts them.
        Input: secret_name is the secret to be retrieved from Secrets Manager.
               region_name is the region which the secret is stored.
        Output: JSON with decoded credentials from Secrets Manager.
        """
        try:
            while True:
                # Create a Secrets Manager client
                session = boto3.session.Session()
                client = session.client(
                    service_name='secretsmanager',
                    region_name=region_name
                )
                LOGGING.info("Fetching the details for the secret name %s", secret_name)
                get_secret_value_response = client.get_secret_value(
                    SecretId=secret_name
                )
                if get_secret_value_response['SecretString']:
                    LOGGING.info("Fetched the Encrypted Secret from Secrets Manager for %s", secret_name)
                    break
        except Exception:
            raise Exception("Unable to fetch secrets.")
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                LOGGING.info("Decrypted the Secret")
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                LOGGING.info("Decrypted the Secret")
            return json.loads(secret)

    @staticmethod
    def get_s3_data(bucket_name, file_path):
        """
        Purpose: This reads the required file from s3 bucket..
        Input: bucket_name is the bucket_name for s3 destination.
               region_name is the region which the s3 bucket is located.
               file_path is the path of the input file.
        Output: JSON with file details.
        """
        try:
            client = boto3.client('s3')
            LOGGING.info("Fetching file details from s3")
            obj = client.get_object(Bucket=bucket_name, Key=file_path)
            contents = obj['Body'].read()
            return contents
        except Exception as ex:
            LOGGING.error(str(ex))
            raise Exception("Unable to fetch file details.")

    def execute_query(self, query, host, port, username, password, database_name, query_params=None, conn=None):
        """
        Purpose   :   Executes the query on postgreSQL
        Input     :   Query to execute
        Output    :   result in dictionary format
        """
        LOGGING.info("Executing Query")
        try:
            if query_params:
                output = self.database_utility_object.execute(query=query, host=host, username=username,
                                                              password=password,
                                                              database_name=database_name,
                                                              port=port,
                                                              conn=conn, query_params=query_params
                                                              )
            else:
                output = self.database_utility_object.execute(query=query, host=host, username=username,
                                                              password=password,
                                                              database_name=database_name,
                                                              port=port,
                                                              conn=conn
                                                              )

            if output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
                LOGGING.error("Failed because - %s", str(output[CommonServicesConstants.ERROR_KEY]))
                raise Exception("Error Executing Query --> " + str(output[CommonServicesConstants.ERROR_KEY]))
            LOGGING.info("Query execution finished")
            return output

        except Exception as e:
            LOGGING.error("Failed because - %s", str(e))
            raise Exception("Error executing query --> " + str(e))

    def execute_query_without_g(self, query, host, port, username, password, database_name, query_params=None,
                                conn=None):
        """
        Purpose   :   Executes the query on postgreSQL
        Input     :   Query to execute
        Output    :   result in dictionary format
        """
        LOGGING.info("Executing Query")
        try:
            if query_params:
                output = self.database_utility_object.execute_without_g(query=query, host=host, username=username,
                                                                        password=password,
                                                                        database_name=database_name,
                                                                        port=port,
                                                                        conn=conn, query_params=query_params
                                                                        )
            else:
                output = self.database_utility_object.execute_without_g(query=query, host=host, username=username,
                                                                        password=password,
                                                                        database_name=database_name,
                                                                        port=port,
                                                                        conn=conn
                                                                        )

            if output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
                LOGGING.error("Failed because - %s", str(output[CommonServicesConstants.ERROR_KEY]))
                raise Exception("Error Executing Query --> " + str(output[CommonServicesConstants.ERROR_KEY]))
            LOGGING.info("Query execution finished")
            return output

        except Exception as e:
            LOGGING.error("Failed because - %s", str(e))
            raise Exception("Error executing query --> " + str(e))

    def execute_many_query(self, query, var_list, host, port, username, password, database_name, conn=None):
        """
        Purpose   :   Executes the query on postgreSQL
        Input     :   Query to execute
        Output    :   result in dictionary format
        """
        LOGGING.info("Executing Many Query")
        try:
            output = self.database_utility_object.executemany(query=query, var_list=var_list, host=host,
                                                              username=username,
                                                              password=password,
                                                              database_name=database_name,
                                                              port=port,
                                                              conn=conn
                                                              )
            if output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
                raise Exception("Error Executing Query --> " + str(output[CommonServicesConstants.ERROR_KEY]))
        except Exception as e:
            raise Exception("Error Executing query --> " + str(e))

        # self.LOGGING.info("Query execution finished - %s ",query)
        LOGGING.info("Query Execution Finished")
        return output

    def execute_many_query_without_g(self, query, var_list, host, port, username, password, database_name, conn=None):
        """
        Purpose   :   Executes the query on postgreSQL
        Input     :   Query to execute
        Output    :   result in dictionary format
        """
        LOGGING.info("Executing Many Query")
        try:
            output = self.database_utility_object.executemany_without_g(query=query, var_list=var_list, host=host,
                                                                        username=username,
                                                                        password=password,
                                                                        database_name=database_name,
                                                                        port=port,
                                                                        conn=conn
                                                                        )
            if output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
                raise Exception("Error Executing Query --> " + str(output[CommonServicesConstants.ERROR_KEY]))
        except Exception as e:
            raise Exception("Error Executing query --> " + str(e))

        LOGGING.info("Query Execution Finished")
        return output

    def signature_verifier(self, input_json, mandatory_field_list):

        """
            Purpose   :   This method will verify the input json
            Input     :   Input json
            Output    :   Status
        """

        not_present_keys = []
        for key in mandatory_field_list:
            if key in input_json.keys():
                pass
            else:
                not_present_keys.append(key)

        if len(not_present_keys) is not 0:
            delimeter = ","
            keys = delimeter.join(not_present_keys)
            raise Exception("Following keys are not present --> " + str(keys))

        else:
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_SUCCESS}

    def execute_shell_command(self, command):
        status_message = ""
        return_status = {}
        try:
            status_message = "Started executing shell command " + command
            LOGGING.debug(status_message)
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            if standard_output:
                standard_output_lines = standard_output.splitlines()
                for line in standard_output_lines:
                    LOGGING.debug(line)
            if standard_error:
                standard_error_lines = standard_error.splitlines()
                for line in standard_error_lines:
                    LOGGING.debug(line)
            if command_output.returncode == 0:
                return_status[CommonServicesConstants.STATUS_KEY] = CommonServicesConstants.STATUS_SUCCESS
                return return_status
            else:
                return_status[CommonServicesConstants.STATUS_KEY] = CommonServicesConstants.STATUS_FAILED
                return return_status
        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e)
            LOGGING.error(error)
            return_status[CommonServicesConstants.STATUS_KEY] = CommonServicesConstants.STATUS_FAILED
            return_status[CommonServicesConstants.ERROR_KEY] = str(e)
            return return_status

    def create_and_get_connection(self, host, username, password, port=DEFAULT_POSTGRESQL_PORT, database_name=EMPTY,
                                  auto_commit=DEFAULT_AUTOCOMMIT_ENABLED):

        try:
            # Check for mandatory parameters
            if host is None or username is None or password is None:
                raise Exception("Please provide host, username and password for creating connection")

            if database_name is None:
                LOGGING.warning("Database name is not provided...")

            # Create a connection to PostgreSQL database
            connection = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=database_name)
            LOGGING.debug("PostgreSQL connection created for Host: " + host + ", Port: " + str(port) + ", Username: " +
                          username + ", Password: *******, Database: " + database_name)
            if auto_commit:
                connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                LOGGING.debug("Autocommit enabled for connection")

            return {
                CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_SUCCESS,
                CommonServicesConstants.RESULT_KEY: connection
            }

        except Exception as ex:
            LOGGING.error("Error while creating database connection. ERROR - " +
                          str(traceback.format_exc()))
            return {
                CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                CommonServicesConstants.ERROR_KEY: str(ex)
            }

    def close_connection(self, conn=None):
        """
        Purpose   :   Closes the PostgreSQL connection
        Input     :   PostgreSQL connection object
        Output    :   None
        """
        try:
            conn.close()
            LOGGING.debug("PostgreSQL connection closed")
        except Exception as ex:
            LOGGING.error("Error while closing database connection. ERROR - " +
                          str(traceback.format_exc()))
            return {
                CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                CommonServicesConstants.ERROR_KEY: str(ex)
            }

    def postgres_csv_export(self, query, location, filename, conn,
                            host=None, username=None, password=None, port=DEFAULT_POSTGRESQL_PORT,
                            database_name="", auto_commit=DEFAULT_AUTOCOMMIT_ENABLED,
                            delimeter=",", file_extension="csv", header=True):
        conn_created = False
        try:

            # Check whether connection is passed
            if conn and auto_commit:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                LOGGING.debug("Autocommit enabled for connection")
            if conn is None:
                # Check for mandatory arguments if connection not passed
                if host is None or username is None or password is None:
                    raise Exception("Database connection not provided. Please provide host, username "
                                    "and password for creating connection")
                elif database_name is None:
                    LOGGING.warning("Database name is not provided...")
                else:
                    # Create a new connection
                    LOGGING.debug("Connection not found for query. Creating new connection")
                    output = self.create_and_get_connection(host, username, password, port,
                                                            database_name, auto_commit)
                    if output[CommonServicesConstants.STATUS_KEY] ==\
                            CommonServicesConstants.STATUS_SUCCESS:
                        conn = output[CommonServicesConstants.RESULT_KEY]
                        conn_created = True
                    else:
                        return output
            # Export as table as csv
            cur = conn.cursor()
            header = "" if header == False else "HEADER"
            filepath = os.path.join(location, filename + ".csv")
            LOGGING.debug("Location used for copying")
            export_query = "COPY ({}) TO STDOUT WITH CSV {} delimiter '{}'".format(query, header,
                                                                                   delimeter)
            LOGGING.debug("Query - " + export_query)
            with open(filepath, 'w') as f:
                cur.copy_expert(export_query, f)
            conn.commit()
            cur.close()

            # Return status and query result
            return {
                CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_SUCCESS,
                CommonServicesConstants.RESULT_KEY: EMPTY
            }

        except Exception as ex:
            LOGGING.error("Error in csv export. ERROR - " + str(traceback.format_exc()))
            return {
                CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                CommonServicesConstants.ERROR_KEY: str(ex)
            }
        finally:
            if conn_created and conn:
                self.close_connection(conn)

    def send_email(self, sender_email, recipient_list, cc_recipient_list, bcc_recipient_list=None,
                   reply_to_list=None, subject=None, email_body=None, smtp_server=None, smtp_port=None,
                   email_parameters=None):
        """
                    Purpose             : send email
                    Input payload       : sender, recipient_list, subject, email_body, smtp_server, cc_recipient_list,
                                          bcc_recipient_list, reply_to_list
                    Output payload      :
        """
        try:
            LOGGING.debug("Initiating Email to %s", recipient_list)
            msg = MIMEMultipart('alternative')

            msg['From'] = sender_email

            msg['To'] = recipient_list

            if cc_recipient_list is not None:
                msg['Cc'] = cc_recipient_list

            if bcc_recipient_list is not None:
                msg['Bcc'] = bcc_recipient_list

            if reply_to_list is not None:
                msg['Reply-To'] = reply_to_list

            msg['Subject'] = subject
            e_body = MIMEText(email_body, 'html')
            msg.attach(e_body)

            LOGGING.debug("Email content --> %s", str(msg))

            server = smtplib.SMTP(host=smtp_server, port=smtp_port)
            server.connect(smtp_server, smtp_port)

            server.starttls()
            server.send_message(msg)
            server.quit()
        except:
            raise Exception("Issue Encountered while sending the email to recipients --> "+ str(traceback.format_exc()))

    def map_string_to_boolean(self, formatter_list, mapper, column_list, prepare_json_with_key=None):
        try:
            LOGGING.debug("Starting Map String 2 Boolean Function - %s", str(mapper))

            if prepare_json_with_key is None:
                df = pd.DataFrame(formatter_list)
            else:
                df = pd.DataFrame(formatter_list[prepare_json_with_key])

            for each_col in column_list:
                df[each_col] = df[each_col].map(mapper)

            result_list = df.to_json(orient='records')

            if prepare_json_with_key is None:
                return json.loads(result_list)
            else:
                dict_output = dict()
                dict_output[prepare_json_with_key] = json.loads(result_list)
                return dict_output
        except Exception as ex:
            LOGGING.error("Error in mapping string to boolean function. ERROR - " + str(traceback.format_exc()))
            return {
                CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                CommonServicesConstants.ERROR_KEY: str(ex)
            }

    def copy_from(self, schema, table, location, port, database_name=None, host=None, conn=None,
                  username=None, password=None,
                  auto_commit=CommonServicesConstants.DEFAULT_AUTOCOMMIT_ENABLED,
                  delimeter=",", file_extension="csv"):
        """
        Purpose   :   Import CSV and write to Postgres table
        Input     :   Connection information, table and CSV details
        Output    :   result in dictionary format
        """

        LOGGING.info("Executing Query")
        output = self.postgres_csv_import_pe(schema, table, location, conn, host, username,
                                             password, port, database_name, auto_commit,
                                             delimeter, file_extension)
        LOGGING.info("Query execution finished")
        return output

    def postgres_csv_import_pe(self, schema, table, location, conn=None,
                               host=None, username=None, password=None, port=DEFAULT_POSTGRESQL_PORT,
                               database_name=EMPTY, auto_commit=DEFAULT_AUTOCOMMIT_ENABLED,
                               delimeter=",", file_extension="csv", header=True):
        conn_created = False

        try:

            # Check whether connection is passed
            if conn and auto_commit:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                LOGGING.debug("Autocommit enabled for connection")
            if conn is None:
                # Check for mandatory arguments if connection not passed
                if host is None or username is None or password is None:
                    raise Exception("Database connection not provided. Please provide host, username "
                                    "and password for creating connection")
                elif database_name is None:
                    LOGGING.warning("Database name is not provided...")
                else:
                    # Create a new connection
                    LOGGING.debug("Connection not found for query. Creating new connection")
                    output = self.create_and_get_connection(host, username, password, port, database_name, auto_commit)

                    LOGGING.info("output from create_and_get_connection ------------> {}".format(output))

                    if output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_SUCCESS:
                        conn = output[CommonServicesConstants.RESULT_KEY]
                        conn_created = True
                    else:
                        return output

            # Import table from csv
            cur = conn.cursor()
            schema_table = schema + "." + table
            header = "" if header == False else "HEADER"
            import_query = "copy {} from STDIN WITH CSV {} delimiter '{}';".format(schema_table, header, delimeter)

            LOGGING.info("import_query - " + import_query)
            LOGGING.info("location - " + location)

            LOGGING.debug("Query - " + import_query)
            LOGGING.debug(" csv location is - " + location)

            # Check if location given is of file level or folder level
            if not location.lower().endswith(".csv"):
                for file_ in glob.glob(location + "/*." + file_extension):
                    with open(file_, "r", encoding="utf8") as f:
                        cur.copy_expert(import_query, f)
            else:
                with open(location, 'r', encoding="utf8") as f:
                    cur.copy_expert(import_query, f)
            conn.commit()
            cur.close()

            # Return status and query result
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.SUCCESS_KEY,
                    CommonServicesConstants.RESULT_KEY: EMPTY}
        except Exception as ex:
            LOGGING.error("Error in csv import. ERROR - %s" + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}

        finally:
            if conn_created and conn:
                self.close_connection(conn)

    def postgres_csv_import(self, conn, schema, table, location, delimeter="^", file_extension="csv", header=True):
        try:
            # Is valid postgres connection...?
            if conn is None:
                raise Exception("Connection not found...")
            else:
                # Create a new connection
                LOGGING.debug("Connection not found for query. Creating new connection")
                output = self.create_and_get_connection(self.host, self.username, self.password, self.port,
                                                        self.database_name, auto_commit=DEFAULT_AUTOCOMMIT_ENABLED)
                if output[CommonServicesConstants.STATUS_KEY] == \
                        CommonServicesConstants.STATUS_SUCCESS:
                    conn = output[CommonServicesConstants.RESULT_KEY]

            # Export as table as csv
            cur = conn.cursor()
            schema_table = schema + "." + table
            LOGGING.info(f"schema_table:-- {schema_table}")
            header = "" if header == False else "HEADER"
            import_query = "copy {} from STDIN CSV {} delimiter '{}'".format(schema_table, header, delimeter)
            LOGGING.debug("Query - " + import_query)

            # Check if location given is of file level or folder level
            if not location.lower().endswith(".csv"):
                for file_ in glob.glob(location + "/*." + file_extension):
                    with open(file_, 'r', encoding='utf-8-sig') as f:
                        cur.copy_expert(import_query, f)
            else:
                with open(location, 'r', encoding='utf-8-sig') as f:
                    cur.copy_expert(import_query, f)
            conn.commit()
            cur.close()

            # Return status and query result
            return {
                CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_SUCCESS,
                CommonServicesConstants.RESULT_KEY: EMPTY
            }
        except Exception as ex:
            LOGGING.error("Error in csv import. ERROR - " + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}

    def replace_environment_details(self, config_json, query):
        schema_name = config_json[CommonServicesConstants.CTFO_SERVICES][CommonServicesConstants.SCHEMA]
        rpt_schema_name = config_json[CommonServicesConstants.CTFO_SERVICES][CommonServicesConstants.SCHEMA_REPORTING]
        foreign_server = config_json[CommonServicesConstants.CTFO_SERVICES][CommonServicesConstants.FOREIGN_SERVER ]
        rpt_schema_name_snapshot = config_json[CommonServicesConstants.CTFO_SERVICES][CommonServicesConstants.SCHEMA_REPORTING_SNAPSHOT]

        query = query.replace("$$schema$$", schema_name). \
            replace("$$schema_reporting_snapshot$$", rpt_schema_name_snapshot). \
            replace("$$schema_reporting$$", rpt_schema_name).\
            replace("$$foreign_server$$",foreign_server)
        return query


    def check_screen_edit_access(self, user_id, scenario_id, lock_status = None):
        """
        Purpose     :   This function is used to check if user is allowed to edit a screen or not
        Input       :   user_id, scenario_id
        Output      :   json
        """

        app_config_dict = Utils.get_application_config_json()
        self.initialize_variable(app_config_dict)

        check_superuser_query = app_config_dict["ctfo_services"]["user_roles_queries"]["check_superuser"]

        check_superuser_query = self.replace_environment_details(app_config_dict, check_superuser_query)

        check_superuser_query_params = {"user_id":  user_id}

        LOGGING.info("check_superuser_query : %s",
                     self.get_replaced_query(check_superuser_query, check_superuser_query_params))

        check_superuser_query_output = self.execute_query(check_superuser_query, self.host, self.port,
                                                         self.username, self.password,
                                                         self.database_name, check_superuser_query_params)

        LOGGING.info("check_superuser_query_output : %s", check_superuser_query_output)

        if check_superuser_query_output["status"].lower() != "success":
            raise Exception('Failed to fetch user group!')

        if len(check_superuser_query_output["result"]) > 0:
            if check_superuser_query_output["result"][0]["app_user_group"].lower() == "superuser":
                user_group = "superuser"
                write_access = 'True'
            else:

                get_user_group_query = app_config_dict["ctfo_services"]["user_roles_queries"]["get_user_group"]

                get_user_group_query = self.replace_environment_details(app_config_dict, get_user_group_query)

                get_user_group_query_params = {"scenario_id":  scenario_id, "user_id":  user_id}

                LOGGING.info("get_user_group_query : %s",
                             self.get_replaced_query(get_user_group_query, get_user_group_query_params))

                get_user_group_query_output = self.execute_query(get_user_group_query, self.host, self.port,
                                                              self.username, self.password,
                                                              self.database_name, get_user_group_query_params)

                LOGGING.info("get_user_group_query_output : %s", get_user_group_query_output)

                if get_user_group_query_output["status"].lower() != "success":
                    raise Exception('Failed to fetch user group!')

                user_group = get_user_group_query_output["result"][0]["user_group"]
                write_access = get_user_group_query_output["result"][0]["write_access"]

                LOGGING.info("user_group, write_access : %s %s ", user_group, write_access)

        result_status = {}
        edit_screen = "disabled"

        if user_group in ["gfl", "superuser"] and write_access == 'True':

            check_if_authorized_to_edit_screen_query = app_config_dict["ctfo_services"]["user_roles_queries"][
                "check_if_authorized_to_edit_screen"]

            check_if_authorized_to_edit_screen_query = self.replace_environment_details(app_config_dict,
                                                                    check_if_authorized_to_edit_screen_query)

            check_if_authorized_to_edit_screen_query_params = {"scenario_id": scenario_id}

            LOGGING.info("check_if_authorized_to_edit_screen_query : %s",
                         self.get_replaced_query(check_if_authorized_to_edit_screen_query,
                                                 check_if_authorized_to_edit_screen_query_params))

            check_if_authorized_to_edit_screen_query_output = self.execute_query(
                check_if_authorized_to_edit_screen_query, self.host, self.port,
                self.username, self.password,
                self.database_name, check_if_authorized_to_edit_screen_query_params)

            LOGGING.info("check_if_authorized_to_edit_screen_query_output : %s",
                         check_if_authorized_to_edit_screen_query_output)

            if check_if_authorized_to_edit_screen_query_output["status"].lower() != "success":
                raise Exception('Failed to execute check_if_authorized_to_edit_screen_query!')

            if int(check_if_authorized_to_edit_screen_query_output["result"][0]["count"]) > 1 and (lock_status is None or lock_status == False):
                LOGGING.info("edit_screen : %s", edit_screen)
                return edit_screen
            elif int(check_if_authorized_to_edit_screen_query_output["result"][0]["count"]) == 1:
                edit_lock_list_user_query = app_config_dict["ctfo_services"]["user_roles_queries"][
                    "edit_lock_list_user"]

                edit_lock_list_user_query = self.replace_environment_details(app_config_dict, edit_lock_list_user_query)

                edit_lock_list_user_query_params = {"scenario_id": scenario_id}

                LOGGING.info("edit_lock_list_user_query : %s",
                             self.get_replaced_query(edit_lock_list_user_query, edit_lock_list_user_query_params))

                edit_lock_list_user_query_output = self.execute_query(
                    edit_lock_list_user_query, self.host, self.port,
                    self.username, self.password,
                    self.database_name, edit_lock_list_user_query_params)

                LOGGING.info("edit_lock_list_user_query_output : %s", edit_lock_list_user_query_output)

                if edit_lock_list_user_query_output["status"].lower() != "success":
                    raise Exception('Failed to execute edit_lock_list_user_query!')
                else:
                    if user_id != edit_lock_list_user_query_output["result"][0]["user_access_id"]:
                        LOGGING.info("edit_screen : %s", edit_screen)
                        return edit_screen
                    else:
                        return "enabled"
            else:
                return "enabled"

        else:
            LOGGING.info("edit_screen : %s", edit_screen)
            return edit_screen

    def get_user_group (self, user_id):
        """
        Purpose     :   This function is used to check if user is allowed to edit a screen or not
        Input       :   user_id, scenario_id
        Output      :   json
        """
        try:
            app_config_dict = Utils.get_application_config_json()
            self.initialize_variable(app_config_dict)

            check_superuser_query = app_config_dict["ctfo_services"]["user_roles_queries"][
                "check_superuser_in_main_table"]

            check_superuser_query = self.replace_environment_details(app_config_dict,
                                                                     check_superuser_query)

            check_superuser_query = check_superuser_query.replace("$$user_id$$", user_id)

            LOGGING.info("check_superuser_query : %s", check_superuser_query)

            check_superuser_query_output = self.execute_query(check_superuser_query, self.host, self.port,
                                                              self.username, self.password,
                                                              self.database_name)

            LOGGING.info("check_superuser_query_output : %s", check_superuser_query_output)

            if check_superuser_query_output["status"].lower() != "success":
                raise Exception('Failed to fetch user group!')
            return check_superuser_query_output

        except Exception as ex:
            LOGGING.error("Error in csv import. ERROR - " + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}

    def get_user_group_scenario (self, user_id,scenario_id):
        """
        Purpose     :   This function is used to check if user is allowed to edit a screen or not
        Input       :   user_id, scenario_id
        Output      :   json
        """
        try:
            app_config_dict = Utils.get_application_config_json()
            self.initialize_variable(app_config_dict)

            check_user_query = app_config_dict["ctfo_services"]["user_roles_queries"][
                "check_user_query"]

            check_user_query = self.replace_environment_details(app_config_dict,
                                                                     check_user_query)

            check_user_query = check_user_query.replace("$$user_id$$", user_id). \
                            replace("$$scenario_id$$", scenario_id)

            LOGGING.info("check_user_query : %s", check_user_query)

            check_user_query_output = self.execute_query(check_user_query, self.host, self.port,
                                                              self.username, self.password,
                                                              self.database_name)

            LOGGING.info("check_user_query_output : %s", check_user_query)

            if check_user_query_output["status"].lower() != "success":
                raise Exception('Failed to fetch user group!')
            return check_user_query_output

        except Exception as ex:
            LOGGING.error("Error in csv import. ERROR - " + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}

    def get_client_role(self, user_id, validation_level='user', scenario_id = None):

        """
                Purpose     :   This function is used to get client role of current user
                Input       :   validation_level, user_id, scenario_id
                Output      :   json
                """
        try:
            app_config_dict = Utils.get_application_config_json()
            self.initialize_variable(app_config_dict)

            get_client_role_query = app_config_dict["ctfo_services"]["user_roles_queries"][
                "get_client_role"]

            get_client_role_query = self.replace_environment_details(app_config_dict,
                                                                get_client_role_query)

            validation_level = 'user'

            if validation_level == "user":
                table_name = "f_rpt_user_details"
                condition = " user_access_id = '{}' AND active_flag = True ".format(user_id)
            elif validation_level == "scenario":
                table_name = "f_rpt_scenario_stdy_sumry"
                condition = " scenario_id = '{}'".format(scenario_id)

            get_client_role_query = get_client_role_query.replace("$$user_id$$", user_id). \
                replace("$$condition$$", condition). \
                replace("$$table_name$$", table_name)

            LOGGING.info("get_client_role_query : %s", get_client_role_query)

            get_client_role_query_ouptut = self.execute_query(get_client_role_query, self.host, self.port,
                                                         self.username, self.password,
                                                         self.database_name)

            LOGGING.info("get_client_role_query_ouptut : %s", get_client_role_query_ouptut)

            if get_client_role_query_ouptut["status"].lower() != "success":
                raise Exception('Failed to fetch user group!')
            return get_client_role_query_ouptut

        except Exception as ex:
            LOGGING.error("Error in csv import. ERROR - " + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}

    def get_replaced_query(self,query, query_params=None):
        print_query = query
        LOGGING.info(f"query:- {query}")
        LOGGING.info(f"query_params:- {query_params}")

        if query_params:
            if isinstance(query_params, list):
                print_query = []
                for each_param in query_params:
                    each_query = query
                    for key, val in each_param.items():
                        if val is None or type(val) is int or type(val) is bool or type(val) is float:
                            val = str(val)
                        each_query = each_query.replace("".join(["%(", key, ")s"]), "".join(["'", val, "'"]))
                    print_query.append(each_query)
            elif isinstance(query_params, dict):
                for key, val in query_params.items():
                    if val is None or type(val) is int or type(val) is bool or type(val) is float:
                        val = str(val)
                    print_query = print_query.replace("".join(["%(", key, ")s"]), "".join(["'", val, "'"]))
        return str((''.join(print_query)))


    def get_table_cols(self, schema_name= None, table_name= None, delimiter=None, cols_list=None):
        """
        delimiter: list of columns separated by delimiter
        """

        try:
            app_config_dict = Utils.get_application_config_json()
            self.initialize_variable(app_config_dict)

            get_table_cols_query = app_config_dict["ctfo_services"]["misc_queries"][
                "get_table_cols"]

            get_table_cols_query = self.replace_environment_details(app_config_dict,
                                                                     get_table_cols_query)

            get_table_cols_query = get_table_cols_query.replace("$$schema$$", schema_name). \
                replace("$$table_name$$", table_name)

            LOGGING.info("get_table_cols_query : %s", get_table_cols_query)

            get_table_cols_query_output = self.execute_query(get_table_cols_query, self.host, self.port,
                                                              self.username, self.password,
                                                              self.database_name)

            LOGGING.info("get_table_cols_query_output : %s", get_table_cols_query_output)

            cols_list = [val["column_names"] for val in get_table_cols_query_output["result"]]

            if get_table_cols_query_output["status"].lower() != "success":
                raise Exception('Failed to execute get_table_cols_query!')
            return cols_list

        except Exception as ex:
            LOGGING.error("Error in csv import. ERROR - " + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}

    def disable_button_aw(self,config_json,scenario_id,user_access_id):
        """
               Function to fetch country locked status of any country is locked for that user then result is false
               which means Apply Weight and Update Metrics buttons has to be disabled.
               If the result is True then Apply Weight and Update Metrics buttons has to be enabled.
               """
        try:
            disable_button_query = """SELECT bool_and(is_locked) as disable_flag FROM $$schema$$.f_rpt_user_activity_scenario_details WHERE user_access_id = %(user_access_id)s AND scenario_id = %(scenario_id)s AND user_active_flag = 'Y'"""
            schema_name = config_json[CommonServicesConstants.CTFO_SERVICES][CommonServicesConstants.SCHEMA]
            disable_button_query = disable_button_query.replace("$$schema$$", schema_name)
            disable_button_query_parms={"scenario_id":scenario_id,"user_access_id":user_access_id}
            LOGGING.info("disable_button_query : %s", disable_button_query)
            disable_button_query_op = self.execute_query(disable_button_query, self.host, self.port,
                                                         self.username, self.password,
                                                         self.database_name,disable_button_query_parms)
            if disable_button_query_op["status"].lower() != "success":
                raise Exception('Failed to execute disable_button_query!')

            return disable_button_query_op["result"][0]["disable_flag"]
        except Exception as ex:
            LOGGING.error("Error in Disable button. ERROR - " + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}

    def update_ds_model_rds_status(self, model_run_id, theme_id, scenario_id, created_by, status=None,
                                   created_timestamp=None, description=None):
        return_status = {}
        updated_timestamp = datetime.datetime.now()
        try:

            app_config_dict = Utils.get_application_config_json()
            self.initialize_variable(app_config_dict)

            if self.db_connection_details is None:
                raise Exception('db_connection_details KEY not present')

            check_id_exists_query = self.replace_environment_details(app_config_dict, """select 
              count(1) 
            from 
              $$schema$$.log_model_run_status 
            WHERE model_run_id = '$$model_run_id$$'
            AND theme_id = '$$theme_id$$'
            AND scenario_id = '$$scenario_id$$'
            AND created_by = '$$created_by$$'
            """)
            check_id_exists_query = check_id_exists_query \
                .replace("$$model_run_id$$", model_run_id) \
                .replace("$$theme_id$$", theme_id) \
                .replace("$$scenario_id$$", scenario_id) \
                .replace("$$created_by$$", created_by)
            LOGGING.info("check_id_exists_query - {}".format(str(check_id_exists_query)))

            check_id_exists_query_params = {}

            id_exists_query_output = self.execute_query(check_id_exists_query, self.host, self.port, self.username,
                                                        self.password, self.database_name, check_id_exists_query_params)

            if id_exists_query_output[CommonServicesConstants.STATUS_KEY] == \
                    CommonServicesConstants.STATUS_FAILED:
                raise Exception(id_exists_query_output[CommonServicesConstants.ERROR_KEY])

            if len(id_exists_query_output[CommonServicesConstants.RESULT_KEY]) > 0:
                if int(id_exists_query_output[CommonServicesConstants.RESULT_KEY][0]['count']) > 0:
                    update_query = self.replace_environment_details(app_config_dict,
                                                                    "UPDATE $$schema$$.log_model_run_status SET $$params$$"
                                                                    " WHERE model_run_id = '$$model_run_id$$' "
                                                                    " AND theme_id='$$theme_id$$' AND  scenario_id='$$scenario_id$$' AND created_by='$$created_by$$'")
                    if status is not None:
                        status_param = "status='" + str(status) + "'"
                    else:
                        status_param = ""
                    if description is not None:
                        description_param = "description='" + str(description) + "'"
                    else:
                        description_param = ""
                    if created_timestamp is not None:
                        created_timestamp_param = "created_timestamp='" + str(created_timestamp) + "'"
                    else:
                        created_timestamp_param = ""
                    updated_timestamp_param = "updated_timestamp='" + str(updated_timestamp) + "'"
                    param_list = [each_param for each_param in
                                  [description_param, status_param, updated_timestamp_param] if
                                  each_param]
                    update_query = update_query.replace("$$params$$", ",".join(param_list))
                    update_query = update_query.replace("$$model_run_id$$", model_run_id) \
                        .replace("$$theme_id$$", theme_id) \
                        .replace("$$scenario_id$$", scenario_id) \
                        .replace("$$created_by$$", created_by)
                    query = update_query
                else:
                    insert_query = self.replace_environment_details(app_config_dict,
                                                                    "INSERT INTO $$schema$$.log_model_run_status values "
                                                                    "('$$model_run_id$$', '$$status$$', '$$description$$', '$$created_timestamp$$',"
                                                                    "'$$updated_timestamp$$','$$theme_id$$','$$scenario_id$$','$$created_by$$')")

                    query = insert_query.replace("$$model_run_id$$", model_run_id) \
                        .replace("$$theme_id$$", theme_id) \
                        .replace("$$scenario_id$$", scenario_id) \
                        .replace("$$created_by$$", created_by) \
                        .replace("$$status$$", status) \
                        .replace("$$description$$", description) \
                        .replace("$$updated_timestamp$$", str(updated_timestamp)) \
                        .replace("$$created_timestamp$$", str(created_timestamp))

                LOGGING.info("query - {}".format(str(query)))
                query_param = {}
                query_status_output = self.execute_query(query, self.host, self.port, self.username, self.password,
                                                         self.database_name, query_param)

                if query_status_output[CommonServicesConstants.STATUS_KEY] == \
                        CommonServicesConstants.STATUS_FAILED:
                    raise Exception(query_status_output[CommonServicesConstants.ERROR_KEY])
            return_status[CommonServicesConstants.STATUS_KEY] = CommonServicesConstants.STATUS_SUCCESS
            return return_status
        except Exception as err:
            error = ' ERROR MESSAGE: ' + str(err)
            LOGGING.error(error + str(traceback.format_exc()))
            return_status[CommonServicesConstants.STATUS_KEY] = CommonServicesConstants.STATUS_FAILED
            return_status[CommonServicesConstants.ERROR_KEY] = error
            return return_status

    def aws_cp_to_mv(self, a, b):
        try:
            aws_copy_command = '/usr/local/bin/aws s3 mv "' + a + '" ' + b
            LOGGING.info("aws_copy_command --> %s", aws_copy_command)

            aws_copy_command_output = Utils().execute_shell_command(aws_copy_command)

            LOGGING.info(
                "-------------------------aws_copy_command_output--------------------> is {} ".format(
                    aws_copy_command_output))

            if aws_copy_command_output[CommonServicesConstants.STATUS_KEY] == \
                    CommonServicesConstants.STATUS_FAILED:
                raise Exception("Failed to run aws s3 cp command")

        except Exception as ex:
            LOGGING.error("Error in aws s3 mv. ERROR - " + str(traceback.format_exc()))
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(ex)}



