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
from LogSetup import get_logger
import CommonServicesConstants
from DatabaseUtility import DatabaseUtility

# Getting the path of Utilities directory which are needed for rule engine application
SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "utilities/"))
sys.path.insert(0, UTILITIES_DIR_PATH)

# Get the custom logger instance for this utility
LOGGING = get_logger()

APP_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/application_config.json"))
ENV_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/environment_config.json"))
SURVEY_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/survey_config.json"))
USER_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/user_config.json"))

EMPTY = ""
DEFAULT_POSTGRESQL_PORT = 5432
DEFAULT_AUTOCOMMIT_ENABLED = True


class Utils:
    def __init__(self):
        pass
        self.database_utility_object = DatabaseUtility()

    @staticmethod
    def generate_job_id(service_name):
        """
            Purpose   :   This method will generate a job_id by hashing client_ip, current timestamp, and service_name
            Input     :   client_ip,Service name
            Output    :   job_id
        """
        try:
            current_timestamp = "" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            rand = random.randint(1, 100)
            concat_string = str(rand) + current_timestamp + service_name + ''.join(
                random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
            job_id = hashlib.md5(concat_string.encode()).hexdigest()
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.SUCCESS_KEY,
                    CommonServicesConstants.RESULT_KEY: {CommonServicesConstants.JOB_ID_KEY: job_id}}
        except Exception as exception:
            return {CommonServicesConstants.STATUS_KEY: CommonServicesConstants.STATUS_FAILED,
                    CommonServicesConstants.ERROR_KEY: str(exception)}

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
        # LOGGING.info("config_Data %s ", application_config_str)
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
            application_config_str = application_config_str.replace("$$" + key + "$$", environment_config[key])
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
        # LOGGING.info("config_Data %s ", application_config_str)
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
        # secret_name = self.db_connection_details.get("password", None)
        # secret_password = self.get_secret(secret_name, self.db_connection_details.get("db_region_name", None))

        # secret_password = self.db_connection_details.get("password", None)
        # self.password = secret_password
        secret_name = self.db_connection_details.get("secret_name", None)
        region_name = self.db_connection_details.get("db_region_name", None)
        # get_secret_dict = self.get_secret(secret_name, region_name)
        # secret_password = get_secret_dict["password"]
        # self.password = secret_password
        self.password = "gKU4y7hWlBeH"
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

    def execute_query(self, query, host, port, username, password, database_name):
        """
        Purpose   :   Executes the query on postgreSQL
        Input     :   Query to execute
        Output    :   result in dictionary format
        """
        # self.LOGGING.info("Executing Query - %s ",query)
        LOGGING.info("Executing Query")
        try:
            output = self.database_utility_object.execute(query=query, host=host, username=username,
                                                          password=password,
                                                          database_name=database_name,
                                                          port=port
                                                          )
            if output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
                raise Exception("Error Executing Query --> " + str(output[CommonServicesConstants.ERROR_KEY]))
        except Exception as e:
            raise Exception("Error executing query --> " + str(e))

        # self.LOGGING.info("Query execution finished - %s ",query)
        LOGGING.info("Query execution finished")
        return output

    def execute_many_query(self, query, var_list, host, port, username, password, database_name):
        """
        Purpose   :   Executes the query on postgreSQL
        Input     :   Query to execute
        Output    :   result in dictionary format
        """
        # self.LOGGING.info("Executing Query - %s ",query)
        LOGGING.info("Executing Many Query")
        try:
            output = self.database_utility_object.executemany(query=query, var_list = var_list, host=host, username=username,
                                                          password=password,
                                                          database_name=database_name,
                                                          port=port
                                                          )
            if output[CommonServicesConstants.STATUS_KEY] == CommonServicesConstants.STATUS_FAILED:
                raise Exception("Error Executing Query --> " + str(output[CommonServicesConstants.ERROR_KEY]))
        except Exception as e:
            raise Exception("Error Executing query --> " + str(e))

        # self.LOGGING.info("Query execution finished - %s ",query)
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
            # msg['To'] = ", ".join(recipient_list)

            msg['To'] = recipient_list

            if cc_recipient_list is not None:
                # msg['Cc'] = ", ".join(cc_recipient_list)
                msg['Cc'] = cc_recipient_list

            if bcc_recipient_list is not None:
                # msg['Bcc'] = ", ".join(bcc_recipient_list)
                msg['Bcc'] = bcc_recipient_list

            if reply_to_list is not None:
                # msg['Reply-To'] = ", ".join(reply_to_list)
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

            result_list = df.to_json(orient='records').replace('\\', '')

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
        query = query.replace("$$schema$$", schema_name). \
            replace("$$schema_reporting$$", rpt_schema_name)
        return query


    def check_screen_edit_access(self, user_id, scenario_id, lock_status = None):
        """
        Purpose     :   This function is used to check if user is allowed to edit a screen or not
        Input       :   user_id, scenario_id
        Output      :   json
        """

        app_config_dict = Utils.get_application_config_json()
        self.initialize_variable(app_config_dict)

        check_superuser_query = app_config_dict["ctfo_services"]["user_roles_queries"][
            "check_superuser"]

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

        if len(check_superuser_query_output["result"]) > 0:
            if check_superuser_query_output["result"][0]["app_user_group"].lower() == "superuser":
                user_group = "superuser"
                write_access = 'True'
            else:

                get_user_group_query = app_config_dict["ctfo_services"]["user_roles_queries"][
                    "get_user_group"]

                get_user_group_query = self.replace_environment_details(app_config_dict,
                                                                               get_user_group_query)

                get_user_group_query = get_user_group_query.replace("$$scenario_id$$", scenario_id).\
                    replace("$$user_id$$", user_id)

                LOGGING.info("get_user_group_query : %s", get_user_group_query)


                get_user_group_query_output = self.execute_query(get_user_group_query, self.host, self.port,
                                                              self.username, self.password,
                                                              self.database_name)

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

            check_if_authorized_to_edit_screen_query = check_if_authorized_to_edit_screen_query.replace("$$scenario_id$$", scenario_id)

            LOGGING.info("check_if_authorized_to_edit_screen_query : %s", check_if_authorized_to_edit_screen_query)

            check_if_authorized_to_edit_screen_query_output = self.execute_query(check_if_authorized_to_edit_screen_query, self.host, self.port,
                                                             self.username, self.password,
                                                             self.database_name)

            LOGGING.info("check_if_authorized_to_edit_screen_query_output : %s", check_if_authorized_to_edit_screen_query_output)

            if check_if_authorized_to_edit_screen_query_output["status"].lower() != "success":
                raise Exception('Failed to execute check_if_authorized_to_edit_screen_query!')

            if int(check_if_authorized_to_edit_screen_query_output["result"][0]["count"]) > 1 and lock_status == False:
                LOGGING.info("edit_screen : %s", edit_screen)
                return edit_screen
            elif int(check_if_authorized_to_edit_screen_query_output["result"][0]["count"]) == 1:
                edit_lock_list_user_query = app_config_dict["ctfo_services"]["user_roles_queries"][
                    "edit_lock_list_user"]

                edit_lock_list_user_query = self.replace_environment_details(app_config_dict,
                                                                                            edit_lock_list_user_query)

                edit_lock_list_user_query = edit_lock_list_user_query.replace(
                    "$$scenario_id$$", scenario_id)

                LOGGING.info("edit_lock_list_user_query : %s", edit_lock_list_user_query)

                edit_lock_list_user_query_output = self.execute_query(
                    edit_lock_list_user_query, self.host, self.port,
                    self.username, self.password,
                    self.database_name)

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