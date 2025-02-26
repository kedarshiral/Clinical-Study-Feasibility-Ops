import os
import sys
import glob
import json
import base64
import ast
import getopt
import datetime
import logging
import boto3
import boto
import shutil
import traceback
import time
# from datetime import datetime
import urllib.parse
from pytz import timezone
import ntpath
import traceback

# Getting the path of Utilities directory which are needed for rule engine application
service_directory_path = os.path.dirname(os.path.abspath(__file__))
utilities_dir_path = service_directory_path
# utilities_dir_path = os.path.abspath(os.path.join(service_directory_path, "../utilities/"))

sys.path.insert(0, utilities_dir_path)

# Get the custom logger instance for this utility
from DILogSetup import get_logger

logging = get_logger(file_log_name="ImportExportUtility.log")

import CommonConstants
from DatabaseUtility import DatabaseUtility
from ConfigUtility import JsonConfigUtility
# from JsonConfigUtility import JsonConfigUtility

from MoveCopy import MoveCopy
import awscli.clidriver

# Module level constants
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"
EMPTY = ""
APP_CONFIG_PATH = os.path.abspath(os.path.join(service_directory_path, "../configs/import_export_config.json"))
ENVIRONMENT_CONFIG_FILE = os.path.abspath(os.path.join(service_directory_path, "../configs/environment_config.json"))

# Only for 33.17 machine
# APP_CONFIG_PATH = os.path.abspath(os.path.join(service_directory_path, "../configs/import_export_config.json"))
# ENVIRONMENT_CONFIG_FILE = os.path.abspath(os.path.join(service_directory_path, "../configs/environment_config.json"))

APP_CONFIG_KEY = "ci_ngii"

DB_HOST = 'db_host'
DB_USERNAME = 'db_username'
DB_PASSWORD = 'db_password'
DB_PORT = 'db_port'
DB_NAME = 'db_name'
DEFAULT_POSTGRESQL_PORT = 5432

import re


def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]+', '', text)


# The usage sting to be displayed to the user for the utility
USAGE_STRING = """
SYNOPSIS
    python ImportExportUtility.py -f/--conf_file_path <conf_file_path> -c/--conf <conf>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string

        Note: Either 'conf_file_path' or 'conf' should be provided.

    example:
        python ImportExportUtility.py -c '{
  "db_connection_details": {
    "host": "dirdidev.ckkp7bzktfbr.us-west-2.rds.amazonaws.com",
    "port": "5432",
    "username": "root",
    "password": "dirdipassword",
    "database_name": "dirdidev"
  },
  "action": "import",
  "import_table_details": {
    "table_1": {
      "location": "/appdata/env/..",
      "s3_location": "s3://amgen-edl-rdi-di-bkt/dev/Temp/21_nov",
      "schema": "public",
      "curr_table": "detailed_descriptions",
      "prev_table": "detailed_descriptions_prev"
    }

  }
}'

"""


class ImportExportUtility(object):
    def __init__(self):

        self.move_copy_object = MoveCopy()
        self.s3_client = boto3.client("s3")
        self.database_utility_object = DatabaseUtility()
        self.configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.connection = None

    def execute_aws_cli_command(self, command):
        """
        Purpose     :   This function executes the aws cli command
        Input       :   aws cli command
        Output      :   Na
        """
        try:
            cli_execution_status = awscli.clidriver.create_clidriver().main(command.split())
            if cli_execution_status != 0:
                raise Exception("Return status = " + str(cli_execution_status))
        except Exception:
            logging.error("Error while executing command - " + command + "\nError: " + str(e))
            raise e

    def export_csv(self, query, location, s3_location, s3_archive_location, conn):
        # get database connection
        return_status = {}
        try:
            if conn is None:
                raise Exception("Database connection is None...!!!")

            # clean csv export location
            files = glob.glob(location + '/*')
            for f in files:
                os.remove(f)

            # exporting csv from postgres
            output = self.database_utility_object.postgres_csv_export(query, location, conn)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])

            # backup current snapshot to archive
            archive_location = os.path.join(s3_archive_location, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
            job_output = self.move_copy_object.s3_move_copy(action="move", source_path_type="s3",
                                                            source_path=s3_location,
                                                            target_path=archive_location,
                                                            sse_flag=True)
            if job_output[STATUS_KEY] == STATUS_FAILED:
                raise Exception("Error while archiving s3 data.\n" + "Error: " + job_output[ERROR_KEY])

            # copy csv export to s3 location
            job_output = self.move_copy_object.s3_move_copy(action="copy", source_path_type="moveit",
                                                            source_path=location,
                                                            target_path=s3_location,
                                                            sse_flag=True)
            if job_output[STATUS_KEY] == STATUS_FAILED:
                raise Exception("Error while copying csv export to s3.\n" + "Error: " + job_output[ERROR_KEY])

            # return status
            return_status[STATUS_KEY] = STATUS_SUCCESS
            return_status[RESULT_KEY] = EMPTY
            logging.info("Finished Fetching Application configuration")
            return return_status

        except Exception:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            logging.error(error)
            return_status[STATUS_KEY] = STATUS_FAILED
            return_status[ERROR_KEY] = str(e)
            return return_status

    def import_csv(self, schema, curr_table, prev_table, location, s3_location, conn, refresh_view, index_name):
        # get database connection
        return_status = {}
        try:
            if conn is None:
                raise Exception("Database connection is None...!!!")

            # clean csv import location
            files = glob.glob(location + '/*')
            for f in files:
                os.remove(f)

            # @ToDo - download from s3
            command = "s3 cp {} {} --recursive --sse".format(s3_location, location)
            logging.info("download from s3 command - " + command)
            self.execute_aws_cli_command(command)

            #         if curr_table in ["f_rpt_site_study_details", "f_rpt_country", "f_rpt_drug_details",
            #                           "f_rpt_investigator_site_study_details", "f_rpt_patient_details"]:
            #             print("--------Inside report ingestion----------", curr_table)
            #             output = self.database_utility_object.postgres_csv_import(conn, schema, curr_table, location)
            #             if output[STATUS_KEY] == STATUS_FAILED:
            #                 raise Exception(output[ERROR_KEY])
            # truncate prev table
            query = "truncate table {}.{}".format(schema, prev_table)
            output = self.database_utility_object.execute(conn=conn, query=query)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])

            # Importing csv into postgres
            # self.logging.info("download from s3 command - " + command)
            output = self.database_utility_object.postgres_csv_import(conn, schema, prev_table, location)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])

            # @ToDo - ALTER TABLE CURR RENAME TO BKP;
            query = "ALTER TABLE {}.{} RENAME TO {}".format(schema, curr_table, curr_table + "_bkp")
            output = self.database_utility_object.execute(conn=conn, query=query)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])

            # @ToDo - ALTER TABLE PREV RENAME TO CURR;
            query = "ALTER TABLE {}.{} RENAME TO {}".format(schema, prev_table, curr_table)
            output = self.database_utility_object.execute(conn=conn, query=query)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])

            # @ToDo - ALTER TABLE BKP RENAME TO PREV;
            query = "ALTER TABLE {}.{} RENAME TO {}".format(schema, curr_table + "_bkp", prev_table)
            output = self.database_utility_object.execute(conn=conn, query=query)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])

            # @ToDo - Rebuild index
            if index_name is not None:
                query = "CREATE INDEX  IF NOT EXISTS nct_id_index ON pub_trial_details_view (nct_id);REINDEX INDEX {}".format(
                    index_name)
                output = self.database_utility_object.execute(conn=conn, query=query)
                if output[STATUS_KEY] == STATUS_FAILED:
                    raise Exception(output[ERROR_KEY])

            # # @ToDo - REFRESH VIEW;
            # if refresh_view is not None:
            #     query = "DROP MATERIALIZED VIEW {schema}.{view_name};CREATE  MATERIALIZED VIEW {schema}.{view_name} as select * from {schema}.{curr_table} ; REFRESH MATERIALIZED VIEW {schema}.{view_name}; ".format(schema=schema, view_name=refresh_view,curr_table=curr_table)
            #     print(query)
            #     output = self.database_utility_object.execute(conn=conn, query=query)
            #     if output[STATUS_KEY] == STATUS_FAILED:
            #         raise Exception(output[ERROR_KEY])
            #
            #     query = "ANALYZE {schema}.{view_name}; ".format(schema=schema, view_name=refresh_view)
            #     output = self.database_utility_object.execute(conn=conn,query=query)
            #     if output[STATUS_KEY] == STATUS_FAILED:
            #         raise Exception(output[ERROR_KEY])

            # return status
            return_status[STATUS_KEY] = STATUS_SUCCESS
            return_status[RESULT_KEY] = EMPTY
            logging.info("Finished Fetching Application configuration")
            return return_status

        except Exception:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            logging.error(error)
            return_status[STATUS_KEY] = STATUS_FAILED
            return_status[ERROR_KEY] = str(e)
            return return_status

    def import_images(self, config, cycle_id):

        action = config["action"]

        run_id = ""

        try:
            if action == "import":
                image_details = config["import_images"]

                local_path_dir = image_details["location"]
                # clean images export location
                shutil.rmtree(local_path_dir + "/")
                logging.info("deleting image directory contents")
                os.mkdir(local_path_dir)

                s3_path = image_details["s3_location"]
                logging.info("deleting image directory contents")

                query_sequenc_id = "select nextval('run_id_sequence')"
                output_sequence = self.database_utility_object.execute(conn=self.conn, query=query_sequenc_id)
                sequence_id = output_sequence[RESULT_KEY]
                run_id = sequence_id[0]["nextval"]

                start_timestamp = datetime.datetime.utcnow()
                insert_query = "INSERT INTO s3_to_rds_data_load_logs values ('{cycle_id}','{run_id}','{process_type}','{step}','{status}','{start_timestamp}','{end_timestamp}');".format(
                    cycle_id=cycle_id, run_id=run_id, process_type="Image Download", step="Image Download",
                    status="running", start_timestamp=start_timestamp, end_timestamp="")
                self.database_utility_object.execute(conn=self.conn, query=insert_query)

                parsed_url = urllib.parse.urlparse(s3_path)
                # Get bucket name and path from s3 location
                bucket_name = parsed_url.netloc
                path = parsed_url.path[1:]
                s3_connection = boto.connect_s3()
                # Check bucket for parquet files
                bucket = s3_connection.get_bucket(bucket_name)
                file_list = bucket.list(prefix=path)
                print(file_list)
                file_path = None
                pmc_list = []

                for file_item in file_list:
                    print(file_item)
                    # Use first parquet file in the folder to get the schema
                    pmc_name = file_item.name

                    pmc = pmc_name[0:pmc_name.rindex('/')]
                    pmc = pmc[0:pmc.rindex('/')]

                    pmc = pmc[pmc.rindex('/'):]
                    pmc = pmc[1:]

                    if pmc not in pmc_list:
                        pmc_list.append(pmc)

                for id in pmc_list:
                    command = "s3 cp {} {} --recursive --sse".format(s3_path + "/" + id + "/Images",
                                                                     local_path_dir + "/" + id)
                    logging.info("download from s3 command - " + command)
                    self.execute_aws_cli_command(command)
                    logging.info("done copying of images")

                status_message = "completing download_s3_object images"
                logging.info(status_message)

                end_timestamp = datetime.datetime.utcnow()
                update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                    status='success', end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
                self.database_utility_object.execute(conn=self.conn, query=update_query)

            else:
                logging.info("export action is not suuported for images ")

        except Exception as e:
            print((str(traceback.format_exc())))
            status = STATUS_FAILED
            end_timestamp = datetime.datetime.utcnow()
            update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}';".format(
                status=status, end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
            self.database_utility_object.execute(conn=self.conn, query=update_query)

            sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError while parsing configuration."
                                                                               " ERROR: " + str(e)}))
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            logging.error(error)

    def view_refresh(self, conf, cycle_id):
        run_id = ""

        try:
            db_connection_details = conf.get("db_connection_details", None)
            if db_connection_details is None:
                raise Exception('db_connection_details not found in configuration....')

            db_host = db_connection_details.get("host", None)
            db_port = int(db_connection_details.get("port", None))
            db_username = db_connection_details.get("username", None)
            password_encoded = self.db_connection_details.get("password", None)
            db_password = base64.b64decode(password_encoded.encode()).decode()
            db_name = db_connection_details.get("database_name", None)

            connection = DatabaseUtility().create_and_get_connection(host=db_host, port=db_port,
                                                                     username=db_username,
                                                                     password=db_password, database_name=db_name)
            logging.info("database connected")
            if connection[RESULT_KEY] == STATUS_FAILED:
                raise Exception(connection[ERROR_KEY])
            conn = connection[RESULT_KEY]

            if (conf.get("run_import_view_details") == 'Y'):
                status = STATUS_SUCCESS

                view_details = conf["import_view_details"]
                view_names = view_details.get("view_names", None)
                schema = view_details["schema"]
                view_definitions = view_details["view_definitions"]
                for view in view_names:

                    query_sequenc_id = "select nextval('run_id_sequence')"
                    output_sequence = self.database_utility_object.execute(conn=conn, query=query_sequenc_id)
                    sequence_id = output_sequence[RESULT_KEY]
                    run_id = sequence_id[0]["nextval"]

                    # @ToDo - REFRESH VIEW;
                    if view is not None:

                        start_timestamp = datetime.datetime.utcnow()
                        insert_query = "INSERT INTO s3_to_rds_data_load_logs values ('{cycle_id}','{run_id}','{process_type}','{step}','{status}','{start_timestamp}','{end_timestamp}');".format(
                            cycle_id=cycle_id, run_id=run_id, process_type="refresh view ", step=view,
                            status="running", start_timestamp=start_timestamp, end_timestamp="")
                        logging.info(insert_query)

                        self.database_utility_object.execute(conn=conn, query=insert_query)
                        view_query = view_definitions[view]
                        query = "DROP MATERIALIZED VIEW {schema}.{view_name};CREATE  MATERIALIZED VIEW {schema}.{view_name} as {view_query} ; REFRESH MATERIALIZED VIEW {schema}.{view_name}; ".format(
                            schema=schema, view_name=view, view_query=view_query)
                        logging.info("query %s ", query)
                        output = self.database_utility_object.execute(conn=conn, query=query)

                        end_timestamp = datetime.datetime.utcnow()
                        if output[STATUS_KEY] == STATUS_FAILED:
                            update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                                status=STATUS_FAILED, end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
                            self.database_utility_object.execute(conn=conn, query=update_query)
                            return output[ERROR_KEY]

                        query = "ANALYZE {schema}.{view_name}; ".format(schema=schema, view_name=view)
                        output = self.database_utility_object.execute(conn=conn, query=query)

                        end_timestamp = datetime.datetime.utcnow()
                        if output[STATUS_KEY] == STATUS_FAILED:
                            update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                                status=STATUS_FAILED, end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
                            self.database_utility_object.execute(conn=conn, query=update_query)
                            return output[ERROR_KEY]

                        update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                            status='success', end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
                    self.database_utility_object.execute(conn=conn, query=update_query)




        except Exception as e:
            logging.info(str(traceback.format_exc()))
            status = STATUS_FAILED
            end_timestamp = datetime.datetime.utcnow()
            update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                status=status, end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
            self.database_utility_object.execute(conn=conn, query=update_query)

            sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError while parsing configuration."
                                                                               " ERROR: " + str(e)}))

    def csv_import(self, conf, cycle_id):

        run_id = ""

        try:
            db_connection_details = conf.get("db_connection_details", None)
            if db_connection_details is None:
                raise Exception('db_connection_details not found in configuration....')

            db_host = db_connection_details.get("host", None)
            db_port = int(db_connection_details.get("port", None))
            db_username = db_connection_details.get("username", None)
            secret_password = self.get_secret(self.db_connection_details.get("password", None),
                                              self.configuration.get_configuration(
                                                  [CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
            db_password = secret_password['password']
            db_name = db_connection_details.get("database_name", None)

            connection = DatabaseUtility().create_and_get_connection(host=db_host, port=db_port,
                                                                     username=db_username,
                                                                     password=db_password, database_name=db_name)

            logging.info("database connected")
            if connection[RESULT_KEY] == STATUS_FAILED:
                raise Exception(connection[ERROR_KEY])
            action = conf["action"]
            conn = connection[RESULT_KEY]
            if action == "import":
                table_details = conf["import_table_details"]

                for table in list(table_details.keys()):
                    status = STATUS_SUCCESS

                    action_conf = table_details[table]
                    if (action_conf["flag_table"] != 'Y'):
                        continue

                    query_sequenc_id = "select nextval('run_id_sequence')"
                    output_sequence = self.database_utility_object.execute(conn=conn, query=query_sequenc_id)
                    sequence_id = output_sequence[RESULT_KEY]
                    run_id = sequence_id[0]["nextval"]

                    location = action_conf["location"]
                    s3_location = action_conf["s3_location"]
                    schema = action_conf["schema"]
                    curr_table = action_conf["curr_table"]
                    print(curr_table)
                    prev_table = action_conf["prev_table"]
                    refresh_view = action_conf["refresh_view"]
                    refresh_view_query = action_conf["refresh_view_query"]
                    index_name = action_conf["refresh_index_name"]
                    if index_name == "":
                        index_name = None
                    logging.info("S3 to rds load for %s ", curr_table)
                    start_timestamp = datetime.datetime.utcnow()
                    insert_query = "INSERT INTO s3_to_rds_data_load_logs values ('{cycle_id}','{run_id}','{process_type}','{step}','{status}','{start_timestamp}','{end_timestamp}');".format(
                        cycle_id=cycle_id, run_id=run_id, process_type="Table load", step=curr_table,
                        status="running", start_timestamp=start_timestamp, end_timestamp="")
                    print(insert_query)

                    print((self.database_utility_object.execute(conn=conn, query=insert_query)))

                    print((schema, curr_table, prev_table, location, s3_location))
                    return_status = self.import_csv(schema, curr_table, prev_table, location, s3_location, conn,
                                                    refresh_view, index_name)
                    end_timestamp = datetime.datetime.utcnow()
                    if return_status[STATUS_KEY] == STATUS_FAILED:
                        update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                            status=STATUS_FAILED, end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
                        self.database_utility_object.execute(conn=conn, query=update_query)
                        return return_status

                    for view in refresh_view:
                        # @ToDo - REFRESH VIEW;
                        if view is not None:

                            view_query = refresh_view_query[view]
                            query = "DROP MATERIALIZED VIEW {schema}.{view_name};CREATE  MATERIALIZED VIEW {schema}.{view_name} as {view_query} ; REFRESH MATERIALIZED VIEW {schema}.{view_name}; ".format(
                                schema=schema, view_name=view, view_query=view_query)
                            print(query)
                            output = self.database_utility_object.execute(conn=conn, query=query)
                            if output[STATUS_KEY] == STATUS_FAILED:
                                raise Exception(output[ERROR_KEY])

                            query = "ANALYZE {schema}.{view_name}; ".format(schema=schema, view_name=view)
                            output = self.database_utility_object.execute(conn=conn, query=query)
                            if output[STATUS_KEY] == STATUS_FAILED:
                                raise Exception(output[ERROR_KEY])

                    update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                        status='success', end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
                    self.database_utility_object.execute(conn=conn, query=update_query)
            elif action == "export":
                table_details = conf["import_table_details"]
                for table in list(table_details.keys()):
                    action_conf = table_details[table]
                    query = action_conf["query"]
                    location = action_conf["location"]
                    s3_location = action_conf["s3_location"]
                    s3_archive_location = action_conf["s3_archive_location"]
                    self.export_csv(query, location, s3_location, s3_archive_location, conn)
            return return_status

        except Exception as e:
            print((str(traceback.format_exc())))
            status = STATUS_FAILED
            end_timestamp = datetime.datetime.utcnow()
            update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                status=status, end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
            self.database_utility_object.execute(conn=conn, query=update_query)

            sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError while parsing configuration."
                                                                               " ERROR: " + str(e)}))

    def get_secret(self, secret_name, region_name):
        """
        Purpose: This fetches the secret details and decrypts them.
        Input: secret_name is the secret to be retrieved from Secrets Manager.
               region_name is the region which the secret is stored.
        Output: JSON with decoded credentials from Secrets Manager.
        """
        MAX_RETRY_COUNT = 5

        while True:
            try:
                print("++ Fetching secrets from secrets manager ++")
                # Create a Secrets Manager client
                if CommonConstants.SECRET_MANAGER_FLAG == 'Y':
                    session = boto3.session.Session()
                    client = session.client(
                        service_name='secretsmanager',
                        region_name=region_name
                    )
                    print("Fetching the details for the secret name %s")
                    get_secret_value_response = client.get_secret_value(
                        SecretId=secret_name
                    )
                    print("Fetched the Encrypted Secret from Secrets Manager for %s", secret_name)

                    if 'SecretString' in get_secret_value_response:
                        secret = get_secret_value_response['SecretString']
                        print("Decrypted the Secret")
                    else:
                        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                        print("Decrypted the Secret")

                else:
                    password = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                                        , "mysql_password"])

                    password = base64.b64decode(password).decode()
                    secret = json.dumps({"password": password})

                return json.loads(secret)

            except Exception as error:
                MAX_RETRY_COUNT -= 1
                if MAX_RETRY_COUNT != 0:
                    time.sleep(5)
                    continue
                raise Exception("Unable to fetch secrets.")

    def main(self, conf, conf_file_path, env_conf_file_path):
        # Check for all the mandatory arguments
        if conf_file_path is None and conf is None and env_conf_file_path is None:
            sys.stderr.write(json.dumps({
                STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: Either JSON configuration file path "
                                                      "or JSON configuration string should be provided\n"}))
            usage(1)

            # Parse the configuration
        if conf_file_path:
            with open(conf_file_path) as conf_file:
                conf = json.load(conf_file)
        else:
            conf = json.loads(conf)

        if env_conf_file_path:
            with open(env_conf_file_path) as env_conf_file:
                env_conf = json.load(env_conf_file)
        else:
            env_conf = json.loads(env_conf)

        application_config_str = json.dumps(conf)

        for key in env_conf:
            application_config_str = application_config_str.replace("$$" + key, env_conf[key])
            conf = ast.literal_eval(application_config_str)

        logging.info(json.dumps(conf))

        self.db_connection_details = conf.get("db_connection_details", None)
        if self.db_connection_details is None:
            raise Exception('db_connection_details not found in configuration....')

        self.db_host = self.db_connection_details.get("host", None)
        self.db_port = int(self.db_connection_details.get("port", None))
        self.db_username = self.db_connection_details.get("username", None)
        secret_password = self.get_secret(self.db_connection_details.get("password", None),
                                          self.configuration.get_configuration(
                                              [CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
        self.db_password = secret_password['password']
        print((self.db_password))
        self.db_name = self.db_connection_details.get("database_name", None)

        self.connection = DatabaseUtility().create_and_get_connection(host=self.db_host, port=self.db_port,
                                                                      username=self.db_username,
                                                                      password=self.db_password,
                                                                      database_name=self.db_name)

        logging.info("database connected")
        if self.connection[RESULT_KEY] == STATUS_FAILED:
            raise Exception(self.connection[ERROR_KEY])
        action = conf["action"]
        self.conn = self.connection[RESULT_KEY]

        query_cycle_id = "select nextval('cycle_id_sequence')"
        output_cycle_id = self.database_utility_object.execute(conn=self.conn, query=query_cycle_id)
        print("Hello", output_cycle_id)
        logging.info(RESULT_KEY)
        cycle_id = output_cycle_id[RESULT_KEY]
        cycle_id = cycle_id[0]["nextval"]
        csv_downlaod_flag = conf.get("csv_downlaod_flag", None)
        image_download_flag = conf.get("image_download_flag", None)
        run_import_view_details = conf.get("run_import_view_details", None)

        if (csv_downlaod_flag == "Y"):
            return_status = self.csv_import(conf, cycle_id)
            if return_status[STATUS_KEY] == STATUS_FAILED:
                return return_status
        if (run_import_view_details == 'Y'):
            self.view_refresh(conf, cycle_id)

        if (image_download_flag == "Y"):
            self.import_images(conf, cycle_id)
            return "Images download Success"

        return "Success"


# Print the usage for the Database Utility
def usage(status=1):
    sys.stdout.write(USAGE_STRING)
    sys.exit(status)


if __name__ == "__main__":
    # Setup basic logging info

    # logging.basicConfig(level=logging.info)
    logging.info("Basic logging info started")

    conf_file_path = None
    conf = None
    opts = None

    try:
        if (len(sys.argv) < 2):
            usage(1)
            sys.exit(2)
        opts, args = getopt.getopt(
            sys.argv[1:], "f:c:h",
            ["conf_file_path=", "conf=", "help"])
        logging.info("checking arguments")
    except Exception as e:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: " + str(e)}))
        usage(1)

    # Parse the input arguments
    for option, arg in opts:
        if option in ("-h", "--help"):
            usage(1)
        elif option in ("-f", "--conf_file_path"):
            conf_file_path = arg
        elif option in ("-c", "--conf"):
            conf = arg

    obj = ImportExportUtility()
    return_status = obj.main(conf, conf_file_path, ENVIRONMENT_CONFIG_FILE)
    logging.info(return_status)
