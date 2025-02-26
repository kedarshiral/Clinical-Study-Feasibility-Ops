# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

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

# from datetime import datetime
import urllib.parse
from pytz import timezone
import ntpath
import traceback
import subprocess

# Getting the path of Utilities directory which are needed for rule engine application
service_directory_path = os.path.dirname(os.path.abspath(__file__))
utilities_dir_path = service_directory_path
# utilities_dir_path = os.path.abspath(os.path.join(service_directory_path, "../utilities/"))

sys.path.insert(0, utilities_dir_path)

# Get the custom logger instance for this utility


logger = logging.getLogger()
from DatabaseUtility import DatabaseUtility

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
ENVIRONMENT_CONFIG_FILE = None

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
      "s3_csv_data_path": "s3://amgen-edl-rdi-di-bkt/dev/Temp/21_nov",
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

    def import_csv(self, schema, curr_table, prev_table, location, s3_location, conn, index_name, file_type,
                   csv_download_flag, delimeter):
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

            if csv_download_flag == 'Y':
                command = "s3 cp {} {} --recursive --sse".format(s3_location, location)
                logging.info("download from s3 command - " + command)
                self.execute_aws_cli_command(command)

            # truncate prev table
            query = "truncate table {}.{}".format(schema, prev_table)
            output = self.database_utility_object.execute(conn=conn, query=query)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception(output[ERROR_KEY])

            # Importing csv into postgres
            # self.logging.info("download from s3 command - " + command)
            print("***************** starting import")

            #############################################
            result = subprocess.run(['hostname'], stdout=subprocess.PIPE)
            print("****************" + str(result.stdout))
            ###########################################3
            output = self.database_utility_object.postgres_csv_import(conn, schema, prev_table, location, delimeter)
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
                query = "REINDEX INDEX {}".format(index_name)
                output = self.database_utility_object.execute(conn=conn, query=query)
                if output[STATUS_KEY] == STATUS_FAILED:
                    raise Exception(output[ERROR_KEY])

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

    def view_refresh(self, conf, cycle_id):
        run_id = ""

        try:
            db_connection_details = conf.get("db_connection_details", None)
            if db_connection_details is None:
                raise Exception('db_connection_details not found in configuration....')

            db_host = db_connection_details.get("host", None)
            db_port = int(db_connection_details.get("port", None))
            db_username = db_connection_details.get("username", None)
            db_password = self.db_connection_details.get("password", None)
            # db_password = base64.b64decode(password_encoded.encode()).decode()
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
                        insert_query = "INSERT INTO s3_to_rds_data_load_logs values ('{cycle_id}','{run_id}','{process_type}','{step}','{status}','','','{start_timestamp}','{end_timestamp}');".format(
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
            db_password = self.db_connection_details.get("password", None)
            # db_password = base64.b64decode(password_encoded.encode()).decode()
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
                    file_type = action_conf["file_type"]
                    s3_data_path = action_conf["s3_csv_data_path"]
                    s3_parquet_data_path = action_conf["s3_parquet_data_path"]
                    file_location = action_conf["location"]
                    delimeter = action_conf["delimeter"]
                    if file_type == 'parquet':
                        from PySparkUtility import PySparkUtility
                        spark = PySparkUtility().get_spark_context('copy_parquet_postgres')
                        print("*************" + s3_data_path)
                        final_df = spark.read.format("parquet").option("header", "true").load(s3_parquet_data_path)
                        final_df.coalesce(1).write.option("header", "false") \
                            .option("sep", "|").option('quote', '"') \
                            .option('escape', '"').option("emptyValue", None).option("nullValue", None) \
                            .option("quoteAll", "false")\
                            .option("multiLine", "true").mode('overwrite').csv(s3_data_path)

                    query_sequenc_id = "select nextval('run_id_sequence')"
                    output_sequence = self.database_utility_object.execute(conn=conn, query=query_sequenc_id)
                    sequence_id = output_sequence[RESULT_KEY]
                    run_id = sequence_id[0]["nextval"]

                    location = action_conf["location"]
                    s3_location = action_conf["s3_csv_data_path"]
                    schema = action_conf["schema"]
                    file_type = action_conf["file_type"]
                    csv_download_flag = action_conf["csv_download_flag"]

                    curr_table = action_conf["curr_table"]
                    print(curr_table)
                    prev_table = action_conf["prev_table"]
                    index_name = action_conf["refresh_index_name"]
                    if index_name == "":
                        index_name = None
                    logging.info("S3 to rds load for %s ", curr_table)
                    start_timestamp = datetime.datetime.utcnow()
                    insert_query = "INSERT INTO s3_to_rds_data_load_logs values ('{cycle_id}','{run_id}','{process_type}','{step}','{status}','','','{start_timestamp}','{end_timestamp}');".format(
                        cycle_id=cycle_id, run_id=run_id, process_type="Table load", step=curr_table,
                        status="running", start_timestamp=start_timestamp, end_timestamp="")
                    print(insert_query)

                    print((self.database_utility_object.execute(conn=conn, query=insert_query)))

                    print((schema, curr_table, prev_table, location, s3_location))
                    return_status = self.import_csv(schema, curr_table, prev_table, location, s3_location,
                                                                  conn,
                                                                  index_name, file_type, csv_download_flag, delimeter)
                    end_timestamp = datetime.datetime.utcnow()
                    if return_status[STATUS_KEY] == STATUS_FAILED:
                        update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}' where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                            status=STATUS_FAILED, end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id)
                        self.database_utility_object.execute(conn=conn, query=update_query)
                        return return_status
                    record_count_query = "select count(*) from {schema}.{table_name}".format(schema = schema, table_name = curr_table)
                    record_count_query_output = self.database_utility_object.execute(conn=conn, query=record_count_query)
                    record_count = record_count_query_output[RESULT_KEY]
                    record_count = record_count[0]["count"]

                    update_query = "UPDATE s3_to_rds_data_load_logs set status = '{status}',end_timestamp = '{end_timestamp}', record_count = '{record_count}' , file_path = '{file_path}'  where cycle_id = '{cycle_id}' and run_id = '{run_id}' ;".format(
                        status='success', end_timestamp=end_timestamp, cycle_id=cycle_id, run_id=run_id,
                        record_count=record_count, file_path=s3_location)
                    self.database_utility_object.execute(conn=conn, query=update_query)



            elif action == "export":
                table_details = conf["import_table_details"]
                for table in list(table_details.keys()):
                    action_conf = table_details[table]
                    query = action_conf["query"]
                    location = action_conf["location"]
                    s3_location = action_conf["s3_csv_data_path"]
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
        self.db_password = self.db_connection_details.get("password", None)
        # self.db_password = base64.b64decode(password_encoded.encode()).decode()
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
        cycle_id = output_cycle_id[RESULT_KEY]
        cycle_id = cycle_id[0]["nextval"]
        csv_download_flag = conf.get("csv_download_flag", None)
        run_import_view_details = conf.get("run_import_view_details", None)

        return_status = self.csv_import(conf, cycle_id)
        if return_status[STATUS_KEY] == STATUS_FAILED:
            return return_status
        if (run_import_view_details == 'Y'):
            self.view_refresh(conf, cycle_id)
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
