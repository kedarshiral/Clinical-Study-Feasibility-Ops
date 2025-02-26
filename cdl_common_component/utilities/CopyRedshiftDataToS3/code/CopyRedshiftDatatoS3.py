# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
# -*- coding: utf-8 -*-
"""
Doc_Type        :   Redshift Data Copy Utility
Tech Description:   This is a helper utility for loading redshift data to s3 
Pre_requisites  :   application config should be updated
Inputs          :    NA
Outputs          :  data uploaded at s3 location 
Command to execute : python3 CopyRedshiftDatatoS3.py
Config_file     :

"""

import base64
import os
import subprocess
import sys
import datetime
import json
import traceback

__author__ = 'Himanshu Khatri'


# Getting the path of Utilities directory which are needed for rule engine application
SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SERVICE_DIRECTORY_PATH, "../"))
from utilities.LogSetup import get_logger
from utilities.DatabaseUtility import DatabaseUtility

APP_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH,
                                               "../configs/application_config.json"))


#Utility Level constants
STATUS = 'status'
ERROR = 'error'
RESULT = 'result'
STATUS_SUCCESS = 'success'
STATUS_FAILED = 'failed'


class RedshiftUnloadUtility():

    def __init__(self):
        self.logging = get_logger()
        self.config = self.get_application_config_json()
        self.host = self.port = self.truncate_load_s3_flag = None
        self.password = self.username = self.cycle_id = None
        self.database_name = self.iam_role = None
        self.initialize_variable(self.config)
        self.database_utility_object = DatabaseUtility()

    def get_application_config_json(self):
        """
        Purpose     :   This function is used to fetch the application configuration
        Input       :   NA
        Output      :   Application configuration dict
        """

        status_message = "Fetching Application configuration"
        self.logging.info(status_message)

        json_file = open(APP_CONFIG_PATH, 'r').read()
        application_config = json.loads(json_file)
        self.logging.info("Finished Fetching Application configuration")
        return application_config

    def initialize_variable(self, app_config_dict):

        """
         Purpose     :   This function will be used to initialize variables for database
         Input       :   config dictionary
         Output      :   N/A
         """
        self.logging.info("Initializing Variable function started")
        self.db_connection_details = app_config_dict["db_connection_details"]
        if self.db_connection_details is None:
            raise Exception('db_connection_details key not present')

        self.host = self.db_connection_details.get("host", None)
        self.port = int(self.db_connection_details.get("port", None))
        self.username = self.db_connection_details.get("username", None)
        secret_password = self.db_connection_details.get("password", None)
        self.password = base64.b64decode(secret_password).decode('utf-8')
        self.database_name = self.db_connection_details.get("database_name", None)
        self.iam_role = self.db_connection_details.get("iam_role",None)
        self.logging.info("Variables initialized successfully")

    def check_unload_type(self):
        """
        Purpose     :   This function is check the unload type user has selected
        Input       :   NA
        Output      :
        """

        return_status = {}
        try:
            status_message = "Started function to check the unload type "
            self.logging.info(status_message)
            unload_type = []
            if self.config['unload_schemas']['flag']:
                self.logging.info("Unload for schema Flag has been set to true")
                unload_type.append('schema')
            if self.config['unload_tables']['flag']:
                self.logging.info("Unload for table Flag has been set to true")
                unload_type.append('table')
            if self.config['query_based_unload']['flag']:
                self.logging.info("Unload using query Flag has been set to true")
                unload_type.append('query')
            status_message = "Ending funcation to check the unload type "
            self.logging.info(status_message)
            return_status[STATUS] = STATUS_SUCCESS
            return_status['unload_type'] = unload_type
            return return_status
        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status
        
    def execute_shell_command(self, command):
        """
                Purpose     :   This function is to execute a shell command
                Input       :   command
                Output      :   Status
        """
        return_status = {}
        try:
            status_message = "Started executing shell command " + command
            self.logging.debug(status_message)
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            if standard_output:
                standard_output_lines = standard_output.splitlines()
                for line in standard_output_lines:
                    self.logging.debug(line)
            if standard_error:
                standard_error_lines = standard_error.splitlines()
                for line in standard_error_lines:
                    self.logging.debug(line)
            if command_output.returncode == 0:
                return_status[STATUS] = STATUS_SUCCESS
                return return_status
            else:
                return_status[STATUS] = STATUS_FAILED
                return return_status
        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def truncate_s3_data(self,table_name):
        """
                Purpose     :   This function is used to truncate data from s3  for a table
                Input       :   table name with schema name
                Output      :   Status
        """
        return_status = {}
        try:
            status_message = f"Started function to truncate data for table - {table_name}"
            self.logging.info(status_message)
            s3_base_path = self.config['s3_base_path']
            s3_path = os.path.join(s3_base_path,
                                   table_name.split(".")[0],
                                   table_name.split(".")[1]
                                   )
            aws_rm_cmd = "aws s3 rm " + s3_path + " --recursive"
            execute_cmd_output = self.execute_shell_command(aws_rm_cmd)
            if execute_cmd_output[STATUS] == STATUS_FAILED:
                self.logging.debug(f"Execute aws rm cmd failed for  - {table_name}")
                raise Exception(execute_cmd_output[ERROR])
            status_message = f"Successfully ended function to truncate s3_data for table - {table_name}"
            self.logging.info(status_message)
            return_status[STATUS] = STATUS_SUCCESS
            return return_status

        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def unload_schema(self):
        """
                Purpose     :   This function is to unload a list of schema
                Input       :   NA
                Output      :   Status
        """
        return_status = {}
        try:
            status_message = "Started function to unload complete schema"
            self.logging.info(status_message)
            s3_base_path = self.config['s3_base_path']

            for schema in self.config['unload_schemas']['schema_name']:
                self.logging.info(f"Starting unload for schema - {schema}")
                table_output = self.get_table_details_from_rds(schema)
                if table_output[STATUS] == STATUS_FAILED:
                    self.logging.debug(f"Getting table details failed - {table_output[ERROR]}")
                    raise Exception(table_output[ERROR])
                table_details_list = []
                for tables in table_output[RESULT]:
                    table_dict = dict()
                    table_dict['partition_string'] = ""
                    table_dict['table_name'] = schema + "." + tables['table_name']
                    if self.truncate_load_s3_flag:
                        self.truncate_s3_data(table_dict['table_name'])
                        table_dict['s3_path'] = os.path.join(s3_base_path,
                                                             table_dict['table_name'].split(".")[0],
                                                             table_dict['table_name'].split(".")[1]
                                                             )
                    else:
                        table_dict['s3_path'] = os.path.join(s3_base_path,
                                                             table_dict['table_name'].split(".")[0],
                                                             table_dict['table_name'].split(".")[1],
                                                             self.cycle_id
                                                             )
                    table_details_list.append(table_dict)
                unload_table_output = self.unload_table(table_details_list)
                if unload_table_output[STATUS] == STATUS_FAILED:
                    self.logging.debug(f"unloading table for {schema} failed - {unload_table_output[ERROR]}")
                    raise Exception(unload_table_output[ERROR])
                self.logging.info(f"Ending unload for schema - {schema}")

            status_message = "Successfully ended function to unload complete schema"
            self.logging.info(status_message)
            return_status[STATUS] = STATUS_SUCCESS
            return return_status

        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def unload_table(self, table_details_list=None):
        """
                Purpose     :   This function is to unload a list of table
                                with schema
                Input       :   ['schema_name.table_name']
                Output      :   Status
        """
        return_status = {}
        try:
            status_message = "Started funcation to unload tables"
            self.logging.info(status_message)
            s3_base_path = self.config['s3_base_path']

            if table_details_list:
                self.logging.info("Table list details are provided")
            else:
                self.logging.info("Table list details are not provided hence creating ")
                table_details_output = self.get_table_details(self.config['unload_tables']['table_list'],
                                                              s3_base_path)

                if table_details_output[STATUS] == STATUS_FAILED:
                    self.logging.debug(f"getting table details failed - {table_details_output[ERROR]}")
                    raise Exception(table_details_output[ERROR])
                table_details_list = table_details_output[RESULT]

            for table_details in table_details_list:
                self.logging.info(f"Starting unload for table - {table_details['table_name']}")
                unload_output = self.unload_data_to_s3(table_details)
                if unload_output[STATUS] == STATUS_FAILED:
                    self.logging.debug(f"unlaoding the table {table_details['table_name']} failed - {unload_output[ERROR]}")
                    raise Exception(unload_output[ERROR])
                self.logging.info(f"Ending function to unload for table - {table_details['table_name']}")

            status_message = "Successfully ended function to unloading tables"
            self.logging.info(status_message)
            return_status[STATUS] = STATUS_SUCCESS
            return return_status

        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def unload_data_to_s3(self, table_details=None):
        """
                Purpose     :   This function is to unload data using unload command
                Input       :   table details  - name with schema , partitions , s3 path etc
                Output      :   Status
        """
        return_status = {}
        try:
            status_message = "Started function to unload data to s3"
            self.logging.info(status_message)
            unload_query = None
            unload_command = self.config['unload_command']
            if not table_details.get('unload_query'):
                unload_query = self.config['unload_template_query']
            else:
                unload_query = table_details['unload_query']

            unload_query = unload_query.replace("$$table_name$$",table_details['table_name'])
            unload_command = unload_command.replace('$$unload_query$$',unload_query).\
                replace('$$s3_path$$',table_details['s3_path'] + "/").\
                replace('$$iam_role$$',self.iam_role).\
                replace("$$format_string$$",self.config['format_string']).\
                replace("$$partition_string$$",table_details['partition_string'])

            self.logging.info(f"Unload Command Created is - {unload_command}")

            unload_output = self.execute_query(unload_command,
                                               self.host,
                                               self.port,
                                               self.username,
                                               self.password,
                                               self.database_name)

            if unload_output[STATUS] == STATUS_FAILED:
                self.logging.debug(f"Unload command run failed for "
                                   f"table - {table_details['table_name']} "
                                   f"because - {unload_output[RESULT]}")

            status_message = "Successfully ended function to unloading data to s3"
            self.logging.info(status_message)
            return_status[STATUS] = STATUS_SUCCESS
            return return_status

        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def execute_query(self, query, host, port, username, password, database_name):
        """
        Purpose   :   Executes the query on Redshift using Postgre engine
        Input     :   Query to execute
        Output    :   result in dictionary format
        """
        return_status = {}
        try:
            self.logging.info("Executing Query")
            output = self.database_utility_object.execute(query=query, host=host, username=username,
                                                          password=password,
                                                          database_name=database_name,
                                                          port=port
                                                          )

            if output[STATUS] == STATUS_FAILED:
                self.logging.error("Failed because - %s", str(output[ERROR]))
                raise Exception(str(output[ERROR]))
            self.logging.info("Query execution finished")
            return output
        except Exception as e:
            self.logging.error("Failed because - %s", str(e))
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def get_table_details_from_rds(self, schema):
        """
                Purpose     :   This function is to get list of table from RDS for a schema
                Input       :   Schema Name
                Output      :   list of tables
        """
        return_status = {}
        try:
            status_message = "Started function to get tables from RDS"
            self.logging.info(status_message)
            get_all_tables_query = self.config["get_table_names_query"]
            get_all_tables_query = get_all_tables_query.replace("$$schema_name$$", schema)
            self.logging.info(f"Query for getting tables for schema {schema} - {get_all_tables_query}")
            query_output = self.execute_query(get_all_tables_query, self.host,
                               self.port, self.username, self.password,
                               self.database_name)
            if query_output[STATUS] == STATUS_FAILED:
                self.logging.debug(f"Getting table name Failed - {query_output[RESULT]}")
            table_details_list = query_output[RESULT]
            self.logging.info(table_details_list)
            status_message = "Successfully ended function to get table details"
            self.logging.info(status_message)
            return_status[STATUS] = STATUS_SUCCESS
            return_status[RESULT] = table_details_list
            return return_status

        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def get_table_details(self, table_details_list=None, s3_base_path=None):
        """
                Purpose     :   This function is to get table details to be used for unload later
                Input       :   list of tables and s3 base path
                Output      :   list of tables
        """
        return_status = {}
        try:
            status_message = "Started function to get table details"
            self.logging.info(status_message)
            for table_details in table_details_list:
                if self.truncate_load_s3_flag:
                    self.truncate_s3_data(table_details['table_name'])
                    table_details['s3_path'] = os.path.join(s3_base_path,
                                                            table_details['table_name'].split(".")[0],
                                                            table_details['table_name'].split(".")[1]
                                                            )
                else:
                    table_details['s3_path'] = os.path.join(s3_base_path,
                                                            table_details['table_name'].split(".")[0],
                                                            table_details['table_name'].split(".")[1],
                                                            self.cycle_id
                                                            )
                if not table_details.get('table_partition_columns'):
                    table_details['partition_string'] = ""
                else:
                    partitions_list = table_details.get('table_partition_columns')
                    if partitions_list:
                        table_details['partition_string'] = "PARTITION BY (" + ",".join(partitions_list) + ")"
                    else:
                        table_details['partition_string'] = ""

            status_message = "Successfully ended function to get table details"
            self.logging.info(status_message)
            return_status[STATUS] = STATUS_SUCCESS
            return_status[RESULT] = table_details_list
            return return_status

        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status

    def main(self):
        return_status = {}
        try:
            self.logging.info("Starting Redshift to s3 copy Utility ")
            self.logging.info("checking for type of data load")
            unload_type_output = self.check_unload_type()
            s3_base_path = self.config['s3_base_path']
            self.cycle_id = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))
            self.truncate_load_s3_flag = self.config['truncate_load_s3_flag']
            if unload_type_output[STATUS] == STATUS_FAILED:
                self.logging.debug(f"Unload type check Failed due to - {unload_type_output[ERROR]}")
                raise Exception(unload_type_output[ERROR])
            unload_type_list = unload_type_output['unload_type']

            for unload_type in unload_type_list:
                unload_status = {}
                if unload_type == 'schema':
                    unload_status = self.unload_schema()
                if unload_type == 'table':
                    unload_status = self.unload_table()
                if unload_type == 'query':
                    table_details_list = self.config['query_based_unload']['table_query_map']
                    for table_details in table_details_list:
                        partitions_list = table_details.get('table_partition_columns')
                        if self.truncate_load_s3_flag:
                            self.truncate_s3_data(table_details['table_name'])
                            table_details['s3_path'] = os.path.join(s3_base_path,
                                                                    table_details['table_name'].split(".")[0],
                                                                    table_details['table_name'].split(".")[1]
                                                                    )
                        else:
                            table_details['s3_path'] = os.path.join(s3_base_path,
                                                                    table_details['table_name'].split(".")[0],
                                                                    table_details['table_name'].split(".")[1],
                                                                    self.cycle_id
                                                                    )
                        if partitions_list:
                            table_details['partition_string'] = "PARTITION BY (" + ",".join(partitions_list) + ")"
                        else:
                            table_details['partition_string'] = ""
                        unload_status = self.unload_data_to_s3(table_details)
                if unload_status[STATUS] == STATUS_FAILED:
                    self.logging.debug(f"Unload failed due to - {unload_status[ERROR]}")
                    raise Exception(unload_status[ERROR])

            return_status[STATUS] = STATUS_SUCCESS
            return return_status
        except Exception as e:
            error = ' ERROR MESSAGE: ' + str(e) + str(traceback.format_exc())
            self.logging.error(error)
            return_status[STATUS] = STATUS_FAILED
            return_status[ERROR] = str(e)
            return return_status


if __name__ == '__main__':
    UnloadObject = RedshiftUnloadUtility()
    UnloadObject.main()
