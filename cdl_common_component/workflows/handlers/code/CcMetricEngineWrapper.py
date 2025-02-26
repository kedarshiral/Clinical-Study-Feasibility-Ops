
#!/usr/bin/python
# -*- coding: utf-8 -*
__author__ = "ZS Associates"
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_cc/browse ) & all static servers.


"""
Doc_Type            : Metric Calculation Engine Wrapper
Tech Description    : This Wrapper is used for collecting all required parameters for MCE Engine and do
                      task like - validate input, log status, launch cluster if required etc
Pre_requisites      : MCEConstants, LogSetup, ExecutionContext
Inputs              : Process ID, Cluster ID(optional), Cycle ID(optional)
Outputs             : execution status(Success/Failed)
Config_file         : environment_params.json
Last modified by    : Sarun Natarajan
"""

import sys
import json
import getopt
import subprocess
import traceback
from datetime import datetime
import boto3
import MCEConstants
import CommonConstants
from LogSetup import logger
from CcMetricEngine import CcMetricEngine
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
from EmrClusterLaunchWrapper import EmrClusterLaunchWrapper

MODULE_NAME = "CcMetricCalculationEngine"
conf_file = MCEConstants.ENVIRONMENT_CONFIG_FILE


class MetricEngineWrapper(object):
    """
    Purpose: This class will fetch all the required inputs and submit it to the metric engine code for execution.
    """

    def __init__(self, execution_context=None):
        self.start_ts = datetime.now().strftime(MCEConstants.DATETIME_FORMAT)
        self.process_id = None
        self.input_metric_id = None
        self.cycle_id = None
        self.data_date = None
        self.cluster_id = None
        self.master_ip_address = None
        self.region_name = MCEConstants.REGION_NAME
        self.cluster_client = boto3.client("emr", region_name=self.region_name)
        try:
            if not execution_context:
                self.EXECUTION_CONTEXT = ExecutionContext()
                self.EXECUTION_CONTEXT.set_context({"module_name": MODULE_NAME})
            else:
                self.EXECUTION_CONTEXT = execution_context
                self.EXECUTION_CONTEXT.set_context({"module_name": MODULE_NAME})
        except Exception as exception:
            error_msg = "ERROR in " + self.EXECUTION_CONTEXT.get_context_param("module_name") + \
                        " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.EXECUTION_CONTEXT.set_context({"traceback": error_msg})
            logger.error(error_msg, extra=self.EXECUTION_CONTEXT.get_context())
            raise exception

    def _validate_input_json(self, input_json):
        """
        Purpose   :   This function is used to validate input metric JSONs
        Input     :   value
        Output    :   Return execution status (Success/Failed)
        """
        try:
            status_message = "Start of _validate_input_json method"
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            # extract process id
            process_id = input_json.get(MCEConstants.PROCESS_ID)
            # extract metric column name
            metric_column_name = input_json.get(MCEConstants.METRIC_COLUMN_NAME)
            # extracting metric_data_type
            metric_data_type = input_json.get(MCEConstants.METRIC_DATA_TYPE_KEY)
            # extract metric_type
            metric_type = input_json.get(MCEConstants.METRIC_TYPE_KEY)
            # extract source_table
            source_table = input_json.get(MCEConstants.SOURCE_TABLE_KEY)
            # extract target_table
            target_table = input_json.get(MCEConstants.TARGET_TABLE_KEY)
            # extract metric_template_name
            metric_template_name = input_json.get(MCEConstants.METRIC_TEMPLATE_NAME_KEY)
            # extract template_params
            template_params = input_json.get(MCEConstants.TEMPLATE_PARAM_KEY)
            # extract table_type
            table_type = input_json.get(MCEConstants.TABLE_TYPE_KEY)
            # extract active_indicator
            active_flag = input_json.get(MCEConstants.ACTIVE_INDICATOR_KEY)
            # checking if process id is present or not
            if process_id is None or process_id == " ":
                status_message = "Process id not present"
                raise Exception(status_message)
            # checking if metric_column_name is present or not
            if metric_column_name is None or metric_column_name == " ":
                status_message = "Metric column name not present"
                raise Exception(status_message)
            # checking if metric_data_type is present or not
            if metric_data_type is None or metric_data_type == " ":
                status_message = "Metric data type not present "
                raise Exception(status_message)
            # checking if metric_type is present or not
            if metric_type is None or metric_type == " ":
                status_message = "Metric type not present "
                raise Exception(status_message)
            # checking if source_table is present or not
            if source_table is None or source_table == " ":
                status_message = "Source table is not present "
                raise Exception(status_message)
            # checking if target_table is present or not
            if target_table is None or target_table == " ":
                status_message = "Target table is not present "
                raise Exception(status_message)
            # checking if metric_template_name is present or not
            if metric_template_name is None or metric_template_name == " ":
                status_message = "Metric template name is not present "
                raise Exception(status_message)
            # checking if template_params is present or not
            if template_params is None or template_params == " ":
                status_message = "Template params are not present "
                raise Exception(status_message)
            # checking if table_type is present or not
            if table_type is None or table_type == " ":
                status_message = "Table type is not present "
                raise Exception(status_message)
            # checking if active_indicator is present or not
            if active_flag is None or active_flag == " ":
                status_message = "Active indicator is not present "
                raise Exception(status_message)

            status_message = "Input JSON is Valid"
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            status_message = "End of _validate_input_json method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: ""}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: "",
                    MCEConstants.ERROR_KEY: exception}

    def _logger_handler(self, metric_id=None, metric_type=None, source_table=None,
                        target_table=None, status=None, operation=None):

        """
        Purpose   :   This function logs status for metric engine execution
        Input     :   process_id, cycle_id, metric_id, table_type, source_table, target_table,
                      status(In-Progress/ Success/ Failed)(optional), operation="select"/"insert"/"update".
        Output    :   Return execution status Success/Failed
        """
        try:
            status_message = "Start of _log_status method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            # check if operation = Insert or Update or Select
            if operation == MCEConstants.INSERT_KEY:
                status_message = "Insert log in logging table"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                # creating insert query for logging table
                engine_process_log_query = """insert into {table} (process_id,mce_process_id, cycle_id, metric_id,
                mce_type, source_table, target_table, mce_status, mce_start_time, mce_end_time) values ('{process_id}',
                '{mce_process_id}', '{cycle_id}', '{metric_id}', '{metric_type}', '{source_table}', '{target_table}',
                '{status}', '{start_ts}', 'null');
                """.format(table=MCEConstants.ENGINE_LOGGING_TABLE_KEY,
                           process_id=self.process_id,
                           mce_process_id = self.input_metric_id,
                           cycle_id=self.cycle_id,
                           metric_id=metric_id,
                           metric_type=metric_type,
                           source_table=source_table,
                           target_table=target_table,
                           status=status,
                           start_ts=self.start_ts,
                           )
                status_message = "Executing query: %s" % engine_process_log_query
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                MySQLConnectionManager().execute_query_mysql(query=engine_process_log_query)
                response = ""

            # when operation is update table
            elif operation == MCEConstants.UPDATE_KEY:

                status_message = "Update record in logging table"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                # creating update query for logging table
                engine_process_log_query = """update {table_name} set mce_status = '{status}' , mce_end_time = '{end_ts}'
                where metric_id='{metric_id}' and cycle_id='{cycle_id}' and mce_start_time = '{started_at}';
                """.format(table_name=MCEConstants.ENGINE_LOGGING_TABLE_KEY,
                           status=status,
                           end_ts=datetime.now().strftime(MCEConstants.DATETIME_FORMAT),
                           metric_id=metric_id,
                           cycle_id=self.cycle_id,
                           started_at=self.start_ts
                           )
                status_message = "Executing query: %s" % engine_process_log_query
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                MySQLConnectionManager().execute_query_mysql(query=engine_process_log_query)
                response = ""

            # when operation is select cycle_id from log table
            elif operation == MCEConstants.SELECT_CYCLE_ID:
                status_message = "Select record from logging table"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

                # creating select query for logging table
                engine_process_log_query = """select * from {table_name} where cycle_id = (SELECT MAX(cycle_id)
                from {table_name} where process_id = {process_id} and cycle_status = "{status}");
                """.format(table_name=MCEConstants.LOG_CYCLE_DTL,
                           process_id=self.process_id,
                           status=status)

                response = MySQLConnectionManager().execute_query_mysql(query=engine_process_log_query)

            else:
                status_message = "Invalid operation"
                raise Exception(status_message)

            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS, MCEConstants.RESULT_KEY: response}

        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}

    def _fetch_and_parse_rules_dict(self):
        """
        Purpose: Retrieve all the metric rules corresponding to a process_id from the RDS table and parse them
        Input: process_id
        Output: list of dict
        """
        status_message = "Started fetching the metrics rules corresponding to the input_metric_id: {input_metric_id}" \
            .format(input_metric_id=self.input_metric_id)
        logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

        # fetch rules query
        query = "SELECT * FROM {rules_table_name} WHERE process_id = {input_metric_id} and lower(active_flag)='y'". \
            format(rules_table_name=MCEConstants.METRICS_RULES_TABLE, input_metric_id=self.input_metric_id)

        # execute query to fetch all rules
        all_rules = MySQLConnectionManager().execute_query_mysql(query=query, get_single_result=False)
        for index in range(len(all_rules)):
            rule = all_rules[index]
            template_params = rule[MCEConstants.TEMPLATE_PARAM_KEY]
            template_params = json.loads(template_params)
            rule[MCEConstants.TEMPLATE_PARAM_KEY] = template_params

        # check if the tuple fetched is empty
        if len(all_rules) == 0:
            error_message = "No rule records found for the process_id: {process_id} given as the input". \
                format(process_id=self.process_id)
            logger.error(error_message, extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception(error_message)

        # loop through each metric rule for validation
        for rule in all_rules:
            self._validate_input_json(rule)

        # grouping rules_dict by table_type
        grouped_rules_dict = dict()

        for rule in all_rules:
            grouped_rules_dict.setdefault(rule[MCEConstants.TARGET_TABLE_KEY], [])
            if rule[MCEConstants.TARGET_TABLE_KEY] in grouped_rules_dict:
                grouped_rules_dict[rule[MCEConstants.TARGET_TABLE_KEY]].append(rule)

        return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                MCEConstants.RESULT_KEY: grouped_rules_dict}

    def _launch_emr(self):
        """
        Purpose: This method launches emr
        Input: Process ID
        Output: Cluster ID
        """
        # read environment parameter JSON
        configuration = JsonConfigUtility(conf_file)
        # fetch audit database name
        audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        status_message = "Name of audit database: %s" % audit_db
        logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
        # create object of EMR launch wrapper
        emr_cluster_launch = EmrClusterLaunchWrapper()
        # extract the cluster configuration of the given process id
        cluster_configs = emr_cluster_launch.extractClusterConfiguration(self.process_id)
        status_message = "Cluster configuration provided: %s" % cluster_configs[0]
        logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
        cluster_configs = cluster_configs[0]
        # fetch the property json from the cluster configuration
        property_json = json.loads(cluster_configs["property_json"])
        # query to fetch cluster id which is active for a given process id
        query = """SELECT cluster_id from {audit_db}.{emr_cluster_details_table} where process_id={process_id}
        and cluster_status='{state}'""".format(
            audit_db=audit_db,
            emr_cluster_details_table=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
            process_id=self.process_id,
            state=CommonConstants.CLUSTER_ACTIVE_STATE)
        status_message = "Query to retrieve the Cluster ID: %s" % query
        logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
        cluster_ids = MySQLConnectionManager().execute_query_mysql(query, False)
        if CommonConstants.CHECK_LAUNCH_COLUMN not in property_json:
            if cluster_ids:
                raise Exception("More than one clusters are in waiting state")
            else:
                emr_cluster_launch.main(self.process_id, "start")
        else:
            # check the value of the key check_launch_flag, if Y it will fetch the cluster ID of already
            # launched cluster for the given process id
            if property_json[CommonConstants.CHECK_LAUNCH_COLUMN] == CommonConstants.CHECK_LAUNCH_ENABLE:
                status_message = "Check flag is present with 'Y' value.Proceeding with existing " \
                                 "cluster Id in WAITING state"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            # check the value of the key check_launch_flag, if N it will still search for any launch cluster
            # for the given process id and launch a new cluster if no active cluster is found
            elif property_json[CommonConstants.CHECK_LAUNCH_COLUMN] == CommonConstants.CHECK_LAUNCH_DISABLE:
                status_message = "Check flag is present with 'N' value.Proceeding with existing " \
                                 "cluster Id in WAITING state"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                if not cluster_ids:
                    status_message = "There are no clusters with process id " + str(
                        self.process_id) + " in WAITING state. Trying to launch new cluster"
                    logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                    emr_cluster_launch.main(self.process_id, "start")
        # return the cluster id for a given process id
        cluster_id = self._get_cluster_id()
        return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                MCEConstants.RESULT_KEY: cluster_id[MCEConstants.RESULT_KEY]}

    def _get_cluster_id(self):
        """
        Purpose: This method gets cluster id for a given Process ID
        Input: Process ID
        Output: Cluster ID
        """
        try:
            # read environment parameter JSON
            configuration = JsonConfigUtility(conf_file)
            # fetch audit database name
            audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
            # query to fetch cluster id which is active for a given process id
            query = """SELECT cluster_id from {audit_db}.{emr_cluster_details_table} where process_id='{process_id}'
            and cluster_status='{state}'""".format(
                audit_db=audit_db,
                emr_cluster_details_table=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                process_id=self.process_id, state=CommonConstants.CLUSTER_ACTIVE_STATE)
            status_message = "Query to retrieve the Cluster ID: %s" % query
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            # execute query
            cluster_ids = MySQLConnectionManager().execute_query_mysql(query, False)
            status_message = "Query Result: %s" % cluster_ids
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: cluster_ids[0]['cluster_id']}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}

    def _terminate_emr(self):
        """
        Purpose: This method will terminate emr
        Input: Process ID
        Output: Termination Status
        """
        try:
            # command to executed
            terminate_string = "python3 " + "TerminateEmrHandler.py" + " " + self.cluster_id
            # executing shell script
            self._execute_shell_command(terminate_string)
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: ""}
        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: None,
                    MCEConstants.ERROR_KEY: exception}

    def _execute_shell_command(self, command):
        """
        Purpose: This method executes shell command
        Input: Command
        Output: Command execution status
        """
        try:
            status_message = "Started executing shell command " + command
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            # executing shell command
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            # fetch output
            if standard_output:
                standard_output_lines = standard_output.splitlines()
                for line in standard_output_lines:
                    logger.debug(line, extra=self.EXECUTION_CONTEXT.get_context())
            # fetch error if any
            if standard_error:
                standard_error_lines = standard_error.splitlines()
                for line in standard_error_lines:
                    logger.debug(line, extra=self.EXECUTION_CONTEXT.get_context())
            # checking exit status code
            if command_output.returncode == 0:
                return True
            else:
                status_message = "Error occurred while executing command on shell :" + command
                raise Exception(status_message)
        except Exception as exception:
            status_message = "Shell command execution error: %s" % exception
            logger.error(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception(status_message)

    def _get_cluster_state(self, jobid):
        """
        Purpose: Method to return cluster state.
        Input: Job ID of the cluster for which the status is to be fetched.
        Output: Status of the cluster.
        """
        try:
            status_message = "Stated fetching the status of the given cluster id"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            cluster_client = self.cluster_client
            state = cluster_client.describe_cluster(ClusterId=jobid)['Cluster']['Status']['State']
            status_message = "Status of the given cluster id: %s" % str(state)
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: str(state)}
        except Exception as exception:
            logger.error("Fetching cluster status failed: %s" % str(exception),
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Fetching cluster status failed: %s" % str(exception))

    def _wrapper_input_validation(self, process_id, input_metric_id , cycle_id, cluster_id):
        try:
            status_message = "Start of wrapper input validation method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            if not process_id:
                error = "Mandatory input process id not provided as an input."
                logger.debug(error, extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception(error)

            self.process_id = process_id

            if not input_metric_id:
                error = "Mandatory input metric id not provided as an input."
                logger.debug(error, extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception(error)

            self.input_metric_id = input_metric_id

            # fetch the cycle_id if not provided in config else check the existence of the cycle_id provided in config
            # in log_cycle_dtl
            if cycle_id:
                status_message = "Checking the existence and status of the cycle_id provided in config in log_cycle_dtl"
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                query = """SELECT * from {table_name} WHERE cycle_id = 'cycle_id'
                """.format(table_name=MCEConstants.LOG_CYCLE_DTL,
                           cycle_id=cycle_id)
                status_message = "Query to check the existence and status of cycle_id provided as config: %s" % query
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                status = MySQLConnectionManager().execute_query_mysql(query=query,
                                                                      get_single_result=False)

                # check if the cycle_id provided in the config is present log_cycle_dtl
                if len(status) == 0:
                    error = "The provided cycle-id has no records in the log_cycle_dtl table"
                    logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
                    raise Exception(error)

                status = status[0][MCEConstants.CYCLE_STATUS]

                # checking the status of the cycle_id in log_cycle_dtl
                if status.strip() != MCEConstants.STATUS_SUCCEEDED:
                    error = "The provided cycle_id did not have a successful run"
                    logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
                    raise Exception(error)
            else:
                status_message = "Fetching the latest cycle_id from log_ctl_dtl as not provided in config"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                logger.info("Fetching cycle ID", extra=self.EXECUTION_CONTEXT.get_context())
                fetch_cycle_id = self._logger_handler(status=MCEConstants.STATUS_IN_PROGRESS,
                                                      operation=MCEConstants.SELECT_CYCLE_ID)
                cycle_id = fetch_cycle_id[MCEConstants.RESULT_KEY][0][MCEConstants.CYCLE_ID]
                status_message = "The cycle_id fetched from log_cycle_dtl is: %s" % cycle_id
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())


            query = "SELECT source_table FROM {rules_table_name} WHERE process_id = {input_metric_id} limit 1". \
                format(rules_table_name=MCEConstants.METRICS_RULES_TABLE, input_metric_id=self.input_metric_id)

            # execute query to fetch all rules
            mce_source_table = MySQLConnectionManager().execute_query_mysql(query=query, get_single_result=False)

            for index in range(len(mce_source_table)):
                source_table = mce_source_table[index]
                source_table = source_table["source_table"]


            # check the run status of cycle_id in metric log table
            query = """SELECT * from {table_name} WHERE cycle_id = '{cycle_id}' and source_table = '{mce_source_table}'
                    """.format(table_name=MCEConstants.ENGINE_LOGGING_TABLE_KEY,
                               cycle_id=cycle_id,mce_source_table=source_table)

            status_message = "Query to check the run status of cycle_id in metric log table: %s" % query
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            cycle_id_status = MySQLConnectionManager().execute_query_mysql(query=query,
                                                                           get_single_result=False)

            if len(cycle_id_status) == 0:
                status_message = "No record in metric log table available for cycle_id: %s" % cycle_id
                logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            else:
                status = cycle_id_status[0][MCEConstants.MCE_STATUS_KEY]

                if status == MCEConstants.STATUS_SUCCEEDED:
                    error = "The metric calculation corresponding to the given cycle id is " \
                            "already successfully calculated"
                    logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
                    sys.exit(0)

            self.cycle_id = cycle_id

            # query to fetch data_date for the given cycle_id
            status_message = "Fetching data_date for the cycle_id: %s" % self.cycle_id
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            query = """SELECT * from {table_name} WHERE cycle_id = '{cycle_id}'
                    """.format(table_name=MCEConstants.LOG_CYCLE_DTL,
                               cycle_id=self.cycle_id)
            status_message = "Query to fetch the date_date of the given cycle_id: %s" % query
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())
            data_date = MySQLConnectionManager().execute_query_mysql(query=query,
                                                                     get_single_result=False)
            data_date = str(data_date[0][MCEConstants.DATA_DATE])

            status_message = "The data_date fetched is: %s" % data_date
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            self.data_date = data_date

            if cluster_id:
                status_message = "Checking the status of the cluster_id provided in the config"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                cluster_status = self._get_cluster_state(cluster_id)
                status = cluster_status[MCEConstants.RESULT_KEY]
                if status != CommonConstants.EMR_WAITING:
                    error = "Provided cluster id not in waiting state"
                    logger.debug(error, extra=self.EXECUTION_CONTEXT.get_context())
                    raise Exception(error)
            else:
                status_message = "Launching new cluster as no cluster id was provided in the config"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                cluster_id_json = self._launch_emr()
                cluster_id = cluster_id_json[MCEConstants.RESULT_KEY]

            self.cluster_id = cluster_id

            status_message = "Final inputs fetched - process_id:%s, cycle_id:%s, cluster_id:%s" \
                             % (self.process_id, self.cycle_id, self.cluster_id)
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: ""}
        except Exception as exception:
            error = "ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: "",
                    MCEConstants.ERROR_KEY: exception}

    def _get_master_node_ip(self, jobid):
        """
        Purpose: Fetch the IP address of the master node of a give cluster.
        Input: Job ID of the cluster.
        Output: IP address of the master node.
        """
        try:
            cluster_client = self.cluster_client
            instance_details_response = cluster_client.list_instances(ClusterId=jobid, InstanceGroupTypes=["MASTER"])
            master_instance_details = instance_details_response["Instances"]
            master_ip_address = master_instance_details[0][MCEConstants.PRIVATE_IP_ADDRESS]
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                    MCEConstants.RESULT_KEY: str(master_ip_address)}
        except Exception as exception:
            logger.error("Fetching master ip address failed: %s" % str(exception),
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Fetching master ip address failed: %s" % str(exception))

    def execute_rule(self, process_id,input_metric_id, cycle_id, cluster_id):
        """
        Purpose: This is main method of Metric Engine Wrapper which will call all other methods one by one
        Input: process_id, cycle_id(optional), cluster_id(optional)
        Output: Return execution status (Success/Failed)
        """
        try:
            status_message = "Start of execute_rule method"
            logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            self._wrapper_input_validation(process_id,input_metric_id, cycle_id, cluster_id)

            rules_fetch = self._fetch_and_parse_rules_dict()
            grouped_rules = rules_fetch[MCEConstants.RESULT_KEY]

            status_message = "Rules dict grouped by table type - %s" % grouped_rules
            logger.debug(status_message, extra=self.EXECUTION_CONTEXT.get_context())

            fetch_master_ip_address = self._get_master_node_ip(cluster_id)
            self.master_ip_address = fetch_master_ip_address[MCEConstants.RESULT_KEY]

            # call metric engine and update MCE log table
            for table, metrics in grouped_rules.items():

                # insert metric details in the log table and mark status as in-progress
                status_message = "Insert the metric details in the MCE log table"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                for metric in metrics:
                    metric_id = metric.get(MCEConstants.METRIC_COLUMN_NAME)
                    metric_type = metric.get(MCEConstants.METRIC_TYPE_KEY)
                    source_table = metric.get(MCEConstants.SOURCE_TABLE_KEY)
                    target_table = metric.get(MCEConstants.TARGET_TABLE_KEY)
                    status = MCEConstants.IN_PROGRESS_KEY
                    operation = MCEConstants.INSERT_KEY
                    self._logger_handler(metric_id=metric_id, metric_type=metric_type, source_table=source_table,
                                         target_table=target_table, status=status, operation=operation)

                response = CcMetricEngine().execute_rules(table, metrics, self.process_id, self.master_ip_address,
                                                          self.cycle_id, self.data_date)
                print("--------->response"+str(response))
                response_status = response[MCEConstants.STATUS_KEY]

                # updating the MCE log table with the execution status
                status_message = "Updating the log table with the execution status of the engine"
                logger.info(status_message, extra=self.EXECUTION_CONTEXT.get_context())
                if response_status == MCEConstants.STATUS_SUCCESS:
                    for metric in metrics:
                        metric_id = metric.get(MCEConstants.METRIC_COLUMN_NAME).strip()
                        status = MCEConstants.STATUS_SUCCEEDED
                        operation = MCEConstants.UPDATE_KEY
                        self._logger_handler(metric_id=metric_id, status=status, operation=operation)
                    return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                            MCEConstants.RESULT_KEY: ""}
                else:
                    for metric in metrics:
                        metric_id = metric.get(MCEConstants.METRIC_COLUMN_NAME)
                        status = MCEConstants.STATUS_FAILED
                        operation = MCEConstants.UPDATE_KEY
                        self._logger_handler(metric_id=metric_id, status=status, operation=operation)
                        if response_status == MCEConstants.STATUS_FAILED:
                            raise Exception
                    return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED,
                            MCEConstants.RESULT_KEY: ""}

        except Exception as exception:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(error, extra=self.EXECUTION_CONTEXT.get_context())
            return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: "",
                    MCEConstants.ERROR_KEY: exception}


if __name__ == '__main__':
    try:
        input_process_id = None
        input_cycle_id = None
        input_cluster_id = None
        input_metric_id = None

        (options, args) = getopt.getopt(sys.argv[1:], "p:m:c:e:z",
                                        ["process_id=","metric_id=", "cycle_id=", "cluster_id="])

        for option, arg in options:
            if option in ("-p", "--process_id"):
                input_process_id = arg
            elif option in ("-m","--metric_id"):
                input_metric_id = arg
            elif option in ("-c", "--cycle_id"):
                input_cycle_id = arg
            elif option in ("-e", "--cluster_id"):
                input_cluster_id = arg

        exec_status = MetricEngineWrapper().execute_rule(input_process_id,
                                                         input_metric_id,
                                                         input_cycle_id,
                                                         input_cluster_id,
                                                         )
        # checking execution status of Wrapper
        print("exec_status----------->  " + str(exec_status))
        if exec_status[MCEConstants.STATUS_KEY]==MCEConstants.STATUS_FAILED:
            raise Exception
        if exec_status[MCEConstants.STATUS_KEY] == MCEConstants.STATUS_SUCCESS:
            sys.stdout.write(str(exec_status))
            sys.exit("SUCCESS")
    except Exception as ex:
        sys.stderr.write(str(ex))
        sys.exit("FAILED")
