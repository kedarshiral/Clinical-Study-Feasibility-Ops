#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

"""
This utility will take input json and form a sqoop command which will be executed on a cluster.
"""

import sys
import time
import json
import base64
import traceback
from datetime import datetime
import configparser
import boto3
import SqoopUtilityConstants
from LogSetup import logger
from SshCluster import ClusterSshClient
from ExecutionContext import ExecutionContext

__author__ = 'ZS Associates'

"""
Module Name         : SqoopIngestor.py
Tech Description    : Import RDBMS tables to s3 using sqoop.
Pre_requisites      : N/A
Inputs              : "dbType": "mysql/postgresql",
                      "clusterID": "",
                      "secretName": "",
                      "targetPath": "", //s3 path format: s3://bucket_name/relative_path
                      "host": "", 
                      "port": "",
                      "dbname": "",
                      "tableNames": [],
                      "password": "", // input base64 encoded
                      "outputFileType:" "csv/parquet" //parquet only if HDFS is target path
                      "query": "", //optional
                      "whereClause": "", //optional
                      "lastValue": "", //optional
                      "columnLookUp": "" //optional
                      "params": "" //optional 
Outputs             : {'status': 'SUCCESS/FAILED'}
Config_file         : cluster_config.conf
Last changed on     : 29th April 2019
Last changed by     : Sarun Natarajan
Reason for change   : Changes made to file transfer function.
"""

MODULE_NAME = "SqoopIngestor"
CONFIGURATION_FILE = "cluster_config.conf"
CONFIG = configparser.RawConfigParser()
CONFIG.read(CONFIGURATION_FILE)


class SqoopIngestor(object):
    """
    Purpose: Import data from RDBMS to s3
    """

    def __init__(self, input_json, execution_context=None):
        try:
            self.cluster_id = input_json.get("clusterId", None)
            self.cluster_name = CONFIG.get("CLUSTER_CONFIG", "CLUSTER_NAME")
            self.log_uri = CONFIG.get("CLUSTER_CONFIG", "LOG_URI")
            self.ami_version = CONFIG.get("CLUSTER_CONFIG", "AMI_VERSION")
            self.release_label = CONFIG.get("CLUSTER_CONFIG", "RELEASE_LABEL")
            self.instance_type = CONFIG.get("CLUSTER_CONFIG", "INSTANCE_TYPE")
            self.region_name = CONFIG.get("CLUSTER_CONFIG", "REGION_NAME")
            self.master_instance_count = int(CONFIG.get("CLUSTER_CONFIG", "MASTER_INSTANCE_COUNT"))
            self.worker_node_count = int(CONFIG.get("CLUSTER_CONFIG", "WORKER_NODE_COUNT"))
            self.task_node_count = int(CONFIG.get("CLUSTER_CONFIG", "TASK_NODE_COUNT"))
            self.ec2_key_name = CONFIG.get("CLUSTER_CONFIG", "EC2_KEY_NAME")
            self.ec2_subnet_id = CONFIG.get("CLUSTER_CONFIG", "EC2_SUBNET_ID")
            self.master_sg = CONFIG.get("CLUSTER_CONFIG", "MANAGED_MASTER_SECURITY_GROUP")
            self.slave_sg = CONFIG.get("CLUSTER_CONFIG", "MANAGED_SLAVE_SECURITY_GROUP")
            self.access_sg = CONFIG.get("CLUSTER_CONFIG", "SERVICE_ACCESS_SECURITY_GROUP")
            self.job_flow_role = CONFIG.get("CLUSTER_CONFIG", "JOB_FLOW_ROLE")
            self.service_role = CONFIG.get("CLUSTER_CONFIG", "SERVICE_ROLE")
            self.cluster_bootstrap_script_path = CONFIG.get("CLUSTER_CONFIG", "CLUSTER_BOOTSTRAP_SCRIPT_PATH")
            self.cluster_bootstrap_arg_1 = CONFIG.get("CLUSTER_CONFIG", "CLUSTER_BOOTSTRAP_ARGUMENT_S3_BUCKET")
            self.cluster_bootstrap_arg_2 = CONFIG.get("CLUSTER_CONFIG",
                                                      "CLUSTER_BOOTSTRAP_ARGUMENT_RELATIVE_CODE_PATH")
            self.crowdstrike_script_path = CONFIG.get("CLUSTER_CONFIG", "CROWDSTRIKE_BOOTSTRAP_SCRIPT_PATH")
            self.tag_key_1 = CONFIG.get("CLUSTER_CONFIG", "TAGS_KEY_1")
            self.tag_value_1 = CONFIG.get("CLUSTER_CONFIG", "TAGS_VALUE_1")
            self.tag_key_2 = CONFIG.get("CLUSTER_CONFIG", "TAGS_KEY_2")
            self.tag_value_2 = CONFIG.get("CLUSTER_CONFIG", "TAGS_VALUE_2")
            self.tag_key_3 = CONFIG.get("CLUSTER_CONFIG", "TAGS_KEY_3")
            self.tag_value_3 = CONFIG.get("CLUSTER_CONFIG", "TAGS_VALUE_3")
            self.cluster_client = boto3.client("emr", region_name=self.region_name)
            self.hdfs_temp_path = SqoopUtilityConstants.HDFS_TEMP_PATH
            self.status = list()
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

    def input_validation(self, input_values):
        """
        Purpose: Validate input json given to the utility.
        Inputs: The json file with required keys and values.
        Output: Validated values for each key.
        """
        logger.debug("Validating inputs provided to the utility",
                     extra=self.EXECUTION_CONTEXT.get_context())
        query = None
        where = None
        params = None
        password = None
        last_value = None
        table_names = None
        dataset_cred = None
        column_lookup = None
        output_file_type = SqoopUtilityConstants.CSV
        if "outputFileType" in input_values:
            output_file_type = input_values["outputFileType"].lower()
            if output_file_type not in SqoopUtilityConstants.PERMITTED_OUTPUT_FILE_TYPES:
                logger.error("Incorrect output file type specified. Permitted types are: %s"
                             % str(SqoopUtilityConstants.PERMITTED_OUTPUT_FILE_TYPES),
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Incorrect output file type specified. Permitted types are: %s"
                                % str(SqoopUtilityConstants.PERMITTED_OUTPUT_FILE_TYPES))
        if "secretName" in input_values and "password" in input_values:
            if input_values["secretName"] and input_values["password"]:
                logger.error("Either secret name or password should be given as input. Not Both",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Either secret name or password should be given as input. Not Both")
        if "secretName" in input_values:
            secret_name = input_values["secretName"]
            if secret_name:
                dataset_cred = SqoopIngestor.get_secret(self, secret_name)
                dataset_cred = json.loads(dataset_cred)
        if "dbType" in input_values:
            db_type = input_values["dbType"]
            if not db_type or db_type not in SqoopUtilityConstants.PERMITTED_DB_TYPES:
                logger.error("No database type was provided or invalid database type provided",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception(
                    "Provide a database type. Supported types are: " + str(SqoopUtilityConstants.PERMITTED_DB_TYPES))
        else:
            logger.error("Mandatory key: dbType not found in input", extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Mandatory key: dbType not found in input")
        if "host" in input_values:
            host_name = input_values["host"]
            if not host_name:
                logger.error("Host name not provided which is a mandatory field",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Host name not provided which is a mandatory field")
        else:
            logger.error("Mandatory key: host not found in input", extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Mandatory key: host not found in input")
        if "port" in input_values:
            port_no = input_values["port"]
            if not port_no:
                logger.error("Port number not provided which is a mandatory field",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Port number not provided which is a mandatory field")
        else:
            logger.error("Mandatory key: port not found in input", extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Mandatory key: port not found in input")
        if "dbname" in input_values:
            database = input_values["dbname"]
            if not database:
                logger.error("Database name not provided which is a mandatory field",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Database name not provided which is a mandatory field")
        else:
            logger.error("Mandatory key: dbname not found in input",
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Mandatory key: dbname not found in input")
        if "username" in input_values:
            user_name = input_values["username"]
            if not user_name:
                logger.error("Username not provided which is a mandatory field",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Username not provided which is a mandatory field")
        else:
            logger.error("Mandatory key: username not found in input",
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Mandatory key: username not found in input")
        if dataset_cred:
            if "password" in dataset_cred:
                password = dataset_cred["password"]
                if not password:
                    logger.error("Password not provided which is a mandatory field",
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    raise Exception("Password not provided which is a mandatory field")
        if not password:
            if "password" in input_values:
                password = input_values["password"]
                if not password:
                    logger.error("Password not provided which is a mandatory field",
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    raise Exception("Password not provided which is a mandatory field")
        if not password:
            logger.error("Password of the database was not given as an input.",
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Password of the database was not given as an input.")
        if "tableNames" in input_values:
            table_names = input_values["tableNames"]
            if type(table_names) is not list:
                logger.error("Input table name should be given in the form of list even if it is to be empty",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Input table name should be given in the form of list even if it is to be empty")
        if "targetPath" in input_values:
            target_path = input_values["targetPath"]
            target_path = target_path.rstrip("/")
            target_path = target_path + "/"
            if not target_path:
                logger.error("Target path not provided which is a mandatory field",
                             extra=self.EXECUTION_CONTEXT.get_context())
                raise Exception("Target path not provided which is a mandatory field")
        else:
            logger.error("Mandatory key: targetPath not found in input",
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Mandatory key: targetPath not found in input")
        if "query" in input_values:
            query = input_values["query"]
            query = query.lstrip('"').rstrip('"')
            query = query.lstrip("'").rstrip("'")
        if "whereClause" in input_values:
            where = input_values["whereClause"]
        if "lastValue" in input_values:
            last_value = input_values["lastValue"]
        if "columnLookUp" in input_values:
            column_lookup = input_values["columnLookUp"]
        if "params" in input_values:
            params = input_values["params"]
        if not table_names and not query:
            logger.error("Either table name or query should be given as an input",
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Either table name or query should be given as an input")
        validated_json = dict()
        validated_json["dbType"] = db_type
        validated_json["targetPath"] = target_path
        validated_json["host"] = host_name
        validated_json["port"] = port_no
        validated_json["dbname"] = database
        validated_json["username"] = user_name
        validated_json["password"] = password
        validated_json["tableNames"] = table_names
        validated_json["outputFileType"] = output_file_type
        validated_json["query"] = query
        validated_json["whereClause"] = where
        validated_json["lastValue"] = last_value
        validated_json["columnLookUp"] = column_lookup
        validated_json["params"] = params
        logger.debug("Inputs have been validated",
                     extra=self.EXECUTION_CONTEXT.get_context())
        return validated_json

    def ingest_data(self, ingestion_input):
        """
        Purpose: Run sqoop command on EMR and ingest data to specified target.
        Input: Table specific inputs and credentials.
        Output: Status of sqoop job execution in EMR: Success/Failed.
        """
        try:
            logger.debug("Reading the validated inputs",
                         extra=self.EXECUTION_CONTEXT.get_context())
            db_type = ingestion_input["dbType"]
            target_path = ingestion_input["targetPath"]
            host_name = ingestion_input["host"]
            port_no = ingestion_input["port"]
            database = ingestion_input["dbname"]
            user_name = ingestion_input["username"]
            password = ingestion_input["password"]
            table_names = ingestion_input["tableNames"]
            output_file_type = ingestion_input["outputFileType"]
            query = ingestion_input["query"]
            where = ingestion_input["whereClause"]
            last_value = ingestion_input["lastValue"]
            column_lookup = ingestion_input["columnLookUp"]
            params = ingestion_input["params"]
            logger.info("Importing tables from the database type: %s" % db_type,
                        extra=self.EXECUTION_CONTEXT.get_context())
            if db_type.lower() == SqoopUtilityConstants.MYSQL_KEY or \
                    db_type.lower() == SqoopUtilityConstants.POSTGRESQL_KEY:
                sqoop_command = "sqoop import"
                if query:
                    query = "'" + query
                if not last_value:
                    if table_names and not where:
                        logger.debug("Importing table(s) without a where clause",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        for table in table_names:
                            sqoop_command = "sqoop import"
                            target_path = ingestion_input["targetPath"]
                            if "s3" in target_path:
                                timestamp = datetime.now().strftime(
                                    SqoopUtilityConstants.S3_FOLDER_NAME_TIMESTAMP_FORMAT)
                                target_path = target_path + table + "/" + timestamp + "/"
                            else:
                                raise Exception("Incorrect target path")
                            if output_file_type == SqoopUtilityConstants.CSV:
                                sqoop_command = sqoop_command + " --connect jdbc:" + db_type + "://" + \
                                                str(host_name) + ":" + str(port_no) + "/" + str(database) \
                                                + " --username " + str(user_name) + " --password " + \
                                                str(password) + " --table " + str(table) + \
                                                " --fields-terminated-by , " + "--escaped-by \\\\ " \
                                                + "--optionally-enclosed-by '\"'" + " --num-mappers 1" \
                                                + " --target-dir " + str(target_path) + " " + params
                                logger.info("Executing sqoop command for table: %s" % table,
                                            extra=self.EXECUTION_CONTEXT.get_context())
                                self.sqoop_command_executor(sqoop_command=sqoop_command, table=table)
                            elif output_file_type == SqoopUtilityConstants.PARQUET:
                                sqoop_command = sqoop_command + " --connect jdbc:" + db_type + "://" + \
                                                str(host_name) + ":" + str(port_no) + "/" + str(database) \
                                                + " --username " + str(user_name) + " --password " + \
                                                str(password) + " --table " + str(table) + \
                                                " --fields-terminated-by , " + "--escaped-by \\\\ " \
                                                + "--optionally-enclosed-by '\"'" + " --num-mappers 1" \
                                                + " --delete-target-dir" + " --target-dir " + self.hdfs_temp_path \
                                                + " --as-parquetfile" + " " + params
                                logger.info("Executing sqoop command for table: %s" % table,
                                            extra=self.EXECUTION_CONTEXT.get_context())
                                self.sqoop_command_executor(sqoop_command=sqoop_command, table=table)
                                file_name = self.fetch_filename()
                                target_path = target_path + file_name
                                self.file_transfer(file_name=file_name, target_path=target_path)
                        logger.debug("Terminating cluster",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        terminate_cluster_id = self.cluster_id
                        self.terminate_cluster(terminate_cluster_id)
                        logger.debug("Cluster terminated successfully",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        return self.status
                    if query:
                        if output_file_type == SqoopUtilityConstants.CSV:
                            sqoop_command = sqoop_command + " --connect jdbc:" + db_type + "://" + \
                                            str(host_name) + ":" + str(port_no) + "/" + str(database) \
                                            + " --username " + str(user_name) + " --password " + str(password) \
                                            + " --query " + query + " WHERE $CONDITIONS'" + \
                                            " --fields-terminated-by , " + "--escaped-by \\\\ " \
                                            + "--optionally-enclosed-by '\"'" + " --num-mappers 1" \
                                            + " --target-dir " + str(target_path) + " " + params
                            logger.info("Executing sqoop command for query: %s" % query,
                                        extra=self.EXECUTION_CONTEXT.get_context())
                            self.sqoop_command_executor(sqoop_command=sqoop_command, table=query)
                        elif output_file_type == SqoopUtilityConstants.PARQUET:
                            sqoop_command = sqoop_command + " --connect jdbc:" + db_type + "://" + \
                                            str(host_name) + ":" + str(port_no) + "/" + str(database) \
                                            + " --username " + str(user_name) + " --password " + str(password) \
                                            + " --query " + query + " WHERE $CONDITIONS'" + \
                                            " --fields-terminated-by , " + "--escaped-by \\\\ " \
                                            + "--optionally-enclosed-by '\"'" + " --num-mappers 1" \
                                            + " --delete-target-dir" + " --target-dir " + self.hdfs_temp_path \
                                            + " --as-parquetfile" + " " + params
                            logger.info("Executing sqoop command for query: %s" % query,
                                        extra=self.EXECUTION_CONTEXT.get_context())
                            self.sqoop_command_executor(sqoop_command=sqoop_command, table=query)
                            file_name = self.fetch_filename()
                            target_path = target_path + file_name
                            self.file_transfer(file_name=file_name, target_path=target_path)
                        terminate_cluster_id = self.cluster_id
                        self.terminate_cluster(terminate_cluster_id)
                        logger.debug("Cluster terminated successfully",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                    if table_names and where:
                        if len(table_names) > 1:
                            logger.error("For importing table with a where clause, more than one table name is invalid",
                                         extra=self.EXECUTION_CONTEXT.get_context())
                            raise Exception("For importing table with a where clause, "
                                            "more than one table name is invalid")
                        if output_file_type == SqoopUtilityConstants.CSV:
                            sqoop_command = sqoop_command + " --connect jdbc:" + db_type + "://" + \
                                            str(host_name) + ":" + str(port_no) + "/" + str(database) \
                                            + " --username " + str(user_name) + " --password " + \
                                            str(password) + " --table " + str(table_names[0]) + \
                                            " --fields-terminated-by , " + "--escaped-by \\\\ " \
                                            + "--optionally-enclosed-by '\"'" + " --num-mappers 1" \
                                            + ' --where "' + where + '" --target-dir ' + str(target_path) \
                                            + " " + params
                            logger.info("Executing sqoop command for table: %s" % table_names[0],
                                        extra=self.EXECUTION_CONTEXT.get_context())
                            self.sqoop_command_executor(sqoop_command=sqoop_command, table=table_names[0])
                        elif output_file_type == SqoopUtilityConstants.PARQUET:
                            sqoop_command = sqoop_command + " --connect jdbc:" + db_type + "://" + \
                                            str(host_name) + ":" + str(port_no) + "/" + str(database) \
                                            + " --username " + str(user_name) + " --password " + \
                                            str(password) + " --table " + str(table_names[0]) + \
                                            " --fields-terminated-by , " + "--escaped-by \\\\ " \
                                            + "--optionally-enclosed-by '\"'" + " --num-mappers 1" \
                                            + ' --where "' + where + '" --delete-target-dir' + \
                                            " --target-dir " + self.hdfs_temp_path + " --as-parquetfile" + " " + params
                            logger.info("Executing sqoop command for table: %s" % table_names[0],
                                        extra=self.EXECUTION_CONTEXT.get_context())
                            self.sqoop_command_executor(sqoop_command=sqoop_command, table=table_names[0])
                            file_name = self.fetch_filename()
                            target_path = target_path + file_name
                            self.file_transfer(file_name=file_name, target_path=target_path)
                        logger.debug("Terminating cluster",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        terminate_cluster_id = self.cluster_id
                        self.terminate_cluster(terminate_cluster_id)
                        logger.debug("Cluster terminated successfully",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                if last_value and column_lookup:
                    status = list()
                    logger.info("Performing incremental import",
                                extra=self.EXECUTION_CONTEXT.get_context())
                    if output_file_type == SqoopUtilityConstants.CSV and \
                            SqoopUtilityConstants.S3 in target_path:
                        if not table_names:
                            logger.error("No table name given as an input for incremental input",
                                         extra=self.EXECUTION_CONTEXT.get_context())
                            raise Exception("No table name given as an input for incremental input")
                        if len(table_names) > 1:
                            logger.error("For incremental import more than one table name is invalid",
                                         extra=self.EXECUTION_CONTEXT.get_context())
                            raise Exception("For incremental import more than one table name is invalid")
                        sqoop_command = sqoop_command + " --connect jdbc:" + db_type + "://" \
                                        + str(host_name) + ":" + str(port_no) + "/" + str(database) \
                                        + " --username " + str(user_name) + " --password " + str(password) \
                                        + " --table " + str(table_names[0]) + " --fields-terminated-by , " \
                                        + "--escaped-by \\\\ " + "--optionally-enclosed-by '\"'" \
                                        + " --num-mappers 1" + " --target-dir " + str(target_path) \
                                        + " --incremental append" + " --check-column " + column_lookup \
                                        + " --last-value " + last_value + " --temporary-rootdir " \
                                        + SqoopUtilityConstants.TEMP_S3_PATH + " " + params
                        self.sqoop_command_executor(sqoop_command=sqoop_command, table=table_names[0])
                        logger.info("Executing sqoop command for table: %s" % table_names[0],
                                    extra=self.EXECUTION_CONTEXT.get_context())
                    else:
                        logger.error("Incremental imported not supported with HDFS as target.",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        raise Exception("Incremental imported not supported with HDFS as target.")
                    logger.debug("Terminating cluster",
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    terminate_cluster_id = status[0]["executedClusterId"]
                    self.terminate_cluster(terminate_cluster_id)
                    logger.debug("Cluster terminated successfully",
                                 extra=self.EXECUTION_CONTEXT.get_context())
            else:
                raise Exception("Specified database type is not supported yet.")
        except Exception as exception:
            if self.cluster_id:
                logger.debug("Terminating cluster after sqoop job failure",
                             extra=self.EXECUTION_CONTEXT.get_context())
                self.terminate_cluster(self.cluster_id)
            logger.error("Error while table import: %s" % exception,
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Error while table import: %s" % exception)

    def sqoop_command_executor(self, sqoop_command, table):
        """
        Purpose: Execute sqoop command on cluster
        Input: Sqoop command and table name
        Output: N/A
        """
        try:
            if self.cluster_id:
                logger.debug("Checking the status of the cluster provided as input",
                             extra=self.EXECUTION_CONTEXT.get_context())
                cluster_status = self.get_cluster_state(self.cluster_id)
                if cluster_status == SqoopUtilityConstants.CLUSTER_STATUS_WAITING:
                    logger.info("Cluster status: Waiting",
                                extra=self.EXECUTION_CONTEXT.get_context())
                    temp_dict = dict()
                    logger.debug("Fetching IP address of the master node",
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    master_ip_address = self.get_master_node_ip(self.cluster_id)
                    logger.info("The master node IP address is: %s" % master_ip_address,
                                extra=self.EXECUTION_CONTEXT.get_context())
                    logger.debug("Initializing the object to SSH into cluster",
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    ssh_object = ClusterSshClient()
                    logger.debug("Sending the sqoop command to the cluster to be executed",
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    response = ssh_object.execute_command(sqoop_command, master_ip_address)
                    temp_dict["tableName"] = table
                    temp_dict["importStatus"] = response
                    temp_dict["executedClusterId"] = self.cluster_id
                    logger.info("Import status of table {table_name} is: {status}".
                                format(table_name=table, status=str(temp_dict)),
                                extra=self.EXECUTION_CONTEXT.get_context())
                    self.status.append(temp_dict)
                else:
                    logger.error("Invalid cluster ID. Provided cluster not in waiting state",
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    raise Exception("Invalid cluster ID. Provided cluster not in waiting state.")
            else:
                logger.debug("Launching new cluster as no cluster ID was provided as input",
                             extra=self.EXECUTION_CONTEXT.get_context())
                cluster_launch_id = self.launch_cluster()
                while True:
                    new_cluster_status = self.get_cluster_state(cluster_launch_id)
                    logger.debug("Polling cluster status. Current status: %s" % new_cluster_status,
                                 extra=self.EXECUTION_CONTEXT.get_context())
                    if new_cluster_status in SqoopUtilityConstants.CLUSTER_STATUS:
                        time.sleep(60)
                    elif new_cluster_status in SqoopUtilityConstants.CLUSTER_STATUS_TERMINATE:
                        logger.error("Cluster launch failed. Trigger the sqoop job again",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        raise Exception("Cluster launch failed. Trigger the sqoop job again")
                    elif new_cluster_status == SqoopUtilityConstants.CLUSTER_STATUS_WAITING:
                        logger.info("Cluster status now in waiting state",
                                    extra=self.EXECUTION_CONTEXT.get_context())
                        temp_dict = dict()
                        logger.debug("Fetching IP address of the master node",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        master_ip_address = self.get_master_node_ip(cluster_launch_id)
                        logger.debug(
                            "Assigning the cluster id of newly launched cluster "
                            "for any pending tables imports",
                            extra=self.EXECUTION_CONTEXT.get_context())
                        self.cluster_id = cluster_launch_id
                        logger.info("The master node IP address is: %s" % master_ip_address,
                                    extra=self.EXECUTION_CONTEXT.get_context())
                        logger.debug("Initializing the object to SSH into cluster",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        ssh_object = ClusterSshClient()
                        logger.debug("Sending the sqoop command to the cluster to be executed",
                                     extra=self.EXECUTION_CONTEXT.get_context())
                        response = ssh_object.execute_command(sqoop_command, master_ip_address)
                        temp_dict["tableName"] = table
                        temp_dict["importStatus"] = response
                        temp_dict["executedClusterId"] = cluster_launch_id
                        logger.info("Execution status of table {table_name} is: {status}".
                                    format(table_name=table, status=str(temp_dict)),
                                    extra=self.EXECUTION_CONTEXT.get_context())
                        self.status.append(temp_dict)
                        break
        except Exception as exception:
            if self.cluster_id:
                self.terminate_cluster(self.cluster_id)
            logger.error("Error while executing sqoop command: %s" % exception,
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Error while executing sqoop command: %s" % exception)

    def get_secret(self, secret_name):
        """
        Purpose: Fetch the credentials from AWS Secret Manager
        Input: secret_name stored in AWS Secret Manager:
        Output: Credentials to respective secret_name
        """
        region_name = self.region_name
        logger.info("Create a Secrets Manager client", extra=self.EXECUTION_CONTEXT.get_context())
        session = boto3.session.Session()
        client = session.client(
            service_name=SqoopUtilityConstants.SECRET_MANAGER_SERVICE_NAME,
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except Exception as ae:
            logger.error(ae, extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception(ae)
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                logger.info("Credentials successfully fetched from Secrets Manager",
                            extra=self.EXECUTION_CONTEXT.get_context())
                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                logger.info("Credentials successfully fetched from Secrets Manager after decoding",
                            extra=self.EXECUTION_CONTEXT.get_context())
                return decoded_binary_secret

    def launch_cluster(self):
        """
        Purpose: Launch cluster with sqoop and spark installed
        Output: Cluster ID of the launched cluster.
        """
        try:
            cluster_client = self.cluster_client
            response = cluster_client.run_job_flow(
                Name=self.cluster_name,
                LogUri=self.log_uri,
                ReleaseLabel=self.release_label,
                Instances={
                    "InstanceGroups": [
                        {
                            "Name": "Main node",
                            "Market": "ON_DEMAND",
                            "InstanceRole": "MASTER",
                            "InstanceType": self.instance_type,
                            "InstanceCount": self.master_instance_count,
                            "Configurations": [
                                {
                                    "Classification": "yarn-site",
                                    "Properties": {
                                        "yarn.resourcemanager.scheduler.class":
                                            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"
                                    }
                                },
                                {
                                    "Classification": "spark",
                                    "Properties": {
                                        "spark.scheduler.mode": "FAIR"
                                    }
                                }
                            ]
                        },
                        {
                            "Name": "Worker nodes",
                            "Market": "ON_DEMAND",
                            "InstanceRole": "CORE",
                            "InstanceType": self.instance_type,
                            "InstanceCount": self.worker_node_count,
                            "Configurations": [
                                {
                                    "Classification": "yarn-site",
                                    "Properties": {
                                        "yarn.resourcemanager.scheduler.class":
                                            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"
                                    }
                                },
                                {
                                    "Classification": "spark",
                                    "Properties": {
                                        "spark.scheduler.mode": "FAIR"
                                    }
                                }
                            ]
                        },
                        {
                            "Name": "Worker nodes",
                            "Market": "SPOT",
                            "InstanceRole": "TASK",
                            "BidPrice": "1",
                            "InstanceType": self.instance_type,
                            "InstanceCount": self.worker_node_count,
                            "Configurations": [
                                {
                                    "Classification": "yarn-site",
                                    "Properties": {
                                        "yarn.resourcemanager.scheduler.class":
                                            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"
                                    }
                                },
                                {
                                    "Classification": "spark",
                                    "Properties": {
                                        "spark.scheduler.mode": "FAIR"
                                    }
                                }
                            ]
                        }
                    ],
                    "KeepJobFlowAliveWhenNoSteps": True,
                    "TerminationProtected": False,
                    "Ec2KeyName": self.ec2_key_name,
                    "Ec2SubnetId": self.ec2_subnet_id,
                    "EmrManagedMasterSecurityGroup": self.master_sg,
                    "EmrManagedSlaveSecurityGroup": self.slave_sg,
                    "ServiceAccessSecurityGroup": self.access_sg
                },
                Applications=[
                    {
                        "Name": "Hive"
                    },
                    {
                        "Name": "Pig"
                    },
                    {
                        "Name": "Sqoop"
                    },
                    {
                        "Name": "Hadoop"
                    },
                    {
                        "Name": "Hue"
                    },
                    {
                        "Name": "Spark"
                    }
                ],
                VisibleToAllUsers=True,
                JobFlowRole=self.job_flow_role,
                ServiceRole=self.service_role,
                BootstrapActions=[
                    {
                        "Name": "EMR bootstrap",
                        "ScriptBootstrapAction": {
                            "Path": self.cluster_bootstrap_script_path,
                            "Args": [
                                self.cluster_bootstrap_arg_1,
                                self.cluster_bootstrap_arg_2
                            ]
                        }
                    },
                    {
                        "Name": "Install crowdstrike and splunk",
                        "ScriptBootstrapAction": {
                            "Path": self.crowdstrike_script_path
                        }
                    }
                ],
                Tags=[
                    {
                        "Key": self.tag_key_1,
                        "Value": self.tag_value_1
                    },
                    {
                        "Key": self.tag_key_2,
                        "Value": self.tag_value_2
                    },
                    {
                        "Key": self.tag_key_3,
                        "Value": self.tag_value_3
                    }
                ]
            )
            cluster_id = response["JobFlowId"]
            return str(cluster_id)
        except Exception as exception:
            logger.error("Cluster launch failed: %s" % str(exception), extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Cluster launch failed: %s" % str(exception))

    def get_cluster_state(self, jobid):
        """
        Purpose: Method to return cluster state.
        Input: Job ID of the cluster for which the status is to be fetched.
        Output: Status of the cluster.
        """
        try:
            cluster_client = self.cluster_client
            state = cluster_client.describe_cluster(ClusterId=jobid)['Cluster']['Status']['State']
            return str(state)
        except Exception as exception:
            logger.error("Fetching cluster status failed: %s" % str(exception),
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Fetching cluster status failed: %s" % str(exception))

    def get_master_node_ip(self, jobid):
        """
        Purpose: Fetch the IP address of the master node of a give cluster.
        Input: Job ID of the cluster.
        Output: IP address of the master node.
        """
        try:
            cluster_client = self.cluster_client
            instance_details_response = cluster_client.list_instances(ClusterId=jobid, InstanceGroupTypes=["MASTER"])
            master_instance_details = instance_details_response["Instances"]
            master_ip_address = master_instance_details[0]["PrivateIpAddress"]
            return str(master_ip_address)
        except Exception as exception:
            logger.error("Fetching master ip address failed: %s" % str(exception),
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Fetching master ip address failed: %s" % str(exception))

    def terminate_cluster(self, jobid):
        """
        Purpose: Terminate cluster
        Input: Job ID of the cluster.
        Output: None
        """
        try:
            logger.debug("Terminating the cluster: %s" % jobid,
                         extra=self.EXECUTION_CONTEXT.get_context())
            cluster_client = self.cluster_client
            cluster_client.set_termination_protection(
                JobFlowIds=[
                    jobid
                ],
                TerminationProtected=False
            )
            cluster_client.terminate_job_flows(
                JobFlowIds=[
                    jobid
                ]
            )
        except Exception as exception:
            logger.error("Cluster termination failed: %s" % str(exception),
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Cluster termination failed: %s" % str(exception))

    def file_transfer(self, file_name, target_path):
        try:
            ssh_object = ClusterSshClient()
            master_ip_address = self.get_master_node_ip(self.cluster_id)
            command = SqoopUtilityConstants.HADOOP_DISTCP + self.hdfs_temp_path \
                      + "/" + file_name + " " + target_path
            logger.debug("Command to transfer file: %s" % command, extra=self.EXECUTION_CONTEXT.get_context())
            response = ssh_object.file_transfer(command=command, host=master_ip_address)
            if response:
                logger.info("File copied from HDFS to S3",
                            extra=self.EXECUTION_CONTEXT.get_context())
            else:
                logger.error("File transfer failed", extra=self.EXECUTION_CONTEXT.get_context())
        except Exception as exception:
            logger.error("File transfer failed: %s" % str(exception),
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("File transfer failed: %s" % str(exception))

    def fetch_filename(self):
        logger.info("Starting the fetch filename function", extra=self.EXECUTION_CONTEXT.get_context())
        filename = ''
        try:
            ssh_object = ClusterSshClient()
            master_ip_address = self.get_master_node_ip(self.cluster_id)
            command = SqoopUtilityConstants.HADOOP_LIST_FILE_COMMAND
            logger.debug("The command to list files: %s" % command, extra=self.EXECUTION_CONTEXT.get_context())
            response = ssh_object.fetch_file_name(command=command, host=master_ip_address)
            for val in response:
                if '.parquet' in val:
                    filename = val.split('/')[-1].strip()
            logger.info("Filename fetched is: %s" % filename, extra=self.EXECUTION_CONTEXT.get_context())
            return filename
        except Exception as exception:
            logger.error("Filename fetching failed: %s" % str(exception),
                         extra=self.EXECUTION_CONTEXT.get_context())
            raise Exception("Filename fetching failed: %s" % str(exception))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        CONFIG_FILE = sys.argv[1]
        try:
            with open(CONFIG_FILE) as f:
                json_input = json.load(f)
        except Exception:
            raise Exception("Invalid input file")
        sqoop_ingest = SqoopIngestor(json_input)
        validation_output = sqoop_ingest.input_validation(json_input)
        STATUS = sqoop_ingest.ingest_data(validation_output)
        logger.debug("Sqoop job execution status: %s" % STATUS,
                     extra=sqoop_ingest.EXECUTION_CONTEXT.get_context())
    else:
        raise Exception("Provide input JSON as argument")
