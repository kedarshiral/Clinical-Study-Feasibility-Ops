#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

import argparse
import logging
import traceback

import CommonConstants
from ConfigUtility import JsonConfigUtility
from LaunchDDLCreationIngestionUtility import LaunchDDLCreationIngestionUtility
from MySQLConnectionManager import MySQLConnectionManager
from PySparkUtility import PySparkUtility
from CommonUtils import CommonUtils

MODULE_NAME = "LaunchDDLCreationIngestionHandler"

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')
hndlr = logging.StreamHandler()
hndlr.setLevel('INFO')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)

TABLE_NAME_LOCATION_DICT = {"landing": ["s3_landing_location", "landing_table"],
                            "pre_dqm": ["s3_pre_dqm_location", "pre_dqm_table"],
                            # "staging":["staging_location","staging_table"],
                            # "staging": ["s3_post_dqm_location","post_dqm_table"],
                            "staging": ["s3_post_dqm_location", "staging_table"],
                            "publish_view": ["publish_s3_path", "publish_table_name"],
                            "publish_table": ["publish_s3_path", "publish_table_name"],
                            "staging_dw": ["table_s3_path", "table_name"],
                            "publish_dw_view": ["publish_s3_path", "publish_table_name"],
                            "publish_dw_table": ["publish_s3_path", "publish_table_name"]}


class LaunchDDLCreationIngestionHandler:

    def __init__(self, dataset_id, step, batch_id=None, cycle_id=None):
        self.dataset_id = dataset_id
        self.step = step
        self.batch_id = batch_id
        self.cycle_id = cycle_id
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.region = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "aws_region"])

        self.hive_query_conf = JsonConfigUtility(CommonConstants.HIVE_QUERY_TEMPLATE_FILE)

        self.spark = PySparkUtility().get_spark_context(dataset_id)

        self.db = None
        self.target_table_name = None
        self.dqm_database_name = None

    def get_table_details(self):
        try:
            status_message = "Starting Glue Table Creation method for dataset id:" + str(self.dataset_id) + " " \
                             + "and step:" + str(self.step)
            logger.info(status_message)
            step = self.step
            mode = 'create'

            get_dataset_details_query = "Select * from {audit_db}.{dataset_master_table} where " \
                                        "dataset_id={dataset_id}".format(audit_db=self.audit_db,
                                                                         dataset_master_table=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                                                                         dataset_id=self.dataset_id
                                                                         )
            print("get_dataset_details_query",get_dataset_details_query)
            get_dataset_details_query_result = MySQLConnectionManager().execute_query_mysql \
                (get_dataset_details_query, True)

            if get_dataset_details_query_result["header_available_flag"]:
                if get_dataset_details_query_result["header_available_flag"].lower() == 'v':
                    mode = 'create_alter'

            target_table_location = None
            target_table_name = None
            staging_table_name = None
            staging_db = None
            db = None

            row_id_exclusion_flag = get_dataset_details_query_result["row_id_exclusion_flag"]

            #Skip creating table for publish if dataset_publish_flag is N
            publish_flag = get_dataset_details_query_result["dataset_publish_flag"]

            if step == "publish_table" or step == "publish_view":
                if publish_flag is not None and publish_flag.upper() == "N":
                    return None

            ddl_query_template = self.hive_query_conf.get_configuration(["DDL_TABLE_QUERY_TEMPLATE"])
            logger.info("ddl_query_template" + ddl_query_template)

            if mode == 'create_alter':
                ddl_query_template = ddl_query_template + ';' + self.hive_query_conf.get_configuration(
                    ["ALTER_DDL_QUERY_TEMPLATE"])

            if step in TABLE_NAME_LOCATION_DICT.keys():
                target_table_location = get_dataset_details_query_result[TABLE_NAME_LOCATION_DICT[step][0]]
                target_table_name_db = get_dataset_details_query_result[TABLE_NAME_LOCATION_DICT[step][1]]
                self.db = target_table_name_db.split(".")[0]
                self.target_table_name = target_table_name_db.split(".")[1].lower()
                table_partition = get_dataset_details_query_result["table_partition_by"]
                table_partition_sql_str = None
                if table_partition:
                    table_partition = table_partition.replace(' ', '')
                    table_partition = table_partition.split(',')
                    table_partition_sql_str = 'PARTITIONED BY ( '
                    for partition in table_partition:
                        if not partition.startswith('pt_'):
                            table_partition_sql_str = table_partition_sql_str + "pt_" + partition + " " + "string,"
                        else:
                            table_partition_sql_str = table_partition_sql_str + partition + " " + "string,"
                    table_partition_sql_str = table_partition_sql_str.rstrip(',')
                    table_partition_sql_str = table_partition_sql_str + ")"
                    table_partition_sql_str = table_partition_sql_str.replace('-', '')

                view_name_full = None
                view_name_latest = None
            else:
                raise Exception("Invalid Step name provided")

            if step in ["publish_view"]:
                ddl_query_template = self.hive_query_conf.get_configuration(["DDL_VIEW_QUERY_TEMPLATE"])
                view_name_full = self.target_table_name + "_full"
                view_name_latest = self.target_table_name + "_latest"
                staging_table_name_db = get_dataset_details_query_result[TABLE_NAME_LOCATION_DICT["staging"][1]]
                staging_db = staging_table_name_db.split(".")[0]
                staging_table_name = staging_table_name_db.split(".")[1].lower()
                if not staging_table_name:
                    raise Exception("Staging Table name was not provided")

            if step in ["publish_dw_view"]:
                ddl_query_template = self.hive_query_conf.get_configuration(["DDL_VIEW_QUERY_DW_TEMPLATE"])
                view_name_full = self.target_table_name + "_full"
                view_name_latest = self.target_table_name + "_latest"
                staging_table_name_db = get_dataset_details_query_result["table_name"]
                staging_db = staging_table_name_db.split(".")[0]
                staging_table_name = staging_table_name_db.split(".")[1].lower()
                if not staging_table_name:
                    raise Exception("Staging Table name was not provided")

            if not self.target_table_name:
                raise Exception("Table name for step: {step} is not configured in {dataset_master}".format(
                    step=self.step,
                    dataset_master=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME
                ))

            if step not in ["publish_view", "publish_dw_view"] and not target_table_location:
                raise Exception("S3 Path for step: {step} is not configured in {dataset_master}".format(
                    step=self.step,
                    dataset_master=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME
                ))

            replace_dict = {"$$table_name": self.target_table_name,
                            "$$columns": '',
                            "$$target_table_location": target_table_location,
                            "$$view_name_full": view_name_full if view_name_full else '',
                            "$$view_name_latest": view_name_latest if view_name_latest else '',
                            "$$latest_batch_id": self.batch_id if self.batch_id else '',
                            '$$latest_cycle_id': self.cycle_id if self.cycle_id else '',
                            "$$staging_table_name": staging_table_name if staging_table_name else '',
                            "$$db": self.db,
                            "$$staging_db": staging_db if staging_db else '',
                            "$$partition": table_partition_sql_str if table_partition_sql_str else ''}

            if step not in ["publish_view", "publish_dw_view"]:
                column_name_dict = self.get_column_details()

                ddl_column_str = ''
                for name in column_name_dict.keys():
                    ddl_column_str = ddl_column_str + name + ' ' + column_name_dict[name] + ','

                if row_id_exclusion_flag and row_id_exclusion_flag.upper() == "N" and step not in ["staging_dw", "publish_dw_table"]:
                    ddl_column_str = ddl_column_str + CommonConstants.ROW_ID + ' ' + 'bigint' + ','

                ddl_column_str = ddl_column_str.rstrip(',')

                replace_dict["$$columns"] = ddl_column_str

                for replace_value in replace_dict.keys():
                    ddl_query_template = ddl_query_template.replace(replace_value, replace_dict[replace_value])

                logger.info("Query after replacing parameters:" + ddl_query_template)

                # Creating the Table if it doesnt exist.
                LaunchDDLCreationIngestionUtility().execute_hive_query(query=ddl_query_template,
                                                                       db=self.db,
                                                                       table=self.target_table_name,
                                                                       mode=mode)

            else:
                for replace_value in replace_dict.keys():
                    ddl_query_template = ddl_query_template.replace(replace_value, replace_dict[replace_value])
                ddl_query_template = ddl_query_template.split(";")
                logger.info("Splitted query list:" + str(ddl_query_template))

                for query in ddl_query_template:
                    query = query.lstrip('\n')
                    logger.info(query)
                    if query.startswith("CREATE") and (view_name_latest in query):
                        LaunchDDLCreationIngestionUtility().create_cross_platform_view(self.db, view_name_latest, query)
                    elif query.startswith("CREATE"):
                        LaunchDDLCreationIngestionUtility().create_cross_platform_view(self.db, view_name_full, query)
                    elif query.startswith("DROP"):
                        self.spark.sql(query)

            return True

        except Exception as exc:
            logger.error(str(exc))
            logger.error(traceback.format_exc())
            raise Exception(exc)

    def get_column_details(self):
        status_message = "Starting to get column details for dataset_id id:" + str(self.dataset_id) + " " \
                         + "and step:" + str(self.step)
        logger.info(status_message)

        get_column_details_query = "Select * from {audit_db}.{column_metadata} where dataset_id={dataset_id}".format(
            audit_db=self.audit_db,
            column_metadata=CommonConstants.COLUMN_METADATA_TABLE,
            dataset_id=self.dataset_id
        )

        column_name_dict = {}

        get_column_details_query_result = MySQLConnectionManager().execute_query_mysql \
            (get_column_details_query)

        if self.step in ["landing", "pre_dqm"]:
            for column_result in get_column_details_query_result:
                column_name_dict[column_result['column_name']] = "string"

        if self.step in ["staging", "publish_table", "staging_dw", "publish_dw_table"]:
            for column_result in get_column_details_query_result:
                if str(column_result['column_data_type']).lower() == 'integer':
                    column_name_dict[column_result['column_name']] = 'int'
                else:
                    column_name_dict[column_result['column_name']] = column_result['column_data_type']

        return column_name_dict

    def call_add_partition_data(self, batch_id=None, cycle_id=None, data_dt=None):
        try:
            if CommonConstants.PARTITION_MODE == 'FULL':
                LaunchDDLCreationIngestionUtility().execute_hive_query(db=self.db,
                                                                       table=self.target_table_name,
                                                                       mode='refresh')
            else:
                if batch_id:
                    LaunchDDLCreationIngestionUtility().add_partitions(db=self.db,
                                                                       table=self.target_table_name,
                                                                       batch_id=batch_id)
                elif cycle_id:
                    LaunchDDLCreationIngestionUtility().add_partitions_dw(db=self.db,
                                                                          table=self.target_table_name,
                                                                          cycle_id=cycle_id,
                                                                          data_dt=data_dt)
                else:
                    raise Exception("Neither cycle id or batch id was provided to add partitions")

        except Exception as exc:
            logger.error(exc)
            raise exc

    def create_dqm_tables(self):
        """
        Create dqm error and summary tables on AWS Glue
        :return: None
        """
        try:
            dqm_s3_error_location = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                          "dqm_s3_error_location"])
            dqm_s3_summary_location = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                            "dqm_s3_summary_location"])
            self.dqm_database_name = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                           "dqm_database_name"])

            err_summary_ddl_query = self.hive_query_conf.get_configuration(["DQM_SUMMARY_DDL_TEMPLATE"])
            err_details_ddl_query = self.hive_query_conf.get_configuration(["DQM_ERROR_DDL_TEMPLATE"])

            dqm_replace_dict = {
                "$$summary_location": dqm_s3_summary_location,
                "$$error_location": dqm_s3_error_location,
                "$$db": self.dqm_database_name

            }
            for replace_value in dqm_replace_dict.keys():
                err_summary_ddl_query = err_summary_ddl_query.replace(replace_value, dqm_replace_dict[replace_value])
                err_details_ddl_query = err_details_ddl_query.replace(replace_value, dqm_replace_dict[replace_value])

            # Error Summary

            LaunchDDLCreationIngestionUtility().execute_hive_query(query=err_summary_ddl_query,
                                                                   db=self.dqm_database_name,
                                                                   mode='create')

            # Error Details

            LaunchDDLCreationIngestionUtility().execute_hive_query(query=err_details_ddl_query,
                                                                   db=self.dqm_database_name,
                                                                   mode='create')

            return None
        except Exception as exc:
            logger.error(exc)
            raise exc

    def call_add_partition_dqm(self,batch_id):
        try:
            # Add partition
            logger.info("Adding Partitions for the dqm table")
            LaunchDDLCreationIngestionUtility().add_partitions(db=self.dqm_database_name,
                                                               table="dqm_summary",
                                                               batch_id=batch_id)
            LaunchDDLCreationIngestionUtility().add_partitions(db=self.dqm_database_name,
                                                               table="dqm_error_details",
                                                               batch_id=batch_id)
            logger.info("Successfully Added Partitons for the dqm table")
        except Exception as exc:
            logger.error(exc)
            raise exc


    def create_dw_dqm_tables(self):
        """
        Create dwn dqm error and summary tables on AWS Glue
        :return: None
        """
        try:
            dqm_s3_error_location = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                          "dw_dqm_error_location"])
            dqm_s3_summary_location = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                            "dw_dqm_error_summary_location"])
            self.dqm_database_name = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                           "dqm_database_name"])

            err_summary_ddl_query = self.hive_query_conf.get_configuration(["DQM_DW_ERROR_SMRY_DDL_TEMPLATE"])
            err_details_ddl_query = self.hive_query_conf.get_configuration(["DQM_DW_ERROR_DTL_DDL_TEMPLATE"])

            dqm_replace_dict = {
                "$$summary_location": dqm_s3_summary_location,
                "$$error_location": dqm_s3_error_location,
                "$$db": self.dqm_database_name,
                "$$error_detail_table_name": CommonConstants.DQM_DW_ERROR_DTL_TABLE_NAME,
                "$$error_summary_table_name": CommonConstants.DQM_DW_SMRY_DTL_TABLE_NAME,
            }
            for replace_value in dqm_replace_dict.keys():
                err_summary_ddl_query = err_summary_ddl_query.replace(replace_value, dqm_replace_dict[replace_value])
                err_details_ddl_query = err_details_ddl_query.replace(replace_value, dqm_replace_dict[replace_value])

            # Error Summary

            LaunchDDLCreationIngestionUtility().execute_hive_query(query=err_summary_ddl_query,
                                                                   db=self.dqm_database_name,
                                                                   mode='create')

            # Error Details

            LaunchDDLCreationIngestionUtility().execute_hive_query(query=err_details_ddl_query,
                                                                   db=self.dqm_database_name,
                                                                   mode='create')

            return None
        except Exception as exc:
            logger.error(exc)
            raise exc

    def call_add_partition_dqm_dw(self, cycle_id, data_date):
        try:
            # Add partition
            logger.info("Adding Partitions for the DW dqm table")
            LaunchDDLCreationIngestionUtility().add_partitions_dw(db=self.dqm_database_name,
                                                               table=CommonConstants.DQM_DW_ERROR_DTL_TABLE_NAME,
                                                               cycle_id=cycle_id,
                                                                  data_dt=data_date)
            LaunchDDLCreationIngestionUtility().add_partitions_dw(db=self.dqm_database_name,
                                                                  table=CommonConstants.DQM_DW_SMRY_DTL_TABLE_NAME,
                                                                  cycle_id=cycle_id,
                                                                  data_dt=data_date)
            logger.info("Successfully Added Partitons for the DW dqm table")
        except Exception as exc:
            logger.error(exc)
            raise exc


if __name__ == "__main__":
    try:
        #command = "sudo /usr/bin/pip-3.7 uninstall boto3 -y"
        #execution_status = CommonUtils().execute_shell_command(command)

        #command1 = "sudo /usr/bin/pip-3.7 install boto3"
        #execution_status = CommonUtils().execute_shell_command(command1)

        #command2 = "sudo /usr/bin/pip-3.7 install --upgrade awscli"
        #execution_status = CommonUtils().execute_shell_command(command2)

        my_parser = argparse.ArgumentParser()
        my_parser.add_argument('--dataset_id', type=str, required=True)
        my_parser.add_argument('--step', type=str, required=True)
        my_parser.add_argument('--batch_id', type=str)
        my_parser.add_argument('--cycle_id', type=str)
        my_parser.add_argument('--data_dt', type=str)

        args = my_parser.parse_args()

        dataset_ids = args.dataset_id.split(',')
        steps = args.step.split(',')
        batch_id = args.batch_id
        cycle_id = args.cycle_id
        data_dt = args.data_dt

        logger.info(str(dataset_ids), steps, batch_id, cycle_id)

        for dataset_id in dataset_ids:
            for step in steps:
                createDDLobj = LaunchDDLCreationIngestionHandler(dataset_id=dataset_id,
                                                                 step=step,
                                                                 batch_id=batch_id,
                                                                 cycle_id=cycle_id)
                get_table_output = createDDLobj.get_table_details()
                #time.sleep(2)
                if get_table_output:
                    createDDLobj.call_add_partition_data(batch_id=batch_id, cycle_id=cycle_id, data_dt=data_dt)

            if(batch_id):
                logger.info("Starting to Create/Update Tables for DQM")
                createDDLobj.create_dqm_tables()
                #time.sleep(3)
                createDDLobj.call_add_partition_dqm(batch_id=batch_id)
            elif cycle_id:
                logger.info("Starting to Create/Update Tables for DW DQM tables")
                #createDDLobj.create_dw_dqm_tables()
                # time.sleep(3)
                #createDDLobj.call_add_partition_dqm_dw(cycle_id=cycle_id, data_date=data_dt)

    except Exception as exc:
        logger.error(str(exc))
        logger.error(traceback.format_exc())
        raise exc