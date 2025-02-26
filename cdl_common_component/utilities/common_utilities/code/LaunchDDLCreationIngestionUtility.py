#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

import logging
import os
import time
from copy import deepcopy

import CommonConstants
import boto3
from CommonUtils import CommonUtils
from ConfigUtility import JsonConfigUtility
from ExecutionContext import ExecutionContext
from PySparkUtility import PySparkUtility

MODULE_NAME = "LaunchDDLCreationIngestionUtility"

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')
hndlr = logging.StreamHandler()
hndlr.setLevel('INFO')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


class LaunchDDLCreationIngestionUtility:

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.bucket = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])
        self.region = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "aws_region"])
        self.athena_work_group = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "athena_work_group"])
        self.spark = PySparkUtility().get_spark_context()

    def execute_blocking_athena_query(self, query):
        try:
            logger.info("Executing query on athena for query: {0}".format(query))
            athena = boto3.client("athena", region_name=self.region)
            res = athena.start_query_execution(QueryString=query,WorkGroup=self.athena_work_group,ResultConfiguration={
                'OutputLocation': 's3://{s3_bucket}/Athena_Output/'.format(s3_bucket=self.bucket)
            })
            execution_id = res["QueryExecutionId"]
            while True:
                res = athena.get_query_execution(QueryExecutionId=execution_id)
                state = res["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    logger.info("Successfully Executed query on athena for query: {0}".format(query))
                    return
                if state in ["FAILED", "CANCELLED"]:
                    raise Exception(res["QueryExecution"]["Status"]["StateChangeReason"])
                time.sleep(1)
        except Exception as exc:
            logger.error(str(exc))
            raise exc

    def create_cross_platform_view(self, db, table, query):
        try:
            logger.info("Starting function to create cross-platform view")
            logger.info("database:{0}".format(db))
            logger.info("view_name:{0}".format(table))
            logger.info("query:{0}".format(query))
            glue = boto3.client("glue", region_name=self.region)
            # glue.delete_table(DatabaseName=db, Name=table)
            create_view_sql = query
            self.spark.sql("CREATE DATABASE IF NOT EXISTS {db}".format(db=db))
            self.execute_blocking_athena_query(create_view_sql)
            presto_schema = glue.get_table(DatabaseName=db, Name=table)["Table"][
                "ViewOriginalText"
            ]
            glue.delete_table(DatabaseName=db, Name=table)

            self.spark.sql(create_view_sql).show()
            spark_view = glue.get_table(DatabaseName=db, Name=table)["Table"]
            for key in [
                    "DatabaseName",
                    "CreateTime",
                    "UpdateTime",
                    "CreatedBy",
                    "IsRegisteredWithLakeFormation",
                    "CatalogId",
            ]:
                if key in spark_view:
                    del spark_view[key]
            spark_view["ViewOriginalText"] = presto_schema
            spark_view["Parameters"]["presto_view"] = "true"
            spark_view = glue.update_table(DatabaseName=db, TableInput=spark_view)
            return
        except Exception as exc:
            logger.error(str(exc))
            raise exc

    def execute_hive_query(self, query=None, table=None, db=None, mode='create'):
        try:
            self.spark.sql("CREATE database IF NOT EXISTS {db}".format(db=db))
            if mode == 'create':
                logger.info("Executing query:{query}".format(query=query))
                # self.spark.sql(query)
                self.execute_blocking_athena_query(query)
                logger.info("Successfully executed query:{query}".format(query=query))
            elif mode == 'create_alter':
                # drop_query = "DROP table {db}.{table_name}".format(db=db,table_name=table)
                create_query, alter_query = query.split(";")
                logger.info("Executing query:{query}".format(query=create_query))
                # self.spark.sql(create_query)
                self.execute_blocking_athena_query(create_query)
                logger.info("Successfully executed query:{query}".format(query=create_query))
                logger.info("Executing query:{query}".format(query=alter_query))
                ## Executing on athena as this type of alter query is not supported by spark sql.
                self.execute_blocking_athena_query(alter_query)
                # self.spark.sql(alter_query)
                logger.info("Successfully executed query:{query}".format(query=alter_query))
            elif mode == 'refresh':
                # refresh_query = "Refresh table {0}.{1}".format(db,table)
                repair_query = "MSCK REPAIR TABLE {0}.{1}".format(db, table)
                # logger.info("Executing query:{query}".format(query=refresh_query))
                # self.execute_blocking_athena_query(refresh_query)
                # self.spark.sql(refresh_query)
                # logger.info("Successfully execute query:{query}".format(query=refresh_query))
                logger.info("Executing query:{query}".format(query=repair_query))
                self.execute_blocking_athena_query(repair_query)
                # self.spark.sql(repair_query)
                logger.info("Successfully executed query:{query}".format(query=repair_query))
            else:
                logger.error("Mode was:" + mode)
                raise Exception("Invalid mode provided:Supported operations are create,create_alter,refresh")
        except Exception as exc:
            logger.error(str(exc))
            raise exc

    def add_partitions(self, db, table, batch_id):
        error_count = 0
        try:
            client = boto3.client('glue', region_name=self.region)

            for error_count in range(CommonConstants.RETRY_COUNT):
                try:
                    get_table = client.get_table(
                        DatabaseName=db,
                        Name=table
                    )
                    break
                except client.exceptions.EntityNotFoundException as exc:
                    time.sleep(2)

            storage_descriptor = get_table['Table']['StorageDescriptor']
            location = get_table['Table']['StorageDescriptor']['Location']

            file_id_list = []
            file_id_list_result = CommonUtils().get_file_ids_by_batch_id(self.audit_db, batch_id)
            for i in range(0, len(file_id_list_result)):
                file_id_list.append(file_id_list_result[i]['file_id'])

            values_list = []
            for file_id in file_id_list:
                values_list.append([batch_id, str(file_id)])

            logger.info("Values List:" + str(values_list))

            partition_input = []
            # temp_dict = {}
            for value in values_list:
                temp_dict = dict()
                temp_dict["Values"] = value
                temp_dict["StorageDescriptor"] = storage_descriptor
                temp_dict["StorageDescriptor"]["Location"] = os.path.join(location,
                                                                          "pt_batch_id=" + value[0],
                                                                          "pt_file_id=" + value[1])
                partition_input.append(deepcopy(temp_dict))

            create_partition_response = client.batch_create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInputList=partition_input
            )
            logger.info("Added Partitions: {partitions}".format(partitions=values_list))

        except Exception as exc:
            logger.error(exc)
            raise exc

    def add_partitions_dw(self, db, table, cycle_id, data_dt):
        try:
            client = boto3.client('glue', region_name=self.region)

            for error_count in range(CommonConstants.RETRY_COUNT):
                try:
                    get_table = client.get_table(
                        DatabaseName=db,
                        Name=table
                    )
                    break
                except client.exceptions.EntityNotFoundException as exc:
                    time.sleep(2)

            storage_descriptor = get_table['Table']['StorageDescriptor']
            location = get_table['Table']['StorageDescriptor']['Location']

            data_dt = data_dt.replace('-', '')
            values_list = [[data_dt, cycle_id]]

            partition_input = []
            for value in values_list:
                temp_dict = dict()
                temp_dict["Values"] = value
                temp_dict["StorageDescriptor"] = storage_descriptor
                temp_dict["StorageDescriptor"]["Location"] = os.path.join(location,
                                                                          "pt_data_dt=" + value[0],
                                                                          "pt_cycle_id=" + value[1])
                partition_input.append(deepcopy(temp_dict))

            create_partition_response = client.batch_create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInputList=partition_input
            )
            logger.info("Added Partitions: {partitions}".format(partitions=values_list))

        except Exception as exc:
            logger.error(exc)
            raise exc
