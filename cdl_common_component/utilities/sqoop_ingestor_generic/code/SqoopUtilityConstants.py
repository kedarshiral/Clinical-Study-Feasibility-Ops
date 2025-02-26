#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

__author__ = 'ZS Associates'

"""
Module Name     :   SqoopUtilityConstants.py
Doc Type        :   Utility Constants
Tech Description:   This file contains the constants for SqoopIngestor.py
"""

SQL_KEY = "sqlserver"
ORACLE_KEY = "oracle"
POSTGRESQL_KEY = "postgresql"
MYSQL_KEY = "mysql"
CSV = "csv"
PARQUET = "parquet"
S3 = "s3"
S3_FOLDER_NAME_TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M-%S'
PERMITTED_DB_TYPES = [SQL_KEY, ORACLE_KEY, POSTGRESQL_KEY, MYSQL_KEY]
PERMITTED_OUTPUT_FILE_TYPES = [CSV, PARQUET]
DEFAULT_CLUSTER_USERNAME = "hadoop"
CLUSTER_SSH_PORT = "22"
CLUSTER_KEY_PATH = "/home/airflow/common_components/keys/aws-bigdatapod-emr-keypair.pem"
SECRET_MANAGER_SERVICE_NAME = "secretsmanager"
TEMP_S3_PATH = "s3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/onboarding/sqoop/tmp/"
CLUSTER_STATUS = ["STARTING", "BOOTSTRAPPING", "RUNNING"]
CLUSTER_STATUS_TERMINATE = ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']
CLUSTER_STATUS_WAITING = "WAITING"
HDFS_TEMP_PATH = "/home/sqoop"
HADOOP_DISTCP = "hadoop distcp "
HADOOP_LIST_FILE_COMMAND = "hadoop fs -ls /home/sqoop"
