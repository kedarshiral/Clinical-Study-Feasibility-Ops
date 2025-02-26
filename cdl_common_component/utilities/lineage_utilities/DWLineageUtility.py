#!/usr/bin/python
# -*- coding: utf-8 -*

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = "ZS Associates"

import logging
import traceback
import boto3
import CommonConstants as ProfilerConstants
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
from CustomS3Utility import CustomS3Utility

# """
# Doc_Type            : DWLineageUtility
# Tech Description    : To get the relation triplets from code files to create the lineage.
# Pre_requisites      : N/A
# Inputs              : rule_file_path, process_id
# Outputs             : N/A
# Config_file         : ProfilerConstants/CommonConstants file, environment_params.json
# Last modified by    : Sukhwinder Singh
# Last modified on    : 16th March 2021
# """


MODULE_NAME = "DWLineageUtility"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(MODULE_NAME)


class DWLineageUtility:

    def __init__(self):
        """
        Purpose: To prepare the triplets for the DW lineage
        """
        self.s3 = boto3.resource("s3")
        self.configuration = JsonConfigUtility(ProfilerConstants.AIRFLOW_CODE_PATH + '/' + ProfilerConstants.ENVIRONMENT_CONFIG_FILE)
        self.profiler_db = self.configuration.get_configuration([ProfilerConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.mysql_conn = MySQLConnectionManager()

    def get_table_details(self, rule_file_name, dataset_detail, process_id=None):
        """

        :param rule_file_name:
        :param dataset_detail:
        :param process_id:
        :return:
        """
        response = dict()
        output=[]
        try:
            logger.info(dataset_detail)
            if dataset_detail[ProfilerConstants.TABLE_TYPE_COL_NAME].lower() == ProfilerConstants.SOURCE_TABLE_KEY.lower():
                table_detail_query = "select {staging_table_col} from {profiler_db}.{dataset_master_tbl} " \
                                       "where {dataset_id_col} = {dataset_id}".format(
                    staging_table_col=ProfilerConstants.STAGING_TABLE_COL_NAME,
                    profiler_db=self.profiler_db,
                    dataset_master_tbl=ProfilerConstants.DATASET_MASTER_TBL,
                    dataset_id_col=ProfilerConstants.DATASET_ID_COL_NAME,
                    dataset_id=dataset_detail[ProfilerConstants.DATASET_ID_COL_NAME]
                )
                logger.info(table_detail_query)
                table_detail = self.mysql_conn.execute_query_mysql(table_detail_query, True)
                if len(table_detail) != 0 and table_detail is not None and table_detail[ProfilerConstants.STAGING_TABLE_COL_NAME] is not None:
                    logger.info(table_detail_query )
                    staging_table = str(table_detail[ProfilerConstants.STAGING_TABLE_COL_NAME]).strip().split('.')[-1]
                    if(len(str(table_detail[ProfilerConstants.STAGING_TABLE_COL_NAME]).split('.'))>1):
                        staging_database=str(table_detail[ProfilerConstants.STAGING_TABLE_COL_NAME]).split('.')[-2]
                    else:
                        staging_database="default"
                    staging_table_json={ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY:ProfilerConstants.TABLE_NODE_TYPE, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: staging_table, ProfilerConstants.LINEAGE_DB_NAME_KEY: staging_database}
                    script_json={ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: ProfilerConstants.SUBJECT_PYTHON_SCRIPT_KEY, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: rule_file_name}

                    table_script_sop=self.create_sop(staging_table_json,script_json,ProfilerConstants.INPUT_PROCESS_KEY)
                    output.append(table_script_sop)
                    response[ProfilerConstants.TABLE_NAME_KEY] = table_detail[ProfilerConstants.STAGING_TABLE_COL_NAME].strip()

                else:
                    response[ProfilerConstants.TABLE_NAME_KEY] = None
            elif dataset_detail[ProfilerConstants.TABLE_TYPE_COL_NAME].lower() == ProfilerConstants.TARGET_TABLE_KEY.lower():
                table_detail_query = "select {table_name_col},{provider},{geography_nm},{domain},{dataset_name} from {profiler_db}.{dataset_master_tbl} " \
                                     "inner join {profiler_db}.{geography_table} on {dataset_master_tbl}.{geo_id_clmn}={geography_table}.{geo_id_clmn} and {dataset_id_col}='{dataset_id}'".format(
                    table_name_col=ProfilerConstants.TABLE_NAME_COL,
                    profiler_db=self.profiler_db,
                    provider=ProfilerConstants.PROVIDER_COL,
                    dataset_master_tbl=ProfilerConstants.DATASET_MASTER_TBL,
                    dataset_id_col=ProfilerConstants.DATASET_ID_COL_NAME,
                    dataset_id=dataset_detail[ProfilerConstants.DATASET_ID_COL_NAME],
                    geo_id_clmn=ProfilerConstants.GEO_ID_CLMN,
                    geography_table=ProfilerConstants.GEOGRAPHY_TABLE_NM,
                    geography_nm=ProfilerConstants.GEO_NM,
                    domain=ProfilerConstants.DOMAIN_CLMN,
                    dataset_name=ProfilerConstants.DATASET_NAME_COL
                )
                logger.info(table_detail_query )
                table_detail = self.mysql_conn.execute_query_mysql(table_detail_query, True)
                if len(table_detail) != 0 and table_detail is not None and table_detail[ProfilerConstants.TABLE_NAME_COL] is not None:

                    dataset_name = table_detail[ProfilerConstants.DATASET_NAME_COL]
                    provider = table_detail[ProfilerConstants.PROVIDER_COL]
                    geography_nm=table_detail[ProfilerConstants.GEO_NM]
                    domain=table_detail[ProfilerConstants.DOMAIN_CLMN]
                    target_table = str(table_detail[ProfilerConstants.TABLE_NAME_COL]).strip().split('.')[-1]
                    if(len(str(table_detail[ProfilerConstants.TABLE_NAME_COL]).split('.'))>1):
                        target_database=str(table_detail[ProfilerConstants.TABLE_NAME_COL]).split('.')[-2]
                    else:
                        target_database="default"

                    dataset_json = {ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: ProfilerConstants.DATASET_NODE_TYPE, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: dataset_name}
                    provider_json = {ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: ProfilerConstants.DATASOURCE_NODE_TYPE, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: provider}
                    target_table_json={ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY:ProfilerConstants.TABLE_NODE_TYPE, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: target_table, ProfilerConstants.LINEAGE_DB_NAME_KEY: target_database}
                    geography_json={ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: ProfilerConstants.GEOGRAPHY_NODE_TYPE, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: geography_nm}
                    domain_json={ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: ProfilerConstants.DOMAIN_NODE_TYPE, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: domain}
                    source_dataset_sop=self.create_sop(provider_json,dataset_json,ProfilerConstants.DATA_SOURCE_DATASET_PREDICATE)
                    output.append(source_dataset_sop)
                    dataset_target_table_sop=self.create_sop(dataset_json,target_table_json,ProfilerConstants.DATASET_TABLE_PREDICATE)
                    output.append(dataset_target_table_sop)
                    dataset_geography_sop=self.create_sop(dataset_json,geography_json,ProfilerConstants.GEOGRAPHY_PREDICATE)
                    output.append(dataset_geography_sop)
                    dataset_domain_sop=self.create_sop(dataset_json,domain_json,ProfilerConstants.DOMAIN_PREDICATE)
                    output.append(dataset_domain_sop)
                    script_json={ProfilerConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: ProfilerConstants.SUBJECT_PYTHON_SCRIPT_KEY, ProfilerConstants.LINEAGE_ENTITY_NAME_KEY: rule_file_name}

                    script_table_sop=self.create_sop(script_json,target_table_json,ProfilerConstants.OUTPUT_PROCESS_KEY)
                    output.append(script_table_sop)
                    response[ProfilerConstants.TABLE_NAME_KEY] = table_detail[ProfilerConstants.TABLE_NAME_COL].strip()
                else:
                    response[ProfilerConstants.TABLE_NAME_KEY] = None
            else:
                response[ProfilerConstants.TABLE_NAME_KEY] = None
            logger.debug("output sop list for ingestion lineage\n{output}".format(output=str(output)))
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_SUCCESS
            response[ProfilerConstants.RESULT_KEY] = output
            logger.info("table detail is: {0}".format(response))
            return response

        except Exception as e:
            error = "Failed to get the table name for dataset id '{dataset_id}'. " \
                    "ERROR is: {error}".format(dataset_id=dataset_detail[ProfilerConstants.DATASET_ID_COL_NAME],
                                               error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_FAILED
            response[ProfilerConstants.RESULT_KEY] = "Error"
            response[ProfilerConstants.ERROR_KEY] = error
            return response

    def create_sop(self,subject,object,predicate):
        try:
            sop={}
            sop["subject"]=subject
            sop["object"]=object
            sop["predicate"]=predicate
            return sop
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def main(self, rule_file_path, process_id):
        """

        :param rule_file_path:
        :param process_id:
        :return:
        """
        response = dict()
        triplets_list = []
        try:
            bucket_name = rule_file_path.split("//")[1].split("/", maxsplit=1)[0]
            s3_file_path = rule_file_path.split("//")[1].split("/", maxsplit=1)[1]
            rule_file_name = rule_file_path.rsplit("/", maxsplit=1)[1]
            rule_file_data = CustomS3Utility()._read_code_file_from_s3(bucket_name=bucket_name,key=s3_file_path)
            # for process_id in process_id_list:
            logger.info("process_id is: " + str(process_id))
            dataset_detail_query = "select {dataset_id_col}, {table_type_col} from {profiler_db}.{process_dependency_master_tbl} " \
                                   "where {process_id_col} = {process_id}".format(
                dataset_id_col=ProfilerConstants.DATASET_ID_COL_NAME,
                table_type_col=ProfilerConstants.TABLE_TYPE_COL_NAME,
                profiler_db=self.profiler_db,
                process_dependency_master_tbl=ProfilerConstants.PROCESS_DEPENDENCY_MASTER_TBL,
                process_id_col=ProfilerConstants.PROCESS_ID_COL_NAME,
                process_id=process_id
            )
            logger.info(dataset_detail_query)
            dataset_details = self.mysql_conn.execute_query_mysql(dataset_detail_query, False)
            for dataset_detail in dataset_details:
                table_details = self.get_table_details(rule_file_name=rule_file_name, dataset_detail=dataset_detail)
                if table_details[ProfilerConstants.STATUS_KEY] == ProfilerConstants.STATUS_SUCCESS:
                    table = table_details[ProfilerConstants.TABLE_NAME_KEY]
                    if table is not None:
                        #logger.info(rule_file_data)
                        for line in rule_file_data[ProfilerConstants.RESULT_KEY].split("\n"):
                            if line.strip().startswith("#"):
                                continue
                            elif table.strip().lower() in line.lower():
                                if table_details[ProfilerConstants.RESULT_KEY] not in triplets_list:
                                    triplets_list.extend(table_details[ProfilerConstants.RESULT_KEY])
                else:
                    logger.info(table_details)
                    return table_details
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_SUCCESS
            response[ProfilerConstants.RESULT_KEY] = triplets_list
            logger.info("triplet is: {0}".format(response))
            return response
        except Exception as e:
            error = "Failed to get the lineage for file '{file_name}'. " \
                    "ERROR is: {error}".format(file_name=rule_file_path, error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_FAILED
            response[ProfilerConstants.RESULT_KEY] = "Error"
            response[ProfilerConstants.ERROR_KEY] = error
            response[ProfilerConstants.FILEPATH_KEY] = rule_file_path
            return response

