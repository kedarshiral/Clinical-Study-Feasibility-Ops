#!/usr/bin/python
# -*- coding: utf-8 -*

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = "ZS Associates"

import logging
import sys
import traceback
import argparse
import boto3
import json
from datetime import datetime
import CommonConstants as ProfilerConstants
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
from CustomS3Utility import CustomS3Utility
from DWLineageUtility import DWLineageUtility

# """
# Doc_Type            : DWLineageWrapper
# Tech Description    : Get the lineage triplets for DW Modules
# Pre_requisites      : N/A
# Inputs              : rule_files_s3_path, cycle_id
# Outputs             : Lineage triplets will be saved in S3 location
# Config_file         : ProfilerConstants/CommonConstants file, environment_params.json
# Last modified by    : Sukhwinder
# Last modified on    : 16th April 2021
# """


MODULE_NAME = "DWLineageWrapper"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(MODULE_NAME)


class DWLineageWrapper:
    def __init__(self):
        """

        """
        self.s3 = boto3.resource("s3")
        self.configuration = JsonConfigUtility(ProfilerConstants.AIRFLOW_CODE_PATH + '/' + ProfilerConstants.ENVIRONMENT_CONFIG_FILE)
        self.profiler_db = self.configuration.get_configuration([ProfilerConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.bucket = self.configuration.get_configuration(
            [ProfilerConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])
        self.dw_lineage_history_path = self.configuration.get_configuration([ProfilerConstants.ENVIRONMENT_PARAMS_KEY, "dw_lineage_history_path"])
        self.dw_lineage_latest_path = self.configuration.get_configuration(
            [ProfilerConstants.ENVIRONMENT_PARAMS_KEY, "dw_lineage_latest_path"])
        self.mysql_conn = MySQLConnectionManager()

    def check_rule_and_run_id(self,run_id,entity_name):
        try:
            query="select {run_id_col} from {db}.{tbl_nm} where {run_id_col}='{run_id}' and {entity_name_col}='{entity_name}'".format(
                db=self.profiler_db,
                tbl_nm=ProfilerConstants.LINEAGE_TBL_NM,
                run_id_col=ProfilerConstants.RUN_ID,
                run_id=run_id,
                entity_name_col=ProfilerConstants.ENTITY_NAME,
                entity_name=entity_name
            )
            logger.info("Query to check_run_id in lineage table: " + query)
            result=MySQLConnectionManager().execute_query_mysql(query, True)
            if result:
                return True
            else:
                return False
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def log_start_process(self, run_id, entity_type, entity_name, process_name, status):
        """

        :param run_id:
        :param entity_type:
        :param entity_name:
        :param process_name:
        :param status:
        :return:
        """
        response = dict()
        try:
            start_query = "insert into {profiler_db}.{log_lineage_tbl} values({run_id},'{entity_type}'," \
                          "'{entity_name}','{process_name}','{status}',now(),NULL,'N',NULL,NULL)".format(
                profiler_db=self.profiler_db,
                log_lineage_tbl=ProfilerConstants.LOG_LINEAGE_TBL_NAME,
                run_id=run_id,
                entity_type=entity_type,
                entity_name=entity_name,
                process_name=process_name,
                status=status
            )
            self.mysql_conn.execute_query_mysql(start_query, False)
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_SUCCESS
            return response
        except Exception as e:
            error = "Failed while putting the process start log in lineage log table'. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_FAILED
            response[ProfilerConstants.RESULT_KEY] = "Error"
            response[ProfilerConstants.ERROR_KEY] = error
            return response


    def log_update_process(self, run_id, updated_status,entity_name,s3_location=None):
        """

        :param run_id:
        :param updated_status:
        :return:
        """
        response = dict()
        try:
            update_s3_location_subquery = ""
            if s3_location:
                update_s3_location_subquery = ",{s3_location_col}='{s3_location}'".format(
                    s3_location_col=ProfilerConstants.S3_LOCATION_COLUMN,
                    s3_location=s3_location
                )
            update_query = "update {profiler_db}.{log_lineage_tbl} set {status_col} = '{updated_status}', " \
                             "{end_time_col} = now(){subquery} where {run_id_col} = {run_id} and {entity_name_col}='{entity_name}'".format(
                profiler_db=self.profiler_db,
                log_lineage_tbl=ProfilerConstants.LOG_LINEAGE_TBL_NAME,
                status_col=ProfilerConstants.STATUS_COL,
                end_time_col=ProfilerConstants.END_TIME_COL,
                run_id_col=ProfilerConstants.RUN_ID_COL,
                run_id=run_id,
                updated_status=updated_status,
                subquery=update_s3_location_subquery,
                entity_name_col=ProfilerConstants.ENTITY_NAME,
                entity_name=entity_name
                )

            self.mysql_conn.execute_query_mysql(update_query, False)
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_SUCCESS
            return response
        except Exception as e:
            error = "Failed while updating the process log in lineage log table'. " \
                    "ERROR is: {error}".format(error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_FAILED
            response[ProfilerConstants.RESULT_KEY] = "Error"
            response[ProfilerConstants.ERROR_KEY] = error
            return response


    def get_process_id(self, file_path):
        """

        :param file_path:
        :return:
        """
        response = dict()
        try:
            file_name = file_path.rsplit("/", maxsplit=1)[1]
            process_ids_query = "select distinct {process_id_col_name}, {rule_id_col_name} from {profiler_db}.{rule_config_table} " \
                        "where {script_name_col} = '{file_name}'".format(
                process_id_col_name=ProfilerConstants.PROCESS_ID_COL_NAME,
                rule_id_col_name=ProfilerConstants.RULE_ID_COL_NAME,
                profiler_db=self.profiler_db,
                rule_config_table=ProfilerConstants.RULE_CONFIG_TABLE,
                script_name_col=ProfilerConstants.SCRIPT_NAME_COL,
                file_name=file_name
            )

            process_id_res = self.mysql_conn.execute_query_mysql(process_ids_query, False)
            process_id = process_id_res[0][ProfilerConstants.PROCESS_ID_COL_NAME]
            rule_id = process_id_res[0][ProfilerConstants.RULE_ID_COL_NAME]
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_SUCCESS
            response[ProfilerConstants.PROCESS_ID_KEY] = process_id
            response[ProfilerConstants.RULE_ID_KEY] = rule_id
            return response

        except Exception as e:
            error = "Failed to get the process ids for file '{file_path}'. " \
                    "ERROR is: {error}".format(file_path=file_path, error=str(e) + '\n' + str(traceback.format_exc()))
            logger.error(error)
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_FAILED
            response[ProfilerConstants.RESULT_KEY] = "Error"
            response[ProfilerConstants.ERROR_KEY] = error
            return response

    def main(self, rule_files_s3_path, cycle_id=None):
        """

        :param rule_files_s3_path:
        :return:
        """
        response = dict()
        try:
            s3_bucket = self.s3.Bucket(self.bucket)
            rule_files = CustomS3Utility()._s3_list_files(rule_files_s3_path)
            if rule_files[ProfilerConstants.STATUS_KEY] == ProfilerConstants.STATUS_FAILED:
                raise Exception(rule_files[ProfilerConstants.ERROR_KEY])
            bucket_name = rule_files_s3_path.split("//")[1].split("/", maxsplit=1)[0]
            rule_file_list = rule_files["result"]
            for file_path in rule_file_list:
                if len(file_path.strip().rsplit(".", maxsplit=1)) == 2 and file_path.strip().rsplit(".", maxsplit=1)[1] == ProfilerConstants.PYTHON_EXTENSION_KEY:
                    if cycle_id:
                        run_id = cycle_id
                    else:
                        run_id = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
                    entity_type = ProfilerConstants.ENTITY_TYPE_KEY
                    entity_name = file_path.strip().rsplit("/", maxsplit=1)[1].rsplit(".", maxsplit=1)[0]
                    process_name = ProfilerConstants.DW_PROCESS_NAME_KEY
                    status = ProfilerConstants.DW_RUNNING_STATUS_KEY
                    run_id_exist = self.check_rule_and_run_id(run_id,entity_name)
                    process_id_res = self.get_process_id(file_path=file_path)
                    history_lineage_path = str(self.dw_lineage_history_path).format(
                        process_id=str(process_id_res[ProfilerConstants.PROCESS_ID_KEY]),
                        cycle_id=str(run_id)) + ProfilerConstants.RULE_ID_KEY + "=" + str(process_id_res[
                                                                                              ProfilerConstants.RULE_ID_KEY]) + "/" + ProfilerConstants.LINEAGE_FILE_KEY + ".json"
                    history_lineage_s3_full_path="s3://{bucket_name}/{history_lineage_path}".format(
                        bucket_name=self.bucket,
                        history_lineage_path=history_lineage_path
                    )
                    latest_lineage_path = str(self.dw_lineage_latest_path).format(
                        process_id=str(process_id_res[ProfilerConstants.PROCESS_ID_KEY]),
                        cycle_id=str(run_id)) + ProfilerConstants.RULE_ID_KEY + "=" + str(process_id_res[
                                                                                              ProfilerConstants.RULE_ID_KEY]) + "/" + ProfilerConstants.LINEAGE_FILE_KEY + ".json"
                    if not run_id_exist:
                        log_start_process_res = self.log_start_process(run_id=run_id,
                                                                       entity_type=entity_type,
                                                                       entity_name=entity_name,
                                                                       process_name=process_name,
                                                                       status=status)

                        if log_start_process_res[ProfilerConstants.STATUS_KEY] == ProfilerConstants.STATUS_FAILED:
                            raise Exception(log_start_process_res[ProfilerConstants.ERROR_KEY])


                    logger.info("process_id is: " + str(process_id_res[ProfilerConstants.PROCESS_ID_KEY]))
                    if process_id_res[ProfilerConstants.STATUS_KEY] == ProfilerConstants.STATUS_FAILED:
                        self.log_update_process(run_id=run_id, updated_status=ProfilerConstants.DW_FAILED_STATUS_KEY,entity_name=entity_name)
                        raise Exception(process_id_res[ProfilerConstants.ERROR_KEY])
                    dw_lineage_obj = DWLineageUtility()
                    triplet_detail = dw_lineage_obj.main(rule_file_path=file_path, process_id=process_id_res[ProfilerConstants.PROCESS_ID_KEY])
                    if triplet_detail[ProfilerConstants.STATUS_KEY] == ProfilerConstants.STATUS_FAILED:
                        self.log_update_process(run_id=run_id, updated_status=ProfilerConstants.DW_FAILED_STATUS_KEY,entity_name=entity_name)
                        raise Exception(triplet_detail[ProfilerConstants.ERROR_KEY])

                    put_triplet_history_data_on_s3 = CustomS3Utility()._write_json_to_s3(bucket_name=bucket_name,
                                                                                 key=history_lineage_path,
                                                                                 data=triplet_detail[ProfilerConstants.RESULT_KEY])
                    if put_triplet_history_data_on_s3[ProfilerConstants.STATUS_KEY] == ProfilerConstants.STATUS_FAILED:
                        self.log_update_process(run_id=run_id, updated_status=ProfilerConstants.DW_FAILED_STATUS_KEY,entity_name=entity_name)
                        raise Exception(put_triplet_history_data_on_s3[ProfilerConstants.ERROR_KEY])
                    s3_bucket.objects.filter(Prefix=latest_lineage_path.rsplit("/", maxsplit=2)[0]).delete()
                    put_triplet_latest_data_on_s3 = CustomS3Utility()._write_json_to_s3(bucket_name=bucket_name,
                                                                                        key=latest_lineage_path,
                                                                                        data=triplet_detail[
                                                                                            ProfilerConstants.RESULT_KEY])
                    if put_triplet_latest_data_on_s3[ProfilerConstants.STATUS_KEY] == ProfilerConstants.STATUS_FAILED:
                        self.log_update_process(run_id=run_id, updated_status=ProfilerConstants.DW_FAILED_STATUS_KEY,entity_name=entity_name)
                        raise Exception(put_triplet_latest_data_on_s3[ProfilerConstants.ERROR_KEY])
                    self.log_update_process(run_id=run_id, updated_status=ProfilerConstants.DW_COMPLETED_STATUS_KEY,entity_name=entity_name,
                                                                       s3_location=history_lineage_s3_full_path)

        except Exception as e:
            error = "Failed to execute the lineage for files present in '{file_path}'. " \
                    "ERROR is: {error}".format(file_path=rule_files_s3_path, error=str(e) + '\n' + str(traceback.format_exc()))
            response[ProfilerConstants.STATUS_KEY] = ProfilerConstants.STATUS_FAILED
            response[ProfilerConstants.RESULT_KEY] = "Error"
            response[ProfilerConstants.ERROR_KEY] = error
            logger.error(response)
            raise e 


if __name__ == "__main__":
    RULE_FILES_S3_PATH=sys.argv[1]
    cycle_id=sys.argv[2]
    dw_lineage_wrapper_obj = DWLineageWrapper()
    dw_lineage_wrapper_obj.main(rule_files_s3_path=RULE_FILES_S3_PATH,cycle_id=cycle_id)



