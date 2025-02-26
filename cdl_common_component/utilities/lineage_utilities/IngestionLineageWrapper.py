#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   IngestionLineageWrapper
#  Purpose             :   Wrapper for Ingestion Lineage Utility.
#  Input Parameters    :   process_name,dataset_id,batch_id
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   25th June 2021
#  Last changed by     :   Ashish Unadkat
#  Reason for change   :
# ######################################################################################################################


import sys

from CustomS3Utility import CustomS3Utility
import os
import logging
import traceback
import CommonConstants as CommonConstants
from IngestionLineageUtility import IngestionLineageUtility
from ConfigUtility import JsonConfigUtility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
MODULE_NAME="IngestionLineageWrapper"
logger = logging.getLogger(MODULE_NAME)
from MySQLConnectionManager import MySQLConnectionManager


class IngestionLineageWrapper(object):
    def __init__(self,process_name,dataset_id=None,batch_id=None):
        self.dataset_id=dataset_id
        self.process_name=process_name

        self.batch_id=batch_id
        configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.bucket_name=configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])
        self.lineage_history_path=configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "ingestion_lineage_history_path"])
        self.lineage_latest_path = configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "ingestion_lineage_latest_path"])

    def get_success_dataset_list(self):
        try:
            query="select {dataset_id_col} from {db}.{tbl_nm}".format(
                dataset_id_col=CommonConstants.DATASET_ID_COL,
                db=self.audit_db,
                tbl_nm=CommonConstants.CTL_DATASET_MASTER_TBL
            )
            result = MySQLConnectionManager().execute_query_mysql(query, False)
            result_list=[]
            for row in result:
                dataset_detail={}
                dataset_id=row["dataset_id"]
                query="select {batch_id_col} from {db}.{tbl_nm} where " \
                      "{dataset_id_col}='{dataset_id}' and {batch_status_col}='{batch_status}' order by {batch_start_time_col} desc limit 1".format(
                    batch_id_col=CommonConstants.BATCH_ID_KEY,
                    db=self.audit_db,
                    tbl_nm=CommonConstants.BATCH_TABLE,
                    dataset_id_col=CommonConstants.DATASET_ID_COL,
                    dataset_id=dataset_id,
                    batch_status_col=CommonConstants.BATCH_STATUS_COL,
                    batch_status=CommonConstants.STATUS_SUCCEEDED,
                    batch_start_time_col=CommonConstants.BATCH_START_TIME_COL
                )
                logger.info("query to fetch success dataset details for standalone lineage is {query}".format(query=query))
                result = MySQLConnectionManager().execute_query_mysql(query, True)
                dataset_detail[CommonConstants.DATASET_ID_COL]=dataset_id
                dataset_detail[CommonConstants.BATCH_ID_KEY]=result[CommonConstants.BATCH_ID_KEY]
                result_list.append(dataset_detail)
            return result_list
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def get_standalone_lineage(self):
        try:
            result_list=self.get_success_dataset_list()
            logger.info("get_standalone_lineage result_list ==>>{result_list}".format(result_list=str(result_list)))
            for row in result_list:
                self.dataset_id=row[CommonConstants.DATASET_ID_COL]
                self.batch_id=row[CommonConstants.BATCH_ID_KEY]
                logger.info("running standalone lineage for dataset_id {dataset_id} and batch_id {batch_id}".format(
                    dataset_id=self.dataset_id,
                    batch_id=self.batch_id
                ))
                self.get_lineage()
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def save_lineage_output(self,lineage_json):
        try:
            logger.info("saving lineage to s3")
            lineage_history_batch_key = self.lineage_history_path.format(
                dataset_id=self.dataset_id
            )+"{batch_id_key}={batch_id}".format(batch_id_key=CommonConstants.BATCH_ID_KEY,batch_id=self.batch_id)
            lineage_latest_dataset_key = self.lineage_latest_path.format(
                dataset_id=self.dataset_id
            )
            lineage_latest_batch_key = lineage_latest_dataset_key+"{batch_id_key}={batch_id}".format(batch_id_key=CommonConstants.BATCH_ID_KEY,batch_id=self.batch_id)

            response = CustomS3Utility()._write_json_to_s3(self.bucket_name, lineage_history_batch_key+"/{lineage_file_nm}".format(lineage_file_nm=CommonConstants.LINEAGE_FILE_NM), lineage_json)
            logger.info("response for dataset lineage history {response}".format(response=str(response)))
            if response[CommonConstants.STATUS]==CommonConstants.FAILED:
                raise Exception("error in saving lineage output to path {path}".format(path=lineage_history_batch_key))
            CustomS3Utility().delete_all_files_from_folder(self.bucket_name,lineage_latest_dataset_key)
            response = CustomS3Utility()._write_json_to_s3(self.bucket_name, lineage_latest_batch_key+"/{lineage_file_nm}".format(lineage_file_nm=CommonConstants.LINEAGE_FILE_NM), lineage_json)
            logger.info("response for dataset lineage latest {response}".format(response=str(response)))
            if response[CommonConstants.STATUS]==CommonConstants.FAILED:
                raise Exception("error in saving lineage output to path {path}".format(path=lineage_latest_batch_key))

        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def check_run_id(self,run_id):
        try:
            query="select {run_id_col} from {db}.{tbl_nm} where {run_id_col}='{run_id}'".format(
                db=self.audit_db,
                tbl_nm=CommonConstants.LINEAGE_TBL_NM,
                run_id_col=CommonConstants.RUN_ID,
                run_id=run_id
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

    def get_lineage(self):
        try:
            run_id_already_present=self.check_run_id(self.batch_id)
            if not run_id_already_present:
                col_list=[CommonConstants.RUN_ID,CommonConstants.ENTITY_NAME,CommonConstants.ENTITY_TYPE,CommonConstants.PROCESS_NAME,CommonConstants.STATUS_COL,CommonConstants.START_TIME,CommonConstants.GRAPH_LOAD_COL]
                val_list=["'{run_id}'".format(run_id=self.batch_id),"'{entity_name}'".format(
                    entity_name=self.dataset_id),"'{entity_type}'".format(entity_type=CommonConstants.ENTITY_TYPE_DATASET),"'{process_name}'".format(process_name=self.process_name),
                          "'{status}'".format(status=CommonConstants.STATUS_RUNNING),"NOW()","'N'"]
                self.insert_lineage_dtl(col_list=col_list,val_list=val_list,table_name=CommonConstants.LINEAGE_TBL_NM)
            lineage=IngestionLineageUtility(self.dataset_id).generate_triplets()
            self.save_lineage_output(lineage)
            lineage_history_key = "s3://{bucket_name}/{lineage_history_path}{batch_id_key}={batch_id}/{lineage_file_name}".format(
                bucket_name=self.bucket_name,
                lineage_history_path=self.lineage_history_path.format(dataset_id=self.dataset_id),
                batch_id_key=CommonConstants.BATCH_ID_KEY,
                batch_id=self.batch_id,
                lineage_file_name=CommonConstants.LINEAGE_FILE_NM
            )
            self.update_lineage_status(status=CommonConstants.STATUS_SUCCEEDED,s3_location=lineage_history_key)
        except Exception as e:
            logger.error(traceback.print_exc())
            self.update_lineage_status(status=CommonConstants.FAILED)
            raise e

    def insert_lineage_dtl(self, col_list, val_list, table_name):
        try:
            logger.info("Starting function to insert_record for table {tbl_nm}".format(tbl_nm=table_name))
            columns = ",".join(col_list)
            values = ",".join(val_list)
            query = "Insert into {db_name}.{tbl_nm} ({columns}) values ({values})".format(
                db_name=self.audit_db,
                tbl_nm=table_name,
                columns=columns,
                values=values
            )
            logger.info("Query to insert the record: " + query)
            MySQLConnectionManager().execute_query_mysql(query, False)
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def update_lineage_status(self,status,s3_location=None):
        """

        :param status:
        :param s3_location:
        :return:
        """
        try:
            update_s3_location_subquery=""
            if s3_location:
                update_s3_location_subquery=",{s3_location_col}='{s3_location}'".format(
                    s3_location_col=CommonConstants.S3_LOCATION_COLUMN,
                    s3_location=s3_location
                )
            query="update {db}.{tbl_nm} set {status_col}='{status}',{end_time_col}=NOW(){subquery} where run_id='{run_id}'".format(
                db=self.audit_db,
                tbl_nm=CommonConstants.LINEAGE_TBL_NM,
                status_col=CommonConstants.STATUS_COL,
                end_time_col=CommonConstants.END_TIME_COL,
                status=status,
                run_id=self.batch_id,
                subquery=update_s3_location_subquery
            )
            logger.info("Query to update_lineage_status : " + query)
            MySQLConnectionManager().execute_query_mysql(query, False)
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e
        
if __name__ == '__main__':
    process_name = sys.argv[1]
    dataset_id = sys.argv[2]
    batch_id = sys.argv[3]
    obj=IngestionLineageWrapper(process_name,dataset_id=dataset_id,batch_id=batch_id)
    obj.get_lineage()