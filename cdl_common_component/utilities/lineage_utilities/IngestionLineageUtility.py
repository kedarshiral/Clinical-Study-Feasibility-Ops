#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   IngestionLineageUtility
#  Purpose             :   Generates Lineage for Ingestion Process
#  Input Parameters    :   dataset_id
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   25th June 2021
#  Last changed by     :   Ashish Unadkat
#  Reason for change   :
# ######################################################################################################################



from MySQLConnectionManager import MySQLConnectionManager
import traceback
import logging
import os
from ConfigUtility import JsonConfigUtility
import CommonConstants as CommonConstants
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
MODULE_NAME="ingestion_lineage_utility"
logger = logging.getLogger(MODULE_NAME)
class IngestionLineageUtility(object):
    def __init__(self,dataset_id):
        self.dataset_id=dataset_id
        configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def get_dataset_details(self):
        try:
            query="select {dataset_name},{provider},{staging_table},{geography_nm},{domain} from {db}.{dataset_master_table_nm}  " \
                  "inner join {db}.{geography_table} on {dataset_master_table_nm}.{geo_id_clmn}={geography_table}.{geo_id_clmn} and {dataset_id_col}='{dataset_id}'".format(
                db=self.audit_db,
                dataset_name=CommonConstants.DATASET_NAME_COL,
                provider=CommonConstants.PROVIDER_COL,
                staging_table=CommonConstants.STAGING_TABLE_COL,
                dataset_master_table_nm=CommonConstants.CTL_DATASET_MASTER_TBL,
                dataset_id_col=CommonConstants.DATASET_ID_COL,
                dataset_id=self.dataset_id,
                geography_nm=CommonConstants.GEO_NM,
                geography_table=CommonConstants.GEOGRAPHY_TABLE_NM,
                geo_id_clmn=CommonConstants.GEO_ID_CLMN,
                domain=CommonConstants.DOMAIN_CLMN
            )
            mysql_result = MySQLConnectionManager().execute_query_mysql(query, True)
            return mysql_result
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def generate_triplets(self):
        try:
            output=[]
            dataset_results=self.get_dataset_details()
            if not dataset_results:
                raise Exception("no records found for given dataset_id {dataset_id}".format(dataset_id=self.dataset_id))
            dataset_name = dataset_results[CommonConstants.DATASET_NAME_COL]
            provider = dataset_results[CommonConstants.PROVIDER_COL]
            geography_nm=dataset_results[CommonConstants.GEO_NM]
            domain=dataset_results[CommonConstants.DOMAIN_CLMN]
            staging_table = str(dataset_results[CommonConstants.STAGING_TABLE_COL]).split('.')[-1]
            if(len(str(dataset_results[CommonConstants.STAGING_TABLE_COL]).split('.'))>1):
                staging_database=str(dataset_results[CommonConstants.STAGING_TABLE_COL]).split('.')[-2]
            else:
                staging_database="default"

            dataset_json = {CommonConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: CommonConstants.DATASET_NODE_TYPE, CommonConstants.LINEAGE_ENTITY_NAME_KEY: dataset_name}
            provider_json = {CommonConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: CommonConstants.DATASOURCE_NODE_TYPE, CommonConstants.LINEAGE_ENTITY_NAME_KEY: provider}
            staging_table_json={CommonConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY:CommonConstants.TABLE_NODE_TYPE, CommonConstants.LINEAGE_ENTITY_NAME_KEY: staging_table, CommonConstants.LINEAGE_DB_NAME_KEY: staging_database}
            geography_json={CommonConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: CommonConstants.GEOGRAPHY_NODE_TYPE, CommonConstants.LINEAGE_ENTITY_NAME_KEY: geography_nm}
            domain_json={CommonConstants.LINEAGE_SUBJECT_OBJECT_TYPE_KEY: CommonConstants.DOMAIN_NODE_TYPE, CommonConstants.LINEAGE_ENTITY_NAME_KEY: domain}
            source_dataset_sop=self.create_sop(provider_json,dataset_json,CommonConstants.DATA_SOURCE_DATASET_PREDICATE)
            output.append(source_dataset_sop)
            dataset_staging_table_sop=self.create_sop(dataset_json,staging_table_json,CommonConstants.DATASET_TABLE_PREDICATE)
            output.append(dataset_staging_table_sop)
            dataset_geography_sop=self.create_sop(dataset_json,geography_json,CommonConstants.GEOGRAPHY_PREDICATE)
            output.append(dataset_geography_sop)
            dataset_domain_sop=self.create_sop(dataset_json,domain_json,CommonConstants.DOMAIN_PREDICATE)
            output.append(dataset_domain_sop)
            logger.debug("output sop list for ingestion lineage\n{output}".format(output=str(output)))
            return output
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

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




