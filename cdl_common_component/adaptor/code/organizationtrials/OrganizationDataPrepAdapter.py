#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Citeline Adaptor Data Prep | Organization
#  Purpose             :   This module will perform the will check for delta records in sponsor, status , disease and
#                          phase mappings.
#  Input Parameters    :
#  Output Value        :   Mail will be sent
#  Pre-requisites      :
#  Last changed on     :   15th Feb 2022
#  Last changed by     :   Sankarshana Kadambari
#  Reason for change   :   Enhancement to provide restartability at file level
# ######################################################################################################################

import traceback
from datetime import datetime
import json
import sys
import os

sys.path.insert(0, os.getcwd())
from os import path
from pyspark.sql import *

FILE_ABS_PATH = path.abspath(os.path.dirname(__file__))
SERVICE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, SERVICE_DIR_PATH)
CODE_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIR_PATH, "../common_utilities"))
sys.path.insert(1, CODE_DIR_PATH)
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants
from pyspark.sql import SQLContext
from CommonUtils import CommonUtils

spark_application_name = "creating structured file for organization"
spark = (SparkSession.builder.appName(spark_application_name).enableHiveSupport().getOrCreate())
sqlContext = SQLContext(spark)

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

try:
    APPLICATION_CONFIG_FILE = "application_config.json"
    print("os.getcwd()", os.getcwd())

    configuration = json.load(open(APPLICATION_CONFIG_FILE))
    audit_db = configuration["adapter_details"]["generic_config"]["mysql_db"]
    print("audit_db ", audit_db)

    query_org_str = (
        "SELECT structured_file_path FROM {audit_db}.{log_data_acquisition_dtl_table} where adapter_id=5 and payload_id=5 and lower(trim(step_name))='create_structure_file'" \
        "and status='SUCCEEDED' order by end_time desc  limit 1"
    )
    output_query = query_org_str.format(audit_db=audit_db,
                                        log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE
                                        )
    print("Query to get latest file from the log table:", output_query)
    response_org_path = MySQLConnectionManager().execute_query_mysql(output_query, False)

    # DJ
    response_org_path_full = response_org_path[0]["structured_file_path"]

    query_org_tri_str = (
        "SELECT structured_file_path FROM {audit_db}.{log_data_acquisition_dtl_table} where adapter_id=7 and payload_id=7 and lower(trim(step_name))='create_structure_file'" \
        "and status='SUCCEEDED' order by end_time desc  limit 1"
    )
    output_query = query_org_tri_str.format(audit_db=audit_db,
                                            log_data_acquisition_dtl_table=CommonConstants.LOG_DATA_ACQUISITION_DTL_TABLE
                                            )
    print("Query to get latest file from the log table:", output_query)
    response_org_tri_path = MySQLConnectionManager().execute_query_mysql(output_query, False)

    # DJ
    response_org_tri_path_full = response_org_tri_path[0]["structured_file_path"]

    print("Query output org tri  path structured: ", response_org_tri_path_full)
    print("Query output org path structured: ", response_org_path_full)

    org_data = spark.read.parquet(response_org_path_full)
    org_data = org_data.dropDuplicates()
    org_data.registerTempTable('org_data')

    org_tri_data = spark.read.parquet(response_org_tri_path_full)
    org_tri_data = org_tri_data.dropDuplicates()
    org_tri_data.registerTempTable('org_tri_data')

    organization_trial_pivot = spark.sql("""select organization_id,
    concat_ws('; ', collect_list(trial_id)) AS site_trial_id,
    concat_ws('; ', collect_list(trial_phase)) as site_trial_phase,
    concat_ws('; ', collect_list(trial_status)) as site_trial_status,
    concat_ws('; ', collect_list(trial_start_date)) as site_trial_start_date,
    concat_ws('; ', collect_list(trial_status_id)) as site_trial_status_id
    from org_tri_data order
    group by 1 """)
    organization_trial_pivot = organization_trial_pivot.dropDuplicates()
    organization_trial_pivot.createOrReplaceTempView('organization_trial_pivot')

    final_df = spark.sql("""select cc.site_trial_id,cc.site_trial_phase,cc.site_trial_status,cc.site_trial_status_id,cc.site_trial_start_date,bb.* from org_data bb 
    left join organization_trial_pivot cc on lower(trim(bb.site_id))=lower(trim(cc.organization_id))
    """)
    final_df = final_df.dropDuplicates()
    final_df.createOrReplaceTempView('final_df')

    final_df.repartition(1).write.format('parquet').mode('overwrite').save('/user/hive/warehouse/Organization')

    print(CommonConstants.AIRFLOW_CODE_PATH)
    org_file_path = CommonConstants.AIRFLOW_CODE_PATH + "/Organization"
    print(org_file_path)
    if os.path.exists(org_file_path):
        print("Inside remove if ", org_file_path)
        os.system("sudo rm -r " + org_file_path)

    os.system(
        "hadoop fs -copyToLocal /user/hive/warehouse/Organization/ "
        + CommonConstants.AIRFLOW_CODE_PATH
        + "/"
    )

    spark.stop()

    batch_id = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M%S")
    print("batch_id ", batch_id)
    final_org_file_name = "organization_ALL_" + batch_id + "_8.parquet"
    print("final_org_file_name", final_org_file_name)
    os.system("mv "
              + CommonConstants.AIRFLOW_CODE_PATH
              + "/Organization/*.parquet "
              + CommonConstants.AIRFLOW_CODE_PATH
              + "/" + final_org_file_name
              )

    s3_bucket_name = configuration["adapter_details"]["generic_config"]["s3_bucket_name"]
    print("s3_bucket_name ", s3_bucket_name)
    # os.system('aws s3 cp {Path}/{final_org_file_name} s3://{s3_bucket_name}/clinical-data-lake/pre-ingestion/CITELINE/CITELINE_ORGANIZATION/'.format(Path=CommonConstants.AIRFLOW_CODE_PATH,final_org_file_name=final_org_file_name,s3_bucket_name=s3_bucket_name))

    command_to_copy_parquet_file_to_s3 = "aws s3 cp {Path}/{final_org_file_name} s3://{s3_bucket_name}/clinical-data-lake/pre-ingestion/CITELINE/CITELINE_ORGANIZATION/".format(
        Path=CommonConstants.AIRFLOW_CODE_PATH, final_org_file_name=final_org_file_name, s3_bucket_name=s3_bucket_name)
    print(command_to_copy_parquet_file_to_s3)
    CommonUtils().execute_shell_command(command_to_copy_parquet_file_to_s3)
    print("File copied sucessfully")

except Exception as exception:
    message = "Error occurred in main Error : %s\nTraceback : %s" % (
        str(exception),
        str(traceback.format_exc()),
    )
    raise exception
