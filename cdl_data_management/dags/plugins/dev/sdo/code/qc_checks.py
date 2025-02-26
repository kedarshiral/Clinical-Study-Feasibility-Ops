#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   File Validation
#  Purpose             :   This module will perform the pre-ingestion data validations.
#  Input Parameters    :   dataset_id
#  Last changed on     :   14th April 2022
#  Last changed by     :   Prashant Karan
# ######################################################################################################################

# Library and external modules declaration
import os.path
from datetime import datetime
import pandas as pd
import os
from pyspark.sql.functions import *
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from LogSetup import logger
from ConfigUtility import JsonConfigUtility
from PySparkUtility import PySparkUtility
from MySQLConnectionManager import MySQLConnectionManager
from pyspark.sql.types import StructType, StructField, StringType
from ExecutionContext import ExecutionContext

protocol_burden_df = ""
procedure_all_df = ""

module_name_for_spark = str("Pre Ingestion Data Validation")
spark_context = PySparkUtility(ExecutionContext()).get_spark_context(module_name_for_spark, CommonConstants.HADOOP_CONF_PROPERTY_DICT)

def execute_file_data_validations(master_ids=None, process_id=None, **kwargs):
    # threshold_count represents the total count of failed values that are acceptable wrt standard procedure name
    threshold_count = 0

    master_ids = ["901", "903", "902", "904", "905","906"]
    # spark_context = SparkSession.builder.appName("Pre Ingestion Data Validation").getOrCreate()

    data_validation_failed_flag = False
    count_mismatch_flag = False
    time_events_smry = {}
    time_events_data_valid_flag = True
    investigational_arm_flag = True
    file_validation_summary = {
    }

    # Primary grains of the files
    primary_grain = {
        "901": ['protocol_no', 'revision_no', 'arm_id'],  # DS_STUDY_METADATA
        "902": ['protocol_no', 'revision_no', 'arm_id', 'inclusion_exclusion_criteria_flag'],  # DS_ELIGIBILITY
        "903": ['protocol_no', 'revision_no','endpoint_type', 'standardized_endpoint','arm_id'],  # DS_ENDPOINTS
        "904": ['concept_id','standard_procedure_name'],  # DS_PROTOCOL_BURDEN
        "905": ['protocol_no', 'revision_no', 'arm_id', 'scheduled_stage', 'standard_procedure_name', 'visit_no'] , # DS_TIME_EVENTS
        "906": ['protocol_no', 'revision_no','standard_procedure_name','endpoint_type','standard_endpoint_name']  # DS_PROCEDURE_ENDPOINT
    }
    configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    cluster_mode = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "cluster_mode"])
    study_metadata_count = 0
    dataset_records_count = {}
    dataset_null_duplicate_smry = {}

    #count check
    start_time_now = datetime.utcnow()
    aws_move_commands_for_file =[]
    aws_rm_commands_for_file = []

    # Dataset info's to be validated and files to be put in the pre ingestion location after renaming in the required format
    for dataset_id in master_ids:
        dataset_info = CommonUtils().get_dataset_info(dataset_id)
        source_file_pattern = dataset_info['file_name_pattern']
        source_file_location = dataset_info['file_source_location'].replace('pre-ingestion','pre-ingestion-datavalidation')
        # To fetch s3 file list located at source location
        s3_file_list = CommonUtils().list_files_in_s3_directory(source_file_location, cluster_mode, source_file_pattern)
        if not s3_file_list:
            raise Exception("No files available for Processing")
        logger.info("s3_file_list --> %s", str(s3_file_list))
        # retrieve schema from ctl_column_metadata
        schema_query_str = "select column_name from {audit_db}.{column_metadata_table} where dataset_id={dataset_id} order by column_sequence_number"
        schema_query = schema_query_str.format(audit_db=audit_db,column_metadata_table='ctl_column_metadata',dataset_id=dataset_id)
        column_names_result = MySQLConnectionManager().execute_query_mysql(schema_query)
        column_name_list = []
        for column_result in column_names_result:
            column_name_list.append(column_result['column_name'])
        schema = StructType([StructField(column_name, StringType(), True) for (column_name) in column_name_list])
        print("--- Scehma for dataset : {}".format(dataset_id))
        print(schema)
        for files in s3_file_list:
            file_df = spark_context.read.format("csv").option("header", "true").option("delimiter", ",").option("quoteAll", "true").option("multiLine", "true").option("escape", "\"").schema(schema).load(files[:((files.rfind('/'))+1)])
            total_rows_count = file_df.count()
            file_df = file_df.replace({r'\r': ''})
        if dataset_id not in ["904","906"]:
                total_grouped_rows_count=file_df.groupBy('protocol_no', 'revision_no','arm_id').count().count()
                if source_file_pattern.__contains__("studymetadata"):
                    study_metadata_count=total_grouped_rows_count
                    dataset_records_count[dataset_id] = total_grouped_rows_count
                else:
                    if total_grouped_rows_count==study_metadata_count:
                        dataset_records_count[dataset_id]=total_grouped_rows_count
                    else:
                        count_mismatch_flag=True
                        data_validation_failed_flag = True
                        dataset_records_count[dataset_id] = total_grouped_rows_count



        # null and duplicate check
        null_duplicate_validation_result = execute_primarykey_data_check(dataset_id=dataset_id,file_dataframe=file_df,primary_grain=primary_grain[dataset_id])
        null_duplicate_validation_smry = null_duplicate_validation_result[0]
        final_df = null_duplicate_validation_result[1]
        if null_duplicate_validation_smry["data_validation_failed_flag"]:
            data_validation_failed_flag = True
        dataset_null_duplicate_smry[dataset_id]=null_duplicate_validation_smry
        output_path_s3 = dataset_info['file_source_location'] + datetime.strftime((start_time_now), "%Y%m%d%H%M%S")
        final_df.write.format("csv").option("header", "true").option("delimiter", ",").option("quoteAll", "true").option("multiLine", "true").option("escape", "\"").save(output_path_s3)
        print("Dataset {} Completed ".format(dataset_id))

        if dataset_id == "904":
            protocol_burden_df = final_df
            protocol_burden_df = protocol_burden_df.withColumn('standard_procedure_name', trim(col('standard_procedure_name')))
            protocol_burden_df = protocol_burden_df.withColumn('standard_procedure_name', lower(col('standard_procedure_name')))
            protocol_burden_df.registerTempTable('protocol_burden_table')

        if dataset_id == "901":
            study_metadata_df = final_df
            study_metadata_df.registerTempTable('study_metadata_table')

            # investigational_arm should be from arm
            investigational_arm_df = spark_context.sql(""" select protocol_no,revision_no,sum(inv_number) as inv_sum from
                                                        (select protocol_no,revision_no,
                                                            case when investigational_arm = arm_id then 1 else 0 end as inv_number
                                                          from 
                                                            (select protocol_no,revision_no,
                                                            lower(trim(investigational_arm_id)) as investigational_arm,
                                                            lower(trim(arm_ids)) as arm_id
                                                            from study_metadata_table 
                                                            lateral view explode(split(investigational_arm, '\\\|')) as investigational_arm_id
                                                            lateral view explode(split(arm_id, '\\\|')) as arm_ids
                                                            group by 1,2,3,4) q1) q2 group by 1,2 having inv_sum<1""")

            if investigational_arm_df.count () > 0 :
                data_validation_failed_flag = True
                investigational_arm_flag = False
                investigational_arm_df_1 = "Study Metadata Data Invalid Smry :: study_metadata contains investigational_arm level issues "
                investigational_arm_df_2 = investigational_arm_df.collect()
            else:
                investigational_arm_df_1 = ""
                investigational_arm_df_2 = ""

        if dataset_id == "905":
            time_events_df = final_df
            time_events_df.registerTempTable('time_events_table')

            # no multiple visit_day for a visit_no
            # for a particular SOA (protocol_no , revision_no , arm_id) no two visit_day can have same visit_no
            multiple_visit_day_df = spark_context.sql(""" select protocol_no,revision_no,arm_id,scheduled_stage,standard_procedure_name,visit_no,count(distinct visit_day)
              from time_events_table group by 1,2,3,4,5,6 having count(distinct visit_day) > 1""")

            # no multiple visit_no for a visit_day
            # for a particular SOA (protocol_no , revision_no , arm_id) no two visit_no can have same visit_day
            multiple_visit_number_df = spark_context.sql(""" select protocol_no,revision_no,arm_id,scheduled_stage,standard_procedure_name,visit_day,count(distinct visit_no)
                          from time_events_table group by 1,2,3,4,5,6 having count(distinct visit_no) > 1 """)

            # no visit_no can be present in multiple stages
            # for a particular SOA (protocol_no , revision_no , arm_id) a particular visit_no can have only one unique scheduled_stage
            multiple_stages_df = spark_context.sql(""" select protocol_no,revision_no,arm_id,visit_no,count(distinct scheduled_stage)
                                      from time_events_table group by 1,2,3,4 having count(distinct scheduled_stage) > 1 """)

            # treatment stage present in every protocol
            # checking if treatment stage is present in every protocol or not at arm level
            treatment_stages_df = spark_context.sql(""" select protocol_no,revision_no,arm_id,count(distinct scheduled_stage)
                                                  from time_events_table where lower(trim(scheduled_stage)) like '%treatment%'
                                                  group by 1,2,3 having count(distinct scheduled_stage) = 0 """)

            if multiple_visit_day_df.count () > 0 :
                time_events_data_valid_flag = False
                multiple_visit_day_df_1 = "Time Events Data Invalid Smry :: ds_time_events contains visit level issues , no multiple visit_day for a visit_no "
                multiple_visit_day_df_2 = multiple_visit_day_df.collect()
            else:
                multiple_visit_day_df_1=""
                multiple_visit_day_df_2=""

            if multiple_visit_number_df.count () > 0 :
                time_events_data_valid_flag = False
                multiple_visit_number_df_1 = "Time Events Data Invalid Smry :: ds_time_events contains visit level issues, no multiple visit_no for a visit_day "
                multiple_visit_number_df_2 = multiple_visit_number_df.collect()
            else:
                multiple_visit_number_df_1=""
                multiple_visit_number_df_2=""

            if multiple_stages_df.count () > 0 :
                time_events_data_valid_flag = False
                multiple_stages_df_1 = "Time Events Data Invalid Smry :: ds_time_events contains visit level issues, no visit_no can be present in multiple stages "
                multiple_stages_df_2 = multiple_stages_df.collect()
            else:
                multiple_stages_df_1=""
                multiple_stages_df_2=""

            if treatment_stages_df.count () > 0 :
                time_events_data_valid_flag = False
                treatment_stages_df_1 = "Time Events Data Invalid Smry :: ds_time_events contains visit level issues, Treatment stage is not present in every SOA "
                treatment_stages_df_2 = treatment_stages_df.collect()
            else:
                treatment_stages_df_1=""
                treatment_stages_df_2=""

            if time_events_data_valid_flag == False:
                data_validation_failed_flag = True

        #SUCCESS FILE REMOVAL
        success_file_path = dataset_info['file_source_location'] + datetime.strftime((start_time_now), "%Y%m%d%H%M%S") +"/_SUCCESS"
        success_file_remove_cmd="aws s3 rm {source}".format(source=success_file_path)
        os.system(success_file_remove_cmd)
        # Renaming the CSV files created
        source_file_location_files = dataset_info['file_source_location'] + datetime.strftime((start_time_now), "%Y%m%d%H%M%S") +"/"
        source_file_location_files_pattern = None
        s3_file_list_to_move = CommonUtils().list_files_in_s3_directory(source_file_location_files, cluster_mode, source_file_location_files_pattern)
        logger.info("s3_file_list --> %s", str(s3_file_list_to_move))
        final_move_path_s3 = dataset_info['file_source_location'] + source_file_pattern[:(source_file_pattern.find('_'))]+"_all_" + datetime.strftime((start_time_now), "%Y%m%d%H%M%S") + "_01.csv"
        aws_mv_command="aws s3 mv {source} {destination}".format(source=s3_file_list_to_move[0], destination=final_move_path_s3)
        aws_move_commands_for_file.append(aws_mv_command)
        aws_rm_command = "aws s3 rm {source}".format(source=s3_file_list_to_move[0])
        aws_rm_commands_for_file.append(aws_rm_command)

    print("AWS_RM_COMMANDS -----")
    print(aws_rm_commands_for_file)
    standard_procedure_check_failed_flag = False

    # Column names for null check wrt standard procedure name
    list_of_not_null_columns_wrt_procedures = ["tufts_procedure_name", "oncology_burden_score", "non_oncology_burden_score", "median_burden_score", "site_burden_score", "cost_burden_score", "procedure_category"]
    where_str = ""
    if len(list_of_not_null_columns_wrt_procedures) > 0:
        where_str = " where "
    else:
        where_str = " where false    "
    for cols in list_of_not_null_columns_wrt_procedures:
        where_str += " " + cols + " is null OR trim(" + cols + ")='' OR "


    file_validation_summary["grouped_record_count"]=dataset_records_count
    file_validation_summary["null_duplicate_check_smry"] = dataset_null_duplicate_smry
    file_validation_summary["data_validation_failed_flag"] = data_validation_failed_flag
    validation_results_path = (dataset_info['file_source_location'].replace('pre-ingestion', 'validation-summary'))[:-1]
    validation_file_name="file_validation_smry_{}.csv".format(datetime.strftime((start_time_now), "%Y%m%d%H%M%S"))
    validation_results_path = validation_results_path[:(validation_results_path.rfind('/'))]+"/" + validation_file_name
    validation_results_path_procedure = validation_results_path[:(validation_results_path.rfind('/'))] + "/standard_procedure_name/" + validation_file_name
    validation_results_path_procedure_score_not_null = validation_results_path[:(validation_results_path.rfind('/'))] + "/standard_procedure_name_for_null_scores/" + validation_file_name
    tem_path_emr_smry = os.getcwd()
    tem_path_emr_smry = str(tem_path_emr_smry) + "file_validation_smry_{}.csv".format(datetime.strftime((start_time_now), "%Y%m%d%H%M%S"))
    smry_df = pd.DataFrame.from_dict(file_validation_summary)
    smry_df.to_csv(tem_path_emr_smry)
    aws_cmmd_smry = "aws s3 mv {source} {destination}".format(source=tem_path_emr_smry, destination=validation_results_path)
    os.system(aws_cmmd_smry)
    print("Validation Summary :")
    print(file_validation_summary)
    print(multiple_visit_day_df_1)
    print(multiple_visit_day_df_2)
    print(multiple_visit_number_df_1)
    print(multiple_visit_number_df_2)
    print(multiple_stages_df_1)
    print(multiple_stages_df_2)
    print(treatment_stages_df_1)
    print(treatment_stages_df_2)
    print(investigational_arm_df_1)
    print(investigational_arm_df_2)

    # Copies the files to the required location if the validation is successful else flags the error.
    try:
        if file_validation_summary["data_validation_failed_flag"]:
            print("Source file data validation error. Data is not as expected")
            for rm_cmds in aws_rm_commands_for_file:
                os.system(rm_cmds)
            raise exception
        else:
            for mv_cmds in aws_move_commands_for_file:
                os.system(mv_cmds)
            print("File validation Successfull")
    except exception as e:
        print("Source file data validation error. Data is not as expected")

# This function performs the primary key data checks for each dataset
def execute_primarykey_data_check(dataset_id=None, file_dataframe=None, primary_grain=None):
    for grain_name in primary_grain:
        try:
            if dataset_id!="904":
                file_dataframe = file_dataframe.withColumn(grain_name, trim(col(grain_name)))
                file_dataframe = file_dataframe.withColumn(grain_name, lower(col(grain_name)))
        except:
            print("In exception block")
    dataset_null_duplicate_summary = {
        "is_null" : {},
        "is_duplicates_present":False,
        "duplicate_records_count":0,
        "data_validation_failed_flag": False
    }
    for grain_name in primary_grain:
        # null checks on grain
        null_flag_for_grain=False
        null_record_count = file_dataframe.filter(col(grain_name).isNull()).count()
        if null_record_count > 0: null_flag_for_grain=True
        dataset_null_duplicate_summary["is_null"][grain_name]= null_flag_for_grain
        if null_flag_for_grain == True:
            dataset_null_duplicate_summary["data_validation_failed_flag"] = True
    duplicate_record_count= file_dataframe.groupBy(primary_grain).count().where('count>1').count()
    dataset_null_duplicate_summary["duplicate_records_count"]=duplicate_record_count
    if duplicate_record_count>0:
        dataset_null_duplicate_summary["data_validation_failed_flag"] = True
        dataset_null_duplicate_summary["is_duplicates_present"] = True
    return dataset_null_duplicate_summary,file_dataframe

if __name__ == '__main__':
    execute_file_data_validations()


