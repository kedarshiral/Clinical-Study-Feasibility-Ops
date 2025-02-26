# The below script is create backup table and remove data in actual table and to send the backup data into s3
"""
######################################################Module Information################################################
#   Module Name         :   Backup script
#   Purpose             :   The below script is create backup table and remove data in actual table and to send the backup data into s3
#   Created on          :   8th Febraury 2021
#   Created by          :   D S BRUNDA
#   Reason for change   :   Development
########################################################################################################################
"""
import datetime
import json
import os
import sys
import boto3
import pandas as pd

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../utilities/"))
sys.path.insert(0, UTILITIES_DIR_PATH)

import CommonServicesConstants as Constants
from urllib.parse import urlparse

from LogSetup import get_logger
LOGGING = get_logger()

from utils import Utils
UTILS_OBJ = Utils()

ENV_CONFIG_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "../configs/environment_config.json"))

env_file = open(ENV_CONFIG_PATH, 'r').read()
environment_config = json.loads(env_file)
app_config_dict = UTILS_OBJ.get_application_config_json()
UTILS_OBJ.initialize_variable(app_config_dict)


def to_check_table_exsists(schema_name, backup_table):
    try:
        check_table_exsists_query = """SELECT EXISTS( SELECT FROM pg_tables WHERE schemaname = '$$schema_name$$' AND tablename = '$$backup_table_name$$')"""
        check_table_exsists_query = check_table_exsists_query. \
            replace('$$backup_table_name$$', backup_table). \
            replace('$$schema_name$$', schema_name)
        LOGGING.info("check_table_exsists_query")
        LOGGING.info(check_table_exsists_query)
        check_table_exsists_query_output = UTILS_OBJ.execute_query(
            check_table_exsists_query, UTILS_OBJ.host, UTILS_OBJ.port, UTILS_OBJ.username,
            UTILS_OBJ.password, UTILS_OBJ.database_name)
        LOGGING.info("check_table_exsists_query_output")
        LOGGING.info(check_table_exsists_query_output)
        if check_table_exsists_query_output["status"] == "FAILED":
            raise Exception("Exception while running Query")
        if str(check_table_exsists_query_output["result"][0]["exists"]).lower() == "true":
            LOGGING.info("Table already exsists,Removing the table")
            drop_table_query = """DROP TABLE $$schema_name$$.$$backup_table_name$$"""
            drop_table_query = drop_table_query.replace('$$backup_table_name$$', backup_table). \
                replace('$$schema_name$$', schema_name)
            LOGGING.info("drop_table_query")
            LOGGING.info(drop_table_query)
            drop_table_query_output = UTILS_OBJ.execute_query(
                drop_table_query, UTILS_OBJ.host, UTILS_OBJ.port, UTILS_OBJ.username,
                UTILS_OBJ.password, UTILS_OBJ.database_name)
            if drop_table_query_output["status"] == "FAILED":
                raise Exception(" Exception while dropping table")
            else:
                LOGGING.info("Successfully dropped  the Table")
        else:
            LOGGING.info("Table do not exsists")
        return {"status": True}
    except Exception as e:
        LOGGING.info(str(e))
        return {"status": False}


def copy_data_to_table(schema_name, backup_table, table_name):
    try:
        LOGGING.info("Entered statement to create the table")
        create_table_query = """SELECT * INTO $$schema_name$$.$$backup_table_name$$ from( select * from $$schema_name$$.$$table_name$$) vb"""
        create_table_query = create_table_query.replace('$$backup_table_name$$', backup_table). \
            replace('$$schema_name$$', schema_name). \
            replace('$$table_name$$', table_name)
        LOGGING.info("create_table_query")
        LOGGING.info(create_table_query)
        create_table_query_output = UTILS_OBJ.execute_query(
            create_table_query, UTILS_OBJ.host, UTILS_OBJ.port, UTILS_OBJ.username,
            UTILS_OBJ.password, UTILS_OBJ.database_name)
        LOGGING.info("create_table_query_output status")
        LOGGING.info(create_table_query_output["status"])
        if create_table_query_output["status"] == "FAILED":
            raise Exception(" Exception while creating table")
        else:
            LOGGING.info("Successfully Created the Table")
        return {"status": True}

    except Exception as e:
        LOGGING.info(str(e))
        return {"status": False}


def copy_data_to_s3(schema_name, backup_table, bucket_name, environment, table_file_name,date_time_string_for_s3):
    try:
        # temp_path = os.path.join(SERVICE_DIRECTORY_PATH, "backup_scripts_temp_files_dir")
        temp_path = "/tmp/backup_scripts_temp_files_dir"
        LOGGING.info(temp_path)
        if not os.path.exists(temp_path):
            os.mkdir(temp_path)
        location = temp_path
        file_name = table_file_name + "_" + date_time_string_for_s3
        LOGGING.info("file name")
        LOGGING.info(file_name)
        date_folder = datetime.datetime.now().strftime('%Y-%m-%d')
        key = "/flask_table_unload/" + date_folder + "/" + file_name + ".csv"
        upload_s3_path = "s3://" + bucket_name + "/" + environment + "/flask_table_unload/" + file_name
        LOGGING.info("upload_s3_path name")
        LOGGING.info(upload_s3_path)
        select_file_data_query = """select * from $$schema_name$$.$$backup_table_name$$"""
        select_file_data_query = select_file_data_query.replace('$$backup_table_name$$', backup_table). \
            replace('$$schema_name$$', schema_name)
        LOGGING.info("file_data_query")
        LOGGING.info(select_file_data_query)
        file_export_list_output = UTILS_OBJ.postgres_csv_export(
            select_file_data_query, location, file_name, None, UTILS_OBJ.host,
            UTILS_OBJ.username, UTILS_OBJ.password, UTILS_OBJ.port, UTILS_OBJ.database_name
        )

        source_file = os.path.join(location, file_name) + ".csv"

        df = pd.read_csv(source_file)
        LOGGING.info(df)

        copy_result = copy_file_to_s3(source_file_path=source_file, destination_path=upload_s3_path,environment=environment, key=key, bucket_name=bucket_name)
        if copy_result["status"] == "FAILED":
            LOGGING.info("Failed to upload the file to S3. ERROR - " + copy_result["error"])
            raise Exception("Failed to upload the file to S3")
        else:
            LOGGING.info("Successfully added data to s3")
            os.remove(os.path.join(location, file_name) + ".csv")
            LOGGING.info("Sucessfully removed file in temp location")
            return {"status": True}

    except Exception as e:
        LOGGING.info(str(e))
        return {"status": False}


def remove_inactive_data_from_main_table(schema_name, main_table, active_flag_str):
    try:
        query_to_remove_data = """delete  from $$schema_name$$.$$table_name$$ where $$active_flag_str$$ """
        query_to_remove_data = query_to_remove_data.replace('$$table_name$$', main_table). \
            replace('$$schema_name$$', schema_name). \
            replace('$$active_flag_str$$', active_flag_str)
        LOGGING.info("query_to_remove_data")
        LOGGING.info(query_to_remove_data)
        query_to_remove_data_output = UTILS_OBJ.execute_query(
            query_to_remove_data, UTILS_OBJ.host, UTILS_OBJ.port, UTILS_OBJ.username,
            UTILS_OBJ.password, UTILS_OBJ.database_name)
        LOGGING.info("query_to_remove_data_output")
        LOGGING.info(query_to_remove_data_output)
        LOGGING.info("successfully removed in active data from " + main_table)
        vaccum_analyze_query = """VACUUM  ANALYZE $$schema_name$$.$$table_name$$"""
        vaccum_analyze_query = vaccum_analyze_query.replace('$$table_name$$', main_table). \
            replace('$$schema_name$$', schema_name). \
            replace('$$active_flag_str$$', active_flag_str)
        LOGGING.info("vaccum_analyze_query")
        LOGGING.info(vaccum_analyze_query)
        LOGGING.info("Vaccum Analyze Completed")
        return {"status": True}
    except Exception as e:
        LOGGING.info(str(e))
        return {"status": False}


def copy_file_to_s3(source_file_path, destination_path,environment, key, bucket_name, log_params=None):
        """
        Purpose : Copies file to S3 location
        :param source_file_path. string type. The file to be copied from the location.
        :param destination_path. string type. The file to be copied to.
        :return: output dict. json type.
        """
        output = dict()
        try:
            if source_file_path is None or len(source_file_path.strip()) == 0:
                error = "source_file_path can not be empty."
                raise Exception(error)

            if destination_path is None or len(destination_path.strip()) == 0:
                error = "destination_path can not be empty."
                raise Exception(error)

            # client = boto3.client('s3')
            s3 = boto3.resource('s3')
            key = environment + key

            LOGGING.info("Received request to copy file to s3 - " + source_file_path, extra=log_params)

            s3.meta.client.upload_file(source_file_path, bucket_name, key)

            # Original Code
            # if environment == "prod":
            #     file_copy_command = "aws s3 cp '{src}' '{target}' --sse".format(src=source_file_path,
            #                                                                                target=destination_path)
            # else:
            #     file_copy_command = "/usr/local/bin/aws s3 cp '{src}' '{target}' --sse".format(src=source_file_path,
            #                                                                     target=destination_path)
            # LOGGING.info("Command to copy the file - {}".format(file_copy_command))
            # file_copy_status = os.system(file_copy_command)
            # if file_copy_status != 0:
            #     raise Exception("Failed to upload the file")
            # path = urlparse(destination_path)
            message = "File copy to S3 was successful"
            LOGGING.debug(message)

            output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            output[Constants.RESULT_KEY] = message
            LOGGING.info("End of copy_file_to_s3", extra=log_params)
            return output
        except Exception as ex:
            error = "ERROR in copy_file_to_s3 - S3Utility ERROR MESSAGE: " + str(ex)
            LOGGING.error(error)
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = error
            return output



def main(schema_name, backup_table, table_name, bucket_name, environment, table_file_name, active_flag_str,date_time_string_for_s3):
    LOGGING.info("Checking table exisits")
    check_table_status = to_check_table_exsists(schema_name=schema_name, backup_table=backup_table)
    if check_table_status["status"]:
        LOGGING.info("Successfully checked the table")
        copy_data_to_table_status = copy_data_to_table(schema_name=schema_name, backup_table=backup_table,
                                                       table_name=table_name)
        if copy_data_to_table_status["status"]:
            LOGGING.info("Successfully Copied data to table")
            copy_data_to_s3_status = copy_data_to_s3(schema_name=schema_name, backup_table=backup_table,
                                                     bucket_name=bucket_name, environment=environment,
                                                     table_file_name=table_file_name,date_time_string_for_s3=date_time_string_for_s3)
            if copy_data_to_s3_status["status"]:
                LOGGING.info("sucessfully copied data to s3")
                remove_inactive_data_from_main_table_status = remove_inactive_data_from_main_table(
                    schema_name=schema_name, main_table=table_name, active_flag_str=active_flag_str)
                if remove_inactive_data_from_main_table_status['status']:
                    LOGGING.info("Sucessfully removed inactive data from main table")
                else:
                    raise Exception("Unable to remove inactive data")
            else:
                raise Exception("Unable to copy data to s3")
        else:
            raise Exception("Unable to copy data to table")
    else:
        raise Exception("Unable to check table exsists or not")


def start_job():
    table_list = ["f_rpt_scenario_country_ranking", "f_rpt_scenario_site_ranking", "f_rpt_user_scenario_survey_country_dtl",
                  "f_rpt_user_scenario_survey_site_investigator_dtl"]

    bucket_name = environment_config["bucket_name"]
    environment = environment_config["environment"]
    if environment == "demo":
        environment = "prod"
    # schema_name = "_" + environment

    if environment == "dev":
        schema_name = ""
    else:
        schema_name = "_" + environment

    LOGGING.info("bucket name  -->" + bucket_name)
    LOGGING.info("environment name  -->" + environment)
    date_time_string_for_s3 = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')

    for table in table_list:
        LOGGING.info("********************************")
        LOGGING.info("Performing Actions for " + table)
        backup_table = table +"_"+"flask_table_bckup_unload"
        active_flag_str = ""
        if table in ["f_rpt_scenario_country_ranking", "f_rpt_scenario_site_ranking"]:
            active_flag_str = "active_flag = 'N'"
        elif table in ["f_rpt_user_scenario_survey_country_dtl", "f_rpt_user_scenario_survey_site_investigator_dtl"]:
            active_flag_str = "is_active = false"

        if active_flag_str != "":
            main(schema_name=schema_name, backup_table=backup_table,
                 table_name=table, bucket_name=bucket_name, environment=environment,
                 table_file_name=table, active_flag_str=active_flag_str,date_time_string_for_s3=date_time_string_for_s3)
        else:
            LOGGING.info("active flag columns are choosen as null for table  -->" + str(table))
