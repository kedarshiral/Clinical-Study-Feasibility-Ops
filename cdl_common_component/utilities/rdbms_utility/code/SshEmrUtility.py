# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import pysftp
import argparse
import traceback
import json
from CommonUtils import CommonUtils
from DILogSetup import get_logger
logging = get_logger()
from SystemManagerUtility import SystemManagerUtility

STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCESS"
ERROR_KEY = "error"
validate_message = ""
JSON_FILE_PATH_KEY = "json_file_path"
CLUSTER_ID_KEY = "cluster_id"
REGION_NAME_KEY = "region_name"
PRIVATE_KEY_LOCATION_KEY = "private_key_location"
COMMAND_KEY = "command"

class SshEmrUtility:

    # def validate_inputs(self, cluster_id, region_name, private_key_location):
    #     """
    #         Purpose             :   This module performs below operation:
    #                                   a. Checks whether cluster_id,region_name,private_key_location
    #                                   is not empty and is of type string
    #         Input               :   cluster_id,region name,private_key_location(key-pair location)
    #         Output              :   returns status:SUCCESS/FAILED ,error:exception traceback
    #     """
    #     logging.info("Starting funtion validate")
    #     if (cluster_id != "" and type(cluster_id) == str):
    #         if (region_name != "" and type(region_name) == str):
    #
    #             if (private_key_location != "" and type(private_key_location) == str):
    #                 logging.debug("success")
    #                 return {STATUS_KEY: SUCCESS_KEY}
    #             else:
    #                 validate_message = "private_key_location must be a valid string"
    #                 logging.debug(validate_message)
    #         else:
    #             validate_message = "region_name must be a valid string"
    #             logging.debug(validate_message)
    #
    #     else:
    #         validate_message = "cluster_id must be a valid string"
    #         logging.debug(validate_message)
    #     logging.info("Completing funtion validate")
    #
    #     return {STATUS_KEY: FAILED_KEY, ERROR_KEY: validate_message}

    def execute_command(self, cluster_id, region_name, private_key_location, command):
        """
            Purpose             :   This module performs below operation:
                                      a. makes SSH connection and submits spark jobs
            Input               :   cluster_id,region name,private_key_location(key-pair location)
            Output              :   returns status:SUCCESS/FAILED
        """
        output=""
        try:
            logging.info("Starting function execute_command")

            # validate_message=self.validate_inputs(cluster_id, region_name, private_key_location)
            # if validate_message[STATUS_KEY]==SUCCESS_KEY:

            ip_list = CommonUtils().get_emr_ip_list(cluster_id,region_name)[RESULT_KEY]
            logging.debug(ip_list)

            #ip list returns a list containing one ip that is master's IP
            ip=ip_list[0]
            # print(ip)
            logging.debug("ip is "+ip)
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            with pysftp.Connection(ip, username='hadoop', private_key=private_key_location, cnopts=cnopts) as sftp:
                logging.info(command)  
                # Eg - command = " cd /home/hadoop/di_internal/code/;/usr/lib/spark/bin/spark-submit test_spark.py"
                output = sftp.execute(command)
                logging.debug(output)
            sftp.close()
            logging.info("Completing function execute_command")

        except:
            error_message=str(traceback.format_exc())
            logging.error("error message"+error_message)
            raise Exception(output)
            # return {STATUS_KEY:FAILED_KEY,ERROR_KEY:error_message}

        return {STATUS_KEY:SUCCESS_KEY,RESULT_KEY:output}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python utility to move/copy file from source to destination")
    parser.add_argument("--" + JSON_FILE_PATH_KEY, help="json file path containing file arguments", required=True)
    args = vars(parser.parse_args())
    file_path = args[JSON_FILE_PATH_KEY]
    # open json file and read key values
    with open(file_path) as file_object:
        data = json.load(file_object)
        cluster_id_input = data[CLUSTER_ID_KEY] if CLUSTER_ID_KEY in data else None
        region_name_input = data[REGION_NAME_KEY] if REGION_NAME_KEY in data else None
        private_key_location_input = data[PRIVATE_KEY_LOCATION_KEY] if PRIVATE_KEY_LOCATION_KEY in data else None
        command_input = data[COMMAND_KEY] if COMMAND_KEY in data else None

        execute_command_output = SystemManagerUtility().execute_command(
            cluster_id_input,command_input)

      
