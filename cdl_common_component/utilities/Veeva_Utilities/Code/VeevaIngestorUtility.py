#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

import re
import argparse
import json
import time
from urllib.parse import urlparse
import requests
import boto3
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
import XmlPayloadVeeva as XmlPayload
import SecretsManagerUtility as secret
from ConfigUtility import JsonConfigUtility


MODULE_NAME = "VeevaIngestorUtility"
PROCESS_NAME = "SalesForce Data Ingestor"


class VeevaIngestor(object):

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                               CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.client = boto3.client('s3')

    def trigger_veeva_ingestor(self, config_file=None):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_RUNNING, CommonConstants.ERROR_KEY: ""}

        try:
            credentials = secret.get_secret(config_file['credentials'],
                                            self.configuration.get_configuration
                                            ([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
            username = credentials['username']
            password = credentials['password']
            query = config_file['query']
            tb_name = config_file['tb_name']
            tgt_s3path = config_file['target_s3_path']
            sf_base_url = config_file['base_url']
            query_type = config_file['query_type']
            content_type = config_file['content_type'].upper()
            if not config_file['credentials'] or not config_file['query'] or not \
                    config_file['tb_name'] or not config_file['target_s3_path'] or not config_file['base_url'] or not \
                    config_file['query_type'] or not config_file['content_type']:
                status = "Failed due to missing parameters from Json Input"
                logger.info(status, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: "Must provide all the inputs needed - username, "
                                                                "password, query, table name,target s3 path, "
                                                                "base URL, query type, content type"}
                return result_dictionary
        except Exception as e:
            error_message = "Json is missing mandatory keys, Please check the Json load and rectify. " \
                            "The error is: %s" % str(e)
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: error_message}
            return result_dictionary

        try:
            base_url = sf_base_url + "/Soap/u/44.0"
            login = XmlPayload.login
            login = login.replace('$username', username)
            login = login.replace('$password', password)

            # Prepare header
            headers = {'Content-Type': 'text/xml; charset=UTF-8', 'SOAPAction': 'login'}
            # Execute the request through request package
            response = requests.post(base_url, data=login, headers=headers)

            # Parse response and get information
            match = re.search('(?<=<sessionId>)(.*)(?=</sessionId>)', response.content.decode("utf-8"))

            if match is not None:
                session_id = match.group()
                message = "session id generated:" + session_id
                logger.info(message, extra=self.execution_context.get_context())
            else:
                match = re.search('(?<=<exceptionMessage>)(.*)(?=</exceptionMessage>)', response.content)
                error_message = "session id not generated and Fault Code is:" + match.group()
                logger.error(error_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: "Authentication Failed." + error_message}
                return result_dictionary

            # Define URL
            base_url = sf_base_url + "/async/41.0"
            job = XmlPayload.job
            job = job.replace('$query_type', query_type)
            job = job.replace('$tb_name', tb_name)
            job = job.replace('$content_type', content_type)

            message = "Job has been initiated"
            logger.info(message, extra=self.execution_context.get_context())

            headers = {'X-SFDC-Session': session_id, 'Content-Type': 'application/xml; charset=UTF-8'}
            response = requests.post(base_url + "/job", data=job, headers=headers)
            match = re.search('(?<=<id>)(.*)(?=</id>)', response.content.decode("utf-8"))
            if match is not None:
                job_id = match.group()
                message = "Job ID created is:" + job_id
                logger.info(message, extra=self.execution_context.get_context())
            else:
                match = re.search('(?<=<exceptionMessage>)(.*)(?=</exceptionMessage>)',
                                  response.content.decode("utf-8"))
                error_message = "Job id not generated and Fault Code is:" + match.group()
                logger.error(error_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: error_message}
                return result_dictionary

            match = re.search('(?<=<state>)(.*)(?=</state>)', response.content.decode("utf-8"))
            if match is not None:
                job_state = match.group()
            else:
                match = re.search('(?<=<exceptionMessage>)(.*)(?=</exceptionMessage>)',
                                  response.content.decode("utf-8"))
                error_message = "Job State not generated and Fault Code is:" + match.group()
                logger.error(error_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: error_message}
                return result_dictionary

            if job_state != 'Open':
                error_message = "account - Job State is Not Open" + job_state
                logger.debug(message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: error_message}
                return result_dictionary

            base_url = sf_base_url + "/async/41.0/job/" + job_id + "/batch"

            headers = {'X-SFDC-Session': session_id, 'Content-Type': 'text/csv; charset=UTF-8'}
            response = requests.post(base_url, data=query, headers=headers)
            match = re.search('(?<=<id>)(.*)(?=</id>)', response.content.decode("utf-8"))

            if match is not None:
                batch_id = match.group()
            else:
                match = re.search('(?<=<exceptionMessage>)(.*)(?=</exceptionMessage>)',
                                  response.content.decode("utf-8"))
                error_message = "Batch ID not generated and Fault Code is:" + match.group()
                logger.error(error_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: error_message}
                return result_dictionary

            match = re.search('(?<=<state>)(.*)(?=</state>)', response.content.decode("utf-8"))

            if match is not None:
                batch_status = match.group()
            else:
                match = re.search('(?<=<exceptionMessage>)(.*)(?=</exceptionMessage>)',
                                  response.content.decode("utf-8"))
                error_message = "Batch Status not generated and Fault Code is:" + match.group()
                logger.error(error_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: error_message}
                return result_dictionary

            if batch_status == "Failed":
                match = re.search('(?<=<stateMessage>)(.*)(?=</stateMessage>)', (response.content).decode("utf-8"))
                batch_status_message = match.group()
                error_message = "account - In Failed" + batch_status_message
                logger.error(error_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: error_message}
                return result_dictionary

            headers = {'X-SFDC-Session': session_id, 'Content-Type': 'application/JSON; charset=UTF-8'}

            response = requests.get(base_url, headers=headers)
            batch_list = re.finditer('(?<=<id>)(.*)(?=</id>)', response.content.decode("utf-8"))
            base_uri = sf_base_url + "/async/41.0"

            for ele in batch_list:
                id_val = ele.group()
                message = "Executing for ID:" + id_val
                logger.info(message, extra=self.execution_context.get_context())
                batch_status = 'blank'
                while batch_status != "Completed" and batch_status != "NotProcessed":
                    response = requests.get(base_uri + "/job/" + job_id + "/batch/" + id_val, headers=headers)
                    match = re.search('(?<=<state>)(.*)(?=</state>)', response.content.decode("utf-8"))
                    batch_status = match.group()
                    time.sleep(10)
                    message = "waiting for Batch" + batch_id + " |-------->" + batch_status
                    logger.info(message, extra=self.execution_context.get_context())

                    if batch_status == "Failed":
                        match = re.search('(?<=<stateMessage>)(.*)(?=</stateMessage>)',
                                          response.content.decode("utf-8"))
                        batch_status_message = match.group()
                        error_message = "Failed Status" + batch_status_message
                        logger.error(error_message, extra=self.execution_context.get_context())
                        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                             CommonConstants.ERROR_KEY: error_message}
                        return result_dictionary
                    message = "waiting for Batch" + batch_id + " |-------->" + batch_status
                    logger.info(message, extra=self.execution_context.get_context())

                headers2 = {'X-SFDC-Session': session_id, 'Content-Type': 'text/csv; charset=UTF-8'}
                response = requests.get(base_uri + "/job/" + job_id + "/batch/" + id_val + "/result", headers=headers)
                result_list = re.finditer('(?<=<result>)([0-9A-Za-z]*)(?=</result>)', response.content.decode("utf-8"))

                for resultId in result_list:
                    rs_id = resultId.group()
                    response = requests.get(base_uri + "/job/" + job_id + "/batch/" + id_val + "/result/" + rs_id,
                                            headers=headers2)
                    pattern = re.compile(r'".*?"', re.DOTALL)
                    res = pattern.sub(lambda x: x.group().replace('\n', '').replace('\r', ''),
                                      response.content.decode("utf-8"))

                    if res == "Records not found for this query":
                        message = "Records not found for this query, hence writing as blank file"
                        logger.info(message, extra=self.execution_context.get_context())
                        input_body = ""
                    else:
                        input_body = res
                    try:
                        status_message = "starting put_s3_object object"
                        logger.info(status_message, extra=self.execution_context.get_context())

                        output = urlparse(tgt_s3path + "/" + id_val + "/Result_" + job_id + ".txt")
                        message = "Writing into s3 with the path:" + tgt_s3path + "/" + id_val + \
                                  "/Result_" + job_id + ".txt"
                        logger.info(message, extra=self.execution_context.get_context())
                        bucket_name = output.netloc
                        key_path = output.path.lstrip("/")
                        # s3 = boto3.resource('s3')
                        response = self.client.put_object(Body=input_body, Bucket=bucket_name, Key=key_path)
                        logger.debug(response, extra=self.execution_context.get_context())
                        logger.info("Completing putting a string in a file", extra=self.execution_context.get_context())
                        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED,
                                             CommonConstants.ERROR_KEY: "None"}
                    except Exception as exc:
                        error_message = "Could not write to the given S3 location. The error is %s" % str(exc)
                        logger.error(error_message, extra=self.execution_context.get_context())
                        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                             CommonConstants.ERROR_KEY: error_message}
                        return result_dictionary

        except "JobError":
            logger.error("Job failed raising exception", extra=self.execution_context.get_context())
            error_message = "Status returned by job_id" + job_id + " with status " + job_state
            logger.error(error_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: error_message}
            return result_dictionary

        except "BatchError":
            logger.error("Batch failed raising exception", extra=self.execution_context.get_context())
            error_message = "Status returned by Batch with id" + id_val + " with status " + batch_status_message
            logger.error(error_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: error_message}
            return result_dictionary

        finally:
            headers = {'X-SFDC-Session': session_id, 'Content-Type': 'application/xml; charset=UTF-8'}

            job_close = XmlPayload.jobclose
            base_uri = sf_base_url + "/async/41.0"
            response = requests.post(base_uri + "/job/" + job_id, data=job_close, headers=headers)
            logger.info(response, extra=self.execution_context.get_context())
            return result_dictionary


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="VEEVA_INGESTOR")
    PARSER.add_argument("-c", "--config_json_path", help="The path of the config json to be used", required=False)
    ARGS = vars(PARSER.parse_args())

    try:
        if ARGS['config_json_path'] is not None and ARGS['config_json_path'] != "":
            CONFIG_FILE_PATH = ARGS['config_json_path']
            try:
                config_file = json.loads(str(open(CONFIG_FILE_PATH, 'r').read()))
            except Exception as ex:
                ERROR = "Config file must be in a valid JSON format! The error is: %s" % str(ex)
                EXEC_STATUS = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                               CommonConstants.ERROR_KEY: ERROR}
                print(EXEC_STATUS)

            EXEC_STATUS = VeevaIngestor().trigger_veeva_ingestor(config_file=config_file)
            print(EXEC_STATUS)

    except Exception as ex:
        ERROR = "Check for error in execution of function. The error is: %s" % str(ex)
        EXEC_STATUS = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                       CommonConstants.ERROR_KEY: ERROR}
        print(EXEC_STATUS)
