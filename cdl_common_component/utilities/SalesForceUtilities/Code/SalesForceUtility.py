#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

"""
  Module Name         :   SalesForceUtility
  Purpose             :   This module performs below operation:
                              a. extract data from salesforce DB
                              b. Perform logging on adapter tables
  Input               :   Payload ID
  Output              :   Return status SUCCESS/FAILED result dictionary
  Pre-requisites      :   The control tables to be configured with needed payload and the 
                          valid config json configured must be present on the code base.
  Created on          :   27th April 2020
  Created by          :   Nishant Nijaguna
  Last changed by     :   
  Reason for change   :   
"""

import sys
import os
import re
import argparse
import json
import time
from urllib.parse import urlparse
import requests
import boto3
from datetime import datetime
import datetime

SERVICE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
print(str(SERVICE_DIR_PATH))
sys.path.insert(1, SERVICE_DIR_PATH)
CODE_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIR_PATH, "../common_utilities"))
print(str(CODE_DIR_PATH))
sys.path.insert(1, CODE_DIR_PATH)

from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
import XmlPayloadSF as XmlPayload
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager

MODULE_NAME = "SalesforceUtility"
PROCESS_NAME = "SalesForce Data Ingestor"
APPLICATION_CONFIG_FILE = "application_config.json"
ADAPTER_DETAILS_CONTROL_TABLE = "ctl_adapter_details"
ADAPTER_PAYLOAD_CONTROL_TABLE = "ctl_adapter_payload_details"
LOG_DATA_ACQUISITION_DETAIL_TABLE = "log_data_acquisition_dtl"
LOG_DATA_ACQUISITION_SMRY_TABLE = "log_data_acquisition_smry"


class SalesForceIngestor(object):

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.client = boto3.client('s3')
        self.configuration = json.load(open(APPLICATION_CONFIG_FILE))
        self.data_source_name = "salesforce"
        self.audit_db = self.configuration["adapter_details"]["generic_config"]["mysql_db"]
        self.s3_region = self.configuration["adapter_details"]["generic_config"]["s3_region"]
        self.sf_base_url = self.configuration["adapter_details"]["salesforce"]["base_url"]

    def trigger_sf_ingestor(self, config_file=None, sse_flag=None, payload_id=None, adapter_id=None,
                            cycle_id_generated=None):
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_RUNNING, CommonConstants.ERROR_KEY: ""}

        try:
            load_type = config_file['load_type']
            step_name = "data_extract"

            # Fetching details from adapter payload details table
            adapter_payload_details_query = "select * from {audit_db}." \
                                    "{adapter_payload_details_table} where payload_id={payload_id}".format\
                (audit_db=self.audit_db,adapter_payload_details_table=ADAPTER_PAYLOAD_CONTROL_TABLE,
                 payload_id=payload_id)
            adapter_payload_details_result = MySQLConnectionManager().execute_query_mysql(adapter_payload_details_query)

            # Fetching details from adapter details table
            adapter_details_query = "select * from {audit_db}.{adapter_details_table} where adapter_id={adapter_id}"\
                .format(audit_db=self.audit_db,adapter_details_table=ADAPTER_DETAILS_CONTROL_TABLE,
                        adapter_id=adapter_id)
            adapter_details_result = MySQLConnectionManager().execute_query_mysql(adapter_details_query)

            reference_payload_template = adapter_details_result[0]['reference_payload_template']
            tgt_s3path = adapter_payload_details_result[0]['structured_file_dir']
            raw_file_dir = adapter_payload_details_result[0]['raw_file_dir']

            # Inserting into log_data_acquisition_dtl_entry
            log_data_acquisition_dtl_entry = "insert into {audit_db}.{log_dtl_table} values('" \
                                             "{cycle_id}',{adapter_id},{payload_id},'{source_name}','{step_name}'," \
                                             "'{raw_file_dir}','{structured_file_dir}','{status}',NOW(),NULL)".format\
                (audit_db=self.audit_db,log_dtl_table=LOG_DATA_ACQUISITION_DETAIL_TABLE,cycle_id=cycle_id_generated,
                 adapter_id=adapter_id,payload_id=payload_id,source_name=self.data_source_name,step_name=step_name,
                 raw_file_dir=raw_file_dir,structured_file_dir=tgt_s3path,status=CommonConstants.STATUS_RUNNING)
            MySQLConnectionManager().execute_query_mysql(log_data_acquisition_dtl_entry)

            # Inserting into LOG_DATA_ACQUISITION_SMRY_TABLE
            log_data_acquisition_smry_entry = "insert into {audit_db}.{log_smry_table} values('{cycle_id}'," \
                                              "{adapter_id},{payload_id},'{source_name}','{payload_details}'," \
                                              "'{load_type}','','{status}',NOW(),NULL)".\
                format(audit_db=self.audit_db,log_smry_table=LOG_DATA_ACQUISITION_SMRY_TABLE,
                       cycle_id=cycle_id_generated,adapter_id=adapter_id,payload_id=payload_id,
                       source_name=self.data_source_name,payload_details=reference_payload_template,
                       load_type=load_type,status=CommonConstants.STATUS_RUNNING)
            MySQLConnectionManager().execute_query_mysql(log_data_acquisition_smry_entry)
            credentials = SalesForceIngestor.get_secret(self, config_file['password'] , self.s3_region)

            username = config_file['username']
            password = credentials['password']

            sf_base_url = self.sf_base_url

            query = config_file['query']
            tb_name = config_file['tb_name']
            query_type = config_file['query_type']
            content_type = config_file['content_type'].upper()

            if not config_file['password'] or not config_file['query'] or not \
                    config_file['tb_name'] or not config_file['load_type'] or not \
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

                        if sse_flag == 'y':
                            response = self.client.put_object(Body=input_body, Bucket=bucket_name,
                                                              Key=key_path, ServerSideEncryption='AES256')
                        else:
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

    def get_secret(self, secret_name, region_name):
        """
        Purpose: This fetches the secret details and decrypts them.
        Input: secret_name is the secret to be retrieved from Secrets Manager.
               region_name is the region which the secret is stored.
        Output: JSON with decoded credentials from Secrets Manager.
        """
        try:
            # Create a Secrets Manager client
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
            logger.info("Fetching the details for the secret name %s", secret_name)
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            print("secret gotten")
            logger.info(
                "Fetched the Encrypted Secret from Secrets Manager for %s", secret_name)
        except Exception as ex:
            raise Exception("Unable to fetch secrets.")
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                logger.info("Decrypted the Secret")
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                logger.info("Decrypted the Secret")
            return json.loads(secret)


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="SF_INGESTOR")
    PARSER.add_argument("-p", "--payload_id", help="The payload_id to be executed", required=False)
    PARSER.add_argument("-d", "--payload_json", help="The payload_json to be executed", required=False)
    PARSER.add_argument("-c", "--cluster_id", help="The cluster_id being used", required=False, default="")

    ARGS = vars(PARSER.parse_args())

    try:
        if ARGS['cluster_id'] is not None and ARGS['cluster_id'] != "":
            cluster_id = ARGS['cluster_id']
        else:
            cluster_id = "EXECUTED ON EC2"
        if ARGS['payload_id'] is not None and ARGS['payload_id'] != "":
            payload_id = ARGS['payload_id']
        else:
            print("Please provide payload ID for process")
            raise Exception

        get_payload_query = "select * from " + ADAPTER_PAYLOAD_CONTROL_TABLE + " where payload_id={payload_id}".format\
            (payload_id=payload_id)
        get_payload_result = MySQLConnectionManager().execute_query_mysql(get_payload_query)

        adapter_id = get_payload_result[0]["adapter_id"]
        config_file_path = get_payload_result[0]["payload_details_file_name"]

        try:
            if ARGS['payload_json'] is not None and ARGS['payload_json'] != "":
                config_file = eval(str(ARGS['payload_json']))
            else:
                config_file = json.loads(str(open(config_file_path, 'r').read()))
        except Exception as ex:
            ERROR = "Config file must be in a valid JSON format! The error is: %s" % str(ex)
            EXEC_STATUS = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                           CommonConstants.ERROR_KEY: ERROR}
            print(EXEC_STATUS)
            raise Exception

        current_date = datetime.datetime.now()
        cycle_id_generated = current_date.strftime("%Y%m%d%H%M%S")
        try:
            sse_flag = config_file["sse_flag"]
        except Exception as ex:
            sse_flag = "n"

        EXEC_STATUS = SalesForceIngestor().trigger_sf_ingestor(config_file=config_file, sse_flag=sse_flag,
                                                               payload_id=payload_id, adapter_id=adapter_id,
                                                               cycle_id_generated=cycle_id_generated)
        print(EXEC_STATUS)

        configuration = json.load(open(APPLICATION_CONFIG_FILE))
        audit_db = configuration["adapter_details"]["generic_config"]["mysql_db"]

        if EXEC_STATUS['status'] == "FAILED":
            # Updating log details table
            log_smr_update_query = "update {audit_db}.{log_dtl_table} set status='{status}', end_time=NOW() " \
                                   "where payload_id={payload_id} and adapter_id={adapter_id} and cycle_id=" \
                                   "'{cycle_id}'".format(audit_db=audit_db,
                                                         log_dtl_table=LOG_DATA_ACQUISITION_DETAIL_TABLE,
                                                         status=CommonConstants.STATUS_FAILED, payload_id=payload_id,
                                                         adapter_id=adapter_id, cycle_id=cycle_id_generated)
            MySQLConnectionManager().execute_query_mysql(log_smr_update_query)

            # Update log smry table to failed
            log_dtl_update_query = "update {audit_db}.{log_smr_table} set status='{status}', cluster_id='{cluster_id}' ," \
                                   "end_time=NOW() where payload_id={payload_id} and adapter_id={adapter_id} " \
                                   "and cycle_id='{cycle_id}'".format(audit_db=audit_db,
                                                         log_smr_table=LOG_DATA_ACQUISITION_SMRY_TABLE,
                                                         status=CommonConstants.STATUS_FAILED,cluster_id=cluster_id,payload_id=payload_id,
                                                         adapter_id=adapter_id, cycle_id=cycle_id_generated)
            MySQLConnectionManager().execute_query_mysql(log_dtl_update_query)

        elif EXEC_STATUS['status'] == "SUCCEEDED":
            # Updating log details table
            log_dtl_update_query = "update {audit_db}.{log_dtl_table} set status='{status}', end_time=NOW() " \
                                   "where payload_id={payload_id} and adapter_id={adapter_id} and cycle_id=" \
                                   "'{cycle_id}'".format(audit_db=audit_db,
                                                         log_dtl_table=LOG_DATA_ACQUISITION_DETAIL_TABLE,
                                                         status=CommonConstants.STATUS_SUCCEEDED, payload_id=payload_id,
                                                         adapter_id=adapter_id, cycle_id=cycle_id_generated)
            MySQLConnectionManager().execute_query_mysql(log_dtl_update_query)

            # Update log smry table to failed
            log_smry_update_query = "update {audit_db}.{log_smr_table} set status='{status}', cluster_id='{cluster_id}' , " \
                                   "end_time=NOW() where payload_id={payload_id} and adapter_id={adapter_id} " \
                                   "and cycle_id='{cycle_id}'".format(audit_db=audit_db,
                                                                      log_smr_table=LOG_DATA_ACQUISITION_SMRY_TABLE,
                                                                      status=CommonConstants.STATUS_SUCCEEDED,
                                                                      cluster_id=cluster_id, payload_id=payload_id,
                                                                      adapter_id=adapter_id,
                                                                      cycle_id=cycle_id_generated)
            MySQLConnectionManager().execute_query_mysql(log_smry_update_query)


    except Exception as ex:
        ERROR = "Check for error in execution of function. The error is: %s" % str(ex)
        EXEC_STATUS = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                       CommonConstants.ERROR_KEY: ERROR}
        print(EXEC_STATUS)
