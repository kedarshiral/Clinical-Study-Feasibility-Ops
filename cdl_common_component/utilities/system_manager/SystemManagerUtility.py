#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   SystemManagerUtility
#  Purpose             :   This module executes shell commands on cluster using AWS Systems Manager
#  Input Parameters    :   command,cluster_id
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   25th June 2021
#  Last changed by     :   Ashish Unadkat
#  Reason for change   :
# ######################################################################################################################

import boto3
import time
import CommonConstants
import logging
import traceback

MODULE_NAME = "SystemManagerUtility"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(MODULE_NAME)


class SystemManagerUtility(object):

    def __init__(self,log_type='stdout'):
        self.ssm_client = boto3.client('ssm')
        self.cloudwatchlogs_client=boto3.client('logs')
        self.log_type = log_type

    def get_invocation_response(self,command_id):
        try:
            list_command_invocation_retry_limit=CommonConstants.SSM_LIST_CMD_INV_RETRY_LIMIT
            for retry in range(0,list_command_invocation_retry_limit):
                try:
                    response = self.ssm_client.list_command_invocations(
                        CommandId=command_id,
                        MaxResults=50,
                        Details=True)
                    logger.debug("response of command invocation for command_id {command_id} is {response}".format(
                        command_id=command_id,
                        response=str(response)
                    ))
                    if not response['CommandInvocations']:
                        time.sleep(CommonConstants.SSM_CMD_INV_RETRY_INTERVAL)
                    else:
                        return response

                except self.ssm_client.exceptions.InvalidCommandId as e:
                    if (retry+1)==list_command_invocation_retry_limit:
                        raise Exception("command invocation failed due to {error}".format(error=str(e)))
                    time.sleep(CommonConstants.SSM_CMD_INV_RETRY_INTERVAL)
                except Exception as e:
                    logger.error(str(e))
                    raise e
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def get_cloudwatch_logs(self,command_id,instance_id,next_token):
        try:

            logger.debug("log-stream name--->>>{command_id}/{instance_id}/{step_name}/{log_type}".format(
                command_id=command_id,
                instance_id=instance_id,
                step_name=CommonConstants.SSM_DOC_STEP_NAME,
                log_type=self.log_type))
            logger.debug("next_token===>"+str(next_token))
            if(next_token):
                logger.debug("next token found")
                log_response = self.cloudwatchlogs_client.get_log_events(
                    logGroupName=CommonConstants.SSM_CLOUDWATCH_LOG_GRP_NM,
                    logStreamName="{command_id}/{instance_id}/{step_name}/{log_type}".format(
                        command_id=command_id,
                        instance_id=instance_id,
                        step_name=CommonConstants.SSM_DOC_STEP_NAME,
                        log_type=self.log_type),
                    nextToken=next_token,
                    startFromHead=True
                )
            else:
                logger.debug("next token not found")
                log_response = self.cloudwatchlogs_client.get_log_events(
                    logGroupName=CommonConstants.SSM_CLOUDWATCH_LOG_GRP_NM,
                    logStreamName="{command_id}/{instance_id}/{step_name}/{log_type}".format(
                        command_id=command_id,
                        instance_id=instance_id,
                        step_name=CommonConstants.SSM_DOC_STEP_NAME,
                        log_type=self.log_type),
                    startFromHead=True)
            logger.debug("log_response=-=====>>>"+str(log_response))

            return log_response
        except self.cloudwatchlogs_client.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def execute_command(self,cluster_id,command):
        try:
            response = self.ssm_client.send_command(
                #InstanceIds = ['i-0b3413695d4d0f738'],
                Targets = [
                    {
                        "Key": "tag:aws:elasticmapreduce:instance-group-role",
                        "Values": ["MASTER"]
                    },
                    {
                        "Key": "tag:aws:elasticmapreduce:job-flow-id",
                        "Values": [cluster_id]

                    }
                ],
                DocumentName=CommonConstants.SSM_DOC_NAME,
                TimeoutSeconds=CommonConstants.SSM_TIMEOUT,
                Comment='Execute commands on EMR',
                Parameters={
                    'cmd': [command]
                },
                CloudWatchOutputConfig={
                    'CloudWatchLogGroupName': CommonConstants.SSM_CLOUDWATCH_LOG_GRP_NM,
                    'CloudWatchOutputEnabled': CommonConstants.SSM_CLOUDWATCH_ENABLE_FLAG
                }
            )
            logger.info("SSM response for command {command} is {response}".format(
                command=command,
                response=str(response)
            ))


            command_id=response["Command"]["CommandId"]
            next_token=None
            while True:
                time.sleep(CommonConstants.SSM_CMD_INV_RETRY_INTERVAL)
                invocation_response=self.get_invocation_response(command_id)
                logger.debug("invocation_response==>>>>"+str(invocation_response))
                instance_id=invocation_response["CommandInvocations"][0]['InstanceId']
                    ## getting info logs
                log_response=self.get_cloudwatch_logs(command_id,instance_id,next_token)

                if log_response:
                    if "nextForwardToken" in log_response:
                        next_token = log_response["nextForwardToken"]
                    if "events" in log_response:
                        for event in log_response["events"]:
                            logger.info("{message}".format(message=event["message"]))
                            #last_event_time = event["timestamp"]
                status=invocation_response["CommandInvocations"][0]['Status']
                logger.debug("status=====>>>"+str(status))
                if status =="Success":
                    next_token_prev = next_token
                    next_token_new = next_token
                    while True:
                        log_response = self.get_cloudwatch_logs(command_id, instance_id, next_token_new)
                        if log_response:
                            if "events" in log_response:
                                for event in log_response["events"]:
                                    logger.info("{message}".format(message=event["message"]))
                            next_token_prev = next_token_new
                            if "nextForwardToken" in log_response:
                                next_token_new = log_response["nextForwardToken"]
                                if next_token_new == next_token_prev:
                                    break
                        else:
                            break

                    return status
                elif status in ["Cancelled","Failed","TimedOut"]:
                    time.sleep(10)
                    log_response = self.get_cloudwatch_logs(command_id, instance_id, next_token)

                    if log_response:
                        if "nextForwardToken" in log_response:
                            next_token = log_response["nextForwardToken"]
                        if "events" in log_response:
                            for event in log_response["events"]:
                                logger.info("{message}".format(message=event["message"]))

                    if self.log_type != "stderr":
                        log_response = self.cloudwatchlogs_client.get_log_events(
                            logGroupName=CommonConstants.SSM_CLOUDWATCH_LOG_GRP_NM,
                            logStreamName="{command_id}/{instance_id}/{step_name}/stderr".format(
                                command_id=command_id,
                                instance_id=instance_id,
                                step_name=CommonConstants.SSM_DOC_STEP_NAME),
                            startFromHead=True
                        )
                        if "events" in log_response:
                            for event in log_response["events"]:
                                logger.error("{message}".format(message=event["message"]))
                    raise Exception("command failed with status {status}".format(status=status))
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

