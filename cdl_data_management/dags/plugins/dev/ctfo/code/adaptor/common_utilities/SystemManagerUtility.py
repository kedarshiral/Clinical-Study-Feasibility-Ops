

import boto3
import time
import CommonConstants
import logging
import traceback

MODULE_NAME = "SystemManagerUtility"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(MODULE_NAME)


class SystemManagerUtility(object):

    def __init__(self):
        self.ssm_client = boto3.client('ssm')
        self.cloudwatchlogs_client=boto3.client('logs')

    def get_invocation_response(self,command_id):
        try:
            list_command_invocation_retry_limit=CommonConstants.SSM_LIST_CMD_INV_RETRY_LIMIT
            for retry in range(0,list_command_invocation_retry_limit):
                try:
                    response = self.ssm_client.list_command_invocations(
                        CommandId=command_id,
                        MaxResults=50,
                        Details=True)
                    logger.info("response of command invocation for command_id {command_id} is {response}".format(
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

            logger.info("log-stream name--->>>{command_id}/{instance_id}/run_shell_command/stdout".format(
                command_id=command_id,
                instance_id=instance_id))
            logger.info("next_token===>"+str(next_token))
            if(next_token):
                logger.info("next token found")
                log_response = self.cloudwatchlogs_client.get_log_events(
                    logGroupName=CommonConstants.SSM_CLOUDWATCH_LOG_GRP_NM,
                    logStreamName="{command_id}/{instance_id}/run_shell_command/stdout".format(
                        command_id=command_id,
                        instance_id=instance_id),
                    nextToken=next_token,
                    startFromHead=True
                )
            else:
                logger.info("next token not found")
                log_response = self.cloudwatchlogs_client.get_log_events(
                    logGroupName=CommonConstants.SSM_CLOUDWATCH_LOG_GRP_NM,
                    logStreamName="{command_id}/{instance_id}/run_shell_command/stdout".format(
                        command_id=command_id,
                        instance_id=instance_id),
                    startFromHead=True)
            logger.info("log_responnse=-=====>>>"+str(log_response))

            return log_response
        except self.cloudwatchlogs_client.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def execute_command(self,cluster_id,command):
        try:
            logger.info("Cluster_id and Command:", cluster_id, command)
            response = self.ssm_client.send_command(
                # InstanceIds = ['i-008510bf942a2dead'],
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
            logger.info("*******************", response, "***********************************", command)
            logger.info("SSM response for command {command} is {response}".format(
                command=command,
                response=str(response)
            ))


            command_id=response["Command"]["CommandId"]
            next_token=None
            while True:
                time.sleep(CommonConstants.SSM_CMD_INV_RETRY_INTERVAL)
                invocation_response=self.get_invocation_response(command_id)
                logger.info("invocation_response==>>>>"+str(invocation_response))
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
                logger.info("status=====>>>"+str(status))
                if status =="Success":
                    return status
                elif status in ["Cancelled","Failed","TimedOut"]:
                    raise Exception("command failed with status {status}".format(status=status))
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e

    def fetch_file_name(self, cluster_id, command):
        try:
            logger.info("Cluster_id and Command:", cluster_id, command)
            response = self.ssm_client.send_command(
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
            logger.info("*******************", response, "of fetch_file_name ***************************", command)
            logger.info("SSM response for command {command} is {response}".format(
                command=command,
                response=str(response)
            ))
            command_id=response["Command"]["CommandId"]
            next_token=None
            while True:
                time.sleep(CommonConstants.SSM_CMD_INV_RETRY_INTERVAL)
                invocation_response=self.get_invocation_response(command_id)
                logger.info("invocation_response==>>>>"+str(invocation_response))
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
                logger.info("status=====>>>"+str(status))
                result_string = invocation_response["CommandInvocations"][0]["CommandPlugins"][0]["Output"]
                if status =="Success":
                    return status, result_string
                elif status in ["Cancelled","Failed","TimedOut"]:
                    raise Exception("command failed with status {status}".format(status=status))
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e