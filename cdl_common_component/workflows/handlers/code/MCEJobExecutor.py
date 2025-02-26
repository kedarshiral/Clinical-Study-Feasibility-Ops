
#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_cc/browse ) & all static servers.

"""
Job Executor
"""

import socket
#import paramiko
import os
import MCEConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from CommonUtils import CommonUtils

MODULE_NAME = "MCEJobExecutor"
EXECUTION_CONTEXT = ExecutionContext()
EXECUTION_CONTEXT.set_context({"module_name": MODULE_NAME})


class MCEJobExecutor(object):
    """
    Purpose: Spark-submit a job to cluster.
    """

    def __init__(self):
        self.client = None
        self.username = MCEConstants.CLUSTER_USERNAME
        self.password = ''
        self.emr_ssh_secret_name = 'aws-a0036-use1-00-d-secm-shrd-shr-emr01'
        self.emr_region_name = 'us-east-1'
        self.timeout = None
        self.pkey = MCEConstants.PEM_FILE_LOCATION
        self.port = MCEConstants.CLUSTER_SSH_PORT
        self.ssh_error = None

    def connect(self, host):
        """
        Purpose: Establish connection with EMR.
        Input: Host, port, username, password/key path of the EMR
        Output: True/False flag describing if the connection with EMR was established or not.
        """
        try:
            logger.info("Establishing ssh connection", extra=EXECUTION_CONTEXT.get_context())
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # if not self.password:
            #     private_key = paramiko.RSAKey.from_private_key_file(self.pkey)
            #     self.client.connect(hostname=host, port=self.port, username=self.username,
            #                         pkey=private_key, timeout=self.timeout, allow_agent=False,
            #                         look_for_keys=False)
            #     logger.info("Connected to the server. Host: %s" % str(host),
            #                 extra=EXECUTION_CONTEXT.get_context())
            # else:
            self.fetch_emr_password = CommonUtils().get_secret(self.emr_ssh_secret_name, self.emr_region_name)
            emr_password = self.fetch_emr_password["password"]
            logger.info("emr_password:-->> " + str(emr_password))
            self.client.connect(hostname=host, port=self.port, username=self.username,
                            password=emr_password, timeout=self.timeout,
                                allow_agent=False, look_for_keys=False)
            logger.info("Connected to the server. Host: %s" % str(host),
                        extra=EXECUTION_CONTEXT.get_context())
        except paramiko.AuthenticationException:
            logger.error("Authentication failed, please verify your credentials",
                         extra=EXECUTION_CONTEXT.get_context())
            result_flag = False
        except paramiko.SSHException as ssh_exception:
            logger.error("Could not establish SSH connection: %s" % str(ssh_exception),
                         extra=EXECUTION_CONTEXT.get_context())
            result_flag = False
        except socket.timeout:
            logger.error("Connection timed out", extra=EXECUTION_CONTEXT.get_context())
            result_flag = False
        except Exception:
            logger.error("Exception in connecting to the server",
                         extra=EXECUTION_CONTEXT.get_context())
            result_flag = False
            self.client.close()
        else:
            result_flag = True

        return result_flag

    def execute_command(self, command, host):
        """
        Purpose: Execute sqoop command on an EMR.
        Input: Sqoop command to be executed.
        Output: Status if the command was executed successfully or not.
        """
        try:
            # if self.connect(host):
            logger.info("Executing spark-submit command: %s" % command, extra=EXECUTION_CONTEXT.get_context())
            if str(os.system(command)) == "0":
                logger.info("Command execution completed successfully",
                            extra=EXECUTION_CONTEXT.get_context())
                return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_SUCCESS,
                        MCEConstants.RESULT_KEY: ""}
            else:
                logger.error(
                    "Problem occurred while running command" + " The error is: %s" % str(
                        self.ssh_error), extra=EXECUTION_CONTEXT.get_context())
                error = str(self.ssh_error)
                return {MCEConstants.STATUS_KEY: MCEConstants.STATUS_FAILED, MCEConstants.RESULT_KEY: False,
                        MCEConstants.ERROR_KEY: error}
            # else:
            #     logger.error("Could not establish SSH connection", extra=EXECUTION_CONTEXT.get_context())
            #     raise Exception("Could not establish SSH connection")

        except :
            logger.error("Failed to execute the command",
                         extra=EXECUTION_CONTEXT.get_context())
            self.client.close()