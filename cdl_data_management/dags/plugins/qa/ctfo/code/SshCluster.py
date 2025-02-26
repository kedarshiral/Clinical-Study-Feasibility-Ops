#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

"""
SSH into Cluster
Developed for testing only. This code will not be integrated into CC.
"""

import socket
import paramiko
import SqoopUtilityConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext

MODULE_NAME = "SshCluster"
EXECUTION_CONTEXT = ExecutionContext()
EXECUTION_CONTEXT.set_context({"module_name": MODULE_NAME})


class ClusterSshClient(object):
    """
    Purpose: SSH into already running EMR and execute sqoop command.
    """

    def __init__(self):
        self.client = None
        self.username = SqoopUtilityConstants.DEFAULT_CLUSTER_USERNAME
        self.password = ''
        self.timeout = None
        self.pkey = SqoopUtilityConstants.CLUSTER_KEY_PATH
        self.port = SqoopUtilityConstants.CLUSTER_SSH_PORT
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
            if not self.password:
                private_key = paramiko.RSAKey.from_private_key_file(self.pkey)
                self.client.connect(hostname=host, port=self.port, username=self.username,
                                    pkey=private_key, timeout=self.timeout, allow_agent=False,
                                    look_for_keys=False)
                logger.info("Connected to the server. Host: %s" % str(host),
                            extra=EXECUTION_CONTEXT.get_context())
            else:
                self.client.connect(hostname=host, port=self.port, username=self.username,
                                    password=self.password, timeout=self.timeout,
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
            if self.connect(host):
                logger.info("Executing sqoop command", extra=EXECUTION_CONTEXT.get_context())
                stdin, stdout, stderr = self.client.exec_command(command)
                self.ssh_error = stderr.read()
                if str(stdout.channel.recv_exit_status()) == "0":
                    logger.info("Command execution completed successfully",
                                extra=EXECUTION_CONTEXT.get_context())
                    self.client.close()
                    return "Command executed successfully"
                else:
                    logger.error(
                        "Problem occurred while running command" + " The error is: %s" % str(
                            self.ssh_error), extra=EXECUTION_CONTEXT.get_context())
                    self.client.close()
                    return "Command execution failed"
            else:
                logger.error("Could not establish SSH connection", extra=EXECUTION_CONTEXT.get_context())
                raise Exception("Could not establish SSH connection")
        except socket.timeout:
            logger.error("Command timed out", extra=EXECUTION_CONTEXT.get_context())
            self.client.close()
        except paramiko.SSHException:
            logger.error("Failed to execute the command",
                         extra=EXECUTION_CONTEXT.get_context())
            self.client.close()

    def file_transfer(self, command, host):
        try:
            if self.connect(host):
                logger.info("Executing file transfer command: %s" % command, extra=EXECUTION_CONTEXT.get_context())
                stdin, stdout, stderr = self.client.exec_command(command)
                self.ssh_error = stderr.read()
                if str(stdout.channel.recv_exit_status()) == "0":
                    logger.info("File transfer from HDFS to S3 was successful",
                                extra=EXECUTION_CONTEXT.get_context())
                    self.client.close()
                    return True
                else:
                    logger.error(
                        "Problem occurred while running command." + " The error is: %s" % str(
                            self.ssh_error), extra=EXECUTION_CONTEXT.get_context())
                    self.client.close()
                    return False
            else:
                logger.error("Could not establish SSH connection", extra=EXECUTION_CONTEXT.get_context())
                raise Exception("Could not establish SSH connection")
        except socket.timeout:
            logger.error("Command timed out", extra=EXECUTION_CONTEXT.get_context())
            self.client.close()
        except paramiko.SSHException:
            logger.error("Failed to execute the command",
                         extra=EXECUTION_CONTEXT.get_context())
            self.client.close()

    def fetch_file_name(self, command, host):
        try:
            if self.connect(host):
                logger.info("Executing file transfer command", extra=EXECUTION_CONTEXT.get_context())
                stdin, stdout, stderr = self.client.exec_command(command)
                self.ssh_error = stderr.read()
                if str(stdout.channel.recv_exit_status()) == "0":
                    logger.info("Fetching the required file name",
                                extra=EXECUTION_CONTEXT.get_context())
                    self.client.close()
                    return stdout.readlines()
                else:
                    logger.error(
                        "Problem occurred while running command" + " The error is: %s" % str(
                            self.ssh_error), extra=EXECUTION_CONTEXT.get_context())
                    self.client.close()
                    return False
            else:
                logger.error("Could not establish SSH connection", extra=EXECUTION_CONTEXT.get_context())
                raise Exception("Could not establish SSH connection")
        except socket.timeout:
            logger.error("Command timed out", extra=EXECUTION_CONTEXT.get_context())
            self.client.close()
        except paramiko.SSHException:
            logger.error("Failed to execute the command",
                         extra=EXECUTION_CONTEXT.get_context())
            self.client.close()
			