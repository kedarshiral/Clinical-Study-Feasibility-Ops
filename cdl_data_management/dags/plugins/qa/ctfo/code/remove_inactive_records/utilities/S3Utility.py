#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
"""
######################################################Module Information################################################
#   Module Name         :   S3Utility
#   Purpose             :   Module to define application logic for the flask services
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate the class and invoke functions using objects
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Last changed on     :   25th Aug 2021
#   Last changed by     :   Shubham Kothale
#   Reason for change   :   Refactoring
########################################################################################################################
"""

import os
import copy
import json
import boto3
import traceback
from utilities.LogSetup import get_logger
from urllib.parse import urlparse
from utilities import CommonServicesConstants as Constants


logger = get_logger()


class S3Utility:
    """
    Purpose : Copies the file to S3, reads the file from s3 location and check that file exist or not
    """

    def __init__(self):
        self.client = boto3.client('s3')

    def download_file_from_s3(self, source_file_path, destination_path, log_params=None):
        """
        Purpose : Copies file from S3 to local
        :param source_file_path. string type. The file to be copied from the s3 location.
        :param destination_path. string type. The file to be copied to.
        :param log_params : getting used for logging purpose
        :return: output dict. json type.
        """
        output = dict()
        try:
            logger.info("Start of download_file_from_s3", extra=log_params)
            if source_file_path is None or len(source_file_path.strip()) == 0:
                error = "source_file_path can not be empty."
                raise Exception(error)

            if destination_path is None or len(destination_path.strip()) == 0:
                error = "destination_path can not be empty."
                raise Exception(error)
            logger.debug("Received request to download file from s3 - " + source_file_path, extra=log_params)
            path = urlparse(source_file_path)
            bucket_name = path.netloc
            key = path.path.lstrip('/')
            s3 = self.client
            s3.download_file(bucket_name, key, destination_path)

            message = "Download file from s3 was successful"
            logger.debug(message)

            output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            output[Constants.RESULT_KEY] = message
            logger.info("End of copy_file_to_s3", extra=log_params)
            return output
        except Exception as ex:
            error = "ERROR in copy_file_to_s3 - S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc())
            logger.error(error)
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = error
            return output

    def download_file_from_s3_to_s3(self, source_file_path, destination_path, log_params=None):
        """
        Purpose : Copies file from S3 to local
        :param source_file_path. string type. The file to be copied from the s3 location.
        :param destination_path. string type. The file to be copied to.
        :return: output dict. json type.
        """
        output = dict()
        try:
            logger.info("Start of download_file_from_s3_to_s3", extra=log_params)
            if source_file_path is None or len(source_file_path.strip()) == 0:
                error = "source_file_path can not be empty."
                raise Exception(error)

            if destination_path is None or len(destination_path.strip()) == 0:
                error = "destination_path can not be empty."
                raise Exception(error)

            logger.debug("Received request to download file from s3 - " + source_file_path, extra=log_params)
            source_path = urlparse(source_file_path)
            source_bucket_name = source_path.netloc
            source_key = source_path.path.lstrip('/')
            s3 = self.client

            dest_path = urlparse(destination_path)
            dest_bucket_name = dest_path.netloc
            dest_key = dest_path.path.lstrip('/')

            copy_source = {
                'Bucket': source_bucket_name,
                'Key': source_key
            }
            s3.copy(copy_source, dest_bucket_name, dest_key, ExtraArgs={'ServerSideEncryption': 'AES256'})

            message = "Download file from s3 was successful"
            logger.debug(message)

            output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            output[Constants.RESULT_KEY] = message
            logger.info("End of copy_file_to_s3", extra=log_params)
            return output

        except Exception as ex:
            error = "ERROR in copy_file_to_s3 - S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc())
            logger.error(error)
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = error
            return output

    def delete_file_from_s3(self, file_path, log_params=None):
        """
        Purpose : Copies file from S3 to local
        :param file_path. string type. The file to be copied from the s3 location.
         :return: output dict. json type.
        """
        output = dict()
        try:
            logger.info("Start of delete_file_from_s3", extra=log_params)
            if file_path is None or len(file_path.strip()) == 0:
                error = "file_path can not be empty."
                raise Exception(error)

            logger.debug("Received request to delete file from s3 - " + file_path, extra=log_params)
            path = urlparse(file_path)
            bucket_name = path.netloc
            key = path.path.lstrip('/')
            s3 = self.client
            logger.debug("Bucket Name :- {}".format(bucket_name), extra=log_params)
            logger.debug("Key : - {}".format(key), extra=log_params)
            response = s3.delete_object(Bucket=bucket_name, Key=key)
            if response['ResponseMetadata']['HTTPStatusCode'] == 204:
                logger.info("Successfully deleted object {} from bucket {}".format(key, bucket_name), extra=log_params)
            else:
                logger.error("Failed to delete th file", extra=log_params)
                raise Exception("Failed to delete the file")
            message = "Delete file from s3 was successful"
            logger.debug(message)

            output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            output[Constants.RESULT_KEY] = message
            logger.info("End of delete_file_from_s3", extra=log_params)
            return output
        except Exception as ex:
            error = "ERROR in copy_file_to_s3 - S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc())
            logger.error(error)
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = error
            return output

    def read_s3_file(self, s3_path):
        """
        Purpose : Read the S3 file content and returns the output
        :param s3_path:
        :return: dictionary. json object.
                {
                    "status" : "success",
                    "result" : "<file content>"
                }
        """
        output = dict()
        try:
            logger.info("Start of read_s3_file")

            if s3_path is None or len(s3_path.strip()) == 0:
                raise Exception("s3_path can not be empty")

            path = urlparse(s3_path)
            bucket_name = path.netloc

            logger.info("Verifying file existance for the file - " + s3_path)
            s3 = self.client
            bucket = s3.Bucket(bucket_name)
            key = path.path.lstrip('/').rstrip('/')

            objs = list(bucket.objects.filter(Prefix=key))
            if objs is not None and len(objs) > 0 and objs[0].key == key:
                logger.info("File - {key} exist on S3".format(key=objs[0].key))

                obj = s3.Object(bucket_name, objs[0].key)
                file_content = obj.get()['Body'].read()

                output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
                output[Constants.RESULT_KEY] = file_content
            else:
                message = "File {key} not found on S3".format(key=key)
                logger.info(message)
                output[Constants.STATUS_KEY] = Constants.FAILED_KEY
                output[Constants.ERROR_KEY] = message

            logger.info("End of read_s3_file")
            return output
        except Exception as ex:
            logger.error("ERROR in S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc()))
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = str(ex)
            return output

    def list_files(self, s3_path):
        json_output = dict()
        try:
            logger.info("Start of list_files.\nListing the files for the path - {path}".format(path=s3_path))

            if s3_path is None or len(s3_path.strip()) == 0:
                raise Exception("s3_path can not be empty")

            url = urlparse(s3_path)
            bucket = url.netloc
            logger.info("Start of list_files.\nListing the files for the bucket - {path}".format(path=bucket))
            prefix = url.path.lstrip('/').rstrip('/')
            client = self.client

            response = client.list_objects(
                Bucket=bucket,
                Prefix=prefix
            )

            file_names = list()
            for each in response['Contents']:
                if each['Key'].endswith("/"):
                    continue
                file_names.append(each['Key'])

            for i in range(len(file_names)):
                file_names[i] = "s3://" + bucket + "/" + file_names[i]

            logger.info("{file_count} files found in the given path".format(file_count=len(file_names)))
            logger.info("End of list_files")

            return {
                Constants.STATUS_KEY: Constants.SUCCESS_KEY,
                Constants.RESULT_KEY: {
                    "file_list": file_names
                }
            }

        except Exception as ex:
            error = "ERROR in S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc())
            logger.error(error)
            json_output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            json_output[Constants.ERROR_KEY] = error
            return json_output

    def write_file_to_s3(self, file_content, s3_path):
        output = dict()
        try:
            logger.info("Start of write_file_to_s3")

            if s3_path is None or len(s3_path.strip()) == 0:
                error = "S3 path can not be empty."
                raise Exception(error)

            if file_content is None or len(file_content.strip()) == 0:
                error = "File content can not be empty."
                raise Exception(error)

            logger.debug("Received request to write file on s3 - " + s3_path)
            path = urlparse(s3_path)
            bucket_name = path.netloc

            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket_name)
            key = path.path.lstrip('/').rstrip('/')
            s3object = s3.Object(bucket_name, key)
            s3object.put(
                Body=(bytes(file_content.encode('utf-8')))
            )

            message = "File written to S3. Path - " + s3_path
            logger.debug(message)
            output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            output[Constants.RESULT_KEY] = message
            logger.info("End of write_file_to_s3")
            return output
        except Exception as e:
            error = "ERROR in write_file_to_s3 - S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc())
            logger.error(error)
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = error
            return output

    def copy_file_to_s3(self, source_file_path, destination_path, log_params=None):
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

            file_copy_command = "/usr/local/bin/aws s3 cp '{src}' '{target}' --sse".format(src=source_file_path,
                                                                                           target=destination_path)
            logger.info("Command to copy the file - {}".format(file_copy_command))
            file_copy_status = os.system(file_copy_command)
            if file_copy_status != 0:
                raise Exception("Failed to upload the file")

            logger.debug("Received request to copy file to s3 - " + source_file_path, extra=log_params)
            path = urlparse(destination_path)
            message = "File copy to S3 was successful"
            logger.debug(message)

            output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            output[Constants.RESULT_KEY] = message
            logger.info("End of copy_file_to_s3", extra=log_params)
            return output
        except Exception as ex:
            error = "ERROR in copy_file_to_s3 - S3Utility ERROR MESSAGE: " + str(ex) + str(
                traceback.format_exc())
            logger.error(error)
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = error
            return output

    def check_file_existance(self, s3_path):
        """
        Purpose : Check file exist on S3
        :param: s3_path. string type. .
        :return: output dict. json type.
                {
                    "status" : "success/ failed",
                    "result" : True/ False
                }
        """
        output = dict()
        try:
            logger.info("Start of check_file_existance")
            if s3_path is None or len(s3_path.strip()) == 0:
                raise Exception("s3_path can not be empty")

            logger.debug("Verifying file existance for the file - " + s3_path)

            path = urlparse(s3_path)
            bucket_name = path.netloc

            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket_name)
            key = path.path.lstrip('/').rstrip('/')

            objs = list(bucket.objects.filter(Prefix=key))
            output[Constants.RESULT_KEY] = False
            if objs is not None and len(objs) > 0 and objs[0].key == key:
                logger.info("File - {key} exist on S3".format(key=objs[0].key))
                output[Constants.RESULT_KEY] = True

            logger.info("End of check_file_existance")
            return output
        except Exception as ex:
            error = "ERROR in check_file_existance - S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc())
            logger.error(error)
            output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            output[Constants.ERROR_KEY] = error
            return output

    def get_file_list(self, s3_path):
        """
        Purpose: List the files in a given s3 location.
        :param s3_path:
        :return: Files present in the input path as dictionary
        """
        json_output = dict()
        try:
            logger.info("Start of get_file_list")
            logger.info("The input s3 path = %s" % s3_path)
            url = urlparse(s3_path)
            bucket = url.netloc
            prefix = url.path.lstrip('/').rstrip('/')
            client = self.client
            result = client.list_objects(Bucket=bucket,
                                         Prefix=prefix + "/",
                                         Delimiter='/'
                                         )
            files_temp = list()
            for o in result.get('CommonPrefixes'):
                files_temp.append(o.get('Prefix'))
            file_names = list()
            for file in files_temp:
                temp_file = "s3a://" + bucket + '/' + file
                file_names.append(temp_file)
            logger.debug("{file_count} files found in the given path".format(file_count=len(file_names)))
            json_output[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            json_output[Constants.RESULT_KEY] = {}
            json_output[Constants.RESULT_KEY]["file_list"] = file_names
            logger.info("End of get_file_list")
            return json_output
        except Exception as ex:
            msg = "Failed to get file list. ERROR - " + str(ex)
            traceback.format_exc()
            logger.info(msg)
            json_output[Constants.STATUS_KEY] = Constants.FAILED_KEY
            json_output[Constants.ERROR_KEY] = str(ex)
            return json_output

    def _s3_list_files(self, file_path):
        """
        Purpose : list all the files in particular s3 directory
        :param file_path:
        :return: list of files.
        """
        response = dict()
        try:
            logger.info("Started fetching the files in the s3 path: %s" % file_path)
            url = urlparse(file_path)
            bucket = url.netloc
            logger.info("Started fetching the files in the s3 path: %s" % bucket)
            prefix = url.path.lstrip('/').rstrip('/')
            response = self.client.list_objects(
                Bucket=bucket,
                Prefix=prefix
            )
            file_names = list()
            if response['Contents']:
                for file in response['Contents']:
                    if file['Key'].endswith("/"):
                        continue
                    file_names.append(file['Key'])
            if file_names:
                for i in range(len(file_names)):
                    file_names[i] = 's3://' + bucket + '/' + file_names[i]
            response[Constants.STATUS_KEY] = Constants.SUCCESS_KEY
            response[Constants.RESULT_KEY] = file_names
            return response
        except Exception as ex:
            error = "Failed to list files in the s3 path. ERROR - " + str(ex)
            logger.error(error)
            response[Constants.STATUS_KEY] = Constants.FAILED_KEY
            response[Constants.ERROR_KEY] = error
            return response

    def s3_move_copy(self, source_file_path, destination_path, operation, object_type, log_params=None):
        """
        Purpose : copies or moves files or folder from s3 to s3
        :param source_file_path. string type. The file to be copied from the s3 location.
        :param destination_path. string type. The file to be copied to.
        :param operation 'move' or 'copy' based on the operation that needs to be performed
        :param object_type 'file' or 'folder' based on the type of object to be copied
        :param log_params used for logging
        :return: output dict. json type.
        """
        try:
            logger.info("Start of s3_move_copy", extra=log_params)
            if source_file_path is None or len(source_file_path.strip()) == 0:
                error = "source_file_path can not be empty"
                raise Exception(error)

            if destination_path is None or len(destination_path.strip()) == 0:
                error = "destination_path can not be empty."
                raise Exception(error)

            if operation == "copy":
                ops = "cp"
            elif operation == "move":
                ops = "mv"
            else:
                raise Exception("Invalid Operation")

            if object_type == "file":
                additional_flags = "--sse"
            elif object_type == "folder":
                if not source_file_path.endswith("/"):
                    source_file_path = source_file_path + "/"
                if not destination_path.endswith("/"):
                    destination_path = destination_path + "/"
                additional_flags = "--sse --recursive"
            else:
                raise Exception("Invalid Operation")
            file_copy_command = "aws s3 {ops} '{src}' '{target}' {additional_flags}".format(src=source_file_path,
                                                                                            target=destination_path,
                                                                                            ops=ops,
                                                                                            additional_flags=additional_flags)
            logger.debug("Command to copy file - {}".format(file_copy_command))
            file_copy_status = os.system(file_copy_command)
            if file_copy_status != 0:
                raise Exception("Operation failed")

            logger.info("Operation was successful", extra=log_params)
            output = copy.deepcopy(Constants.SUCCESS_RESPONSE)
            output[Constants.RESULT_KEY] = "Operation was successful"
            return output
        except Exception as ex:
            error = "Operation failed S3Utility ERROR MESSAGE: " + str(
                traceback.format_exc())
            output = copy.deepcopy(Constants.ERROR_RESPONSE)
            output[Constants.ERROR_KEY] = error
            return output

    def create_presigned_url(self, s3_path, expiration=3600):
        """
        Purpose - Generate a presigned URL to share an S3 object
        :param bucket_name: string
        :param object_name: string
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Presigned URL as string. If error, returns None.
        """

        # Generate a presigned URL for the S3 object
        s3_client = self.client
        try:
            logger.debug("Start of create_presigned_url")
            url = urlparse(s3_path)
            bucket_name = url.netloc
            key = url.path.lstrip('/').rstrip('/')

            bucket = boto3.resource('s3').Bucket(bucket_name)
            if len(list(bucket.objects.filter(Prefix=key))) == 0:
                raise FileNotFoundError("File doesn't exist")

            url = s3_client.generate_presigned_url('get_object',
                                                   Params={'Bucket': bucket_name,
                                                           'Key': key},
                                                   ExpiresIn=expiration)
            logger.debug("End of create_presigned_url")
            success_response = {"status": "success", "result": url}

            return success_response
        except FileNotFoundError as error:
            logger.error("Failed to create presigned url. ERROR - {}".format(json.dumps(error)))
            failure_response = {"status": "failed", "result": ""}
            return failure_response
