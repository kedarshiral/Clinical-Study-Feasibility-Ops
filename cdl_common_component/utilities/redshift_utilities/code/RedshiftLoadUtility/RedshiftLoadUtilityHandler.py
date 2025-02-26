#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import boto3
import re
import argparse
import traceback
import CommonConstants
import json
from LogSetup import logger
import multiprocessing as mp
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from RedshiftUtility import RedShiftUtility
from MySQLConnectionManager import MySQLConnectionManager

__author__ = 'ZS Associates'

# #################################################Module Information############################################# #
#   Module Name         :   Redshift Load Utility Handler
#   Purpose             :   To allow users to load multiple tables that are part of a layer or a group.
#   Input Parameters    :   layer_name, schema_name, group_name, table_name, operation_name
#   Output              :   NA
#   Execution Steps     :   Run from shell as 'python RedshiftLoadUtilityHandler.py --layer_name ... --schema_name ...'
#   Predecessor module  :   NA
#   Successor module    :   RedshiftUtility.py
#   Last changed on     :   6 December 2018
#   Last changed by     :   Vignesh Ravishankar
#   Reason for change   :   Enhancements - TRUNCATE and APPEND feature
# ################################################################################################################ #

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                  CommonConstants.ENVIRONMENT_CONFIG_FILE)

file_hosted_path = os.path.abspath(os.path.dirname(sys.argv[0]))

user = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,CommonConstants.REDSHIFT_PARAMS,
                                     "SERVER", "user"])
secret_password = MySQLConnectionManager().get_secret(
    configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,CommonConstants.REDSHIFT_PARAMS,
                                                                                 "SERVER", "redshift_password_secret_name"]),
    configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))

password = secret_password['password']

host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                        "SERVER", "host"])
port = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                        "SERVER", "port"])
db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                      "SERVER", "db"])
iam_role = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                            "SERVER", "iam_role"])
access_key = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                              "SERVER", "s3_access_key"])
secret_key = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                              "SERVER", "s3_secret_key"])
region = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                          "SERVER", "region"])
s3_bucket = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                             "APPLICATION", "s3_bucket"])
table_format = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                                "APPLICATION", "table_format"])
delimiter = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                             "APPLICATION", "delimiter"])
retry_limit = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                               "APPLICATION", "retry_limit"])
timeout = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.REDSHIFT_PARAMS,
                                           "APPLICATION", "timeout"])
aws_region = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.WLM_PARAMS,
                                              "APPLICATION", "aws_region"])
parameter_group = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, CommonConstants.WLM_PARAMS,
                                                   "APPLICATION", "parameter_group"])

parser = argparse.ArgumentParser(description="This Utility loads data from S3 data lake to Redshift")
parser.add_argument("--layer_name", nargs='?', default=None, help="The name of the layer in S3")
parser.add_argument("--schema_name", nargs='?', default=None, help="The schema in redshift")
parser.add_argument("--group_name", nargs='+', default=[], help="The group under which the data resides in S3")
parser.add_argument("--table_name", nargs='+', default=[], help="The name of the table in redshift")
parser.add_argument("--operation_name", nargs='?', default=None, help="The operation for the table in redshift")
# parser.add_argument("--bucket_name", nargs='?', default=None, help="The name of the bucket in redshift")


class RedshiftLoadUtilityHandler(object):
    def __init__(self):
        """
        Initialization of the handler. This gets values from the parseargs entered by the user, validates them and
         assigns them as objects to this class.
        """
        try:
            self.execution_context = ExecutionContext()
            self.s3_bucket = s3_bucket

            # TODO: s3_bucket as a user defined parameter
            '''
            if self.verify_inputs("bucket_name") is None:
                self.s3_bucket = s3_bucket
            else:
                self.s3_bucket = self.verify_inputs("bucket_name")
            '''

            schema = self.verify_inputs("schema_name")
            self.input_load = dict(host=host, port=port, user=user, password=password, db=db, iam_role=iam_role,
                                   table_format=table_format, delimiter=delimiter, timeout=timeout,
                                   retry_limit=retry_limit, schema=schema, primary_key="1", primary_key_value="1")
            self.s3_conn = boto3.client('s3')

        except ValueError as err:
            raise ValueError(err)

    def verify_inputs(self, parameter):
        """
        This function verifies if an argument is parsed to a parameter from the command line.

        Parameters
        ----------
            :param parameter: The parameter name that needs to be verified.
            :type parameter: str

        Returns
        -------
            :return: Returns the value of the argument passed in the command line.
            :rtype: str or list

        Raises
        ------
            Raises an error if there no argument is passed to the parameter or of the parameter to be verified does not
            exist as part of the ArgumentParser class.
            :raises ValueError
        """

        logger.debug("Validating user input {}".format(parameter), extra=self.execution_context.get_context())
        try:
            if getattr(parser.parse_args(), parameter) is None or getattr(parser.parse_args(), parameter) is "":
                parser.print_usage()
                logger.error("No arguments were passed to {}".format(parameter),
                             extra=self.execution_context.get_context())
                raise ValueError("\nThere are no arguments passed to the parameter {}\n".
                                 format(parameter.replace('_', ' ').upper()))
            else:
                args = getattr(parser.parse_args(), parameter)
                logger.debug("Parameter {} validated successfully".format(parameter),
                             extra=self.execution_context.get_context())
                return args
        except AttributeError:
            logger.error("{} is not a part of the ArgumentParser class implemented".format(parameter),
                         extra=self.execution_context.get_context())
            raise NotImplementedError("The parameter {} is not implemented as part of the ArgumentParser class.".
                                      format(parameter.replace('_', ' ').upper()))

    def get_matching_s3_objects(self, bucket, prefix='', suffix=''):
        """
        Generate objects in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch objects whose key starts with
            this prefix (optional).
        :param suffix: Only fetch objects whose keys end with
            this suffix (optional).
        """

        kwargs = {'Bucket': bucket}
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:

            resp = self.s3_conn.list_objects_v2(**kwargs)

            try:
                contents = resp['Contents']
            except KeyError:
                return

            for obj in contents:
                key = obj['Key']

                if re.search(suffix, key):
                    yield obj
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def get_matching_s3_keys(self, bucket, prefix='', suffix=''):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        for obj in self.get_matching_s3_objects(bucket, prefix, suffix):
            yield obj['Key']

    def call_operation(self, layers, operation_name):
        if operation_name.upper() == "APPEND":
            operation = "LOAD"
            self.pass_to_redshift(list(sorted(set(layers))), operation)
        elif operation_name.upper() == "TRUNCATE":
            operation = "TRUNCATE"
            self.pass_to_redshift(list(sorted(set(layers))), operation)
            operation = "LOAD"
            self.pass_to_redshift(list(sorted(set(layers))), operation)
        else:
            raise ValueError("Entered operation '{}' does not exist".format(operation_name))

    def pass_to_redshift(self, s3_location_list, operation_name):
        args = []
        for s3_location in s3_location_list:
            location = s3_location.split("s3://")
            location_split = location[1].split("/")
            args_dict = dict(s3_location=s3_location, table=location_split[-2], operation=operation_name)
            args.append(args_dict)

        pool = mp.Pool(processes=int(self.get_concurrency()))
        pool.map(call_redshift_utility, args)

    @staticmethod
    def get_concurrency():
        client = boto3.client('redshift', region_name=aws_region)
        for parameters in client.describe_cluster_parameters(
                ParameterGroupName=parameter_group)[CommonConstants.PARAMETER_KEY]:
            if parameters['ParameterName'] == 'wlm_json_configuration':
                parameter_value = json.loads(parameters[CommonConstants.PARAMETER_VALUE_KEY])
                for itr in parameter_value:
                    if 'query_concurrency' in itr:
                        if 'us_commercial_service_etl' in itr['user_group']:
                            return itr['query_concurrency']
                        else:
                            raise ValueError("User group {} does not exist in parameter group.".
                                             format(itr['user_group']))

    def call_redshift_utility(self, args):
        """
        Calling the redshift utility.
        Parameters
        ----------
            :param args: Dictionary of s3_location, table name and the operation to be done.
            :type args: dict

        Raises
        ------
        Raises an error if any exception is found while loading the data to Redshift.
            :raises RuntimeError
        """
        try:
            for args_dict in args:
                self.input_load["source_s3_location"] = args_dict["s3_location"]
                self.input_load["table"] = args_dict["table"]
                self.input_load["operation"] = args_dict["operatioin"]
                redshift_obj = RedShiftUtility()
                redshift_obj.execute_redshift_operation(input_json=self.input_load)
                logger.debug("Successfully loaded table {}".format(args["table"]),
                             extra=self.execution_context.get_context())
        except Exception as err:
            logger.error(str(err), extra=self.execution_context.get_context())
            raise RuntimeError("Faced a problem when loading data to Redshift. "
                               "Please contact your admin for more help...")


def call_redshift_utility(args):
    """
    Calling the redshift utility.
    Parameters
    ----------
        :param args: Dictionary of s3_location, table name and the operation to be done.
        :type args: dict
    Raises
    ------
    Raises an error if any exception is found while loading the data to Redshift.
        :raises RuntimeError
    """
    try:
        input_load = RedshiftLoadUtilityHandler().input_load
        input_load["source_s3_location"] = args["s3_location"]
        input_load["table"] = args["table"]
        input_load["operation"] = args["operation"]
        redshift_obj = RedShiftUtility()
        redshift_obj.execute_redshift_operation(input_json=input_load)
        logger.debug("Successfully loaded table {}".format(args["table"]),
                     extra=RedshiftLoadUtilityHandler().execution_context.get_context())
    except Exception as err:
        logger.error(str(err), extra=RedshiftLoadUtilityHandler().execution_context.get_context())
        raise RuntimeError("Faced a problem when loading data to Redshift. "
                           "Please contact your admin for more help...")


def main():
    """The main method"""
    class_obj = RedshiftLoadUtilityHandler()
    try:
        logger.debug("Connecting to S3", extra=class_obj.execution_context.get_context())
        s3_path = "s3://{}/".format(class_obj.s3_bucket)

        distinct_list = []

        logger.info("Loading tables from S3", extra=class_obj.execution_context.get_context())
        layer_name = class_obj.verify_inputs("layer_name").lstrip('/').rstrip('/')

        operation = class_obj.verify_inputs("operation_name")

        if operation is not None:
            operation_name = operation
        else:
            operation_name = "truncate"

        if class_obj.verify_inputs("table_name") != []:
            if class_obj.verify_inputs("group_name") != []:

                table_name = class_obj.verify_inputs("table_name")[0]
                group_name = class_obj.verify_inputs("group_name")[0]

                layer = list(class_obj.get_matching_s3_keys(class_obj.s3_bucket, layer_name + "/",
                                                            "(/" + group_name + "/)"))

                for i in layer:
                    b = i.split("/" + group_name + "/")
                    c = b[1].split("/")

                    try:
                        if table_name == c[0]:
                            try:
                                s3_path += b[0] + "/" + group_name + "/" + c[0] + "/"
                                distinct_list.append(s3_path)
                            except IndexError:
                                pass
                    except IndexError:
                        pass
                    s3_path = "s3://{}/".format(class_obj.s3_bucket)
                class_obj.call_operation(distinct_list, operation_name)

            else:
                raise ValueError("Group name required when passing table name!")
        elif class_obj.verify_inputs("group_name") != []:
            group_name = class_obj.verify_inputs("group_name")[0]

            layer = list(class_obj.get_matching_s3_keys(class_obj.s3_bucket, layer_name + "/",
                                                        "(/" + group_name + "/)"))

            for i in layer:
                b = i.split("/" + group_name + "/")

                c = b[1].split("/")
                if '$folder$' not in str(c):
                    s3_path += b[0] + "/" + group_name + "/" + c[0] + "/"
                    distinct_list.append(s3_path)
                s3_path = "s3://{}/".format(class_obj.s3_bucket)
            class_obj.call_operation(distinct_list, operation_name)

        else:

            layer = list(class_obj.get_matching_s3_keys(class_obj.s3_bucket, layer_name + "/"))

            for i in layer:
                b = i.split(layer_name)
                c = b[1].split("/")
                if '$folder$' not in str(c):
                    try:
                        s3_path += layer_name + c[0] + "/" + c[1] + "/" + c[2] + "/" + c[3] + "/"
                        distinct_list.append(s3_path)
                    except IndexError:
                        pass
                s3_path = "s3://{}/".format(class_obj.s3_bucket)
            class_obj.call_operation(distinct_list, operation_name)

            logger.info("Successfully loaded all tables in layer '{}' to redshift!!!".format(layer_name),
                        extra=class_obj.execution_context.get_context())

    except ValueError as e:
        logger.error(str(e), extra=class_obj.execution_context.get_context())

    except TypeError as e:
        logger.error(str(e), extra=class_obj.execution_context.get_context())

    except NotImplementedError as e:
        logger.error(str(e), extra=class_obj.execution_context.get_context())

    except RuntimeError as e:
        logger.error(str(e), extra=class_obj.execution_context.get_context())

    except OverflowError as e:
        logger.error(str(e), extra=class_obj.execution_context.get_context())

    except EnvironmentError as e:
        logger.error(str(e), extra=class_obj.execution_context.get_context())

    except Exception as e:
        logger.error(str(e), extra=class_obj.execution_context.get_context())
        logger.error(traceback.print_exc(), extra=class_obj.execution_context.get_context())

    finally:
        try:
            logger.debug("Connection to db closed", extra=class_obj.execution_context.get_context())
        except UnboundLocalError:
            pass
        except OSError:
            pass


if __name__ == '__main__':
    main()
