#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module for S3 Scanner
"""
__author__ = "ZS Associates"

"""
Doc_Type            : Tech Products
Tech Description    : This utility is used for scanning files and folders from S3 based on specified
                      file patterns, folder patterns and corresponding date format

Pre_requisites      : None
Inputs              : S3 path to scan, S3 access key, S3 secret key, Optional S3 region to connect,
                      List of file patterns to scan along with date format, List of folder patterns
                      to scan along
                      with date format, Optional number of months from the last month to filter
                      files

Outputs             : List containing pattern name, filename, directory location and file size as
                      result with a status message indicating success or failure

                      {"status":"SUCCESS/FAILED",
                      "result":"<Result if SUCCESS>",
                      "error":"<Error if FAILED>"}

Example             : Input Dictionary -
                      {
                          "s3_access_key": "************************",
                          "s3_secret_key": "********************************",
                          "s3_directory_path": "s3://amgen-datalake-poc/data",
                          "pattern_list": [
                            {
                              "pattern_name": "PIDL_RLT_???????????????????.####.txt",
                              "date_format": "YYYY-MM-DD-hh_mm_ss"
                            },
                            {
                              "pattern_name": "PIDL_DOT_???????????????????.####.txt",
                              "date_format": "YYYY-MM-DD-hh_mm_ss"
                            }
                          ],
                          "directory_pattern_details": [
                            {
                              "pattern_name": "PIDL_???????????????????",
                              "date_format": "YYYY-MM-DD-hh_mm_ss"
                            },
                            {
                              "pattern_name": "????????",
                              "date_format": "YYYY-MM-DD"
                            }
                          ],
                          "num_months_filter": 2
                        }
                      Command to Execute -
                      python S3Scanner_old.py --json_file_path <input_dictionary_path>
Config_file         : NA
"""

# Library and external modules declaration
import logging
import itertools
import re
import datetime
import traceback
import json
import sys
import getopt
#from LogSetup import logger
from datetime import date
import boto
import boto.s3.connection
from LogSetup import logger
from ExecutionContext import ExecutionContext
import dateutil.relativedelta
# Module level constants
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"

PATTERN_NAME_KEY = "pattern_name"
DATE_FORMAT_KEY = "date_format"
SKIP_WORD = "$folder$"
DEFAULT_LAST_MONTHS_FILTER = 2
VERSION_SPLIT_REGEX = r'(_v\d+$)'

# The usage sting to be displayed to the user for the utility
USAGE_STRING = """
SYNOPSIS
    python S3Scanner_old.py -f/--conf_file_path <conf_file_path> -c/--conf <conf>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string

        Note: Either 'conf_file_path' or 'conf' should be provided.

"""
MODULE_NAME = "S3Scanner"

class S3Scanner(object):
    """
    Class contains all the functions related to S3 File Scanner Utility
    """

    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

        # S3 path without bucket name
        self.s3_path = None
        # S3 bucket name
        self.s3_bucket_name = None
        # S3 bucket object
        self.s3_bucket = None
        # S3 connection object
        self.s3_conn = None

    def _validate_date(self, date_text, date_format):
        """
        Purpose   :   Validates the given date with date format
        Input     :   Actual date and date format
        Output    :   True if the given date matches with the date format, False otherwise
        """
        try:
            datetime.datetime.strptime(date_text, date_format)
            bool_return = True
        except ValueError:
            logging.debug("Date text " + date_text + " doesn't match with date format " +
                          date_format)
            bool_return = False
        return bool_return

    def _custom_match(self, file_name_date, date_format, month_filter):
        """
        Purpose   :   Matches the date portion of the file with the date format and checks whether
                      the date is a valid date and also the date lies within the last months to
                      filter
        Input     :   Date text of the file name, date format, number of months from the last month
                      to filter
                      where the file_name_date should be within
        Output    :   True if the date portion of the file name matches with the date format and
                      withing the month filter, False otherwise
        """
        try:
            if date_format in ["YYYY-MM-DD-hh_mm_ss", "YYYYMMWW"]:
                # Get the month and the year before the number of months to filter
                today_date = date.today()
                date_filtered_months = str(today_date - dateutil.relativedelta.
                                           relativedelta(months=int(month_filter)))
                date_filtered_months = date_filtered_months.replace("-", "")
                filter_year_month = str(date_filtered_months[:6]).strip()
                if date_format == "YYYY-MM-DD-hh_mm_ss":
                    file_year_month = str(file_name_date[:7]).strip().replace("-", "")
                    # Replace the date pattern with actual python date format
                    date_pattern = date_format.replace("YYYY", "%Y").replace("MM", "%m").\
                        replace("DD", "%d"). \
                        replace("hh", "%H").replace("mm", "%M").replace("ss", "%S")
                else:
                    file_year_month = str(file_name_date[:6]).strip()
                    # Replace the date pattern with actual python date format
                    date_pattern = date_format.replace("YYYY", "%Y").replace("MM", "%m").\
                        replace("WW", "")
                    # Extract week part and validate
                    week_part = file_name_date[-2:]
                    file_name_date = file_name_date[:-2]
                    if week_part not in ["01", "02", "03", "04", "05"]:
                        return False

                if file_year_month < filter_year_month:
                    # Check whether the date text in the file is within the last number of
                    # months to filter
                    logging.debug("File year month : " + str(file_year_month) +
                                  " is less than filter year month " + str(filter_year_month))
                    bool_return = False
                else:
                    # Validate the date with the date pattern
                    bool_return = self._validate_date(file_name_date, date_pattern)

            elif "YYYY" in date_format:
                # Replace the date pattern with actual python date format
                date_pattern = date_format.replace("YYYY", "%Y").replace("MM", "%m").\
                    replace("DD", "%d").replace("hh", "%H").replace("mm", "%M").replace("ss", "%S")
                # Validate the date with the date pattern
                bool_return = self._validate_date(file_name_date, date_pattern)

            elif "YY" in date_format:
                # Replace the date pattern with actual python date format
                date_pattern = date_format.replace("YY", "%y").replace("MM", "%m").\
                    replace("DD", "%d")
                # Validate the date with the date pattern
                bool_return = self._validate_date(file_name_date, date_pattern)

            else:
                bool_return = False

            return bool_return

        except:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(extra=self.execution_context.get_context())
            logger.error(str(traceback.format_exc()))
            logging.error("Error while matching file date with date pattern")
            raise

    def _validate_pattern(self, f_name, f_pattern_name, f_date_format, month_filter):
        """
        Purpose   :   Matches the name of the file or folder with the pattern name, date format and
                      validates whether
                      the date is within the last number of months provided
        Input     :   File name or folder name, pattern name to match, date format for the
                      pattern name, number of months from the last month to filter
        Output    :   True if the file name matches with the pattern name, False otherwise
        """
        try:
            if len(f_pattern_name) != len(f_name):
                # Pattern name and file name length should be equal
                bool_match_found = False
            else:
                # Static component equality check i.e. all the characters except # and ? are checked
                temp_pattern_name = f_pattern_name
                wild_card_chars = ["#", "?"]
                while any(wild_char in temp_pattern_name for wild_char in wild_card_chars):
                    for wild_card in wild_card_chars:
                        if wild_card in temp_pattern_name:
                            len_wild_card = max(len(list(temp_pattern_name))
                                                for (char, temp_pattern_name) in
                                                itertools.groupby(temp_pattern_name)
                                                if char == wild_card)
                            replace_str = wild_card*len_wild_card
                            regex = temp_pattern_name.replace(replace_str, ".*")
                            temp_pattern_name = regex

                re_match = re.match(temp_pattern_name, f_name)
                if re_match is None:
                    # Static component match failed
                    bool_match_found = False

                elif not temp_pattern_name == f_name:
                    # Extract the string within the pattern name consisting of '#' and '?' character
                    start_loc_list = [f_pattern_name.find("?"), f_pattern_name.find("#")]
                    start_loc = min([x for x in start_loc_list if x >= 0])
                    end_loc_list = [f_pattern_name[::-1].find("?"), f_pattern_name[::-1].find("#")]
                    end_loc = min([x for x in end_loc_list if x >= 0])
                    short_pattern = f_pattern_name[start_loc:len(f_pattern_name) - end_loc]

                    # Split the pattern based on the string generated above to get all the static
                    # components
                    external_static_component_list = f_pattern_name.split(short_pattern)
                    temp_file_name = f_name

                    for external_static_component in external_static_component_list:
                        temp_file_name = temp_file_name.replace(external_static_component, "")

                    # Process '#' wild char
                    hash_locations = [pos for pos, letter in enumerate(short_pattern) if
                                      letter == "#"]
                    bool_hash = True
                    for hash_location in hash_locations:
                        bool_hash = bool_hash and temp_file_name[hash_location].isdigit()

                    if not bool_hash:
                        # Wild card character '#' check failed
                        return False

                    # Process ? wild char
                    start_location = short_pattern.find("?")
                    end_location = len(short_pattern) - short_pattern[::-1].find("?")
                    # Extracts the date portion of the file
                    file_name_date = temp_file_name[start_location:end_location]
                    bool_match_found = self._custom_match(file_name_date, f_date_format,
                                                          month_filter)
                else:
                    bool_match_found = True

            return bool_match_found

        except:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(str(traceback.format_exc()))
            logger.error(extra=self.execution_context.get_context())
            logging.error("Error while validating file name " + f_name + " with pattern " +
                          f_pattern_name + " and date format " + f_date_format)
            raise

    def _create_s3_connection(self, s3_access_key, s3_secret_key, s3_dir_path, s3_region):
        """
        Purpose   :   Creates a S3 connection and bucket object
        Input     :   S3 access key, S3 secret key, S3 path to scan, S3 region
        Output    :   None
        """
        try:
            logging.info("Creating S3 connection for S3 directory %s", s3_dir_path)

            # Creating S3 connection
            self.s3_conn = boto.connect_s3(aws_access_key_id=s3_access_key,
                                           aws_secret_access_key=s3_secret_key,
                                           calling_format=boto.s3.connection.
                                           OrdinaryCallingFormat())

            # Check for S3 region
            region = None
            if s3_region is None:
                # connect to s3 to get region for buckets
                for bucket in self.s3_conn.get_all_buckets():
                    if bucket.name == self.s3_bucket_name:
                        region = bucket.get_location()
            else:
                # Use user provided S3 region
                region = s3_region

            if region is None:
                raise Exception("S3 region for bucket - " + self.s3_bucket_name + " not found")
            logging.debug("S3 region for bucket - " + self.s3_bucket_name + " is - " + region)

            # Update the S3 connection if region is provided
            if region.strip():
                region = "s3-" + region + ".amazonaws.com"
                self.s3_conn = boto.connect_s3(aws_access_key_id=s3_access_key,
                                               aws_secret_access_key=s3_secret_key,
                                               calling_format=boto.s3.connection.
                                               OrdinaryCallingFormat(), host=region)

            self.s3_bucket = self.s3_conn.get_bucket(self.s3_bucket_name)

            logging.info("S3 connection created for S3 directory %s", s3_dir_path)

        except:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(str(traceback.format_exc()))
            logger.error(extra=self.execution_context.get_context())
            logging.error("Error while creating S3 connection for S3 directory %s", s3_dir_path)
            raise

    def _validate_inputs(self, s3_access_key, s3_secret_key, s3_dir_path, pattern_list,
                         month_filter):
        """
        Purpose   :   Validate the mandatory inputs as well as the value of the inputs wherever
                      applicable
        Input     :   S3 access key, S3 secret key, S3 path to scan, List of file patterns to scan
                      along with date format, Number of months from the last month to filter files
        Output    :   None
        """
        try:
            # Check whether all the mandatory arguments are passed
            if s3_dir_path is None or pattern_list is None:
                raise Exception("Either one of the mandatory arguments S3 access key, "
                                "S3 secret key, S3 directory path or pattern details is missing")
            if s3_access_key or s3_secret_key:
                logging.info("S3 Access and Secret key provided")
            # Remove S3 connection prefix and extract bucket name and S3 path
            s3_tmp_path = s3_dir_path
            s3_tmp_path = s3_tmp_path.replace("s3://", "").replace("s3n://", "").\
                replace("s3a://", "")
            if "//" in s3_tmp_path:
                raise Exception("Entered // in place of / in s3 path")
            s3_tmp_path = (s3_tmp_path + "/").replace("//", "/")
            self.s3_bucket_name = s3_tmp_path.split("/")[0].strip()
            self.s3_path = ("/" + s3_tmp_path.replace(self.s3_bucket_name, "")).replace("//", "")

            # Check whether bucket name is present
            if not self.s3_bucket_name:
                raise Exception("Unable to extract S3 bucket name")
            logging.debug("S3 bucket name - " + self.s3_bucket_name + ", S3 Path - " + self.s3_path)

            # Check whether patterns are provided correctly
            for pattern in pattern_list:
                if PATTERN_NAME_KEY not in pattern or DATE_FORMAT_KEY not in pattern:
                    raise Exception(PATTERN_NAME_KEY + " or " + DATE_FORMAT_KEY +
                                    " missing in pattern - " + str(pattern))

            # Check month filter
            if month_filter < 1:
                raise Exception("Month filter should be greater than 1")
            logging.info("Validation of input parameters completed")

        except:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(str(traceback.format_exc()))
            logger.error(extra=self.execution_context.get_context())
            logging.error("Error while validating inputs")
            raise

    def _get_folder_list(self, s3_access_key, s3_secret_key, s3_dir_path, folder_pattern_list,
                         pattern_list, month_filter, s3_region=None):
        """
        Purpose   :   Fetches the list of folders from S3 which matches the list of folder patterns
                      and fetches the files from inside those folders based on file patterns
        Input     :   S3 access key, S3 secret key, S3 path to scan, List of file patterns to scan
                      along with date format, Optional number of months from the last month to
                      filter files, Optional S3 region
        Output    :   List of file names from folders and corresponding pattern name, file size
                      and file directory
        """
        try:
            logging.info("Getting folder list from S3 dir path %s", s3_dir_path)
            s3_tmp_path = s3_dir_path
            s_domain = s3_tmp_path.split("//")[0] + "//"
            s3_tmp_path = s3_tmp_path.replace("s3://", "").replace("s3n://", "").\
                replace("s3a://", "")
            s3_tmp_path = (s3_tmp_path + '/').replace('//', '/')
            s3_bucket = s3_tmp_path.split('/')[0]
            # Validate inputs
            self._validate_inputs(s3_access_key, s3_secret_key, s3_dir_path, folder_pattern_list,
                                  month_filter)

            # Create S3 connection
            self._create_s3_connection(s3_access_key, s3_secret_key, s3_dir_path, s3_region)

            # Check if S3 path exists or not
            path = self.s3_path.split("/")[0] + "/"
            check_s3_location = self.s3_bucket.list(prefix=path)
            if len(list(check_s3_location)) == 0:
                raise Exception("S3 source location doesn't exist - %s" + s3_dir_path)
            logging.debug("Found S3 path - %s", s3_dir_path)

            # Define an empty list, all the files matching a pattern shall be appended to this list
            #  along with other details file size and pattern name
            folder_match_pattern = []
            file_match_pattern = []

            # Define a empty list, this shall contain the list of files is s3 folder along with
            # the size
            folders_in_source_folder = []

            # Get the number of path separator in s3 directory path
            if not str(s3_dir_path).endswith("/"):
                length = str(s3_dir_path).count("/") + 1 - 3
            else:
                length = str(s3_dir_path).count("/") - 3

            # Loop to iterate through folder names present on s3 and add it to
            # folders_in_source_folder
            for file in self.s3_bucket.list(prefix=self.s3_path.split("/")[0] + "/"):
                if file is None or (SKIP_WORD in str(file)) or (str(file).strip() == ""):
                    continue
                else:
                    # Condition to get only  folders (match count("/") with source length)
                    if str(file).count("/") == length + 1:
                        folder_name = file.name.split("/")[length].strip()
                        if folder_name not in folders_in_source_folder:
                            folders_in_source_folder.append(folder_name)
            logging.debug("Total number of folders found in " + s3_dir_path + " - "
                          + str(len(folders_in_source_folder)) + " and is " +
                          str(folders_in_source_folder))

            # Iterate through all the patterns in the pattern details
            for pattern in folder_pattern_list:
                # For each pattern iterate through the files files_in_source_folder
                for file in folders_in_source_folder:
                    folder_pattern_name = pattern[PATTERN_NAME_KEY]
                    folder_date_format = pattern[DATE_FORMAT_KEY]
                    # Remove the version information from the file if exists and get the actual file
                    #  name
                    file_name = file
                    if self._validate_pattern(file_name, folder_pattern_name, folder_date_format,
                                              month_filter):
                        logging.debug("Pattern %s matched for file %s", folder_pattern_name, file)
                        # Add file name, pattern name, file size to the matched file list
                        folder_match_pattern.append(file_name)
                    else:
                        logging.debug("Pattern %s not matched for folder %s", folder_pattern_name,
                                      file)
            if not folder_match_pattern:
                logging.info("Folders matched pattern list in %s is empty", s3_dir_path)
            for folder in folder_match_pattern:
                # For each pattern iterate through the files files_in_source_folder
                temp_loc = path + folder + "/"
                loc = s_domain + s3_bucket + "/" + temp_loc
                for file in self.s3_bucket.list(prefix=temp_loc):
                    for pattern_type in pattern_list:
                        pattern_name = pattern_type[PATTERN_NAME_KEY]
                        date_format = pattern_type[DATE_FORMAT_KEY]
                        # Remove the version information from the file if exists and get the actual
                        # file name
                        file_name = file.name.split("/")[length + 1].strip()
                        if file_name == "":
                            continue
                        if self._validate_pattern(file_name, pattern_name, date_format,
                                                  month_filter):
                            logging.debug(
                                "Pattern %s matched for file %s", pattern_name, file_name)
                            # Add file name, pattern name, file size, file directory to the matched
                            # file list
                            file_match_pattern.append(
                                {"file_name": file_name, "pattern_name": pattern_name,
                                 "file_size": file.size, "file_directory": loc})
                            logging.debug("Matched file pattern list %s ", str(file_match_pattern))
                        else:
                            logging.debug(
                                "Pattern %s not matched for file %s", pattern_name, file_name)

            # Return the list of files matched
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: file_match_pattern}

        except Exception as ex:
            logger.error(str(traceback.format_exc()))
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def get_file_list(self, s3_access_key, s3_secret_key, s3_dir_path, pattern_list,
                      month_filter, s3_region=None):
        """
        Purpose   :   Fetches the list of files from S3 which matches the list of file patterns
        Input     :   S3 access key, S3 secret key, S3 path to scan, List of file patterns to scan
                      along with
                      date format, Optional number of months from the last month to filter files,
                      Optional S3 region
        Output    :   List of file names and corresponding pattern name, file size and file
                      directory
        """
        try:
            logging.info("Getting file list from S3 dir path %s", s3_dir_path)

            # Validate inputs
            self._validate_inputs(s3_access_key, s3_secret_key, s3_dir_path, pattern_list,
                                  month_filter)

            # Create S3 connection
            self._create_s3_connection(s3_access_key, s3_secret_key, s3_dir_path, s3_region)

            # Check if S3 path exists or not
            check_s3_location = self.s3_bucket.list(prefix=self.s3_path)
            if len(list(check_s3_location)) == 0:
                raise Exception("S3 source location doesn't exist - " + s3_dir_path)
            logging.debug("Found S3 path - %s", s3_dir_path)

            # Define an empty list, all the files matching a pattern shall be appended to this
            # list along with
            # other details file size and pattern name
            file_match_pattern = []

            # Define a empty list, this shall contain the list of files is s3 folder along with
            # the size
            files_in_source_folder = []

            # Get the number of path separator in s3 directory path
            if not str(s3_dir_path).endswith("/"):
                length = str(s3_dir_path).count("/") + 1 - 3
            else:
                length = str(s3_dir_path).count("/") - 3

            # Loop to iterate through file names present on s3 and add it to files_in_source_folder
            for file in self.s3_bucket.list(prefix=self.s3_path):
                if file is None or (SKIP_WORD in str(file)) or (str(file).strip() == ""):
                    continue
                else:
                    # Condition to get only files and not folders (match count("/") with source
                    # length)
                    if str(file).count("/") == length:
                        file_name = file.name.split("/")[length].strip()
                        if file_name:
                            files_in_source_folder.append((file_name, file.size))
            logging.debug("Total number of files found in %s - %s", s3_dir_path,
                          str(len(files_in_source_folder)))
            logging.debug("Files found in %s = %s", s3_dir_path, str(files_in_source_folder))

            # Iterate through all the patterns in the pattern details
            for pattern in pattern_list:
                # For each pattern iterate through the files files_in_source_folder
                for file in files_in_source_folder:
                    pattern_name = pattern[PATTERN_NAME_KEY]
                    date_format = pattern[DATE_FORMAT_KEY]
                    # Remove the version information from the file if exists and get the actual file
                    #  name
                    file_name = re.split(VERSION_SPLIT_REGEX, file[0])[0]
                    if self._validate_pattern(file_name, pattern_name, date_format, month_filter):
                        logging.debug("Pattern %s matched for file %s", pattern_name, file[0])
                        # Add file name, pattern name, file size to the matched file list
                        file_match_pattern.append({"file_name": file[0], "pattern_name":
                                                       pattern_name,
                                                   "file_size": file[1], "file_directory":
                                                       s3_dir_path})
                        logging.debug("Matched file pattern list = %s", str(file_match_pattern))
                    else:
                        logging.debug("Pattern %s not matched for file %s", pattern_name, file[0])

            # Return the list of files matched
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: file_match_pattern}

        except Exception as ex:
            logger.error(str(traceback.format_exc()))
            logging.error("Error while getting file list from S3. ERROR - %s",
                          str(traceback.format_exc()))
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def _get_sorted_list(self, same_pattern_file_list):
        """
        Purpose   :   Receives the list of files of same pattern and generate a sorted list based
                      on date
        Input     :   File list of same pattern
        Output    :   List of file names and corresponding pattern name, file size and file
                      directory in sorted order
        """
        try:
            file_list = []
            final_sorted_list = []
            for each_dict in same_pattern_file_list:
                file_name = each_dict["file_name"]
                file_list.append(file_name)
            sorted_list = sorted(file_list)
            logging.info("Same pattern files generated after sorting is : %s", str(sorted_list))
            for each_file in sorted_list:
                for each_dict in same_pattern_file_list:
                    if each_file == each_dict["file_name"]:
                        final_sorted_list.append(each_dict)
                    else:
                        continue
            return final_sorted_list
        except:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(str(traceback.format_exc()))
            logger.error(extra=self.execution_context.get_context())
            raise

    def _get_same_pattern_files(self, result, pattern_list):
        """
        Purpose   :   To generate a list of files of same pattern
        Input     :   S3 access key, S3 secret key, S3 path to scan, List of file patterns to scan
                      along with date format, Optional number of months from the last month to
                      filter files, Optional S3 region
        Output    :   Full list of file names and corresponding pattern name, file size and file
                      directory
        """
        try:
            same_pattern_file_list = []
            final_list = []
            for each_pattern_dict in pattern_list:
                for each_result_dict in result:
                    if each_result_dict["pattern_name"] == each_pattern_dict["pattern_name"]:
                        same_pattern_file_list.append(each_result_dict)
                    else:
                        continue
                # File list generated of same patterns(for one pattern now)
                logging.debug("Generated file list of similar patterns : %s",
                              str(same_pattern_file_list))
                temp_list = same_pattern_file_list
                same_pattern_file_list = []
                rcv_list = (self._get_sorted_list(temp_list))
                final_list = final_list + rcv_list
                rcv_list = []
            logging.info("Finally finished going through all patterns and the result is : %s",
                         str(final_list))
            return final_list
        except:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(str(traceback.format_exc()))
            logger.error(extra=self.execution_context.get_context())
            raise

    def get_file_details(self, s3_dir_path, pattern_list, month_filter=DEFAULT_LAST_MONTHS_FILTER,
                         s3_access_key=None, s3_secret_key=None, folder_pattern_details=None,
                         s3_region=None):
        """
        Purpose   :   Decides whether to go for file scanning or folder scanning plus file scanning,
                      else if any needed input is not provided then exception is thrown
        Input     :   S3 access key, S3 secret key, S3 path to scan, List of file patterns to scan
                      along with date format, List of file patterns to scan along with date format,
                      Optional number of months from the last month to filter files, Optional S3
                      region
        Output    :   List of file names and corresponding pattern name, file size and file
                      directory
        """
        try:
            output = {}
            if folder_pattern_details is None:
                file_result = self.get_file_list(s3_access_key, s3_secret_key, s3_dir_path,
                                                  pattern_list, month_filter)
                result = file_result[RESULT_KEY]
                final_result = self._get_same_pattern_files(result, pattern_list)
                logging.info("Completed Scanning respective Files ")
                output[STATUS_KEY] = STATUS_SUCCESS
                output[RESULT_KEY] = final_result
                return output
            elif folder_pattern_details is not None:
                folder_result = self._get_folder_list(s3_access_key, s3_secret_key, s3_dir_path,
                                                      folder_pattern_details,
                                                      pattern_list, month_filter)
                file_result = self.get_file_list(s3_access_key, s3_secret_key, s3_dir_path,
                                                  pattern_list, month_filter)
                result = folder_result[RESULT_KEY] + file_result[RESULT_KEY]
                final_result = self._get_same_pattern_files(result, pattern_list)
                logging.info("Completed Scanning Folder and their respective Files ")
                output[STATUS_KEY] = STATUS_SUCCESS
                output[RESULT_KEY] = final_result
                return output
            else:
                raise Exception

        except:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(str(traceback.format_exc()))
            logger.error(extra=self.execution_context.get_context())
            logging.error("Error in inputs provided")
            raise


# Print the usage for the S3 file scanner module
def usage(status=1):
    """
    Function to print the usage for the S3 File Scanner Module
    :param status:
    :return:
    """
    sys.stdout.write(USAGE_STRING)
    sys.exit(status)


if __name__ == '__main__':

    #logging = logger
    conf_file_path = None
    conf = None
    opts = None
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "f:c",
            ["conf_file_path=", "conf="
                                "help"])
    except Exception as e:
        logger.error(str(traceback.format_exc()))
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: " + str(e)}))
        usage(1)

    # Parse the input arguments
    for option, arg in opts:
        if option in ("-h", "--help"):
            usage(1)
        elif option in ("-f", "--conf_file_path"):
            conf_file_path = arg
        elif option in ("-c", "--conf"):
            conf = arg

    # Check for all the mandatory arguments
    if conf_file_path is None and conf is None:
        sys.stderr.write(json.dumps({
            STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: Either JSON configuration file path "
                                                  "or JSON configuration string should be "
                                                  "provided\n"}))
        usage(1)

    access_key = None
    secret_key = None
    s3_path = None
    patterns = None
    num_months_filter = DEFAULT_LAST_MONTHS_FILTER
    s3_region_name = None
    folder_detail = None
    try:
        # Parse the configuration
        if conf_file_path:
            with open(conf_file_path) as conf_file:
                scanner_conf = json.load(conf_file)
        else:
            scanner_conf = json.loads(conf)

        if "s3_access_key" in scanner_conf:
            access_key = scanner_conf["s3_access_key"]
        if "s3_secret_key" in scanner_conf:
            secret_key = scanner_conf["s3_secret_key"]
        if "s3_directory_path" in scanner_conf:
            s3_path = scanner_conf["s3_directory_path"]
        if "pattern_list" in scanner_conf:
            patterns = scanner_conf["pattern_list"]
        if "num_months_filter" in scanner_conf:
            num_months_filter = int(scanner_conf["num_months_filter"])
        if "s3_region" in scanner_conf:
            s3_region_name = scanner_conf["s3_region"]
        if "directory_pattern_details" in scanner_conf:
            folder_detail = scanner_conf["directory_pattern_details"]

    except Exception as e:
        logger.error(str(traceback.format_exc()))
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError while parsing "
                                                                           "configuration."
                                                                           " ERROR: " + str(e)}))
        sys.exit(1)

    # Instantiate and call the S3 scanner utility
    scanner_utility = S3Scanner()
    exec_status = scanner_utility.get_file_details(s3_access_key=access_key,
                                                   s3_secret_key=secret_key, s3_dir_path=s3_path,
                                                   pattern_list=patterns,
                                                   folder_pattern_details=folder_detail,
                                                   month_filter=num_months_filter,
                                                   s3_region=s3_region_name)

    if exec_status[STATUS_KEY] == STATUS_SUCCESS:
        sys.stdout.write(json.dumps(exec_status))
        sys.exit(0)
    else:
        sys.stderr.write(json.dumps(exec_status))
        sys.exit(1)
