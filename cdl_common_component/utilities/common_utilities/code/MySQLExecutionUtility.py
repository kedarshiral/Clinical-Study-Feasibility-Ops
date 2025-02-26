#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# #################################################Module Information############################################# #
#   Module Name         :   MySQL Execution Utility
#   Purpose             :   To restrict INSERT and UPDATE operations on MySQL for UNIX users.
#   Input Parameters    :   host, port, user, password, table, query_type, column-value, where
#   Output              :   NA
#   Execution Steps     :   Run from shell as 'python MySQLExecutionUtility.py --table ... --query-type...'
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Last changed on     :   3 September 2018
#   Last changed by     :   Vignesh Ravishankar
#   Reason for change   :   Enhancements - DELETE, BULK-INSERT
# ################################################################################################################ #

import os
import sys
import grp
import pwd
import time
import logging
import MySQLdb
import argparse
import traceback
import subprocess
import CommonConstants
from LogSetup import logger
from datetime import datetime
from xlrd import open_workbook
from MySQLdb import Error as MySqlError
from ConfigUtility import JsonConfigUtility
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager

execution_context = ExecutionContext()
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                  CommonConstants.ENVIRONMENT_CONFIG_FILE)

default_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_host"])
default_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
default_user = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_username"])
secret_password = MySQLConnectionManager().get_secret(
    configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "rds_password_secret_name"]),
    configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_region"]))
default_password = secret_password['password']
rows_to_update = CommonConstants.ROWS_TO_UPDATE
rows_to_delete = CommonConstants.ROWS_TO_DELETE

parser = argparse.ArgumentParser(description="This Utility updates the MySQL table.")
parser.add_argument("--host", nargs='?', default=default_host, help="The MySQL server to connect to")
parser.add_argument("--port", nargs='?', default=3306, help="The port of the MySQL server. Defaults to 3306")
parser.add_argument("--user", nargs='?', default=default_user, help="The user to login to MySQL")
parser.add_argument("--password", nargs='?', default=default_password, help="The password of the user mentioned")
parser.add_argument("--db", nargs='?', default=default_db, help="The database in MySQL to connect to")
parser.add_argument("--table", nargs='?', default=None, help="Table to be updated")
parser.add_argument("--query-type", nargs='?', default=None, help="Type of query to be executed - INSERT, UPDATE, "
                                                                  "DELETE or BULK-LOAD")
parser.add_argument("--column-value", nargs='+', default=None,
                    help="Enter the column and it's value. Eg:- \"column_name=value\"")
parser.add_argument("--where", nargs='?', default=None,
                    help="Enter the condition by which the columns should be updated. Eg:- \"column_id=value\"")
parser.add_argument("--file-path", nargs='?', default=None,
                    help="Enter the path to your workbook for loading bulk data. Eg:- /tmp/my_workbook.xslx")
parser.add_argument("--exclude-sheets", nargs='+', default=[],
                    help="Exclude sheets that need not be loaded by the utility")


class MySQLUpdateHandler(object):
    def __init__(self, host=None, user=None, password=None, db=None, table=None, query_type=None, columns=None,
                 conditions=None, port=3306):
        """
        Initialization of the handler. This gets values from the parseargs entered by the user, validates them and
         assigns them as objects to this class.
        """
        try:
            self.host = host
            self.mysql_user = user
            self.port = port
            self.password = password
            self.db = db
            self.table = table
            self.query_type = query_type
            self.columns = columns
            self.conditions = conditions
            self.utils_obj = Utils()
            self.user = self.utils_obj.get_effective_username()
            self.execution_context = ExecutionContext()
            self.connect = MySQLdb.connect(host=self.host, port=self.port, user=self.mysql_user, passwd=self.password,
                                           db=self.db, charset='utf8')
            self.authenticate_user_query = """
SELECT {rule_column} FROM ctl_audit_table_update_permission
WHERE table_name=\"{table}\" """
        except ValueError as err:
            raise ValueError(str(err))
        except NotImplementedError as err:
            raise NotImplementedError(str(err))

    def modify_based_on_data_type(self, data):
        """
        Parameters
        ----------
            :param data: The user input that needs to be checked and modified if necessary.
            :type data: str

        Returns
        -------
            :return: The modified/unmodified user input
            ":rtype: str
        """
        logger.info("Verifying the user input values for modification of input based on the column data type",
                    extra=self.execution_context.get_context())
        excluded_data_types = ["int", "float", "double", "numeric", "decimal", "bit"]
        query = """
SHOW columns FROM {table}
WHERE Field="{column}"
""".format(table=self.table, column=data.split('=')[0])
        query_output = self.fetch_values_from_mysql(query)
        for data_type in excluded_data_types:
            if data_type in query_output[0][1].lower():
                logger.debug("The column has a data type '{}'. Not modifying it.".format(data_type),
                             extra=self.execution_context.get_context())
                return data
        if '"' != list(data.split('=')[1])[0] or '"' != list(data.split('=')[1])[len(data.split('=')[1]) - 1]:
            logger.debug("User {} entered value for string datatype without a \". "
                         "Adding \" to the value entered by user".format(self.user),
                         extra=self.execution_context.get_context())
            return data.split('=')[0] + '=\"' + data.split('=')[1].replace('"', "\\\"") + '\"'
        else:
            logger.debug("User {} entered value with \" for string datatype."
                         " Proceeding with user input".format(self.user),
                         extra=self.execution_context.get_context())
            return data

    def check_where_condition(self, restricted_rows):
        """
        Returns
        -------
            :return: The number of rows affected by the UPDATE query.
            :rtype: int

        Raises
        ------
            An error is raised if the user is trying to update more than the configured number of rows.
            :raises: OverflowError
        """
        logger.debug("Validating the WHERE condition entered by user {}.".format(self.user),
                     extra=self.execution_context.get_context())
        query = """SELECT * FROM {table} WHERE {where}""".format(table=self.table, where=self.conditions)
        query_output = self.fetch_values_from_mysql(sql_query=query)
        if len(query_output) == 0:
            logger.error("{}'s query did not modify any value".format(self.user),
                         extra=self.execution_context.get_context())
            raise OverflowError("Your query does not modify any value")
        if len(query_output) > int(restricted_rows):
            if int(restricted_rows) == 1:
                logger.error("More than 1 row was modified by {}'s query. This was more than the configured value".
                             format(self.user), extra=self.execution_context.get_context())
                raise OverflowError("More than 1 row is being modified by your query. Please modify your query "
                                    "or contact your admin for help.".
                                    format(restricted_rows))
            else:
                logger.error("More than {} rows were modified by {}'s query. This was more than the "
                             "configured value.".format(restricted_rows, self.user),
                             extra=self.execution_context.get_context())
                raise OverflowError("More than {} rows are being modified by your query. Please modify your query "
                                    "or contact your admin for help.".
                                    format(restricted_rows))
        logger.info("User {}'s WHERE clause is validated".format(self.user), extra=self.execution_context.get_context())
        return len(query_output)

    def insert_sql_query(self):
        """
        This function forms a MySQL query for inserting columns.

        Raises
        ------
            Raises an error if this method faces an issue while updating in MySQL
            :raises RunTimeError
        """
        logger.info("User {} trying to insert data into table {}".format(self.user, self.table),
                    extra=self.execution_context.get_context())
        columns_list = ""
        values_list = ""
        try:
            logger.debug("Fetching the authentication rules from ctl_audit_table_update_permission for user {}".
                         format(self.user), extra=self.execution_context.get_context())
            rules = self.fetch_values_from_mysql(sql_query=self.authenticate_user_query.
                                                 format(table=self.table,
                                                        rule_column="group_id,insert_flag"))
            if self.utils_obj.authenticate_user(rules=rules):
                logger.debug("User {} authenticated successfully for table {}".format(self.user, self.table),
                             extra=self.execution_context.get_context())
                for conditions_index, column in enumerate(self.columns):
                    column = self.modify_based_on_data_type(column)
                    if (conditions_index + 1) == len(self.columns):
                        columns_list += column.split("=")[0]
                        values_list += column.split("=")[1]
                    else:
                        columns_list += column.split("=")[0] + ", "
                        values_list += column.split("=")[1] + ", "
                insert_query = """
INSERT INTO {table} ({columns})
VALUES ({values})""".format(table=self.table, columns=columns_list, values=values_list)
                logger.debug("Executing query {}".format(insert_query.replace('\n', ' ')),
                             extra=self.execution_context.get_context())
                self.execute_query_in_mysql(sql_query=insert_query)
                logger.debug("Insert query executed successfully!", extra=self.execution_context.get_context())
                logger.info("Logging this INSERT action", extra=self.execution_context.get_context())
                self.log_update(updated_columns="NULL", query=insert_query, num_rows_updated=1)
                logger.debug("Log table updated successfully", extra=self.execution_context.get_context())
            else:
                logger.error("User {} not authorized to INSERT into table '{}'".format(self.user, self.table),
                             extra=self.execution_context.get_context())
                raise EnvironmentError("You are not authorized to INSERT on the table '{}'!".format(self.table))
        except RuntimeError as err:
            raise RuntimeError(err)
        except EnvironmentError as err:
            raise EnvironmentError(err)

    def update_sql_query(self):
        """
        This function forms a MySQL query for updating columns.

        Raises
        ------
            Raises an error if no value is passed to the function or if the datatype of the passed argument
            does not match the expected datatype.
            :raises TypeError

            Raises an error if not permitted to update on a column.
            :raises ValueError

            Raises an error if this method faces an issue while updating in MySQL
            :raises RunTimeError
        """
        logger.info("User {} trying to update on table {}".format(self.user, self.table),
                    extra=self.execution_context.get_context())
        updated_columns = ""
        try:
            if type(self.table) is not str:
                logger.error("User {} hasn't passed a value for table or has entered a wrong data type for the table".
                             format(self.user), extra=self.execution_context.get_context())
                raise TypeError("Please enter a value for table name as a string ")
            if type(self.columns) is not list:
                logger.error("User {} hasn't passed a value for columns or has entered a wrong data type for the "
                             "columns".format(self.user), extra=self.execution_context.get_context())
                raise TypeError("Please enter the values of columns to be updated as a list.")
            if type(self.conditions) is not str:
                logger.error("User {} hasn't passed a value for conditions or has entered a wrong data type for the "
                             "conditions".format(self.user), extra=self.execution_context.get_context())
                raise TypeError("Please enter the values for conditions of update as a string.")

            logger.info("Authenticating {}'s access on the columns for updating.".format(self.user),
                        extra=self.execution_context.get_context())
            for column in self.columns:
                authenticate_column_query = self.authenticate_user_query.format(table=self.table,
                                                                                rule_column="group_id,update_flag")\
                                            + "and column_name=\"{}\"".format(column.split('=')[0])
                rules = self.fetch_values_from_mysql(sql_query=authenticate_column_query)
                if not self.utils_obj.authenticate_user(rules=rules):
                    logger.error("User {} not authorized to edit on column '{}'".format(self.user,
                                                                                        column.split('=')[0]),
                                 extra=self.execution_context.get_context())
                    raise EnvironmentError(
                        "You are not authorized to edit on column '{}'".format(column.split('=')[0]))
            len_of_query_output = self.check_where_condition(rows_to_update)
            sql_query = """UPDATE {}""".format(self.table)

            sql_query += """
SET """
            for columns_index, column in enumerate(self.columns):
                column = self.modify_based_on_data_type(column)
                if (columns_index + 1) == len(self.columns):
                    sql_query += "{}".format(column)
                    updated_columns += column.split("=")[0]
                else:
                    sql_query += "{}, ".format(column)
                    updated_columns += column.split("=")[0] + ", "

            sql_query += """
WHERE {}""".format(self.conditions)
            logger.debug("Executing query {}".format(sql_query.replace('\n', ' ')),
                         extra=self.execution_context.get_context())
            self.execute_query_in_mysql(sql_query=sql_query)
            logger.info("Update done successfully!", extra=self.execution_context.get_context())
            logger.debug("Logging this UPDATE action", extra=self.execution_context.get_context())
            self.log_update(updated_columns=updated_columns, query=sql_query, num_rows_updated=len_of_query_output)
            logger.debug("Logged Successfully", extra=self.execution_context.get_context())
        except RuntimeError as err:
            raise RuntimeError(err)
        except OverflowError as err:
            raise OverflowError(err)
        except EnvironmentError as err:
            raise EnvironmentError(err)

    def delete_sql_query(self):
        """
            This function forms a MySQL query for inserting columns.

            Raises
            ------
                Raises an error if this method faces an issue while updating in MySQL
                :raises RunTimeError
        """
        logger.info("User {} trying to delete data from table {}".format(self.user, self.table),
                    extra=self.execution_context.get_context())
        try:
            logger.debug("Fetching the authentication rules from ctl_audit_table_update_permission for user {}".
                         format(self.user), extra=self.execution_context.get_context())
            rules = self.fetch_values_from_mysql(sql_query=self.authenticate_user_query.
                                                 format(table=self.table,
                                                        rule_column="group_id,delete_flag "))
            if self.utils_obj.authenticate_user(rules=rules):
                logger.debug("User {} authenticated successfully for table {}".format(self.user, self.table),
                             extra=self.execution_context.get_context())
                len_of_query_output = self.check_where_condition(rows_to_delete)
                delete_query = """
DELETE FROM {table}
WHERE {condition}""".format(table=self.table, condition=self.conditions)
                logger.debug("Executing query {}".format(delete_query.replace('\n', ' ')),
                             extra=self.execution_context.get_context())
                self.execute_query_in_mysql(sql_query=delete_query)
                logger.info("Delete done successfully!", extra=self.execution_context.get_context())
                logger.debug("Logging this DELETE action", extra=self.execution_context.get_context())
                self.log_update(updated_columns="NULL", query=delete_query, num_rows_updated=len_of_query_output)
                logger.debug("Logged Successfully", extra=self.execution_context.get_context())
            else:
                logger.error("User {} not authorized to DELETE from table '{}'".format(self.user, self.table),
                             extra=self.execution_context.get_context())
                raise EnvironmentError("You are not authorized to DELETE on the table '{}'!".format(self.table))

        except RuntimeError as err:
            raise RuntimeError(err)
        except OverflowError as err:
            raise OverflowError(err)
        except EnvironmentError as err:
            raise EnvironmentError(err)

    def bulk_load(self, file_path, exclude_sheets):
        """
        Loads bulk data from an  excel workbook to MySQL. The table name will be picked from the sheet name in the
        workbook. If the sheet/table name is more than 31 characters long, the table name can be overridden using the
        argument parser.

        Parameters
        ---------
            :param file_path: The path of the workbook
            :type file_path: str
            :param exclude_sheets: The sheets that need to be excluded when loading bulk data
            :type exclude_sheets: list

        Raises
        ------
            :raises IOError, RuntimeError, EnvironmentError
        """
        try:
            logger.debug("Loading workbook from '{}'".format(file_path),
                         extra=self.execution_context.get_context())
            wb = open_workbook(file_path)
            sheets_names = wb.sheet_names()
            for sheets in sheets_names:
                cursor = self.connect.cursor()
                bulk_query = ""
                success = True
                primary_key = None
                if self.table is None or self.table in sheets_names:
                    if any(sheets in sl for sl in self.fetch_values_from_mysql("show tables")):
                        self.table = str(sheets)
                    else:
                        logger.error("There were no tables of the name '{table}' "
                                     "in '{db}'".format(table=sheets, db=self.db),
                                     extra=self.execution_context.get_context())
                        continue
                    if str(sheets) in exclude_sheets:
                        logger.error("The sheet {} is part of the user's input and has been excluded".
                                     format(str(sheets)), extra=self.execution_context.get_context())
                        continue
                logger.info("User {} trying to insert bulk data into table {}".format(self.user, self.table),
                            extra=self.execution_context.get_context())
                logger.debug("Fetching the authentication rules from ctl_audit_table_update_permission for user {}".
                             format(self.user), extra=self.execution_context.get_context())
                rules = self.fetch_values_from_mysql(sql_query=self.authenticate_user_query.
                                                     format(table=self.table,
                                                            rule_column="group_id,insert_flag"))
                if self.utils_obj.authenticate_user(rules=rules):
                    logger.debug("User {} authenticated successfully for table {}".format(self.user, self.table),
                                 extra=self.execution_context.get_context())
                    sheet = wb.sheet_by_name(str(sheets))
                    query = """
INSERT INTO {table} ({columns})
"""
                    for row_num, row in enumerate(sheet.get_rows()):
                        if row_num == 0:
                            columns = ""
                            for column_index, cell in enumerate(row):
                                try:
                                    if len(self.fetch_values_from_mysql("SHOW columns FROM {} WHERE Field='{}'".
                                                                        format(self.table,
                                                                               cell.value.lstrip(' ').rstrip(' ')))) \
                                            == 1:
                                        print(self.table)
                                        print(type(cell.value))
                                        print(column_index)
                                        if (column_index + 1) == len(row):
                                            if cell.ctype == 1:
                                                columns += str(cell.value)
                                                try:
                                                    index = self.fetch_values_from_mysql(
                                                        "show columns from {} where Field='{}'".
                                                        format(self.table,
                                                               cell.value.decode('utf-8').lstrip(' ').rstrip(' '))).\
                                                        index('PRI')
                                                    primary_key = {"PRI": column_index}
                                                    logger.debug("Primary key for table {} is"
                                                                 " {}".format(self.table,
                                                                              str(cell.value.encode('utf8'))),
                                                                 extra=self.execution_context.get_context())
                                                except ValueError:
                                                    pass
                                                except IndexError:
                                                    pass
                                            else:
                                                success = False
                                                break
                                        else:
                                            if cell.ctype == 1:
                                                columns += str(cell.value) + ", "
                                                try:
                                                    index = self.fetch_values_from_mysql(
                                                        "show columns from {} where Field='{}'".
                                                        format(self.table,
                                                               cell.value.decode('utf-8').rstrip(' ').lstrip(' ')))[0].\
                                                        index('PRI')
                                                    primary_key = {"PRI": column_index}
                                                    logger.debug("Primary key for table {} is"
                                                                 " {}".format(self.table,
                                                                              str(cell.value.encode('utf8'))),
                                                                 extra=self.execution_context.get_context())
                                                except ValueError:
                                                    pass
                                                except IndexError:
                                                    pass
                                            else:
                                                success = False
                                                break
                                    elif len(self.fetch_values_from_mysql("SHOW columns FROM {} WHERE Field='{}'".
                                                                          format(self.table,
                                                                                 cell.value.lstrip(' ').rstrip(' ')))) \
                                            == 0:
                                        logger.warn("Column '{}' not present in table '{}' in '{}'. Skipping it...".
                                                    format(cell.value, self.table, self.db),
                                                    extra=self.execution_context.get_context())
                                    else:
                                        logger.warn("Column '{}' has duplicates in table '{}' in '{}'. Skipping it...".
                                                    format(cell.value, self.table, self.db),
                                                    extra=self.execution_context.get_context())
                                except AttributeError:
                                    pass
                            query = query.format(table=self.table, columns=columns)
                        else:
                            values = ""
                            cells_type = []
                            for cell in row:
                                cells_type.append(cell.ctype)
                            if any(cell_type > 0 for cell_type in cells_type):
                                for column_index, cell in enumerate(row):
                                    if (column_index + 1) == len(row):
                                        if cell.ctype == 0:
                                            values += '\"\"'
                                        elif cell.ctype == 1:
                                            if str(cell.value) == "NULL" \
                                                    or str(cell.value) == "true"\
                                                    or str(cell.value) == "false" \
                                                    or str(cell.value) == "now()":
                                                values += str(cell.value)
                                            else:
                                                values += '\'' + str(cell.value) + '\''
                                        elif cell.ctype == 2:
                                            values += str(cell.value)
                                        elif cell.ctype == 5:
                                            success = False
                                            break
                                        else:
                                            success = False
                                            break
                                    else:
                                        if cell.ctype == 0:
                                            values += '\"\", '
                                        elif cell.ctype == 1:
                                            if str(cell.value) == "NULL" \
                                                    or str(cell.value) == "true" \
                                                    or (cell.value) == "false"\
                                                    or str(cell.value) == "now()":
                                                values += str(cell.value) + ', '
                                            else:
                                                values += '\'' + str(cell.value) + '\', '
                                        elif cell.ctype == 2:
                                            values += str(cell.value) + ', '
                                        elif cell.ctype == 5:
                                            success = False
                                            break

                                        else:
                                            success = False
                                            break
                                sql_query = query + "VALUES ({values})".format(values=values) + ";"
                                bulk_query += sql_query
                                try:
                                    if primary_key is None:
                                        logger.warn("No primary key found for table {}."
                                                    " Duplicates will be inserted".format(self.table),
                                                    extra=self.execution_context.get_context())
                                        logger.debug("Executing query {}".format(sql_query.replace('\n', ' ')),
                                                     extra=self.execution_context.get_context())
                                        cursor.execute(sql_query)
                                    else:
                                        logger.info("Performing an UPSERT action")
                                        delete_query = "DELETE FROM {} where {}={}".format(self.table,
                                                                                           sheet.row(0)
                                                                                           [primary_key['PRI']].value,
                                                                                           row[primary_key['PRI']].
                                                                                           value)
                                        logger.debug("Executing query {}".format(delete_query.lstrip(' ').rstrip(' ')),
                                                     extra=self.execution_context.get_context())
                                        cursor.execute(delete_query)
                                        logger.debug("Executing query {}".format(sql_query.replace('\n', ' ')),
                                                     extra=self.execution_context.get_context())
                                        cursor.execute(sql_query)
                                    logger.debug("Query transaction complete",
                                                 extra=self.execution_context.get_context())
                                except MySqlError as e:
                                    logger.error("Bulk load failed when executing query '{}'".format(sql_query),
                                                 extra=self.execution_context.get_context())
                                    logger.error("Rolling back transaction...",
                                                 extra=self.execution_context.get_context())
                                    logger.error("Error from MySQL --> {}".format(str(e)),
                                                 extra=self.execution_context.get_context())
                                    cursor.close()
                                    self.connect.rollback()
                                    success = False
                                    break
                            else:
                                logger.warn("Met with a row of blanks. Stopped processing the sheet "
                                            "'{}'".format(self.table), extra=self.execution_context.get_context())
                                success = False
                                break
                    if success:
                        cursor.close()
                        self.connect.commit()
                        for log_query in bulk_query.split(";"):
                            logger.info("Insert done successfully!", extra=self.execution_context.get_context())
                            logger.debug("Logging the BULK-LOAD action", extra=self.execution_context.get_context())
                            self.log_update(updated_columns="NULL", query=log_query,
                                            num_rows_updated=sheet.nrows - 1)
                        logger.debug("Logged successfully", extra=self.execution_context.get_context())
                    else:
                        cursor.close()
                        self.connect.rollback()
                        logger.error("All transactions rolled back for sheet {}".format(self.table),
                                     extra=self.execution_context.get_context())
                else:
                    logger.error("User {} not authorized to INSERT into table '{}'".format(self.user, self.table),
                                 extra=self.execution_context.get_context())
        except IOError as err:
            logging.error("There was an error with the file given by the user --> " + str(err))
            raise IOError("Please give a proper file path and ensure the utility has access to your workbook!")
        except RuntimeError as err:
            raise RuntimeError(err)
        except EnvironmentError as err:
            raise EnvironmentError(err)
        except Exception as err:
            logger.error("Faced an error when loading data from notebook to MySQL --> {}".format(str(err)),
                         extra=self.execution_context.get_context())
            logger.exception(traceback.print_exc(), extra=self.execution_context.get_context())
            raise Exception("Faced an issue when loading data from your workbook to MySQL. Please contact your"
                            " admin for help.")

    def execute_query_in_mysql(self, sql_query):
        """
        This function executes the query, that is being passed, in MySQL.

        Parameters
        ----------
            :param sql_query: The query to be executed in MySQL.
            :type sql_query: str

        Returns
        -------
            :return: Returns the success or failure of the MySQL query.
            :rtype: bool

        Raises
        ------
            :raises RuntimeError - If there is an error in executing the query in MySQL
       """
        try:
            cursor = self.connect.cursor()
            try:
                cursor.execute(sql_query)
            except KeyboardInterrupt:
                self.connect.rollback()
                cursor.close()
                logger.error("Execution was interrupted by user {}!".format(self.user),
                             extra=self.execution_context.get_context())
                raise MySqlError("\nThe execution was stopped!\n")
            self.connect.commit()
            cursor.close()
            return True
        except MySqlError as err:
            logger.error("There was an error from MySQL --> {}".format(str(err)),
                         extra=self.execution_context.get_context())
            raise RuntimeError(str(err))

    def fetch_values_from_mysql(self, sql_query):
        """
        Fetches all the rows that are valid from the SELECT query sent to this function.

        Parameters
        ----------
            :param sql_query: The SELECT query to be executed
            :type sql_query: str

        Returns
        -------
            :return: The output data from MySQL.
            :rtype: tuple

        Raises
        ------
            :raises RuntimeError - If there is an error in executing the query in MySQL
        """
        try:
            cursor = self.connect.cursor()
            cursor.execute(sql_query)
            data = cursor.fetchall()
            cursor.close()
            return data
        except MySqlError as err:
            logger.error("There was an error in fetching from MySQl --> {}".format(str(err)),
                         extra=self.execution_context.get_context())
            raise RuntimeError(str(err))

    def log_update(self, updated_columns, query, num_rows_updated):
        """
        This method updates the log table with the necessary information on the update query.

        Parameters
        ----------
            :param updated_columns: The columns that were updated by the user.
            :type updated_columns: str or list
            :param query: The query that was used to update the columns.
            :type query: str
            :param num_rows_updated: The number of rows affected by the UPDATE query
            :type num_rows_updated: int

        Raises
        -------
            Raises any error that come from the execute query method.
            :raises RuntimeError
        """
        try:
            log_query = """
INSERT INTO log_audit_table_update (update_table, update_columns, effective_user, update_time, query_type, update_query, rows_updated)
VALUES ("{table}", "{columns}", "{user}", "{time}", "{query_type}", '{query}', {rows_updated})
""".format(table=self.table, columns=updated_columns, user=self.utils_obj.get_effective_username(),
                time=time.strftime('%Y-%m-%d %H:%M:%S'), query_type=self.query_type,
                query=query.replace('\n', ' ').replace("'", "\\\'"),
                rows_updated=num_rows_updated)
            self.execute_query_in_mysql(sql_query=log_query)
        except RuntimeError as err:
            raise RuntimeError(str(err))


class Utils(object):
    def __init__(self):
        self.execution_context = ExecutionContext()

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
        logger.info("Validating user input {}".format(parameter),
                    extra=self.execution_context.get_context())
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

    def get_effective_username(self):
        """
        This function returns the effective username of the process.
        :return: The effective username.
        :rtype: str
        """
        command_output = subprocess.Popen("who am i | awk '{print $1}'",
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE,
                                          shell=True)
        standard_output, standard_error = command_output.communicate()
        command_output_return_code = command_output.wait()
        if command_output_return_code == 0:
            logger.debug("Effective username was fetched successfully",
                         extra=self.execution_context.get_context())
            return standard_output.decode('utf-8').strip('\n')
        else:
            logger.warn("Current username was returned since there was a problem in fetching the effective user.",
                        extra=self.execution_context.get_context())
            return str(pwd.getpwuid(os.getuid()).pw_name)

    def authenticate_user(self, rules):
        """
        This function authorizes the permission of the UNIX user on the MySQL database.

        Parameters
        ----------
            :param rules: The roles that are allowed to INSERT or UPDATE on a column in a table.
            :type rules: list
        Returns
        -------
            :return: True if any user group exists in the rule in the MySQL rule table else False.
            :rtype: bool
        """
        user = self.get_effective_username()
        groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
        gid = pwd.getpwnam(user).pw_gid
        groups.append(grp.getgrgid(gid).gr_name)
        for group in groups:
            for rule in rules:
                if rule[0] == group:
                    if rule[1] == 1:
                        return True
        return False


def main():
    context = ExecutionContext()
    try:
        utils_obj = Utils()
        if utils_obj.verify_inputs("query_type").upper() == "INSERT":
            class_obj = MySQLUpdateHandler(host=utils_obj.verify_inputs("host"),
                                           port=int(utils_obj.verify_inputs("port")),
                                           user=utils_obj.verify_inputs("user"),
                                           password=utils_obj.verify_inputs("password"),
                                           db=utils_obj.verify_inputs("db"), table=utils_obj.verify_inputs("table"),
                                           query_type=utils_obj.verify_inputs("query_type").upper(),
                                           columns=utils_obj.verify_inputs("column_value"))
            class_obj.insert_sql_query()
            logger.info("Data given by user was succesfully inserted to the table",
                        extra=context.get_context())
        elif utils_obj.verify_inputs("query_type").upper() == "UPDATE":
            class_obj = MySQLUpdateHandler(host=utils_obj.verify_inputs("host"),
                                           port=int(utils_obj.verify_inputs("port")),
                                           user=utils_obj.verify_inputs("user"),
                                           password=utils_obj.verify_inputs("password"),
                                           db=utils_obj.verify_inputs("db"), table=utils_obj.verify_inputs("table"),
                                           query_type=utils_obj.verify_inputs("query_type").upper(),
                                           columns=utils_obj.verify_inputs("column_value"),
                                           conditions=utils_obj.verify_inputs("where"))

            class_obj.update_sql_query()
            logger.info("Data given by user was successfully updated in the table",
                        extra=context.get_context())
        elif utils_obj.verify_inputs("query_type").upper() == "DELETE":
            class_obj = MySQLUpdateHandler(host=utils_obj.verify_inputs("host"),
                                           port=int(utils_obj.verify_inputs("port")),
                                           user=utils_obj.verify_inputs("user"),
                                           password=utils_obj.verify_inputs("password"),
                                           db=utils_obj.verify_inputs("db"),
                                           table=utils_obj.verify_inputs("table"),
                                           query_type=utils_obj.verify_inputs("query_type").upper(),
                                           conditions=utils_obj.verify_inputs("where"))

            class_obj.delete_sql_query()
            logger.info("Data given by user was successfully deleted from the table",
                        extra=context.get_context())
        elif utils_obj.verify_inputs("query_type").upper() == "BULK-LOAD":
            class_obj = MySQLUpdateHandler(host=utils_obj.verify_inputs("host"),
                                           port=int(utils_obj.verify_inputs("port")),
                                           user=utils_obj.verify_inputs("user"),
                                           password=utils_obj.verify_inputs("password"),
                                           db=utils_obj.verify_inputs("db"),
                                           query_type=utils_obj.verify_inputs("query_type").upper(),
                                           table=getattr(parser.parse_args(), "table"))
            class_obj.bulk_load(utils_obj.verify_inputs("file_path"), getattr(parser.parse_args(), "exclude_sheets"))
            logger.info("Data given by user from excel was successfully inserted to the table",
                        extra=context.get_context())
        else:
            logger.error("User passed a query that is not supported")
            raise ValueError("The selected query type is not supported. Available options:- INSERT, UPDATE, DELETE,"
                             " BULK-LOAD")
    except ValueError as e:
        logger.error(str(e), extra=context.get_context())
    except TypeError as e:
        logger.error(str(e), extra=context.get_context())
    except NotImplementedError as e:
        logger.error(str(e), extra=context.get_context())
    except RuntimeError as e:
        logger.error(str(e), extra=context.get_context())
    except OverflowError as e:
        logger.error(str(e), extra=context.get_context())
    except EnvironmentError as e:
        logger.error(str(e), extra=context.get_context())
    except Exception as e:
        logger.error(str(e), extra=context.get_context())
        logger.error(traceback.print_exc(), extra=context.get_context())
    finally:
        try:
            """
            Close the MySQL connection
            """
            class_obj.connect.close()
            logger.info("Connection to db closed", extra=context.get_context())
        except UnboundLocalError:
            pass
        except OSError:
            pass


if __name__ == '__main__':
    main()