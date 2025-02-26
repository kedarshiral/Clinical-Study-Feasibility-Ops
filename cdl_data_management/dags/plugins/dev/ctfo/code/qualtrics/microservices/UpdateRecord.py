import os
import copy
import sys
import traceback
import json
import base64
from datetime import datetime
import ast
import traceback
service_directory_path = os.path.dirname(os.path.abspath(__file__))
utilities_dir_path = os.path.abspath(os.path.join(service_directory_path, "../utilities/"))
sys.path.insert(0, utilities_dir_path)
import CommonServicesConstants as Constants
from DatabaseUtility import DatabaseUtility
from LogSetup import get_logger
logger = get_logger()

class UpdateRecord:

    def update_record(self,table_name, schema_name, connection_object, keys_to_be_updated, update_flag,
                      log_params = None, update_where_clause_json = None,category =None,handle_no_record = False):
        """
            Purpose :   Method to update scenario
            Inputs  :   table_name -> table name
                        schema_name -> schema name
                        connection_object -> connection string to ythe database
                        keys_to_be_updated -> Contains key,,value pairs that needs to updated/inserted
                        update_flag -> If update flag = Y, then the previous record needs to be updated
                                       else if update flag = M, then there are multiple entries for one previous record that needs to be updated
                                       else it is an insert operation
                        update_where_clause_json -> Contains key,value pairs that uniquely identifies a record in table
            Output  :   JSON body of the format - {"status": "SUCCESS"/"FAILED", "result": "", "error": ""}
        """
        try:
            if update_flag == 'Y':
                logger.debug("Update flag == Y",extra=log_params)
                logger.debug("Inside update_record Function")
                logger.debug("Keys to be Updated : {}".format(json.dumps(keys_to_be_updated)))
                select_query = """ select * from {schema_name}.{table_name} """.format(schema_name=schema_name,table_name=table_name)
                where_query = ""
                # where clause should come from update_where_clause_json
                # update_where_clause_json will contain key that will uniquely identify a record like scenario_id
                logger.debug("update_where_clause_json - {}".format(str(update_where_clause_json)), extra=log_params)

                current_time = ''
                last_updated_by = ''
                if "last_updated_by" not in keys_to_be_updated:
                    raise Exception("last_updated_by not in the input json of the keys to be updated")
                else:
                    last_updated_by = keys_to_be_updated["last_updated_by"]

                if "last_updated_time" not in keys_to_be_updated:
                    current_timestamp = datetime.utcnow()
                    current_time = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")
                else:
                    current_time = keys_to_be_updated["last_updated_time"]
                logger.debug("update_where_clause_json - {}".format(str(update_where_clause_json)), extra=log_params)
                for key,value in update_where_clause_json.items():
                    if value!='':
                        if isinstance(value,bool):
                            where_query = where_query + " {key}".format(key=key) + " =" + str(value) + " and "
                        elif isinstance(value,str):
                            where_query = where_query + " {key}".format(key=key) + " = '" + value.replace("'", "''") + "' and "
                        elif isinstance(value,int):
                            where_query = where_query + " {key}".format(key=key) + " =" + str(value) + " and "
                        elif isinstance(value,list):
                            value_list = []
                            for i in value:
                               value_list.append(i.replace("'","''"))
                            value_str = "','".join(value_list)
                            value_str = "'" + value_str + "'"
                            where_query = where_query + " {key} in (".format(key=key)  + value_str + ") and "
                        elif isinstance(value, float):
                            where_query = where_query + " {key}".format(key=key) + " =" + str(value) + " and "
                        elif value == None :
                            where_query = where_query + " {key}".format(key=key) + " is null and "

                where_query = where_query.strip()
                if where_query.endswith("and"):
                    where_query = where_query.rsplit(' ', 1)[0]
                if len(where_query) != 0:
                    select_query = select_query + " where " + where_query
                logger.debug("Where query - {}".format(str(where_query)), extra=log_params)
                logger.debug("Select query - {}".format(str(select_query)), extra=log_params)

                query_output = DatabaseUtility().execute(conn=connection_object, query=select_query,
                                                         auto_commit=False)
                if query_output[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                    raise Exception(" Failed while executing query ")
                logger.debug("Query output for select query - {}".format(str(query_output)), extra=log_params)
                query_output = query_output[Constants.RESULT_KEY]


                if len(query_output) == 0 and handle_no_record:
                    logger.info("No record needs to be updated")
                    return
                if len(query_output) == 0:
                    raise Exception("No record found for update")

                # if more than one record in input json raise exception
                # if len(query_output) > 1:
                #     raise Exception("Got more than one record in select query")

                # added the json received from input into the previous record selected
                logger.debug("keys_to_be_updated: - {}".format(keys_to_be_updated), extra=log_params)
                for key, value in keys_to_be_updated.items():
                    query_output[0][key] = value

                query_output[0]["last_updated_time"] = current_time

                logger.debug("query output after adding keys that needs to be updated - {}".format(str(query_output)), extra=log_params)

                query_output = query_output[0]
                logger.debug("Updated query output result - {}".format(query_output), extra=log_params)

                # updating values for RDS list type columns
                # list_type_columns = ["default_data_driven_list_columns", "default_recommended_list_columns",
                #                      "default_selected_list_columns"]
                # # replacing '[' with '{' for list type columns in RDS
                # for column in list_type_columns:
                #     if column in query_output.keys():
                #         query_output[column] = "{" + query_output[column][1:-1].replace("'", "''") + "}"
                update_existing_record_query = """
                                               update {schema_name}.{table_name}
                                               set is_active = False,
                                               last_updated_by = '{last_updated_by}',
                                               last_updated_time = '{current_time}'
                                               """
                update_existing_record_query = update_existing_record_query.format(schema_name=schema_name,
                                                                                   table_name=table_name,
                                                                                   last_updated_by=last_updated_by,
                                                                                   current_time=current_time)

                update_existing_record_query = update_existing_record_query + " where " + where_query
                logger.debug(" Update query : {} ".format(str(update_existing_record_query)), extra=log_params)
                logger.debug(" Updating the previous record status to 'N' ", extra=log_params)
                update_query_response = DatabaseUtility().execute(query=update_existing_record_query,
                                                                  conn=connection_object,
                                                                  auto_commit=False)
                logger.debug("Update query response: {}".format(json.dumps(update_query_response)), extra=log_params)
                if update_query_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                    logger.debug("Update query failed :{}".format(json.dumps(update_query_response)),
                                 extra=log_params)
                    raise Exception(
                        "Failed to execute the update query. {}".format(update_query_response[Constants.ERROR_KEY]))
                logger.debug("Calling insert in update part")
                insert_response = self.insert_record(connection_object=connection_object, input_json=query_output,schema_name=schema_name, table_name=table_name, log_params=log_params)
                return insert_response

            elif update_flag == 'M':
                logger.debug("Update flag == M",extra=log_params)
                select_query = """ select * from {schema_name}.{table_name} """.format(schema_name=schema_name,table_name=table_name)
                where_query = ""
                # where clause should come from update_where_clause_json
                # update_where_clause_json will contain key that will uniquely identify a record like scenario_id
                logger.debug("update_where_clause_json - {}".format(str(update_where_clause_json)), extra=log_params)

                current_time = ''
                last_updated_by = ''
                if "last_updated_by" not in keys_to_be_updated:
                    raise Exception("last_updated_by not in the input json of the keys to be updated")
                else:
                    last_updated_by = keys_to_be_updated["last_updated_by"]

                if "last_updated_time" not in keys_to_be_updated:
                    current_timestamp = datetime.utcnow()
                    current_time = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")
                else:
                    current_time = keys_to_be_updated["last_updated_time"]
                logger.debug("update_where_clause_json - {}".format(str(update_where_clause_json)), extra=log_params)
                for key,value in update_where_clause_json.items():
                    if value!='':
                        if isinstance(value,bool):
                            where_query = where_query + " {key}".format(key=key) + " =" + str(value) + " and "
                        elif isinstance(value,str):
                            where_query = where_query + " {key}".format(key=key) + " = '" + value.replace("'", "''") + "' and "
                        elif isinstance(value,int):
                            where_query = where_query + " {key}".format(key=key) + " =" + str(value) + " and "
                        elif isinstance(value,list):
                            value_list = []
                            for i in value:
                               value_list.append(i.replace("'","''"))
                            value_str = "','".join(value_list)
                            value_str = "'" + value_str + "'"
                            where_query = where_query + " {key} in (".format(key=key)  + value_str + ") and "
                        elif isinstance(value, float):
                            where_query = where_query + " {key}".format(key=key) + " =" + str(value) + " and "
                        elif value == None :
                            where_query = where_query + " {key}".format(key=key) + " is null and "

                where_query = where_query.strip()
                if where_query.endswith("and"):
                    where_query = where_query.rsplit(' ', 1)[0]

                if len(where_query) != 0:
                    select_query = select_query + " where " + where_query
                logger.debug("Where query - {}".format(str(where_query)), extra=log_params)
                logger.debug("Select query - {}".format(str(select_query)), extra=log_params)

                query_output = DatabaseUtility().execute(conn=connection_object, query=select_query,
                                                         auto_commit=False)
                if query_output[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                    raise Exception(" Failed while executing query ")
                logger.debug("Query output for select query - {}".format(str(query_output)), extra=log_params)
                query_output = query_output[Constants.RESULT_KEY]

                if len(query_output) == 0:
                    raise Exception("No record found for update")

                # added the json received from input into the previous record selected
                for key, value in keys_to_be_updated.items():
                    query_output[0][key] = value

                query_output[0]["last_updated_time"] = current_time

                logger.debug("query output after adding keys that needs to be updated - {}".format(str(query_output)), extra=log_params)

                query_output = query_output[0]
                logger.debug("Updated query output result - {}".format(query_output), extra=log_params)

                if category == 'last_record' :
                    update_existing_record_query = """
                                               update {schema_name}.{table_name}
                                               set is_active = False,
                                               last_updated_by = '{last_updated_by}',
                                               last_updated_time = '{current_time}'
                                               """
                    update_existing_record_query = update_existing_record_query.format(schema_name=schema_name,
                                                                                       table_name=table_name,
                                                                                       last_updated_by=last_updated_by,
                                                                                       current_time=current_time)

                    update_existing_record_query = update_existing_record_query + " where " + where_query
                    logger.debug(" Update query : {} ".format(str(update_existing_record_query)), extra=log_params)
                    logger.debug(" Updating the previous record status to 'N' ", extra=log_params)
                    update_query_response = DatabaseUtility().execute(query=update_existing_record_query,
                                                                      conn=connection_object,
                                                                      auto_commit=False)
                    logger.debug("Update query response: {}".format(json.dumps(update_query_response)), extra=log_params)
                    if update_query_response[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                        logger.debug("Update query failed :{}".format(json.dumps(update_query_response)),
                                     extra=log_params)
                        raise Exception(
                            "Failed to execute the update query. {}".format(update_query_response[Constants.ERROR_KEY]))

                logger.debug("Calling insert in update part")
                insert_response = self.insert_record(connection_object=connection_object, input_json=query_output,schema_name=schema_name, table_name=table_name, log_params=log_params)
                return

            else:
                logger.debug(" Insert operation is to be performed ", extra=log_params)
                current_time = ""
                created_time = ""
                last_updated_time = ""
                current_timestamp = datetime.utcnow()
                current_time = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

                if "created_by" not in keys_to_be_updated:
                    raise Exception(" Created_by key must be present is input json ")
                if "created_time" not in keys_to_be_updated:
                    created_time = current_time
                    keys_to_be_updated["created_time"] = current_time
                if "last_updated_time" not in keys_to_be_updated:
                    last_updated_time = current_time
                    keys_to_be_updated["last_updated_time"] = current_time
                if "last_updated_by" not in keys_to_be_updated:
                    keys_to_be_updated["last_updated_by"] = keys_to_be_updated["created_by"]

                logger.debug("Calling insert in insert part")
                insert_response = self.insert_record(connection_object=connection_object, input_json=keys_to_be_updated,
                                                     schema_name=schema_name, table_name=table_name, log_params=log_params)


        except Exception as e:
            logger.debug("Exception in Update Record : " + str(e))
            raise Exception(traceback.format_exc())

    def insert_record(self,connection_object,input_json,schema_name,table_name,log_params):
        """
                Purpose :   Method to insert record
                Inputs  :   table_name -> table name
                            schema_name -> schema name
                            connection_object -> connection string to ythe database
                            input_json -> Contains key,,value pairs that needs to updated/inserted

                Output  :   JSON body of the format - {"status": "SUCCESS"/"FAILED", "result": "", "error": ""}
        """
        try:
            logger.debug("Input Json: {}".format(json.dumps(input_json)))
            logger.debug("Starting function insert_record")
            insert_query = """insert into {schema_name}.{table_name} (""".format(schema_name=schema_name, table_name=table_name)

            value_query = """"""
            insert_values = """"""
            logger.debug("Value Query :{}".format(value_query))
            logger.debug("Insert Values :{}".format(insert_values))
            for key, value in input_json.items():
                logger.info("Key: {}, Value :{}".format(key, value))
                insert_values = insert_values + key + ","
                if isinstance(value, str):
                    value = "'" + value.replace("'", "''") + "'"
                    value_query = value_query + value + ","
                elif isinstance(value, list):
                    value = ",".join(value)
                    value = "'" + value.replace("'", "''") + "'"
                    value_query = value_query + value + ","
                elif isinstance(value, bool):
                    value_query = value_query + str(value) + ","
                elif isinstance(value, int):
                    value_query = value_query + str(value) + ","
                elif isinstance(value, float):
                    value_query = value_query + str(value) + ","
                elif value is None:
                    logger.debug("Value - {} is None".format(str(value)))
                    value_query = value_query + 'NULL' + ","
            logger.debug("Insert query - {} ".format(str(insert_query)))
            insert_values = insert_values[:-1]
            logger.debug("Insert query after removing last comma- {} ".format(str(insert_query)))
            value_query = value_query[:-1]

            logger.debug("Value query - {}".format(str(value_query)))
            if len(input_json) != 0:
                insert_query = insert_query + insert_values + ") values (" + value_query + ")"

            logger.debug("Final insert query  - {}".format(str(insert_query)), extra=log_params)

            insert_query_status = DatabaseUtility().execute(
                query=insert_query, conn=connection_object, auto_commit=False)
            logger.debug("Output from DatabaseUtility - {}".format(str(insert_query_status)), extra=log_params)
            if insert_query_status[Constants.STATUS_KEY].lower() == Constants.FAILED_KEY.lower():
                raise Exception("Failed to save the list, ERROR - {}".format(insert_query_status[Constants.ERROR_KEY]))
            return
        except Exception as e:
            raise Exception(traceback.format_exc())