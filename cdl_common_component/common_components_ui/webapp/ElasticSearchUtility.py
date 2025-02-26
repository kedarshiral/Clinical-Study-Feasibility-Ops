#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
# ################################################## Module Information ################################################
# Module Name           :   ElasticSearchUtility
# Purpose               :   Performs following operation:
#                               1. Delete indexes, type, document from Elastic Search
#                               2. Insert new document into elastic search
#                               3. Update existing document in elastic search
#                               4. Search information for particular index, type, document
#                               5. Return total document count which matches given condition
#                               6. Create index with customized settings
#                               7. Performs Rest api request
#                               8. Ping elastic search server
#   Input               :   IP address and port no of Elastic Search server.
#   Output              :   Performs the required operation and return result
#   Pre-requisites      :   LogSetup, ConfigUtility, ExecutionContext
#   Last changed on     :   13th August 2015
#   Last changed by     :   Akash Hudge
#   Reason for change   :   Modification in main function
########################################################################################################################


import traceback
import urllib2
import json
import sys
import EdlConstants

from elasticsearch import Elasticsearch
from elasticsearch import client
from LogSetup import logger
from ExecutionContext import ExecutionContext


MODULE_NAME = "ElasticSearchUtility"
DEFAULT_PORT_NO = "9200"
DEFAULT_SIZE = 1000

# ################################### ElasticSearchUtility #############################################################
# Class contains all the functions related ElasticSearchUtility
########################################################################################################################


class ElasticSearchUtility:
    # Initializer method of ElasticSearchUtility which requires IP address and port no
    def __init__(self, ip_address, port_no=DEFAULT_PORT_NO, execution_context=None):
        if execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.ip_address = ip_address
        self.port_no = port_no
        self.elastic_search = Elasticsearch(self.ip_address + ":" + self.port_no, timeout=500)

    # ############################################ create_index ########################################################
    # Purpose   :   This method create the index in elastic search.
    # Input     :   index - index_name, body - dictionary with index settings
    # Output    :   Returns result obtained after creating index
    # ##################################################################################################################
    def create_index(self, index, body=None):
        status_message = ""
        try:
            status_message = "Starting function to create index"
            logger.debug(status_message, extra=self.execution_context.get_context())

            if not index:
                status_message = "Invalid input : function argument 'index' is None"
                raise Exception(status_message)

            for char in index:
                if char.isupper():
                    status_message = "Invalid index: Document index '" + index + "' contains upper character"
                    raise Exception(status_message)

            if self.elastic_search.indices.exists(index=index):
                status_message = "Index '" + index + "' already exists"
                logger.warning(status_message, extra=self.execution_context.get_context())
                return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED,
                               EdlConstants.ERROR_KEY: status_message}
                return return_dict

            else:
                status_message = "Index '" + index + "' does not exists, so creating it"
                logger.debug(status_message, extra=self.execution_context.get_context())
                result = self.elastic_search.indices.create(index=index, body=body)

            status_message = "Completing function to create index"
            logger.debug(status_message, extra=self.execution_context.get_context())

            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: result}
            return return_dict

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ execute_custom_query ################################################
    # Purpose   :   This method executes REST api request on the basis of 'url_query' parameter
    # Input     :   url_query - Elastic search query in url(string) format, query body - dictionary
    # Output    :   Returns the result obtained after executing REST api
    ####################################################################################################################
    def execute_custom_query(self, url_query, query_body=None):
        status_message = ""
        try:
            status_message = "Starting function to execute custom query in elastic search"
            logger.debug(status_message, extra=self.execution_context.get_context())

            if query_body and type(query_body) is not dict:
                status_message = "Invalid input: query_body is not dictionary"
                raise Exception(status_message)

            url = "http://" + self.ip_address + ":" + self.port_no
            if url_query:
                url = url + url_query

            status_message = "URL for elastic search: " + url
            logger.debug(status_message, extra=self.execution_context.get_context())

            if query_body:
                query_body = json.dumps(query_body)

            result = urllib2.urlopen(url, query_body)
            result = result.read()
            try:
                result = json.loads(result)
            except:
                pass

            status_message = "Completing function to execute custom query in elastic search"
            logger.debug(status_message, extra=self.execution_context.get_context())

            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: result}
            return return_dict

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ get_document_count ##################################################
    # Purpose   :   This method gives the documents/records count which matches the given condition.
    # Input     :   Document index, document type, query_body - dictionary which contains condition,
    #               query_param - string which contains condition
    # Output    :   Returns the total count of documents which matches the given condition
    # ##################################################################################################################
    def get_document_count(self, doc_index=None, doc_type=None, query_body=None, query_param=None):
        status_message = ""
        try:
            status_message = "Starting function to get document count"
            logger.debug(status_message, extra=self.execution_context.get_context())

            if query_body and type(query_body) is not dict:
                status_message = "Invalid input: query_body is not a dictionary"
                raise Exception(status_message)
            status_message = "Executing search operation to get document count"
            if query_param:
                result = self.elastic_search.search(index=doc_index, doc_type=doc_type, q=query_param, size=0)
                size = result["hits"]["total"]
            else:
                result = self.elastic_search.search(index=doc_index, doc_type=doc_type, body=query_body, size=0)
                size = result["hits"]["total"]

            status_message = "Total no of document count is: " + str(size)
            logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function get document count"
            logger.debug(status_message, extra=self.execution_context.get_context())

            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: size}
            return return_dict

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ delete ##############################################################
    # Purpose   :   This method delete the document index, document type, document id
    # Input     :   Document index, document type, document id
    # Output    :   Returns the result obtained after deleting document index or document type or document id
    # ##################################################################################################################
    def delete(self, doc_index, doc_type=None, doc_id=None):
        status_message = ""
        try:
            status_message = "Starting function to delete document in elastic search"
            logger.debug(status_message, extra=self.execution_context.get_context())
            result = ""
            if not doc_index:
                status_message = "Invalid input: document index is None"
                raise Exception(status_message)
            elif not doc_type and doc_id:
                status_message = "Invalid input: Document index, Document id has been passed but " \
                                 "Document type is not present"
                raise Exception(status_message)

            status_message = "Executing delete operation on elastic search"
            if doc_index and doc_type and doc_id:
                result = self.elastic_search.delete(index=doc_index, doc_type=doc_type, id=doc_id)
            else:
                index_client = client.IndicesClient(self.elastic_search)
                if doc_index and doc_type:
                    result = index_client.delete_mapping(index=doc_index, doc_type=doc_type)
                elif doc_index:
                    result = index_client.delete(index=doc_index)

            status_message = "Completing function delete document in elastic search"
            logger.debug(status_message, extra=self.execution_context.get_context())

            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: result}
            return return_dict

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ update ##############################################################
    # Purpose   :   This method update the document/record.
    # Input     :   Document index, document type, document id, data - dictionary which will get updated
    # Output    :   Returns result obtained after updating document
    # ##################################################################################################################
    def update(self, doc_index, doc_type, doc_id, data):
        status_message = ""
        try:
            status_message = "Starting function to update document"
            logger.debug(status_message, extra=self.execution_context.get_context())

            if type(data) is not dict:
                status_message = "Invalid input: function argument 'data' is not dictionary"
                raise Exception(status_message)

            if not doc_index or not data or not doc_type or not doc_id:
                status_message = "Invalid input: document index or document type, or " \
                                 "document id or data has None value"
                raise Exception(status_message)

            status_message = "Executing update operation on elastic search"
            result = self.elastic_search.update(index=doc_index, doc_type=doc_type, id=doc_id, body=data)

            status_message = "Completing function update document"
            logger.debug(status_message, extra=self.execution_context.get_context())

            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: result}
            return return_dict

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ insert ##############################################################
    # Purpose   :   This method is used to insert document/record inside elastic search.
    # Input     :   Document index, document type, document id, data - dictionary which will get inserted
    # Output    :   Returns the result obtained after inserting document
    # ##################################################################################################################
    def insert(self, doc_index, doc_type, data, doc_id=None):
        status_message = ""
        try:
            status_message = "Starting function to insert document"
            logger.debug(status_message, extra=self.execution_context.get_context())

            if data and type(data) is not dict:
                status_message = "Invalid input: data is not dictionary"
                raise Exception(status_message)

            if doc_index is None or doc_type is None or data is None:
                status_message = "Invalid input: document index or document type or data has None value"
                raise Exception(status_message)

            for char in doc_index:
                if char.isupper():
                    status_message = "Document index '" + doc_index + "' contains upper character"
                    raise Exception(status_message)

            status_message = "Executing insert operation on elastic search"
            if doc_id is None:
                status_message = "Document id is not passed as parameter, so auto generating document id"
                logger.debug(status_message, extra=self.execution_context.get_context())
                result = self.elastic_search.index(index=doc_index, doc_type=doc_type, body=data)
            else:
                result = self.elastic_search.index(index=doc_index, doc_type=doc_type, body=data, id=doc_id)

            status_message = "Completing function insert document"
            logger.debug(status_message, extra=self.execution_context.get_context())

            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: result}
            return return_dict

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ select ##############################################################
    # Purpose   :   This method returns all the documents which matches the given condition
    # Input     :   Document index, document type, document id, query body - dictionary,
    #               source - True/False, query parameter, size - no of records to be returned, hits - to parse result
    #               obtained from elastic search.
    # Output    :   Returns list of documents which matches the given condition
    ####################################################################################################################
    def select(self, doc_index=None, doc_type=None, doc_id=None, query_body=None, source=True, query_param=None,
               size=DEFAULT_SIZE, hits=False):
        status_message = ""
        try:
            status_message = "Starting function to select document in elastic search"
            logger.debug(status_message, extra=self.execution_context.get_context())

            if query_body and type(query_body) is not dict:
                status_message = "Invalid input: query_body is not dictionary"
                raise Exception(status_message)

            if doc_id and doc_type and not doc_index:
                status_message = "Invalid input: document id and document type is present but document index is None"
                raise Exception(status_message)

            if doc_id and not doc_index:
                status_message = "Invalid input: document id is present but document index is None"
                raise Exception(status_message)

            if doc_index and doc_type and doc_id:
                result = self.elastic_search.get(index=doc_index, doc_type=doc_type, id=doc_id, _source=source)
            elif doc_index and doc_id:
                result = self.elastic_search.get(index=doc_index, id=doc_id, _source=source)
            elif query_param:
                result = self.elastic_search.search(index=doc_index, doc_type=doc_type, body=query_body, q=query_param,
                                                    _source=source, size=size)
            else:
                result = self.elastic_search.search(index=doc_index, doc_type=doc_type, body=query_body, _source=source,
                                                    size=size)
            if hits is False:
                if "hits" in result:
                    if "hits" in result["hits"]:
                        result = result["hits"]["hits"]

            status_message = "Completing function to select document in elastic search"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: result}
            return return_dict

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ ping_elastic_search #################################################
    # Purpose   :   This method is used to check connection with Elastic search.
    # Input     :   The class instance
    # Output    :   Returns True or False
    # ##################################################################################################################
    def ping_elastic_search(self):
        status_message = ""
        try:
            status_message = "Pinging Elastic Search"
            logger.debug(status_message, extra=self.execution_context.get_context())
            flag = self.elastic_search.ping()
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_SUCCESS, EdlConstants.RESULT_KEY: flag}
            return return_dict

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict

    # ############################################ main ################################################################
    # Purpose   :   This method is used to call all methods in ElasticSearchUtility.py module.
    # Input     :   input dictionary - It contains function type and all the keys which are required to
    #               call the function
    # Output    :   Returns result obtained from function
    # ##################################################################################################################
    def main(self, input_dict):
        status_message = ""
        try:
            status_message = "Starting the main function for ElasticSearch utility"
            logger.info(status_message, extra=self.execution_context.get_context())

            function_type = input_dict["function_type"]
            doc_index, index, doc_type, data, doc_id, query_param, query_body, url_query, body = (None,) * 9
            source = True
            hits = True
            size = DEFAULT_SIZE
            
            key_list = {"doc_index": doc_index, "doc_type": doc_type, "doc_id": doc_id, "data": data,
                        "query_body": query_body, "query_param": query_param, "url_query": url_query, "source": source,
                        "size": size, "body": body, "index": index, "hits": hits}

            argument_list = input_dict["function_argument"]

            for key in key_list:
                    if key in argument_list:
                        key_list[key] = argument_list[key]

            if function_type == "ping_elastic_search":
                result = self.ping_elastic_search()

            elif function_type == "select":
                result = self.select(doc_index=key_list["doc_index"], doc_type=key_list["doc_type"],
                                     doc_id=key_list["doc_id"], query_body=key_list["query_body"],
                                     source=key_list["source"], query_param=key_list["query_param"],
                                     size=key_list["size"], hits=key_list["hits"])

            elif function_type == "insert":
                result = self.insert(doc_index=key_list["doc_index"], doc_type=key_list["doc_type"], 
                                     data=key_list["data"], doc_id=key_list["doc_id"])

            elif function_type == "update":
                result = self.update(doc_index=key_list["doc_index"], doc_type=key_list["doc_type"],
                                     doc_id=key_list["doc_id"], data=key_list["data"])

            elif function_type == "delete":
                result = self.delete(doc_index=key_list["doc_index"], doc_type=key_list["doc_type"],
                                     doc_id=key_list["doc_id"])

            elif function_type == "get_document_count":
                result = self.get_document_count(doc_index=key_list["doc_index"], doc_type=key_list["doc_type"],
                                                 query_body=key_list["query_body"], query_param=key_list["query_param"])

            elif function_type == "execute_custom_query":
                result = self.execute_custom_query(url_query=key_list["url_query"], query_body=key_list["query_body"])

            elif function_type == "create_index":
                result = self.create_index(index=key_list["index"], body=key_list["body"])

            else:
                status_message = "function type '" + function_type + "' is invalid"
                raise Exception(status_message)

            status_message = "Completing the main function for ElasticSearch utility"
            logger.info(status_message, extra=self.execution_context.get_context())

            return result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            return_dict = {EdlConstants.STATUS_KEY: EdlConstants.STATUS_FAILED, EdlConstants.ERROR_KEY: str(e)}
            return return_dict


if __name__ == "__main__":
    if sys.argv.__len__() == 1:
        raise Exception("ElasticSearchUtility json path is not passed")
    handle = file(str(sys.argv[1]))
    json_input = json.load(handle)

    if "ip_address" in json_input:
        ip_addr = json_input["ip_address"]
    else:
        raise Exception("'ip_address' key is not present in json file")

    if "port_no" in json_input:
        port = json_input["port_no"]
    else:
        raise Exception("'port_no' key is not present in json file")

    elastic_search = ElasticSearchUtility(ip_address=ip_addr, port_no=port)
    result_output = elastic_search.main(json_input)
    status_msg = "\nCompleted execution for Elastic Search Utility with status: " + json.dumps(result_output) + "\n"
    sys.stdout.write(status_msg)