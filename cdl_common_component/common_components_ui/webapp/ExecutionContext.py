#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

##################################Module Information##############################
#   Module Name         : Execution Context
#   Purpose             : Contains class and functions used for storing and fetching execution context parameters  in
#                         all other python modules
#   How to run          : 1. Initiate an instance of class ExecutionContext
#                         2. Refer to different class components
#   Pre-requisites      : 1. import ExecutionContext class
#   Last changed on     : 26th August 2014
#   Last changed by     : Shreekant Agrawal
#   Reason for change   : comments to the code
##################################################################################

from datetime import datetime
import inspect

MODULE_NAME = "Context Utility"


class ExecutionContext(object):
    #########################################################init#######################################################
    # Purpose            :   Initialize an instance and assigns a default value to the properties of the instance
    ####################################################################################################################
    def __init__(self):
        #fields needed for logging purpose
        self.current_module = ""
        self.function_start_time = datetime.now()
        self.process_name = ""
        self.function_name = ""
        self.parent_function_name = ""
        self.function_status = "STARTED"
        self.traceback = ""
        self.process_identifier = ""
        #dictionary required for the purpose of logging; containing key-value for all the fields of the log
        self.context_dict = {"current_module": self.current_module, "function_start_time": self.function_start_time,
                             "process_name": self.process_name, "function_name": self.function_name,
                             "process_identifier": self.process_identifier,
                             "parent_function_name": self.parent_function_name, "function_status": self.function_status,
                             "traceback": self.traceback}

    ###################################################set_context######################################################
    # Purpose            :   Copy parameters from one context dictionary to another. It is used when execution context
    #                        parameters needs to be passed from one module to another
    # Input              :   input_context_dict is the source instance whose properties is copied to another instance
    # Output             :   output_context with all the properties copied
    ####################################################################################################################
    def set_context(self, input_context_dict):
        #Copy specific fields from one context dictionary to another
        self.current_module = input_context_dict.current_module
        self.process_name = input_context_dict.process_name
        self.context_dict.update({"current_module": self.current_module, "process_name": self.process_name})

    #############################################set_context_param######################################################
    # Purpose            :   Updates parameters of the context dictionary based on input dictionary
    # Input              :   input_dict is the dictionary containing key-value pairs for all the parameters that are
    #                        needed to be updated
    # Output             :   output_context with updated parameters
    ####################################################################################################################
    def set_context_param(self, input_dict):
        self.context_dict.update(input_dict)
        for input_dict_key in input_dict.keys():
            if input_dict_key == "current_module":
                self.current_module = input_dict[input_dict_key]
            elif input_dict_key == "function_start_time":
                self.function_start_time = input_dict[input_dict_key]
            elif input_dict_key == "process_name":
                self.process_name = input_dict[input_dict_key]
            elif input_dict_key == "process_identifier":
                self.process_identifier = input_dict[input_dict_key]
            elif input_dict_key == "parent_function_name":
                self.parent_function_name = input_dict[input_dict_key]
            elif input_dict_key == "function_status":
                self.function_status = input_dict[input_dict_key]
            elif input_dict_key == "traceback":
                self.traceback = input_dict[input_dict_key]

    ##################################################get_context#######################################################
    # Purpose            :   Fetches the dictionary with latest state of context parameters. Function name and parent
    #                        function name are also updated
    # Output             :   output is a context_dictionary containing key-value pairs for all the fields for log
    ####################################################################################################################
    def get_context(self):
        #Create a temporary dictionary storing function name and parent function name
        temp_context_dict = {"function_name": inspect.stack()[1][3], "parent_function_name": inspect.stack()[2][3],
                             "function_start_time": datetime.now()}
        #Update context dictionary with temporary dictionary
        self.context_dict.update(temp_context_dict)
        #return the updated context dictionary
        return self.context_dict

    #############################################set_context_param######################################################
    # Purpose            :   Fetches the latest value of input context parameters from the Execution Context object
    # Input              :   param is the input parameter for which value needs to be retrieved
    # Output             :   output is value corresponding to the input parameter
    ####################################################################################################################
    def get_context_param(self, param):
        if param == "current_module":
            value = self.current_module
        elif param == "function_start_time":
            value = self.function_start_time
        elif param == "process_name":
            value = self.process_name
        elif param == "current_function_name":
            value = self.process_name
        elif param == "process_identifier":
            value = self.process_identifier
        elif param == "parent_function_name":
            value = self.parent_function_name
        elif param == "function_status":
            value = self.function_status
        elif param == "traceback":
            value = self.traceback
        else:
            value = ""

        return value