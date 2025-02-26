#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

#####################################################Module Information#################################################
#   Module Name         :   ConfigUtility
#   Purpose             :   This is used for reading values from configuration file
#   Input Parameters    :   Configuration file, Section in the configuration file and configuration name.
#
#
#   Output Value        :   This utility will return the value corresponding to a configuration parameter
#                           in the configuration file
#   Execution Steps     :   1.Import this class in the class where we need to read values from a configuration file.
#                           2.Instantiate this class
#                           3.Pass the configuration file as input to get_configuration() method
#
#   Predecessor module  :   All modules which reads values from configuration files
#   Successor module    :   NA
#   Pre-requisites      :   NA
#
#   Last changed on     :   28th August 2014
#   Last changed by     :   Mohit Jindal
#   Reason for change   :   Coding standards
########################################################################################################################

#Library and external modules declaration

from ConfigParser import SafeConfigParser


# Define all module level constants here
MODULE_NAME = "ConfigUtility"
PROCESS_NAME = "File Load"

##################Class ConfigUtility#####################################
# Class contains all the functions related to ConfigUtility
###########################################################################

class ConfigUtility(object):

    #Instantiating SafeConfigParser()
    parser = SafeConfigParser()

    #Parametrized constructor with Configuration file as input
    def __init__(self, conf_file):
        try:
            self.parser.read(conf_file)
        except:
            pass


    ################################################# Get Configuration ################################################
    # Purpose            :   This method will read the value of a configuration parameter corresponding to
    #                        a section in the configuration file
    #
    # Input              :   Section in the configuration file, Configuration Name
    #
    # Output             :   Returns the value of the configuration parameter present in the configuration file
    ####################################################################################################################

    def get_configuration(self, conf_section, conf_name):
        try:
            return self.parser.get(conf_section, conf_name)
        except:
            pass