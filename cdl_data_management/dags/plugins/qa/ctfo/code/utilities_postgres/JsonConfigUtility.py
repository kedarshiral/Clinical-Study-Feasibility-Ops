#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

"""
Doc_Type            : Tech Products
Tech Description    : This utility is used for reading values from configuration file
Pre_requisites      :
Inputs              : Configuration file, Section in the configuration file and configuration name.
Outputs             : This utility will return the value corresponding to a configuration parameter
                      in the configuration file
Example             : JsonConfigUtility(conf=input_json).get_configuration(parameter hierarchy)
Config_file         : NA
"""

# System libraries declaration
import json
from functools import reduce


# noinspection PyBroadException
class JsonConfigUtility(object):
    """
    Class contains all the functions related to JsonConfigUtility
    """
    # Parametrized constructor with Configuration file as input
    def __init__(self, conf_file=None, conf=None):
        try:
            if conf_file is not None:
                config_fp = open(conf_file)
                self.json_configuration = json.load(config_fp)
            elif conf is not None:
                self.json_configuration = conf
            else:
                self.json_configuration = {}
        except Exception:
            pass

    def get_configuration(self, conf_hierarchy):
        """
        Purpose   :   This method will read the value of a configuration parameter corresponding to
                      a section in the configuration file
        Input     :   Section in the configuration file, Configuration Name
        Output    :   Returns the value of the configuration parameter present in the configuration file
        """
        try:
            return reduce(lambda dictionary, key: dictionary[key], conf_hierarchy, self.json_configuration)
        except Exception:
            pass
