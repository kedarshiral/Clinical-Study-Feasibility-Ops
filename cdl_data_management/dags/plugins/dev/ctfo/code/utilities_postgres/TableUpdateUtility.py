"""Handler to update table logs status"""
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

"""
######################################################Module Information##########################
#   Module Name         :   Wrapper function for DS model
#   Purpose             :   Module to define application logic for to save extracted data in table 
#   Input Parameters    :   request_data
#   Output              :   Dictionary response
#   Execution Steps     :   Instantiate the class and invoke functions using objects
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Last changed on     :   24 May 2023
#   Last changed by     :   Shivansh Bhasin
#   Reason for change   :   NA
################################################################################################
"""

# Import Tool Packages
import traceback
from datetime import datetime
import logging
import CommonServicesConstants as Constants
import re
from utilities.LogSetup import get_logger

# Import Project Objects
from utils import Utils
import TableUpdateQueries


LOGGING = get_logger()
UTILS_OBJ = Utils()
class UpdateTableLogs:
    """
        Purpose     :   This class will be used to update table logs status
        Input       :   Object
        Output      :
    """

    def __init__(self, environment):
        logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        self.environment_ = environment
        self.utils_object = Utils()
        self.logging = get_logger()
        self.config_json = self.utils_object.get_application_config_json()
        self.utils_object.initialize_variable(self.config_json)
        self.schema_replace = self.config_json[Constants.CTFO_SERVICES]['schema']

    def execute_query_(self, query, auto_commit=True):
        """
        Description:
            This function will be used to execute query
        Usage:
            execute_query(query)
        Arguments:
            query,auto_commit
        Returns:
            json response for UI
        """
        try:
            # Executing Query
            query_output = self.utils_object.execute_query(
                query=query,
                host=self.utils_object.host,
                port=self.utils_object.port,
                username=self.utils_object.username,
                password=self.utils_object.password,
                database_name=self.utils_object.database_name,
               )
            return query_output
        except Exception as er:
            raise Exception(er)

    def update_log_status(self, status, study_id, scenario_id, model_run_id,
                          theme_id, scenario_version, created_by, description, created_timestamp, processing_job_id):
        """
        Description:
            This function will be used to update data in  log_model_run_status
        Usage:
            update_log_status(request_data, column_data)
        Arguments:
            where_cond,column_data
        Returns:
            json response for UI

        """
        app_config_dict = UTILS_OBJ.get_application_config_json()
        UTILS_OBJ.initialize_variable(app_config_dict)
        comment = ""
        study_id = ""
        scenario_version = ""
        print("Start of upadte_log_status")
        # Getting Query:- update data in log_model_run_status table
        update_data_qry = TableUpdateQueries.update_status_query

        if comment:
            parameters = "status='" + status + "', description='" + str(description).replace("'", '"') + \
                         "', updated_timestamp='" + str(datetime.now()) + "', comment='" + comment + "'"
        else:
            parameters = "status='" + status + "', description='" + str(description).replace("'", '"') + \
                         "', updated_timestamp='" + str(datetime.now()) + "'"

        update_data_qry = update_data_qry. \
            replace("$$schema_name$$", self.schema_replace). \
            replace("$$status$$", status). \
            replace("$$study_id$$", study_id). \
            replace("$$scenario_version$$", scenario_version). \
            replace("$$params$$", parameters). \
            replace("$$processing_job_id$$", processing_job_id)
        print(f"Query update logs in table:- {update_data_qry}")
        LOGGING.info("update_data_qry%s", str(update_data_qry))
        # Executing Query

        update_data_qry_parms={"scenario_id":scenario_id,"theme_id":theme_id,"created_by":created_by,"model_run_id":model_run_id}
        LOGGING.info("%s update_data_qry is : {}".format(
            UTILS_OBJ.get_replaced_query(update_data_qry,
                                         update_data_qry_parms)))

        update_data_qry_output=UTILS_OBJ.execute_query(update_data_qry,
                                UTILS_OBJ.host,
                                UTILS_OBJ.port, UTILS_OBJ.username,
                                UTILS_OBJ.password,
                                UTILS_OBJ.database_name,update_data_qry_parms)


        LOGGING.info("update_data_qry_output%s", str(update_data_qry_output))
        # Validating status of the query_output
        if update_data_qry_output[Constants.STATUS_KEY] == Constants.STATUS_FAILED:
            raise Exception(update_data_qry_output[Constants.ERROR_KEY])
        print("End of upadte_log_status")

