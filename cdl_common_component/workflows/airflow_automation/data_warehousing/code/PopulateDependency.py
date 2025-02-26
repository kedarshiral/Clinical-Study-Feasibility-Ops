#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import sys
import os
import traceback
from datetime import datetime, timedelta
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from LogSetup import logger

sys.path.insert(0, os.getcwd())

MODULE_NAME = "PopulateDependency"

"""
Module Name         :   PopulateDependency
Purpose             :   This module will populate process dependency details based on process dependency master
Input Parameters    :   process id, frequency, workflow id
Output Value        :   None
Pre-requisites      :   None
Last changed on     :   22nd Feb 2019
Last changed by     :   Himanshu Khatri
Reason for change   :   Updated query for max batch and cycle id 
"""


class PopulateDependency(object):

    def __init__(self, process_id, frequency, workflow_id):
        self.process_id = process_id
        self.frequency = frequency
        self.workflow_id = workflow_id
        self.data_dt = None
        self.cycle_id = None
        self.configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_id": self.process_id})
        self.execution_context.set_context({"frequency": self.frequency})
        self.execution_context.set_context({"workflow_id": workflow_id})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def populate_dependency_list(self):
        """
            Purpose :   Driver method for calling helper functions required for populating dependency list
            Input   :   NA
            Output  :   NA
        """
        try:
            status_message = "Starting Populate Dependency method for process id:" + str(self.process_id) + " " \
                             + "and frequency:" + str(self.frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            # Input Validations
            if self.process_id is None:
                raise Exception('Process Id is not provided')
            self.execution_context.set_context({"process_id": self.process_id})
            if self.frequency is None:
                raise Exception('Frequency is not provided')
            status_message = "Checking active count for process id:" + " " + str(self.process_id) + " " \
                             + "and frequency:" + str(self.frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            process_active_count = self.check_active_process_count(self.process_id, self.frequency)
            if process_active_count > 0:
                status_message = "Process with process id:" + str(self.process_id) + " " + "and frequency:" \
                                 + str(self.frequency) + " is already in '" + CommonConstants.STATUS_RUNNING + "' state"
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            status_message = "No entry  in '" + CommonConstants.STATUS_RUNNING + "' status for process id:" \
                             + str(self.process_id) + " " + "and frequency:" + str(self.frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            self.data_dt = CommonUtils(self.execution_context).fetch_data_date_process(self.process_id, self.frequency)
            self.execution_context.set_context({"data_date": self.data_dt})
            status_message = "Data date is:" + str(self.data_dt)
            logger.info(status_message, extra=self.execution_context.get_context())
            check_cycle_entry_count = self.check_cycle_log_entry(self.process_id, self.frequency, self.data_dt)
            if check_cycle_entry_count == 0:
                status_message = "No entry in " + CommonConstants.LOG_CYCLE_DTL + " for process id:" \
                             + str(self.process_id) + " " + "and frequency:" + str(self.frequency)
                logger.info(status_message, extra=self.execution_context.get_context())
                status_message = "Generating cycle id"
                logger.debug(status_message, extra=self.execution_context.get_context())
                cycle_id = datetime.utcnow().strftime(CommonConstants.CYCLE_TIMESTAMP_FORMAT)
                self.cycle_id = cycle_id[:-1]
                self.execution_context.set_context({"cycle_id": self.cycle_id})
                status_message = "Generated Cycle id:" + str(self.cycle_id)
                logger.info(status_message, extra=self.execution_context.get_context())
                status = CommonConstants.STATUS_NEW
                self.populate_cycle_details(self.process_id, self.frequency, self.data_dt, self.cycle_id,
                                            self.workflow_id, status)
                try:
                    process_master_resultset = self.fetch_process_master_details(self.process_id, self.frequency)
                    if not process_master_resultset:
                        status_message = "Process dependency master is not configured for process id:" + \
                                         str(self.process_id) + " " + "and frequency:" + str(self.frequency)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    for process_master_result in process_master_resultset:
                        dataset_id = process_master_result['dataset_id']
                        self.execution_context.set_context({"dataset_id": dataset_id})
                        status_message = "Fetched process dependency master details for dataset_id:" +str(dataset_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        logger.debug(process_master_resultset, extra=self.execution_context.get_context())
                        table_type = process_master_result['table_type']
                        status_message = "Table type is '" + table_type + "' for dataset_id:" + str(dataset_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        lag_week_num = process_master_result['lag_week_num']
                        dependency_pattern = process_master_result['dependency_pattern']
                        if table_type.lower() == CommonConstants.SOURCE_TABLE_TYPE:
                            dataset_result = self.fetch_dataset_result(dataset_id)
                            dataset_type = dataset_result['dataset_type']
                            status_message = "Dataset type is '" + dataset_type + "' for dataset_id:" + str(dataset_id)
                            logger.debug(status_message, extra=self.execution_context.get_context())
                            source_path = ""
                            dependency_value = ""
                            if dataset_type.lower() == CommonConstants.RAW_DATASET_TYPE:
                                staging_location = dataset_result['s3_post_dqm_location']
                                if dependency_pattern.lower() == CommonConstants.FULL_LOAD_TYPE:
                                    dependency_value = int(CommonConstants.NOT_APPLICABLE_VALUE)
                                    source_path = staging_location
                                elif dependency_pattern.lower() == CommonConstants.LATEST_LOAD_TYPE:
                                    dependency_value = self.fetch_successful_batch_id_for_dataset(dataset_id)
                                    source_path = staging_location + "/" + CommonConstants.PARTITION_BATCH_ID + "=" + str(dependency_value)
                            elif dataset_type.lower() == CommonConstants.PROCESSED_DATASET_TYPE:
                                table_s3_path = dataset_result['table_s3_path']
                                if dependency_pattern.lower() == CommonConstants.FULL_LOAD_TYPE:
                                    dependency_value = int(CommonConstants.NOT_APPLICABLE_VALUE)
                                    source_path = table_s3_path
                                elif dependency_pattern.lower() == CommonConstants.LATEST_LOAD_TYPE:
                                    status_message = "Lag in week is set to " + str(lag_week_num)
                                    logger.info(status_message, extra=self.execution_context.get_context())
                                    if lag_week_num is not None and lag_week_num > 0:
                                        data_dt = self.data_dt - timedelta(days=lag_week_num*int(7))
                                    else:
                                        data_dt = self.data_dt
                                    dependency_value = self.fetch_successful_cycle_id(dataset_id, data_dt)
                                    source_path = table_s3_path + "/" + CommonConstants.DATE_PARTITION + "=" + str(data_dt.strftime(
                                            CommonConstants.CYCLE_DATE_FORMAT)) + "/" + \
                                                      CommonConstants.CYCLE_PARTITION + "=" + str(dependency_value)
                            else:
                                status_message = " Dataset type is incorrectly configured for dataset_id:" + str(dataset_id)
                                logger.error(status_message, extra=self.execution_context.get_context())
                                raise Exception(status_message)
                        elif table_type.lower() == CommonConstants.TARGET_TABLE_TYPE:
                            dependency_value = CommonConstants.NOT_APPLICABLE_VALUE
                            source_path = CommonConstants.NOT_APPLICABLE_VALUE
                            dependency_pattern = CommonConstants.NOT_APPLICABLE_VALUE
                        else:
                            status_message = "Table type is incorrectly configured for dataset id:" + str(dataset_id)
                            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                                    " ERROR MESSAGE: " + str(traceback.format_exc())
                            self.execution_context.set_context({"traceback": error})
                            logger.error(status_message, extra=self.execution_context.get_context())
                            raise Exception(status_message)
                        self.populate_dependency_details(table_type, dataset_id, dependency_pattern, dependency_value,
                                                         source_path)
                except Exception as exception:
                    status_message = "Exception occured while populating process dependency details"
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    status = CommonConstants.STATUS_FAILED
                    CommonUtils(self.execution_context).update_cycle_audit_entry(self.process_id, self.frequency,
                                                                                 self.data_dt, self.cycle_id, status)
                    raise exception
                interruption_flag = self.fetch_process_interruption_flag(self.process_id, self.frequency)
                if interruption_flag == CommonConstants.ACTIVE_IND_VALUE:
                    status_message = "Interruption flag set to " + interruption_flag + "," + "interrupting the process"
                    self.update_process_interruption_flag(self.process_id, self.frequency)
                    raise Exception(status_message)
            else:
                try:
                    self.cycle_id = CommonUtils(self.execution_context).fetch_new_cycle_id(self.process_id,
                                                                                           self.frequency, self.data_dt)
                    self.update_override_dependency_value(self.process_id, self.frequency, self.data_dt, self.cycle_id)
                except Exception as exception:
                    status_message = "Exception occured while populating process dependency details"
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    status = CommonConstants.STATUS_FAILED
                    CommonUtils(self.execution_context).update_cycle_audit_entry(self.process_id, self.frequency,
                                                                                 self.data_dt, self.cycle_id, status)
                    raise exception
            progress_status = CommonConstants.STATUS_RUNNING
            CommonUtils(self.execution_context).update_cycle_audit_entry(self.process_id, self.frequency, self.data_dt,
                                                                         self.cycle_id, progress_status)
            status_message = "Cycle entry updated to in progress for cycle id:" +str(self.cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Completed executing Populate Dependency for process id:" + str(self.process_id) + " " \
                             + "and frequency:" + str(self.frequency)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in Populate dependency list method"
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def check_active_process_count(self, process_id, frequency):
        """
            Purpose :   This function gives process count, which are in progress state
            Input   :   Process Id, Frequency
            Output  :   Active Process Count
        """
        try:
            status_message = "Started preparing query to fetch process entry count for process_id:" + str(
                process_id) + " " + "and frequency:" + str(self.frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            active_process_count_query = "Select count(*) as active_process_count from {audit_db}.{cycle_details} " \
                                      "where process_id={process_id} and frequency='{frequency}' and cycle_status=" \
                                         "'{cycle_status}'". \
                format(audit_db=self.audit_db, cycle_details=CommonConstants.LOG_CYCLE_DTL,
                       process_id=process_id, frequency=frequency, cycle_status=CommonConstants.STATUS_RUNNING)
            logger.debug(active_process_count_query, extra=self.execution_context.get_context())
            active_process_count_query_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (active_process_count_query, True)
            logger.debug(active_process_count_query_result, extra=self.execution_context.get_context())
            status_message = "Completed executing query to fetch process entry count for process_id:" + str(
                process_id) + " " + "and frequency:" + str(self.frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            process_count = int(active_process_count_query_result['active_process_count'])
            return process_count
        except Exception as exception:
            status_message = "Exception occured in executing query to fetch active process entry count for process_id:"\
                             + str(process_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


    def update_override_dependency_value(self, process_id, frequency, data_dt, cycle_id):
        """
            Purpose :   Driver method for calling helper functions required for override dependency feature
            Input   :   Process Id, Frequency , Data Date, Cycle Id
            Output  :   NA
        """
        try:
            status_message = "Starting function to prepare query for fetching process dependency details for " \
                             "cycle_id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_process_dependency_details_query = "Select dataset_id, dependency_value, source_path, " \
                                                     " override_dependency_value from {audit_db}." \
                                                     "{process_dependency_details} where process_id={process_id}" \
                                                     " and frequency='{frequency}' and data_date='{data_date}'" \
                                                     " and write_dependency_value={cycle_id} and lower(table_type)='" \
                                                     "{source_table}' and override_dependency_value is NOT NULL" \
                                                     " and override_dependency_value!=0". \
                format(audit_db=self.audit_db, process_dependency_details=CommonConstants.
                       PROCESS_DEPENDENCY_DETAILS_TABLE, process_id=process_id, frequency=frequency,
                       data_date=data_dt, cycle_id=cycle_id, source_table=CommonConstants.SOURCE_TABLE_TYPE)
            logger.debug(fetch_process_dependency_details_query, extra=self.execution_context.get_context())
            fetch_process_dependency_details_query_result = MySQLConnectionManager().execute_query_mysql \
                (fetch_process_dependency_details_query)
            status_message = "Completed executing function to prepare query for fetching process dependency details " \
                             "cycle_id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if not fetch_process_dependency_details_query_result:
                status_message = "Override dependency value is not set for any of the datasets in process_id:" \
                                 + str(process_id) + " " + "and frequency:" + str(frequency)
                logger.warn(status_message, extra=self.execution_context.get_context())
            for process_detail in fetch_process_dependency_details_query_result:
                dataset_id = process_detail['dataset_id']
                self.execution_context.set_context({"dataset_id": dataset_id})
                status_message = "Starting function to prepare dataset details for dataset id:" + str(dataset_id)
                logger.debug(status_message, extra=self.execution_context.get_context())
                status_message = "Started fetching override dependency value result for dataset id:" + str(dataset_id)
                logger.debug(status_message, extra=self.execution_context.get_context())
                dataset_result = self.fetch_dataset_result(dataset_id)
                dataset_type = dataset_result['dataset_type']
                status_message = "Dataset type for dataset_id:" + str(dataset_id) + " is " + str(dataset_type)
                logger.debug(status_message, extra=self.execution_context.get_context())
                override_dependency_value = process_detail['override_dependency_value']
                status_message = "Override dependency value is set to:" + str(override_dependency_value)
                logger.debug(status_message, extra=self.execution_context.get_context())
                if dataset_type == CommonConstants.RAW_DATASET_TYPE:
                    batch_count = self.check_batch_detail_status(override_dependency_value)
                    if batch_count == 0:
                        status_message = "Batch Id:" + str(override_dependency_value) + " doesn't have '" + \
                                         CommonConstants.STATUS_SUCCEEDED + "' value in " + \
                                         CommonConstants.BATCH_TABLE
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    stg_location = dataset_result['s3_post_dqm_location']
                    source_path = stg_location + "/" + CommonConstants.PARTITION_BATCH_ID + "=" + \
                                  str(override_dependency_value)
                    self.update_source_path(source_path, cycle_id, dataset_id)
                elif dataset_type == CommonConstants.PROCESSED_DATASET_TYPE:
                    cycle_count = self.check_cycle_detail_status(override_dependency_value)
                    if cycle_count == 0:
                        status_message = "Cycle Id:" + str(override_dependency_value) + " doesn't have '" + \
                                         CommonConstants.STATUS_SUCCEEDED + "' value in " + \
                                         CommonConstants.LOG_CYCLE_DTL
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception(status_message)
                    table_location = dataset_result['table_s3_path']
                    data_dt = self.fetch_data_date_from_cycle(override_dependency_value)
                    source_path = table_location + "/" + CommonConstants.DATE_PARTITION + "=" + \
                                  str(data_dt.strftime(CommonConstants.CYCLE_DATE_FORMAT)) + "/" + \
                                  CommonConstants.CYCLE_PARTITION + "=" + str(override_dependency_value)
                    self.update_source_path(source_path, cycle_id, dataset_id)
                else:
                    status_message = "Dataset type is incorrectly configured for dataset_id:" + str(dataset_id)
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise Exception(status_message)
        except Exception as exception:
            status_message = "Exception occured in update override dependency value for cycle_id:" +str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def update_source_path(self, source_path, cycle_id, dataset_id):
        """
            Purpose :   This method updates source path corresponding for each dataset
            Input   :   Source Path, Cycle Id, Dataset Id
            Output  :   NA
        """
        try:
            status_message = "Starting query to update source path for cycle_id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            update_source_path_query = "Update {audit_db}.{process_dependency_details} set source_path='{source_path}' where process_id" \
                    "={process_id} and frequency='{frequency}' and data_date='{data_date}' and write_dependency_value=" \
                    "{cycle_id} and dataset_id ={dataset_id}".\
                format(audit_db=self.audit_db, process_dependency_details=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                       process_id=self.process_id, frequency=self.frequency, data_date=self.data_dt, cycle_id=cycle_id,
                       source_path=source_path, dataset_id=dataset_id)
            logger.debug(update_source_path_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(update_source_path_query)
            status_message = "Completed executing query to update source path for cycle_id:" + str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in updating source path for cycle_id:" + str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def check_cycle_detail_status(self, cycle_id):
        """
            Purpose :   This method gives cycle success entry count for input cycle id
            Input   :   Cycle Id
            Output  :   Success Cycle Count
        """
        try:
            status_message = "Starting query to fetch status for cycle_id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            cycle_status_check_query = "Select count(*) as cycle_count from {audit_db}.{cycle_details} where " \
                                       "cycle_id={cycle_id} and cycle_status='{cycle_status}'".\
                format(audit_db=self.audit_db, cycle_details=CommonConstants.LOG_CYCLE_DTL, cycle_id=cycle_id,
                       cycle_status=CommonConstants.STATUS_SUCCEEDED)
            logger.debug(cycle_status_check_query, extra=self.execution_context.get_context())
            cycle_status_check_query_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (cycle_status_check_query, True)
            status_message = "Completed executing query to fetch status for cycle_id:" + str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(cycle_status_check_query_result, extra=self.execution_context.get_context())
            cycle_count = int(cycle_status_check_query_result['cycle_count'])
            return cycle_count
        except Exception as exception:
            status_message = "Exception occured in executing query to fetch cycle status for cycle_id:" + str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


    def check_batch_detail_status(self, batch_id):
        """
            Purpose :   This method gives batch success entry count for input batch Id
            Input   :   Batch Id
            Output  :   Success Batch Count
        """
        try:
            status_message = "Starting query to fetch status for batch_id:" + str(batch_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            batch_status_check_query = "Select count(*) as batch_count from {audit_db}.{batch_details} where " \
                                       "batch_id={batch_id} and batch_status='{batch_status}'". \
                format(audit_db=self.audit_db, batch_details=CommonConstants.BATCH_TABLE, batch_id=batch_id,
                       batch_status=CommonConstants.STATUS_SUCCEEDED)
            logger.debug(batch_status_check_query, extra=self.execution_context.get_context())
            batch_status_check_query_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (batch_status_check_query, True)
            status_message = "Completed executing query to fetch status for batch_id:" + str(batch_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(batch_status_check_query_result, extra=self.execution_context.get_context())
            batch_count = int(batch_status_check_query_result['batch_count'])
            return batch_count
        except Exception as exception:
            status_message = "Exception occured in executing query to fetch status for batch_id:" + str(batch_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


    def fetch_data_date_from_cycle(self, cycle_id):
        """
            Purpose :   This method gives data date for input cycle id
            Input   :   Cycle Id
            Output  :   Data date
        """
        try:
            status_message = "Started preparing query to fetch data date for cycle id:" + str(
                cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_date_query = "Select data_date from {audit_db}.{cycle_details} where cycle_id={cycle_id}".\
                format(audit_db=self.audit_db, cycle_details=CommonConstants.LOG_CYCLE_DTL, cycle_id=cycle_id)
            logger.debug(fetch_date_query, extra=self.execution_context.get_context())
            date_result = MySQLConnectionManager(self.execution_context).execute_query_mysql(fetch_date_query, True)
            status_message = "Completed executing query to fetch data date for cycle id:" + str(
                cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            data_date = date_result['data_date']
            return data_date
        except Exception as exception:
            status_message = "Exception in executing query to fetch data date for cycle id:" + str(
                cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def check_cycle_log_entry(self, process_id, frequency, data_dt):
        """
            Purpose :   This method gives count for cycle in new state
            Input   :   Process Id, Frequency , Data Date
            Output  :   New Cycle Count
        """
        try:
            status_message = "Started preparing query to fetch process entry count for process_id:" + str(
                process_id) + " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_dt)
            logger.debug(status_message, extra=self.execution_context.get_context())
            cycle_entry_count_query = "Select count(*) as cycle_count from {audit_db}.{cycle_details} " \
                                      "where process_id={process_id} and frequency='" \
                    "{frequency}' and data_date='{data_date}' and cycle_status='{cycle_status}'".\
                format(audit_db=self.audit_db, cycle_details=CommonConstants.LOG_CYCLE_DTL,
                       process_id=process_id, frequency=frequency, data_date=data_dt, cycle_status= CommonConstants.STATUS_NEW)
            logger.debug(cycle_entry_count_query, extra=self.execution_context.get_context())
            cycle_count_query_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (cycle_entry_count_query,True)
            status_message = "Completed query to fetch process entry count for process_id:" + str(
                process_id) + " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_dt)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(cycle_count_query_result, extra=self.execution_context.get_context())
            cycle_count = int(cycle_count_query_result['cycle_count'])
            return cycle_count
        except Exception as exception:
            status_message = "Exception occured in executing query to fetch process entry count for process_id:" + str(
                process_id) + " and data date:" + str(data_dt)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_process_master_details(self, process_id, frequency):
        """
            Purpose :   This method gives process dependency master resultset
            Input   :   Process Id , Frequency
            Output  :   Process Master Config Resultset
        """
        try:
            status_message = "Started preparing query to fetch process master configs for process_id:" + str(
                process_id) + " " + "and frequency:" + str(frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_process_master_details_query = "Select table_type, dataset_id, lag_week_num" \
                                                 ", dependency_pattern from {audit_db}.{process_dependency_master} " \
                                                 "where process_id={process_id} and active_flag='{active_flag_value}'" \
                                                 " and frequency='{frequency}'" \
                .format(audit_db=self.audit_db, process_dependency_master=CommonConstants.PROCESS_DEPENDENCY_MASTER_TABLE,
                        process_id=process_id, active_flag_value=CommonConstants.ACTIVE_IND_VALUE,
                        frequency=frequency)
            logger.debug(fetch_process_master_details_query, extra=self.execution_context.get_context())
            process_master_resultset = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_process_master_details_query)
            status_message = "Completed query to fetch process master configs for process_id:" + str(
                process_id) + " " + "and frequency:" + str(frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            logger.debug(process_master_resultset, extra=self.execution_context.get_context())
            return process_master_resultset
        except Exception as exception:
            status_message = "Exception occured in fetching process master configs for process id::" \
                             "" + str(process_id) + " " + "and frequency:" + str(frequency)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def update_process_interruption_flag(self, process_id, frequency):
        """
            Purpose :   This method sets interruption flag to NULL
            Input   :   Process Id, Frequency
            Output  :   NA
        """
        try:
            status_message = "Starting function to update interruption flag for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            update_flag_query = " Update {audit_db}.{process_date_table} set process_interruption_flag=NULL " \
                                "where process_id={process_id} and frequency='{frequency}'". \
                format(audit_db=self.audit_db,
                       process_date_table=CommonConstants.PROCESS_DATE_TABLE,
                       process_id=process_id, frequency=frequency
                       )
            logger.debug(update_flag_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(update_flag_query, True)
            status_message = "Completed function to update interruption flag for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in updating interruption flag for process id::" \
                             "" + str(process_id) + " " + "and frequency:" + str(frequency)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


    def fetch_dataset_result(self, dataset_id):
        """
            Purpose :   This method gives dataset details
            Input   :   Dataset Id
            Output  :   Dataset Resultset
        """
        try:
            status_message = "Started preparing query to fetch dataset information for dataset_id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_dataset_details_query = "Select dataset_type, s3_post_dqm_location, table_s3_path from " \
                                          "{audit_db}.{dataset_master} where dataset_id ={dataset_id}". \
                format(audit_db=self.audit_db, dataset_master=
            CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME, dataset_id=dataset_id)
            logger.debug(fetch_dataset_details_query, extra=self.execution_context.get_context())
            status_message = "Completed preparing query to fetch dataset information for dataset_id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            dataset_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_dataset_details_query, True)
            status_message = "Completed executing query to fetch dataset information for dataset_id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if not dataset_result:
                status_message = "No entry in dataset table for dataset_id:" +str(dataset_id)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            logger.debug(dataset_result, extra=self.execution_context.get_context())
            return dataset_result
        except Exception as exception:
            status_message = "Exception occured in fetching dataset information for dataset_id:" \
                             "" + str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_process_interruption_flag(self, process_id, frequency):
        """
            Purpose :   This method will fetch process interruption flag
            Input   :   Process Id , Frequency
            Output  :   Process Interruption Flag Value
        """
        try:
            status_message = "Started preparing query to fetch interruption flag for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_interruption_flag_query = "Select process_interruption_flag from {audit_db}.{process_date_table} where " \
                                    "process_id={process_id} and frequency='{frequency}'". \
                format(audit_db=self.audit_db,
                       process_date_table=CommonConstants.PROCESS_DATE_TABLE,
                       process_id=process_id, frequency=frequency
                       )
            logger.debug(fetch_interruption_flag_query, extra=self.execution_context.get_context())
            flag_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_interruption_flag_query, True)
            status_message = "Completed executing query to fetch interruption flag for process_id:" + str(process_id) \
                             + " " + "and frequency:" + str(frequency)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(flag_result, extra=self.execution_context.get_context())
            interruption_flag_value = flag_result['process_interruption_flag']
            return interruption_flag_value
        except Exception as exception:
            status_message = "Exception in fetching data date for process id:" + str(process_id) +  \
                              " " + "and frequency:" + str(frequency)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def populate_dependency_details(self, table_type, dataset_id, dependency_pattern, dependency_value, source_path):
        """
            Purpose :   This method will make process dependency entry for each dataset
            Input   :   Dataset Id, Table Type, Dependency Pattern, Dependency Value, Source Path
            Output  :   NA
        """
        try:
            process_name = self.fetch_process_name(self.process_id, self.frequency)
            status_message = "Started preparing query to populate process dependency detail for dataset id:" \
                             + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            process_dependency_details_query = "INSERT INTO {audit_db}.{process_dependency_details}(process_id, " \
                                               "process_name, frequency, table_type, dataset_id, dependency_pattern," \
                                               " dependency_value, source_path, data_date, write_dependency_value) " \
                                               "VALUES({process_id},'{process_name}','{frequency}','{table_type}'," \
                                               " {dataset_id},'{dependency_pattern}','{dependency_value}'," \
                                               "'{source_path}', '{data_dt}',{write_dependency_value})". \
                format(audit_db=self.audit_db,
                       process_dependency_details=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                       process_id=self.process_id,
                       process_name=process_name,
                       frequency=self.frequency,
                       table_type=table_type,
                       dataset_id=dataset_id,
                       dependency_pattern=dependency_pattern,
                       dependency_value=dependency_value,
                       source_path=source_path,
                       data_dt=self.data_dt,
                       write_dependency_value=self.cycle_id)
            logger.debug(process_dependency_details_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(process_dependency_details_query)
            status_message = "Completed executing query to populate process dependency detail for dataset_id:" \
                             "" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in populating process dependency detail for dataset_id:" \
                             "" + str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_process_name(self, process_id, frequency):
        """
            Purpose :   This method gives process name for input process id and frequency combination
            Input   :   Process Id, Frequency
            Output  :   Process Name
        """
        try:
            status_message = "Started preparing query to fetch process name from process id:" \
                             ":" + str(self.process_id) + " " + "and frequency:" + frequency
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_process_name_query = "Select process_name from {audit_db}.{cluster_details} where " \
                                       "process_id={process_id} and frequency='{frequency}'". \
                format(audit_db=self.audit_db,
                       cluster_details=CommonConstants.EMR_CLUSTER_CONFIGURATION_TABLE,
                       process_id=process_id, frequency=frequency)
            logger.debug(fetch_process_name_query, extra=self.execution_context.get_context())
            process_name_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_process_name_query, True)
            print ("process name result ",process_name_result)
            status_message = "Completed query to fetch process name from process id:" \
                             ":" + str(self.process_id) + " " + "and frequency:" + frequency
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(process_name_result, extra=self.execution_context.get_context())
            process_name = process_name_result['process_name']
            return process_name
        except Exception as exception:
            status_message = "Exception occured in fetching process name from process id:" \
                             "" + str(process_id) + " " + "and frequency:" + frequency
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def populate_cycle_details(self, process_id, frequency, data_date, cycle_id, workflow_id, status):
        """
            Purpose :   This method will log cycle entry for each process
            Input   :   NA
            Output  :   NA
        """
        try:
            status_message = "Starting to prepare query for populating cycle log for process_id:" +str(process_id) \
                             + " " + "and frequency:" + frequency + "and data date:" + str(data_date) + " " + \
                             "and cycle_id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            populate_cycle_details_query = "INSERT INTO {audit_db}.{cycle_details}(process_id, frequency, " \
                                           "workflow_id, data_date, cycle_id, cycle_status, cycle_start_time)" \
                                           "VALUES({process_id},'{frequency}','{workflow_id}', '{data_dt}', " \
                                           "{cycle_id}, '{cycle_status}', {cycle_start_time})" \
                                           "".format(
                audit_db=self.audit_db,
                cycle_details=CommonConstants.LOG_CYCLE_DTL,
                process_id=process_id,
                frequency=frequency,
                workflow_id = workflow_id,
                data_dt=data_date,
                cycle_id=cycle_id,
                cycle_status=status,
                cycle_start_time="NOW()")
            logger.debug(populate_cycle_details_query, extra=self.execution_context.get_context())
            MySQLConnectionManager(self.execution_context).execute_query_mysql(populate_cycle_details_query)
            status_message = "Completed executing query for populating cycle log for process_id:" + str(process_id) \
                             + " " + "and frequency:" + frequency + "and data date:" + str(data_date) + " " + \
                             "and cycle_id:" + str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in  populating cycle log for process_id:" + str(process_id) \
                             + " " + "and frequency:" + frequency + "and data date:" + str(data_date) + " " + \
                             "and cycle_id:" + str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_successful_batch_id_for_dataset(self, dataset_id):
        """
            Purpose :   This method will fetch latest successful batch Id
            Input   :   Dataset Id
            Output  :   Batch Id
        """
        try:
            status_message = "Started preparing query to fetch latest successful batch id for dataset_id:" \
                             "" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_successful_batch_dataset_query = "select max(batch_id) as batch_id from " \
                                                   "(select  distinct(A.batch_id),A.batch_status from {audit_db}.{batch_details_table} as A inner join {audit_db}.{file_details_table} as B " \
                                                   "on A.batch_id=B.batch_id where A.dataset_id = {dataset_id} " \
                                                   "and A.batch_id not in (select batch_id from {audit_db}.{file_details_table} where lower(file_process_name) = '{withdraw_process_name}' and file_process_status = '{batch_status}'))" \
                                                   " as T where T.batch_status ='{batch_status}'".format(audit_db=self.audit_db,batch_details_table=CommonConstants.BATCH_TABLE,file_details_table=CommonConstants.PROCESS_LOG_TABLE_NAME,dataset_id=dataset_id,withdraw_process_name=CommonConstants.WITHDRAW_PROCESS_NAME,batch_status=CommonConstants.STATUS_SUCCEEDED)
            logger.debug(fetch_successful_batch_dataset_query, extra=self.execution_context.get_context())

            batch_id_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_successful_batch_dataset_query, True)
            status_message = "Completed executing query to fetch latest successful batch id for dataset_id:" \
                             "" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(batch_id_result, extra=self.execution_context.get_context())
            batch_id = batch_id_result['batch_id']
            if batch_id is None:
                status_message = "There is no successful batch id for dataset_id:" + str(dataset_id)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            return batch_id
        except Exception as exception:
            status_message = "Exception occured in fetching successful batch id from dataset_id:" + str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_successful_cycle_id(self, dataset_id, data_date):
        """
            Purpose :   This method will fetch latest cycle id
            Input   :   Dataset Id, Data Date
            Output  :   Cycle Id
        """
        try:
            status_message = "Started preparing query to fetch latest successful cycle id for data date:" \
                             + str(data_date) + " " + "and dataset_id:" +str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_successful_batch_cycle_query = "select max(cycle_id) as cycle_id from {audit_db}.{cycle_details} " \
                                                 "INNER JOIN {audit_db}.{process_dependency_table} ON " \
                                                 "cycle_id=write_dependency_value where " \
                                                 "{cycle_details}.data_date='{data_date}' and " \
                                                 "cycle_status='{cycle_status}' and dataset_id={dataset_id} and table_type = '{target_table}'".\
                format(audit_db=self.audit_db,
                       process_dependency_table=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                       cycle_details=CommonConstants.LOG_CYCLE_DTL, data_date=data_date, dataset_id=dataset_id,
                       cycle_status=CommonConstants.STATUS_SUCCEEDED,target_table = CommonConstants.TARGET_TABLE_TYPE)

            logger.debug(fetch_successful_batch_cycle_query, extra=self.execution_context.get_context())
            cycle_resultset = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_successful_batch_cycle_query, True)
            status_message = "Completed executing query to fetch latest successful cycle id for data date:" \
                             + str(data_date) + " " + "and dataset_id:" +str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(cycle_resultset, extra=self.execution_context.get_context())
            cycle_id = cycle_resultset['cycle_id']
            if cycle_id is None:
                status_message = "There is no successful cycle id available for data date:" \
                                 + str(data_date)  + " " + "and dataset_id:" +str(dataset_id)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            return cycle_id
        except Exception as exception:
            status_message = "Exception occured in fetching latest successful cycle id for data date:" \
                             + str(data_date) + " " + "and dataset_id:" +str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


if __name__ == '__main__':
    try:
        PROCESS_ID = sys.argv[1]
        FREQUENCY = sys.argv[2]
        WORKFLOW_ID = sys.argv[3]
        DEPENDENCY_HANDLER = PopulateDependency(PROCESS_ID, FREQUENCY, WORKFLOW_ID)
        DEPENDENCY_HANDLER.populate_dependency_list()
        STATUS_MSG = "Completed executing Populate Dependency List Utility"
        sys.stdout.write(STATUS_MSG)
    except Exception as exception:
        raise exception
