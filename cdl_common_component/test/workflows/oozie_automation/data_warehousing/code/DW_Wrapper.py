# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import sys
import datetime
import time
import traceback
import json
import getpass
import subprocess
import os
import sys
import re
sys.path.insert(0, os.getcwd())
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants
from ExecutionContext import ExecutionContext
from dim_acct_frz import dim_acct_frz
from dim_acct_frz_bridge import dim_acct_frz_bridge
from dim_cust_frz_bridge import dim_cust_frz_bridge
from dim_cust_frz import dim_cust_frz
from Xpptcurfzdtlswk_dia import Xpptcurfzdtlswk_dia
from Xpptcurfzdtlswk_aar import Xpptcurfzdtlswk_aar
from Xpptcurfzdtlswk_cho import Xpptcurfzdtlswk_cho
from xppd_core import xppd_core
from xppd_switch import xppd_switch
from fia_biweekly import fia_biweekly
from fia_monthly import fia_monthly
from dim_cust_wk import dim_cust_wk
from dim_cust_spec_wk import dim_cust_spec_wk
from dim_cust_pdrp_wk import dim_cust_pdrp_wk
from dim_cust_bridge_hist_wk import dim_cust_bridge_hist_wk
from dim_acct_wk import dim_acct_wk
from dim_acct_bridge_hist_wk import dim_acct_bridge_hist_wk

from LogSetup import logger


MODULE_NAME = "Update DW status"

class DW_Wrapper(object):

    def __init__(self, parent_execution_context=None):
        self.staging_path = sys.argv[1]
        self.cycle_id = sys.argv[2]
        self.cluster_id = sys.argv[3]
        self.target_table = sys.argv[4]
        self.workflow_id = sys.argv[5]
        self.process_name = sys.argv[6]
        self.read_cycle_id =  sys.argv[7]

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    #Get the current datetime in YYYY-MM-DD hh:mm:ss
    def get_date_time(self):

        return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    def create_cycle_load_summary_entry(self):
        try:
            status="IN PROGRESS"
            new_record="Insert into {audit_db}.{cycle_load_summary_table} (cycle_id, process_name, target_table,workflow_id,start_time,status) values('{cycle_id}','{process_name}','{target_table}','{workflow_id}','{date_time}','{status_desc}')".format(audit_db=CommonConstants.AUDIT_DB_NAME,cycle_load_summary_table=CommonConstants.CYCLE_LOAD_SUMMARY,cycle_id=str(self.cycle_id),target_table=self.target_table,workflow_id=self.workflow_id,date_time=str(self.get_date_time()),status_desc=status,process_name = self.process_name)
            result = MySQLConnectionManager().execute_query_mysql(new_record)
            print new_record
            status_message="Added the record "+str(result)
            logger.info(status_message,extra=self.execution_context.get_context())
        except Exception as  e:
            status_message="Error in adding record in create cycle load entry"
            logger.error(status_message,extra=self.execution_context.get_context())
            raise e


    def execute_target_table(self):
        try:
            if self.target_table=='dim_cust_frz':
                dim_cust_frz(self.staging_path,self.cycle_id).dim_cust_frz_business_rule()
            elif self.target_table=='dim_cust_frz_bridge':
                dim_cust_frz_bridge(self.staging_path,self.cycle_id).dim_cust_bridge_frz_business_rule()
            elif self.target_table =='dim_acct_frz_bridge':
                dim_acct_frz_bridge(self.staging_path,self.cycle_id).dim_acct_bridge_frz_business_rule()
            elif self.target_table == 'dim_acct_frz':
                dim_acct_frz(self.staging_path,self.cycle_id).dim_acct_frz_business_rule()
            elif self.target_table == 'Xpptcurfzdtlswk_dia':
                Xpptcurfzdtlswk_dia(self.staging_path,self.cycle_id,self.read_cycle_id).xpptcurfzdtlswk_dia_business_rule()
            elif self.target_table == 'Xpptcurfzdtlswk_aar':
                Xpptcurfzdtlswk_aar(self.staging_path,self.cycle_id,self.read_cycle_id).xpptcurfzdtlswk_aar_business_rule()
            elif self.target_table == 'Xpptcurfzdtlswk_cho':
                Xpptcurfzdtlswk_cho(self.staging_path,self.cycle_id,self.read_cycle_id).xpptcurfzdtlswk_cho_business_rule()
            elif self.target_table == 'xppd_core':
                xppd_core(self.staging_path,self.cycle_id,self.read_cycle_id).xppd_core_business_rule()
            elif self.target_table == 'xppd_switch':
                xppd_switch(self.staging_path,self.cycle_id,self.read_cycle_id).xppd_switch_business_rule()
            elif self.target_table == 'fia_biweekly':
                fia_biweekly(self.staging_path,self.cycle_id,self.read_cycle_id).fia_biweekly_business_rule()
            elif self.target_table == 'fia_monthly':
                fia_monthly(self.staging_path, self.cycle_id, self.read_cycle_id).fia_monthly_business_rule()
            elif self.target_table == 'dim_acct_bridge_hist_wk':
                dim_acct_bridge_hist_wk(self.staging_path,self.cycle_id).dim_acct_bridge_hist_wk_business_rule()
            elif self.target_table == 'dim_acct_wk':
                dim_acct_wk(self.staging_path,self.cycle_id).dim_acct_wk_business_rule()
            elif self.target_table == 'dim_cust_bridge_hist_wk':
                dim_cust_bridge_hist_wk(self.staging_path,self.cycle_id).dim_cust_bridge_hist_wk_business_rules()
            elif self.target_table == 'dim_cust_pdrp_wk':
                dim_cust_pdrp_wk(self.staging_path,self.cycle_id).dim_cust_pdrp_wk_business_rule()
            elif self.target_table == 'dim_cust_spec_wk':
                dim_cust_spec_wk(self.staging_path,self.cycle_id).dim_cust_spec_wk_business_rule()
            elif self.target_table == 'dim_cust_wk':
                dim_cust_wk(self.staging_path,self.cycle_id).dim_cust_wk_business_rule()
            else:
                status_message="Invalid target table name passed"
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            status=CommonConstants.STATUS_SUCCEEDED

        except Exception as e:
            status=CommonConstants.STATUS_FAILED
            status_message="Exception occured while running execute target table"
            logger.error(status_message,extra=self.execution_context.get_context())
            raise e
        finally:
            status_update="update {audit_db}.{cycle_load_summary_table} set status='{status}',end_time='{end_time}' where cycle_id={cycle_id} and target_table='{target_table}'".format(audit_db=CommonConstants.AUDIT_DB_NAME,cycle_load_summary_table=CommonConstants.CYCLE_LOAD_SUMMARY,cycle_id=self.cycle_id,status=status,end_time=self.get_date_time(),target_table=self.target_table)
            print status_update
            result = MySQLConnectionManager().execute_query_mysql(status_update)
            status_message="Updated the status message"
            logger.info(status_message,extra=self.execution_context.get_context())

if __name__ == '__main__':
    try:
        wrapper=DW_Wrapper()
        wrapper.create_cycle_load_summary_entry()
        wrapper.execute_target_table()
        STATUS_MSG = "Completed execution for DW Wrapper  Utility"
        sys.stdout.write(STATUS_MSG)
    except Exception as e:
        raise e

