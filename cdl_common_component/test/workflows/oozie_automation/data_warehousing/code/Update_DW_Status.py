# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
'''
Created on Jan 22, 2018

@author: dm16716
'''
import sys
import datetime
import time
from MySQLConnectionManager import MySQLConnectionManager
from TerminateEmrHandler import TerminateEmrHandler
import CommonConstants as CommonConstants
from TerminateEmrUtility import TerminateEmrUtility
from ExecutionContext import ExecutionContext
from CommonUtils import CommonUtils
from LogSetup import logger
from warnings import catch_warnings

MODULE_NAME="Update DW Status.py"
class Update_DW_Status(object):
    def __init__(self, parent_execution_context=None):

        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


    def poll_cycle_id(self):
        try:
	    date_time = datetime.datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT_ORCHESTRATION)
            result=self.poll_cycle_load_summary()
            for data in result:
                if int(data['progress_count']) ==0:
                    if int(data['failed_count'])==0:
                        print "All  success"
                        update_cycle_run="UPDATE  {audit_db}.{cycle_run_table} SET STATUS='{success}', end_time='{end_time}' where cycle_id={cycle_id} and process_name='{process_name}'".format(audit_db=CommonConstants.AUDIT_DB_NAME,cycle_run_table=CommonConstants.CYCLE_RUN,success=CommonConstants.STATUS_SUCCEEDED,cycle_id=str(data['cycle_id']),process_name=str(data['process_name']),end_time=date_time)
                    else:
                        print "Files failed"
                        update_cycle_run="UPDATE  {audit_db}.{cycle_run_table} SET STATUS='{failed}', end_time='{end_time}' where cycle_id={cycle_id} and process_name='{process_name}'".format(audit_db=CommonConstants.AUDIT_DB_NAME,cycle_run_table=CommonConstants.CYCLE_RUN,failed=CommonConstants.STATUS_FAILED,cycle_id=str(data['cycle_id']),process_name=str(data['process_name']),end_time= date_time)
                    
                    result=MySQLConnectionManager().execute_query_mysql(update_cycle_run) 
                    print "Terminating",str(data['cluster_id'])
		    cluster_id=str(data['cluster_id'])
                    region_name = TerminateEmrHandler().get_region_for_cluster(cluster_id)
                    TerminateEmrUtility().terminate_emr(cluster_id, region_name)
                    CommonUtils().update_emr_termination_status(cluster_id)
                    TerminateEmrHandler().check_emr_status_for_id(cluster_id)

                else:
                    print "Files are in progress for",str(data['cycle_id'])
        except Exception as e:
            raise e


    def poll_cycle_load_summary(self):
        time.sleep(10)
        query_string="select cls.cycle_id, cls.process_name,cr.cluster_id, CAST(SUM(CASE cls.STATUS WHEN cls.status='{in_progress}' then 0 else 1 END)as UNSIGNED) as progress_count, cast(SUM(CASE cls.STATUS WHEN cls.status='{success}' then 0 else 1 END)as UNSIGNED) success_count, CAST(SUM(CASE cls.STATUS WHEN cls.status='{failed}' then 0 else 1 END)as UNSIGNED) failed_count, count(1) total_count from {audit_db}.{cycle_load_summary_table} cls INNER JOIN {audit_db}.{cycle_run_table} cr WHERE cr.cycle_id = cls.cycle_id AND cr.process_name = cls.process_name AND cr.status='{in_progress}' group by cls.cycle_id, cls.process_name;".format(audit_db=CommonConstants.AUDIT_DB_NAME,cycle_run_table=CommonConstants.CYCLE_RUN,failed=CommonConstants.STATUS_FAILED,in_progress=CommonConstants.IN_PROGRESS_DESC,success=CommonConstants.STATUS_SUCCEEDED,cycle_load_summary_table=CommonConstants.CYCLE_LOAD_SUMMARY)
	#" select cls.cycle_id, cls.process_name,cr.cluster_id, CAST(SUM(CASE cls.STATUS WHEN cls.status='IN PROGRESS' then 0 else 1 END)as UNSIGNED) as progress_count, cast(SUM(CASE cls.STATUS WHEN cls.status='SUCCESS' then 0 else 1 END)as UNSIGNED) success_count, CAST(SUM(CASE cls.STATUS WHEN cls.status='FAILED' then 0 else 1 END)as UNSIGNED) failed_count, count(1) total_count from snfiprod_bdp.cycle_load_summary cls INNER JOIN snfiprod_bdp.cycle_run cr WHERE cr.cycle_id = cls.cycle_id AND cr.process_name = cls.process_name AND cr.status='IN PROGRESS' group by cls.cycle_id, cls.process_name;"
        #"select cls.cycle_id, cls.CYCLE_TYPE, SUM(CASE cls.STATUS WHEN cls.status='IN PROGRESS' then 0 else 1 END) as progress_count, SUM(CASE cls.STATUS WHEN cls.status='SUCCESS' then 0 else 1 END) success_count, SUM(CASE cls.STATUS WHEN cls.status='FAILED' then 0 else 1 END) failed_count, count(1) total_count from cycle_load_summary cls INNER JOIN cycle_run cr WHERE cr.cycle_id = cls.cycle_id AND cr.CYCLE_TYPE = cls.CYCLE_TYPE AND cr.status='IN PROGRESS' group by cls.cycle_id, cls.CYCLE_TYPE;"
        print query_string
        return MySQLConnectionManager().execute_query_mysql(query_string)

Update_DW_Status().poll_cycle_id()
