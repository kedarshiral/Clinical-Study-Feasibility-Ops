#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

"""
Description      : Implements SCD2 logic (adds start_date and end_date) based on primary keys
Pre_requisites   : Staging layer table should be ingested successfully
Inputs           : Source table name, target table name, primary key to be configured in rule config table
Outputs          : SCD2 stitched target table
Author           : Nishant Nijaguna
"""

from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from CommonUtils import CommonUtils
from HiveToHDFSHandler import HiveToHDFSHandler
from datetime import datetime
import subprocess

current_datetime = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")

#parameters being replaced
primary_keys = "$$primary_keys$$"
data_dt = "$$data_dt"
data_date = datetime.strptime(data_dt, '%Y%m%d').strftime('%Y-%m-%d')
cycle_id = "$$cycle_id"
source_tablename = "$$source_tablename$$"
target_tablename = "$$target_tablename$$"


#creation of base dfs
query_string = "select * from {source_tablename}".format(source_tablename=source_tablename)
df_source = spark.sql(query_string)
query_string = "select * from {target_tablename}".format(target_tablename=target_tablename)
df_target = spark.sql(query_string)

#creation of temp source table that has start/end date columns appended
df_source_new = df_source.withColumn('edl_start_date', lit(current_datetime)).withColumn('edl_end_date', lit('9999-12-31 00:00:00')).withColumn('active_flag', lit('Y'))

#dropping columns to match schema for unionall
df_source_new=df_source_new.drop('pt_batch_id').drop('pt_file_id')
df_target=df_target.drop('pt_data_dt').drop('pt_cycle_id')

#union all of the target(previous cycle data present) and df_source_new
df_temp_target = df_target.unionAll(df_source_new)


#lead transformation
df_temp_target = df_temp_target.withColumn("next_dt", lead("edl_start_date", 1).over(Window.partitionBy(primary_keys).orderBy('edl_start_date')))
df_temp_target = df_temp_target.withColumn('edl_end_date', when(col('next_dt').isNull(), col('edl_end_date')).otherwise(col('next_dt'))).withColumn('active_flag', when(col('next_dt').isNull(), lit('Y')).otherwise(lit('N'))).drop('next_dt')
df_temp_target.registerTempTable("df_temp_target")

#fetching hive table location for target table
target_hive_location = HiveToHDFSHandler().fetch_hive_table_location(target_tablename)


#fetching previous cycle ID for removal
string_query = "select min(pt_cycle_id) as cid from {target_tablename}".format(target_tablename=target_tablename)
prev_cycle = spark.sql(string_query)
prev_cycle_val = prev_cycle.collect()
prev_cycle_id = prev_cycle_val[0]['cid']

#write scd2 transformed data into target table
string = """INSERT OVERWRITE TABLE {target_table} PARTITION (pt_data_dt, pt_cycle_id) select * , '{data_dt}','{cycle_id}' from df_temp_target""".format(target_table=target_tablename, data_dt=data_dt, cycle_id=cycle_id)
spark.sql(string)

#hdfs path that has previous data partitions
if prev_cycle_id:
	#fetching previous data date for removal
	query_string = "select pt_data_dt as dd from {target_tablename} where pt_cycle_id='{cycle_id}'".format(target_tablename=target_tablename, cycle_id=str(prev_cycle_id))
	prev_date = spark.sql(query_string)
	prev_date_val = prev_date.collect()
	prev_data_dt = prev_date_val[0]['dd']
	hdfs_path_to_remove = target_hive_location + '/pt_data_dt=' + str(prev_data_dt) + '/pt_cycle_id=' + str(prev_cycle_id)

	#deletion of previous cycles data
	command_to_delete_prev_data = "hdfs dfs -rmr " + str(hdfs_path_to_remove)
	command_response = subprocess.Popen(command_to_delete_prev_data, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
	output,error = command_response.communicate()

#msck repair of target table
string_query = "msck repair table {target_tablename}".format(target_tablename=target_tablename)
spark.sql(string_query)

#copying data from hdfs to s3 table path
CommonUtils().copy_hdfs_to_s3(target_tablename)
