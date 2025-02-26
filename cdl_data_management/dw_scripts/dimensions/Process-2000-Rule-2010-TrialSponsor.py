################################# Module Information ######################################
#  Module Name         : r_trial_sponsor
#  Purpose             : This will create target table r_trial_sponsor
#  Pre-requisites      : Source tables required: xref_src_trial, d_sponsor, d_subsidiary_sponsor
#  Last changed on     : 12-1-2023
#  Last changed by     : Dhananjay|Ujjwal
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from source table
# 2. Pass through the source tables on key columns to create final target table
############################################################################################

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

src_r_trial_sponsor = spark.sql("""
select distinct
       a.data_src_nm,
       a.trial_id,
	   a.sponsor_id, 
       b.subsidiary_sponsor_id,
       a.subsidiary_sponsor
from default.temp_trial_subsidiary_sponsor a
left join $$client_name_ctfo_datastore_app_commons_$$db_env.d_subsidiary_sponsor b
on lower(trim(a.sponsor_id)) = lower(trim(b.sponsor_id)) and lower(trim(a.subsidiary_sponsor)) = lower(trim(b.subsidiary_sponsor))
""")
src_r_trial_sponsor.dropDuplicates()
#src_r_trial_sponsor.createOrReplaceTempView('src_r_trial_sponsor')
src_r_trial_sponsor.write.mode("overwrite").saveAsTable('src_r_trial_sponsor')

temp_r_trial_sponsor = spark.sql("""
select distinct
       a.ctfo_trial_id,
       b.sponsor_id,
       concat_ws("\^",sort_array(collect_set(distinct b.subsidiary_sponsor_id),true)) as subsidiary_sponsor_ids     
from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial a
left join default.src_r_trial_sponsor b
on lower(trim(a.src_trial_id)) = lower(trim(b.trial_id)) and lower(trim(a.data_src_nm)) = lower(trim(b.data_src_nm)) 
where b.sponsor_id is not null and b.subsidiary_sponsor_id is not null
group by 1,2
""")
temp_r_trial_sponsor.dropDuplicates()
#temp_r_trial_sponsor.createOrReplaceTempView('temp_r_trial_sponsor')
temp_r_trial_sponsor.write.mode("overwrite").saveAsTable('temp_r_trial_sponsor')

# DJ: Create final r_trial_sponsor
r_trial_sponsor = spark.sql("""
select distinct 
       ctfo_trial_id,
       sponsor_id,
       subsidiary_sponsor_ids
from 
       temp_r_trial_sponsor
""")
r_trial_sponsor.dropDuplicates()
#r_trial_sponsor.createOrReplaceTempView('r_trial_sponsor')
r_trial_sponsor.write.mode("overwrite").saveAsTable('r_trial_sponsor')

# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_sponsor
partition(pt_data_dt='$$data_dt',pt_cycle_id='$$cycle_id')
select *
from
    r_trial_sponsor
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_sponsor')