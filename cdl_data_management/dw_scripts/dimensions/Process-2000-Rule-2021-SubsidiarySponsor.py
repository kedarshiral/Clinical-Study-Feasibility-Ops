################################# Module Information ######################################
#  Module Name         : D_SUBSIDIARY_SPONSOR
#  Purpose             : This will create target table d_subsidiary_sponsor
#  Pre-requisites      : souce tables required: d_sponsor, temp_d_sponsor
#  Last changed on     : 12-01-2023
#  Last changed by     : Dhananjay|Ujjwal
#  Reason for change   : Standard Sponsor Type
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from L1 layer table
# 2. Pass through the L1 table on key columns to create final target table
############################################################################################

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

# using row number function to create temp_subsidiary_sponsor_id
temp_trial_subsidiary_sponsor = spark.sql("""
select distinct
       b.source as data_src_nm,
       a.trial_id,
	   c.sponsor_id, 
	   c.sponsor_name,
       a.subsidiary_sponsor
from default.temp_d_sponsor a
left join (select distinct source, trial_id, raw_sponsor_name from default.citeline_sponsor
union
select distinct source, trial_id, raw_sponsor_name from default.other_sources_union) b
on lower(trim(a.trial_id)) = lower(trim(b.trial_id)) and lower(trim(a.raw_sponsor_name)) = lower(trim(b.raw_sponsor_name))
left join $$client_name_ctfo_datastore_app_commons_$$db_env.d_sponsor c
on lower(trim(a.parent_sponsor)) = lower(trim(c.sponsor_name))
""")
temp_trial_subsidiary_sponsor.dropDuplicates()
#temp_trial_subsidiary_sponsor.createOrReplaceTempView('temp_trial_subsidiary_sponsor')
temp_trial_subsidiary_sponsor.write.mode("overwrite").saveAsTable('temp_trial_subsidiary_sponsor')

temp_d_subsidiary_sponsor = spark.sql("""
select distinct 
       sponsor_id, 
       sponsor_name,
       subsidiary_sponsor,
       dense_rank() over(partition by sponsor_id, sponsor_name order by subsidiary_sponsor) as temp_subsidiary_sponsor_id
from default.temp_trial_subsidiary_sponsor
""")
temp_d_subsidiary_sponsor.dropDuplicates()
#temp_d_subsidiary_sponsor.createOrReplaceTempView('temp_d_subsidiary_sponsor')
temp_d_subsidiary_sponsor.write.mode("overwrite").saveAsTable('temp_d_subsidiary_sponsor')

# Create child dimension - d child sponsor (concat sponsor_id and subsidiary_sponsor_id)
d_subsidiary_sponsor = spark.sql("""
select  distinct 
		sponsor_id, 
        concat(sponsor_id,"_",temp_subsidiary_sponsor_id) as subsidiary_sponsor_id,
        subsidiary_sponsor
from default.temp_d_subsidiary_sponsor
""")
d_subsidiary_sponsor.dropDuplicates()
#d_subsidiary_sponsor.createOrReplaceTempView('d_subsidiary_sponsor')
d_subsidiary_sponsor.write.mode("overwrite").saveAsTable('d_subsidiary_sponsor')


# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.d_subsidiary_sponsor
 partition(
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id')
select *
from d_subsidiary_sponsor
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.d_subsidiary_sponsor')