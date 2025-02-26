
################################# Module Information ######################################
#  Module Name         : r_trial_sponsor
#  Purpose             : This will create target table r_trial_sponsor
#  Pre-requisites      : Source tables required: xref_src_trial, d_sponsor
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from source table
# 2. Pass through the source tables on key columns to create final target table
############################################################################################

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'true')

# Fetching d_sponsor data
d_sponsor = spark.sql("""
select
    sponsor_id,
    sponsor_name
from sanofi_ctfo_datastore_app_commons_$$db_env.d_sponsor
""")
d_sponsor.registerTempTable('d_sponsor')

# Fetching xref_src_trial data
xref_src_trial = spark.sql("""
select
    src_trial_id,
    ctfo_trial_id,
    data_src_nm
from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
""")
xref_src_trial.registerTempTable('xref_src_trial')

r_trial_sponsor = spark.sql("""
select
	final_table.ctfo_trial_id,
	d_sp.sponsor_id
from
(select
	distinct xref_src_trial.ctfo_trial_id,
	temp_sp.sponsor_nm as std_sponsor_nm
from
xref_src_trial left join temp_d_sponsor temp_sp on
lower(trim(xref_src_trial.src_trial_id)) = lower(trim(temp_sp.ctfo_trial_id)) and
 lower(trim(xref_src_trial.data_src_nm)) = lower(trim(temp_sp.data_src_nm))) final_table
left join d_sponsor d_sp on lower(trim(final_table.std_sponsor_nm)) = lower(trim(d_sp.sponsor_name))
where final_table.ctfo_trial_id is not null and d_sp.sponsor_id is not null
group by 1,2
""")
r_trial_sponsor.registerTempTable('r_trial_sponsor')

# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_sponsor
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
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_sponsor')











