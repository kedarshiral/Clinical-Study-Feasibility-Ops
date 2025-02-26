


################################# Module Information ######################################
#  Module Name         : r_trial_investigator
#  Purpose             : This will create target table r_trial_investigator
#  Pre-requisites      : Source tables required: aact_facility_investigators, citeline_investigator
#  ,who_investigators
#  Last changed on     : 27-01-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from source tables
# 2. Pass through the source on key columns to create final target table
############################################################################################

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

# Fetching AACT investigator - trial mapping data
aact_trial_inv = spark.sql("""
select
        trim(id) as src_inv_id,
        trim(nct_id) as src_trial_id,
        'aact' as data_source
from sanofi_ctfo_datastore_staging_$$db_env.aact_facility_investigators
group by 1,2,3
""")
aact_trial_inv.registerTempTable('aact_trial_inv')

# Fetching CITELINE investigator - trial mapping data
citeline_trial_inv = spark.sql("""
select
        trim(investigator_id) as src_inv_id,
        trim(src_trial_id) as src_trial_id,
        'citeline' as data_source
from
sanofi_ctfo_datastore_staging_$$db_env.citeline_investigator
lateral view explode(split(concat_ws('\\\|',ongoing_trials_id,past_trials_id),'\\\|'))
as src_trial_id
group by 1,2,3
""")

citeline_trial_inv.registerTempTable('citeline_trial_inv')
'''
# Fetching WHO investigator - trial mapping data
#who_trial_inv = spark.sql("""
#select
 #   trim(id) as src_inv_id ,
  #  trim(src_trial_id) as src_trial_id,
   # 'who' as data_source
#from
#sanofi_ctfo_datastore_staging_$$db_env.who_investigators
#lateral view explode(split(trialid,'\\\|')) as src_trial_id
#group by 1,2,3
#""")

#who_trial_inv.registerTempTable('who_trial_inv')
'''

# Fetching DQS investigator - trial mapping data
dqs_trial_inv = spark.sql("""
select
    trim(person_golden_id) as src_inv_id,
    trim(member_study_id) as src_trial_id,
    'ir' as data_source
from sanofi_ctfo_datastore_staging_$$db_env.dqs_study
group by 1,2,3
""")

dqs_trial_inv.registerTempTable('dqs_trial_inv')

# Fetching CTMS investigator - trial mapping data
ctms_trial_inv = spark.sql("""
select
    trim(site_staff.PERSON_ID) as src_inv_id,
    trim(study_site.STUDY_CODE) as src_trial_id,
    'ctms' as data_source
from (select CAST(person_id as BIGINT) as person_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff where lower(trim(investigator)) = 'yes' group by 1) site_staff
left join
(select CAST(primary_investigator_id as BIGINT) as primary_investigator_id, study_code from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
group by 1,2) study_site
on site_staff.person_id = study_site.primary_investigator_id
group by 1,2,3
""")
ctms_trial_inv.registerTempTable('ctms_trial_inv')

# Fetching IAWARE investigator - trial mapping data - TBD

# Union investigator - trial mapping from all data sources
all_trial_inv = spark.sql("""
select
        *
from aact_trial_inv
union
select
        *
from citeline_trial_inv
union
select
        *
from dqs_trial_inv
union
select
        *
from ctms_trial_inv
""")

all_trial_inv.registerTempTable('all_trial_inv')

# Joining with xref tables to fetch golden ID corresponding to source ID
r_trial_investigator = spark.sql("""
select
   ctfo_trial_id,
   ctfo_investigator_id
from
    all_trial_inv
inner join (select src_trial_id, ctfo_trial_id, data_src_nm from
sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial xref_src_trial group by 1,2,3)
 xref_src_trial
    on lower(trim(all_trial_inv.src_trial_id)) = lower(trim(xref_src_trial.src_trial_id))
    and lower(trim(all_trial_inv.data_source)) = lower(trim(xref_src_trial.data_src_nm))
inner join
    (select src_investigator_id, ctfo_investigator_id, data_src_nm from
    sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_inv xref_src_inv group by 1,2,3)
     xref_src_inv
    on lower(trim(all_trial_inv.src_inv_id)) = lower(trim(xref_src_inv.src_investigator_id))
    and lower(trim(all_trial_inv.data_source)) = lower(trim(xref_src_inv.data_src_nm))
    group by 1,2
""")
r_trial_investigator.registerTempTable('r_trial_investigator')

# Insert data in hdfs table
spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_investigator
partition(pt_data_dt='$$data_dt',pt_cycle_id='$$cycle_id')
select *
from
    r_trial_investigator where ctfo_trial_id is not null and ctfo_investigator_id is not null
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_investigator')






