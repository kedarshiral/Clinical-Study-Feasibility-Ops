
################################# Module Information ######################################
#  Module Name         : r_trial_site
#  Purpose             : This will create target table r_trial_site
#  Pre-requisites      : Source tables required: aact_facilities, citeline_organization
#  Last changed on     : 27-12-2019
#  Last changed by     : Ashutosh Jha
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from source tables
# 2. Pass through the source tables on key columns to create final target table
############################################################################################

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

# Fetching AACT site - trial mapping data
aact_trial_site = spark.sql("""
select
        trim(id) as src_site_id,
        trim(nct_id) as src_trial_id,
        'aact' as data_source
from
sanofi_ctfo_datastore_staging_$$db_env.aact_facilities
group by 1,2
""")
aact_trial_site.registerTempTable('aact_trial_site')

# Fetching CTMS site - trial mapping data


ctms_trial_site = spark.sql("""
select
        trim(CENTER_ID) as src_site_id,
        trim(STUDY_CODE) as src_trial_id,
        'ctms' as data_source
from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
where lower(trim(confirmed))='yes'
group by 1,2
""")
ctms_trial_site.registerTempTable('ctms_trial_site')

# Fetching CITELINE site - trial mapping data
citeline_trial_site = spark.sql("""
select
        trim(site_id) as src_site_id,
    trim(src_trial_id) as src_trial_id,
        'citeline' as data_source
from
sanofi_ctfo_datastore_staging_$$db_env.citeline_organization
lateral view explode(split(site_trial_id,'\\\;')) as src_trial_id
group by 1,2
""")

citeline_trial_site.registerTempTable('citeline_trial_site')

# Fetching DQS site - trial mapping data
dqs_trial_site = spark.sql("""
select
        trim(facility_golden_id) as src_site_id,
        trim(member_study_id) as src_trial_id,
        'ir' as data_source
from
sanofi_ctfo_datastore_staging_$$db_env.dqs_study
group by 1,2
""")

dqs_trial_site.registerTempTable("dqs_trial_site")

# Union site - trial mappings from all data sources
all_site_trial = spark.sql("""
select
        *
from aact_trial_site
union
select
        *
from citeline_trial_site
union
select
        *
from dqs_trial_site
union
select
        *
from ctms_trial_site
""")

all_site_trial.registerTempTable('all_site_trial')

# Joining with xref tables to fetch golden ID corresponding to source ID
r_trial_site = spark.sql("""
select
   xref_src_trial.ctfo_trial_id as trial_id,
   xref_src_site.ctfo_site_id as institution_id
from
    all_site_trial
inner join (select src_trial_id, ctfo_trial_id, data_src_nm from
sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial group by 1,2,3) xref_src_trial
    on lower(trim(all_site_trial.src_trial_id)) = lower(trim(xref_src_trial.src_trial_id))
    and lower(trim(all_site_trial.data_source)) = lower(trim(xref_src_trial.data_src_nm))
inner join
(select ctfo_site_id,src_site_id ,data_src_nm from
sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site group by 1,2,3) xref_src_site
    on lower(trim(all_site_trial.src_site_id)) = lower(trim(xref_src_site.src_site_id))
    and lower(trim(all_site_trial.data_source)) = lower(trim(xref_src_site.data_src_nm))
    group by 1,2
""")
r_trial_site.registerTempTable('r_trial_site')

# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site
partition(pt_data_dt='$$data_dt',pt_cycle_id='$$cycle_id')
select *
from
    r_trial_site where trial_id is not null and institution_id is not null
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Push data on S3
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site')






