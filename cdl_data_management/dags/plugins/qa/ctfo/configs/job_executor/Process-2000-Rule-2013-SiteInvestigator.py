


################################# Module Information ######################################
#  Module Name         : r_site_investigator
#  Purpose             : This will create target table r_site_investigator
#  Pre-requisites      : Source table required: aact_facilities, aact_facility_investigators,dqs, Ctms
#  aact_studies, citeline_organization,
#  Last changed on     : 27-01-2021
#  Last changed by     : Himanshi
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

# Fetching AACT site - investigator mapping data
aact_site_inv = spark.sql("""
select
        aact_site.src_site_id,
        inv.src_inv_id,
        'aact' as data_source
from
(select trim(id) as src_site_id from sanofi_ctfo_datastore_staging_$$db_env.aact_facilities
 group by 1) aact_site
left outer join
(select facility_id,trim(id) as src_inv_id  from
sanofi_ctfo_datastore_staging_$$db_env.aact_facility_investigators group by 1,2) inv
on lower(trim(aact_site.src_site_id)) = lower(trim(inv.facility_id))
group by 1,2,3
""")
aact_site_inv.registerTempTable('aact_site_inv')

# Fetching AACT site - investigator mapping data
citeline_site_inv = spark.sql("""
select
        site_id as src_site_id,
        trim(src_inv_id) as src_inv_id,
        'citeline' as data_source
from
sanofi_ctfo_datastore_staging_$$db_env.citeline_organization lateral view explode
(split(investigator_id,"\;"))one as src_inv_id
""")

citeline_site_inv.registerTempTable('citeline_site_inv')

# Fetching dqs site data


dqs_site_inv = spark.sql("""
select
        distinct
        trim(facility_golden_id) as src_site_id,
        trim(person_golden_id) as src_inv_id ,
        'ir' as data_source
from sanofi_ctfo_datastore_staging_$$db_env.dqs_study
""")
dqs_site_inv.registerTempTable('dqs_site_inv')

# Fetching CTMS site - investigator mapping data
ctms_site_inv = spark.sql("""
select
        trim(study_site.CENTER_ID) as src_site_id,
        trim(site_staff.person_id) as src_inv_id,
        'ctms' as data_source
from
(select CAST(person_id as BIGINT) as person_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff where lower(trim(investigator)) = 'yes' group by 1) site_staff
left join
(select CAST(primary_investigator_id as BIGINT) as primary_investigator_id, center_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
group by 1,2) study_site
on site_staff.person_id = study_site.primary_investigator_id
""")
ctms_site_inv.registerTempTable('ctms_site_inv')



# Combining records from all data sources
all_site_inv = spark.sql("""
select
        *
from aact_site_inv
union
select
        *
from citeline_site_inv
union
select
        *
from dqs_site_inv
union
select
        *
from ctms_site_inv
""")

all_site_inv.registerTempTable('all_site_inv')

# Joining with xref tables to fetch golden ID corresponding to source ID
r_site_investigator = spark.sql("""
select
        xref_src_site.ctfo_site_id,
        xref_src_inv.ctfo_investigator_id
from
    all_site_inv
inner join
    (select ctfo_site_id,src_site_id ,data_src_nm from
    sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site group by 1,2,3) xref_src_site
    on lower(trim(all_site_inv.src_site_id)) = lower(trim(xref_src_site.src_site_id)) and
    lower(trim(all_site_inv.data_source)) = lower(trim(xref_src_site.data_src_nm))
inner join
    (select src_investigator_id, ctfo_investigator_id, data_src_nm from
     sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_inv xref_src_inv group by 1,2,3)
     xref_src_inv
    on lower(trim(all_site_inv.src_inv_id)) = lower(trim(xref_src_inv.src_investigator_id))
    and lower(trim(all_site_inv.data_source)) = lower(trim(xref_src_inv.data_src_nm))
    where xref_src_site.ctfo_site_id is not null and xref_src_inv.ctfo_investigator_id is not null
    group by 1,2
""")
r_site_investigator.registerTempTable('r_site_investigator')

# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.r_site_investigator
 partition(pt_data_dt='$$data_dt',pt_cycle_id='$$cycle_id')
select *
from
    r_site_investigator where ctfo_site_id is not null and ctfo_investigator_id is not null
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.r_site_investigator')






