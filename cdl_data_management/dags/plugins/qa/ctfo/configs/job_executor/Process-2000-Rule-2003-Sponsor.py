
################################# Module Information ######################################
#  Module Name         : D_SPONSOR
#  Purpose             : This will create target table d_sponsor
#  Pre-requisites      : L1 source table required: aact_sponsors, citeline_trialtrove , ctms_organizations, dqs_study
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from L1 layer table
# 2. Pass through the L1 table on key columns to create final target table
############################################################################################
from SponsorStandardization import SponsorStandardization

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

sponsor_mapping = spark.read.format('csv').option('header', 'true'). \
    load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/'
         'sponsor_mapping.csv')

sponsor_mapping.registerTempTable('sponsor_mapping')

# Filter Citeline for required fields
citeline_sponsor = spark.sql("""
select trial_id, nct_id, trim(sp_name) as sponsor, trim(sponsors_type) as sponsors_type
 from sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove
lateral view posexplode(split(sponsor_name,"\\\|"))one as pos1, sp_name lateral view
posexplode(split(sponsor_type,"\\\|"))two as pos2, sponsors_type where pos1=pos2
""")
citeline_sponsor.write.mode("overwrite").saveAsTable("citeline_sponsor")

sponsor_map = spark.sql("""select sponsor_nm, std_name as standard_name from sponsor_mapping
lateral view explode(split(standard_name, '-'))one as std_name """)
sponsor_map.write.mode('overwrite').saveAsTable('sponsor_map')

# Union citeline and aact data
df_union = spark.sql("""
select 'citeline' as source, trial_id, regexp_replace(sponsor,'\\\"','') as sponsor,
sponsors_type as sponsor_type from citeline_sponsor
union
select 'aact' as source,nct_id as trial_id,regexp_replace(name,'\\\"','') as sponsor, agency_class as sponsor_type from
sanofi_ctfo_datastore_staging_$$db_env.aact_sponsors
union
select 'ctms' as source, STUDY_CODE  as trial_id, 'sanofi' as sponsor, '' as sponsor_type from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study
union
select 'ir' as source,member_study_id  as trial_id, trim(sponsor) as sponsor, '' as sponsor_type from
sanofi_ctfo_datastore_staging_$$db_env.dqs_study
""")
df_union.write.mode("overwrite").saveAsTable("sponsor_union")

# Standardizing Top Names
std_top_pharma = spark.sql("""
select trial_id,sponsor,source,sponsor_type, case when standard_name != 'Other'
then trim(standard_name) else trim(sponsor) end as std_sponsor_name from
 sponsor_union left join sponsor_map on trim(regexp_replace(sponsor_nm,'\\\"','')) = trim(sponsor)
""")
std_top_pharma = std_top_pharma.dropDuplicates()
std_top_pharma.write.mode('overwrite').saveAsTable("std_top_pharma")

# the ones which are not present in existing mapping
df_rem_sponsor = spark.sql("""
 select trial_id, sponsor,source,sponsor_type,std_sponsor_name from std_top_pharma where
                           std_sponsor_name = sponsor
                           """)
# df_rem_sponsor.write.mode('overwrite').saveAsTable("table_rem_sponsor")
df_rem_sponsor.registerTempTable("table_rem_sponsor")

# cleaning the names
rem_sponsor_processed_df = spark.sql("""
select source,sponsor,sponsor_type, regexp_replace(regexp_replace(regexp_replace(sponsor,
" Co Ltd| Co., Ltd.,| Co.,Ltd.| Pvt. Ltd.| Co. Ltd.| Co., Ltd.| A/S| S.L.| SL| Co.|Ltd.|, Ltd.| Ltd.| Ltd|, LLC| LLC.| LLC|, Inc.|
Inc.| Inc| Plc.| L.L.C.| S.A.S.| SAS| SA| S.A.|  Corporation| Limited|,| Therapeutics| Corp.|\\\.",""),"  |\\\*|-"," "),"[^a-zA-Z0-9 {}()\\\[\\\]]+","") as sponsor_processed
from table_rem_sponsor
""")
rem_sponsor_processed_df.registerTempTable('rem_sponsor_processed_df')

rem_sponsor_processed_2 = spark.sql("""
select * from rem_sponsor_processed_df where sponsor_processed is not null and trim(
sponsor_processed) <> '' """)

# writing on HDFS
rem_sponsor_processed_2.repartition(1).write.format('csv').option('header', True).mode(
    'overwrite').option('sep', ',') \
    .save('/user/hive/warehouse/rem_sponsor_processed')

# Calling standardization function
sponsor_std = SponsorStandardization()
sponsor_std.main()



# union of all data sources

merged_sponsor = spark.sql("""
select sponsor as sponsor_nm,
sponsor_type as sponsor_category,
trial_id as ctfo_trial_id,
source as data_src_nm
from std_top_pharma
""")

merged_sponsor.write.mode('overwrite').saveAsTable('merged_sponsor')

std_sponsor_mapping = spark.read.format('csv').option('header', 'true'). \
    load(
    's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/SPONSOR_MAPPING/std_sponsors_mapping.csv')

std_sponsor_mapping.registerTempTable('std_sponsor_mapping')

exact_match = spark.read.format('csv').option('header', 'true'). \
    load(
    's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/SPONSOR_MAPPING/exact_matched_sponsors_mapping.csv')

exact_match.registerTempTable('exact_match')

other_sponsors = spark.read.format('csv').option('header', 'true'). \
    load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/SPONSOR_MAPPING'
         '/other_sponsor_mapping.csv')

other_sponsors.registerTempTable('other_sponsors')

other_sponsors_mapping = spark.sql("""
select sponsor, standard_sponsor,standard_sponsor_type
from other_sponsors where similarity >0.67
""")

other_sponsors_mapping.registerTempTable('other_sponsors_mapping')

temp_d_sponsor = spark.sql("""
select sponsor_nm,
case when sponsor_nm ='Other' then 'Other' else sponsor_category end as sponsor_category,
ctfo_trial_id,
data_src_nm
from
(select
    case when trim(std_sponsor_name_exploded) <> '' or std_sponsor_name_exploded is not null
      then trim(std_sponsor_name_exploded) else 'Other' end as     sponsor_nm,
    case when trim(a.sponsor_category) <> '' or sponsor_category is not null then trim(a.sponsor_category)
    else 'Other' end as     sponsor_category,
        ctfo_trial_id,
        data_src_nm
from
(select
    merged_spnsr.sponsor_nm as sponsor_nm,
    case when data_src_nm ='citeline' then lower(trim(coalesce(spnsr_map.ctln_sponsor,
    exact_match.sponsor_exploded_ctln,'Other')))
    else lower(trim(coalesce(spnsr_map.ctln_sponsor,exact_match.sponsor_exploded_ctln,other.standard_sponsor,'Other')))
     end as     std_sponsor_name_exploded,
     case when data_src_nm ='citeline' then
     lower(trim(coalesce(spnsr_map.processed_ctln_sponsor_type,exact_match.processed_ctln_sponsor_type,
     'Other')))
     else lower(trim(coalesce(spnsr_map.processed_ctln_sponsor_type,
     exact_match.processed_ctln_sponsor_type,other.standard_sponsor_type,
     'Other'))) end as sponsor_category,
         ctfo_trial_id,
        data_src_nm
from
    merged_sponsor merged_spnsr
    left outer join std_sponsor_mapping spnsr_map on
    lower(trim(regexp_replace(merged_spnsr.sponsor_nm,'\\\"',''))) =
     lower(trim(spnsr_map.sponsor))

    left join exact_match on
    lower(trim(regexp_replace(merged_spnsr.sponsor_nm,'\\\"',''))) =
    lower(trim(regexp_replace(exact_match.sponsor_aact,'\\\"','')))

    left join other_sponsors_mapping other
    on lower(trim(regexp_replace(merged_spnsr.sponsor_nm,'\\\"',''))) =
    lower(trim(regexp_replace(other.sponsor,'\\\"','')))
    and merged_spnsr.data_src_nm ='aact'
    )a
     )  b
     group by 1,2,3,4
""")

temp_d_sponsor.write.mode('overwrite').saveAsTable('temp_d_sponsor')
write_path = path.replace('table_name', 'temp_d_sponsor')
temp_d_sponsor.repartition(100).write.mode('overwrite').parquet(write_path)

d_sponsor = spark.sql("""
select sponsor_id,aa.sponsor_nm,b.sponsor_category
from
(select row_number() over(order by a.sponsor_nm) as sponsor_id,a.sponsor_nm  from
(select sponsor_nm from temp_d_sponsor group by 1)a )aa
left join temp_d_sponsor b
on aa.sponsor_nm=b.sponsor_nm
group by 1,2,3
""")

d_sponsor.registerTempTable('d_sponsor')

# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.d_sponsor
 partition(
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id')
select *
from d_sponsor
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.d_sponsor')



