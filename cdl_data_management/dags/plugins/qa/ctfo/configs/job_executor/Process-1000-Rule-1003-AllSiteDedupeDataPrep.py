


################################# Module Information ######################################
#  Module Name         : Site Dedupe Data Prep
#  Purpose             : This module will prepare the data for site dedupe
#  Pre-requisites      : L1 source table required:aact_facilities, citeline_organization
#  Last changed on     : 17-07-2021
#  Last changed by     : Himanshi
#  Reason for change   : Initial development
#  Return Values       : NA
############################################################################################


# Parameter set up for the processing
import hashlib
import csv
import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, col, lit
import pandas as pd
import os
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')


sqlContext = SQLContext(spark)

site_name_address_standardization_path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/' \
                                         'clinical-data-lake/uploads/' \
                                         'name_address_standardization.csv'
site_name_address_standardization_temp_path = spark.read.csv(site_name_address_standardization_path,
                                                             sep=',', header=True, inferSchema=True)

site_name_address_standardization_temp_path.write.mode('overwrite').saveAsTable("site_name_address_standardization")

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/site/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

# country_temp_path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/' \
#                     'uploads/country_standardization.csv'
# country_standardization = spark.read.csv(country_temp_path, sep=',', header=True, inferSchema=True)
# country_standardization.createOrReplaceTempView('country_standardization')

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/standard_country_mapping.csv")
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

sanofi_cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/sanofi_cluster_pta_mapping.csv")
sanofi_cluster_pta_mapping.createOrReplaceTempView("sanofi_cluster_pta_mapping")

country_standardization = spark.sql("""select coalesce(cs.standard_country,'Others') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.sanofi_cluster,scpm.sanofi_csu,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country, country
    from standard_country_mapping )  cs
left join
        sanofi_cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
country_standardization.createOrReplaceTempView("country_standardization")


city_temp_path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/' \
                 'uploads/city_standardization.csv'
city_standardization = spark.read.csv(city_temp_path, sep=',', header=True, inferSchema=True)
city_standardization.createOrReplaceTempView('city_standardization')

# Fetching name and address standardization table
site_name_address_standardization_temp_path = spark.sql("select * from site_name_address_standardization")

# Fetching country standardization
country_standardization_final = spark.sql("""
select
    trim(country) as country,
    max(standard_country) as standard_country
from
(
select
    regexp_replace(trim(lower(country)),'[^0-9A-Za-z]','') as country,
    standard_country
from country_standardization)
group by 1
""")

country_standardization_final.registerTempTable("country_standardization_final")

# Fetching city standardization
city_standardization_final = spark.sql("""
select
    trim(city) as city,
    max(standard_city) as standard_city
from
(
select
    regexp_replace(trim(lower(city)),"[^0-9A-Za-z]",'') as city,
    standard_city
from city_standardization)
group by 1
""")

city_standardization_final.registerTempTable("city_standardization_final")

pandas_name_address_standardization_df = site_name_address_standardization_temp_path.\
    select('*').toPandas()

non_standard_value_list = pandas_name_address_standardization_df['NON_STANDARD_VALUE'].tolist()
standard_value_list = pandas_name_address_standardization_df['STANDARD_VALUE'].tolist()

def standardize_name_address(column_name):
    count = 0
    if column_name is None:
        return column_name
    for value in non_standard_value_list:
        if ' '+value+' ' in column_name:
            new_value = ' '+standard_value_list[count]+' '
            column_name = column_name.replace(' '+value+' ', new_value)
        elif column_name.startswith(value):
            new_value = standard_value_list[count]
            column_name = column_name.replace(value, new_value)
        elif column_name.endswith(value):
            new_value = standard_value_list[count]
            column_name = column_name.replace(value, new_value)
        count = count + 1
    return column_name

sqlContext.udf.register('standardize_name_address', standardize_name_address)

def is_numeric(S):
    try:
        float(S)
        return True
    except ValueError:
        return False

sqlContext.udf.register("is_numeric_type", is_numeric, BooleanType())

def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)

sqlContext.udf.register("get_ascii_value", get_ascii_value)

# get site information from AACT
temp_aact_site_data = spark.sql("""
select
    'aact' as data_src_nm,
    id as src_site_id,
    name,
    city,
    state,
    zip,
    concat(city,' ',state,' ',country,' ',zip) as address,
    country,
    concat_ws('\;', collect_set(aact_facility.nct_id)) as nct_id
from sanofi_ctfo_datastore_staging_$$db_env.aact_facilities aact_facility
group by 1,2,3,4,5,6,7,8
""")
temp_aact_site_data.createOrReplaceTempView('temp_aact_site_data')

# get site information from Citeline
temp_citeline_site_data = spark.sql("""
select
    'citeline' as data_src_nm,
    cite_org.src_site_id as src_site_id,
    cite_org.name as name,
    cite_org.city as city,
    cite_org.state as state,
    cite_org.zip as zip,
    cite_org.address as address,
    cite_org.country as country,
    concat_ws('\;', collect_set(cite_trove.nct_id)) as nct_id
from
(
select
    'citeline' as data_src_nm,
    site_id as src_site_id,
    site_name as name,
    site_location_city as city,
    site_location_state as state,
    site_location_postcode as zip,
    concat(site_location_street,' ',site_location_city,' ',site_location_state,' ',
    site_location_country,' ',site_location_postcode) as address,
    site_location_country as country,
        new_site_trial_id
from sanofi_ctfo_datastore_staging_$$db_env.citeline_organization
lateral view explode(split(site_trial_id,'\\\;'))one as new_site_trial_id)cite_org
left outer join
(select protocol_ids as nct_id,trial_id as trial_id from
sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove)cite_trove
on cite_org.new_site_trial_id = cite_trove.trial_id
group by 1,2,3,4,5,6,7,8
""")
temp_citeline_site_data.createOrReplaceTempView('temp_citeline_site_data')

# Get site information from DQS - eliminated

temp_dqs_site_data_0 = spark.sql("""
select
    'ir' as data_src_nm,
    study_site.facility_golden_id as src_site_id,
    study_site.facility_name as name,
    study_site.city,
    study_site.state,
    study_site.postal as zip,
    study_site.facility_address as address,
    study_site.country,
    study_site.site_open_dt ,
    concat_ws('\;', collect_set(nct_number)) as nct_id
from sanofi_ctfo_datastore_staging_$$db_env.dqs_study study_site
group by 1,2,3,4,5,6,7,8,9
""")
temp_dqs_site_data_0.registerTempTable('temp_dqs_site_data_0')
# Applying rank to get unique records - removed mdid_hcf
temp_dqs_site_data_rnk = spark.sql("""
select data_src_nm,
src_site_id,
name,
city,
state,
zip,
address,
country,
nct_id,
row_number() over (partition by src_site_id order by site_open_dt desc) as rnk
from temp_dqs_site_data_0""")
temp_dqs_site_data_rnk.createOrReplaceTempView('temp_dqs_site_data_rnk')

temp_dqs_site_data = spark.sql("""
select
    data_src_nm,
    src_site_id,
    name,
    city,
    state,
    zip,
    address,
    country,
    nct_id
from temp_dqs_site_data_rnk where rnk=1
""")
temp_dqs_site_data.registerTempTable("temp_dqs_site_data")
temp_dqs_site_data.write.mode('overwrite').saveAsTable('temp_dqs_site_data')


# get site information from CTMS
temp_ctms_site_data_0 = spark.sql("""
select
    'ctms' as data_src_nm,
    b.CENTER_ID as src_site_id,
    b.CENTER_NAME as name,
    a.TOWN_CITY as city,
    a.STATE_PROVINCE_COUNTY as state,
    a.POSTAL_ZIP_CODE as zip,
    case when  a.ADDRESS_LINE_1 = a.ADDRESS_LINE_2 then a.ADDRESS_LINE_1
        else concat(a.ADDRESS_LINE_1,' ',a.ADDRESS_LINE_2) end as address,
    a.COUNTRY as country,
    '' as nct_id,
        b.meta_modification_date
from sanofi_ctfo_datastore_staging_$$db_env.ctms_centre_address a
left outer join sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site b
on lower(trim(b.CENTER_ID)) = lower(trim(a.CENTER_ID))
group by 1,2,3,4,5,6,7,8,10
""")
temp_ctms_site_data_0.createOrReplaceTempView('temp_ctms_site_data_0')

temp_ctms_site_data_rnk = spark.sql("""
select
data_src_nm,
src_site_id,
name,
city,
state,
zip,
address,
country,
nct_id,
row_number() over (partition by src_site_id order by meta_modification_date desc) as rnk
from temp_ctms_site_data_0
""")
temp_ctms_site_data_rnk.createOrReplaceTempView('temp_ctms_site_data_rnk')
temp_ctms_site_data_rnk.write.mode('overwrite').saveAsTable('temp_ctms_site_data_rnk')
temp_ctms_site_data = spark.sql("""
select
data_src_nm,
src_site_id,
name,
city,
state,
zip,
address,
country,
nct_id
from temp_ctms_site_data_rnk where rnk=1
""")
temp_ctms_site_data.createOrReplaceTempView('temp_ctms_site_data')
temp_ctms_site_data.write.mode('overwrite').saveAsTable('temp_ctms_site_data')
# Combining all data
temp_all_site_data = spark.sql("""
select * from temp_aact_site_data
union
select * from temp_citeline_site_data
union
select * from temp_dqs_site_data
union
select * from temp_ctms_site_data
""")

# save data to hdfs
temp_all_site_data.write.mode('overwrite').saveAsTable('temp_all_site_data')
write_path = path.replace('table_name', 'temp_all_site_data')
temp_all_site_data.repartition(100).write.mode('overwrite').parquet(write_path)

# Standardizing city and country
# Removed sites having common site names like research site etc.
# Data cleaning Replace ' ','','null','NA' by null and repace all the characters other
# than A-Z,A-z,0-9 by ' '

temp_all_site_data_cleaned_int_ctms_ir = spark.sql("""
select /* + broadcast(city_std,country_std) */
    data_src_nm,
    concat(data_src_nm,'_',src_site_id) as ds_site_id,
    case when name in (' ','','null','NA', NULL) then null
    else standardize_name_address(lower(regexp_replace(get_ascii_value(name),"[^0-9A-Za-z ]",' '))) end as name,
    lower(trim(get_ascii_value(coalesce(city_std.standard_city,all_data.city)))) as city,
    case when state in (' ','','null','NA') then null else lower(regexp_replace(get_ascii_value(state),
    "[^0-9A-Za-z ]",' ')) end as state,
    trim(case when zip in (' ','','null','NA') then null
    else lower(regexp_replace(case
    when length(split(zip,'-')[0]) > 4 then split(zip,'-')[0]
    when length(split(zip,'-')[1]) > 4 then split(zip,'-')[1]
    else zip end
    ,"[^0-9A-Za-z ]",'')) end) as zip,
    case when address in (' ','','null','NA', NULL) then null
    else standardize_name_address(lower(regexp_replace(get_ascii_value(address),"[^0-9A-Za-z ]",' '))) end
    as address,
    lower(trim(get_ascii_value(coalesce(country_std.standard_country,all_data.country,'others')))) as country,
   substr(sha2(regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]',''),256),1,16) as name_hash_uid,
    substr(sha2(regexp_replace(lower(trim(address)),'[^0-9A-Za-z_ ]',''),256),1,16) as addr_hash_uid
from temp_all_site_data all_data
left outer join city_standardization_final city_std
on regexp_replace(lower(trim(all_data.city)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(city_std.city)),"[^0-9A-Za-z_ -,']",'')
left outer join country_standardization country_std
on regexp_replace(lower(trim(all_data.country)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(country_std.country)),"[^0-9A-Za-z_ -,']",'')
where data_src_nm in ('ctms','ir') and
lower(trim(name)) not like '%investigational site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site reference id%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigator%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site reference%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%study site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigative site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not
like '%clinical investigative site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%influence study site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigator site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site refenrec id%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%s320.2.011 site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%various sites%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%entire usa%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%*not given*%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%additional information%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%information%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%information concerning%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%planned%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%terminated%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%completed%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%dr.med%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'site'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'clinical research site'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'research site'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'research facility'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%dummy%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') != ''
and is_numeric_type(regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'')) != 'true'
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') is not null
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') != ''
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not in ('null', '', 'na')
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_all_site_data_cleaned_int_ctms_ir.write.mode("overwrite").saveAsTable('temp_all_site_data_cleaned_int_ctms_ir')

#cleaning aact and citeline

temp_all_sit_data_cleaned_int_aact_citeline = spark.sql("""
select /* + broadcast(city_std,country_std) */
    data_src_nm,
    concat(data_src_nm,'_',src_site_id) as ds_site_id,
    case when name in (' ','','null','NA', NULL) then null
    else standardize_name_address(lower(regexp_replace(get_ascii_value(name),"[^0-9A-Za-z ]",' '))) end as name,
    lower(trim(get_ascii_value(coalesce(city_std.standard_city,all_data.city)))) as city,
    case when state in (' ','','null','NA') then null else lower(regexp_replace(get_ascii_value(state),
    "[^0-9A-Za-z ]",' ')) end as state,
    trim(case when zip in (' ','','null','NA') then null
    else lower(regexp_replace(case
    when length(split(zip,'-')[0]) > 4 then split(zip,'-')[0]
    when length(split(zip,'-')[1]) > 4 then split(zip,'-')[1]
    else zip end
    ,"[^0-9A-Za-z ]",'')) end) as zip,
    case when address in (' ','','null','NA', NULL) then null
    else standardize_name_address(lower(regexp_replace(get_ascii_value(address),"[^0-9A-Za-z ]",' '))) end
    as address,
    lower(trim(get_ascii_value(coalesce(country_std.standard_country,all_data.country,'others')))) as country,
   substr(sha2(regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]',''),256),1,16) as name_hash_uid,
    substr(sha2(regexp_replace(lower(trim(address)),'[^0-9A-Za-z_ ]',''),256),1,16) as addr_hash_uid
from temp_all_site_data all_data
left outer join city_standardization_final city_std
on regexp_replace(lower(trim(all_data.city)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(city_std.city)),"[^0-9A-Za-z_ -,']",'')
left outer join country_standardization country_std
on regexp_replace(lower(trim(all_data.country)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(country_std.country)),"[^0-9A-Za-z_ -,']",'')
where data_src_nm in ('aact','citeline') and
lower(trim(name)) not like '%investigational site%'
and lower(trim(name)) not like '%clinical research site%'
and lower(trim(name)) not like '%research site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site reference id%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigator%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site reference%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%study site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '% site %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '% site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like 'site-%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like 'site:%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%sites%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigative site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not
like '%clinical investigative site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%influence study site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigator site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site refenrec id%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%s320.2.011 site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%various sites%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%entire usa%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%*not given*%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%additional information%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%information%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%information concerning%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%clinical trials%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%planned%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%terminated%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%completed%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%dr %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%dr. %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%md %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%m.d %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%md. %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%dr.med%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%d.r %'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'site'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%dummy%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%research facility%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') != ''
and regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'') not  like '%Dr.%'
and regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'') not  like '% MD%'
and is_numeric_type(regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'')) != 'true'
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') is not null
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') != ''
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not in ('null', '', 'na')
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_all_sit_data_cleaned_int_aact_citeline.write.mode("overwrite").saveAsTable('temp_all_sit_data_cleaned_int_aact_citeline')


temp_all_site_data_cleaned_int = spark.sql(""" select * from
temp_all_site_data_cleaned_int_ctms_ir
union select * from
temp_all_sit_data_cleaned_int_aact_citeline
""")

temp_all_site_data_cleaned_int.write.mode("overwrite").saveAsTable('temp_all_site_data_cleaned_int')




# Fetching lookup file for name address standardization


try:
    name_addr_std = spark.read.format("parquet").option("header", "true").load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/standardization/name_address/latest/")
except:
    schema = StructType([StructField("hash_uid", StringType(), True),
            StructField("std_name", StringType(), True), StructField("std_address", IntegerType(), True)])
    name_addr_std = spark.createDataFrame([], schema)

name_addr_std.registerTempTable("name_addr_std")


# fetching standardized name and address from lookup file
temp_all_site_data_cleaned_int1 = spark.sql("""
select
    data_src_nm,
    ds_site_id,
    coalesce(std.std_name,base.name) as name,
    city,
    state,
    zip,
    coalesce(std.std_address,base.address) as address,
    country,
    case when std.std_name is null then 'N' else 'Y' end as std_flag
from temp_all_site_data_cleaned_int base
left outer join (select hash_uid, trim(std_name) as std_name, trim(std_address) as std_address from name_addr_std group by 1,2,3) std
on concat(base.name_hash_uid,'|',base.addr_hash_uid) = std.hash_uid
group by 1,2,3,4,5,6,7,8,9
""")


temp_all_site_data_cleaned_int1.write.mode("overwrite").saveAsTable('temp_all_site_data_cleaned_int1')

# Cleaning data further - HH Changes the table name from temp_all_site_data_cleaned_int1 to temp_all_site_data_cleaned_int and removed the where condition where std_flag = 'N'-
#same as previous
temp_all_site_data_cleaned_int2 = spark.sql("""
select
    data_src_nm,
    ds_site_id,
    standardize_name_address(name) as name,
    city,
    state,
    zip,
    standardize_name_address(address) as address,
    country,
    name as raw_name,
    address as raw_address
from
(select * from temp_all_site_data_cleaned_int1
where std_flag = 'N')
""")

temp_all_site_data_cleaned_int2.write.mode("overwrite").saveAsTable('temp_all_site_data_cleaned_int2')

# Fetching delta standard name and address - HH removed union - added union
delta_std_name_addr = spark.sql("""
select
    hash_uid,
    trim(std_name) as std_name,
    trim(std_address) as std_address
from name_addr_std
union
select
    concat(substr(sha2(regexp_replace(lower(trim(raw_name)),'[^0-9A-Za-z_ ]',''),256),1,16),'|',substr(sha2(regexp_replace(lower(trim(raw_address)),'[^0-9A-Za-z_ ]',''),256),1,16))  as hash_uid,
    trim(name) as std_name,
    trim(address) as std_address
from
    temp_all_site_data_cleaned_int2
where raw_name is not null and raw_address is not null
""").repartition(1).write.mode('overwrite').option("header","true").option("delimiter", '^').save('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/standardization/name_address/$$data_dt/')


data_remove_command = "aws s3 rm s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/standardization/name_address/latest/ --recursive"
data_remove_status = os.system(data_remove_command)
if data_remove_status != 0:
    raise Exception("Failed to remove the file from latest location")

data_copy_command = "aws s3 cp s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/standardization/name_address/$$data_dt/ s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/standardization/name_address/latest/ --sse --recursive"
data_copy_status = os.system(data_copy_command)
if data_copy_status != 0:
    raise Exception("Failed to copy the file to latest location")

# Taking union on of both intermediate cleaned tables - HH - Removed union with temp_all_site_data_cleaned_int1
# added the union
temp_all_site_data_cleaned = spark.sql("""
select
    data_src_nm,
    ds_site_id,
    name,
    city,
    state,
    zip,
    address,
    country
from temp_all_site_data_cleaned_int1
where std_flag = 'Y'
group by 1,2,3,4,5,6,7,8
union
select
    data_src_nm,
    ds_site_id,
    name,
    city,
    state,
    zip,
    address,
    country
from temp_all_site_data_cleaned_int2
group by 1,2,3,4,5,6,7,8
""")

temp_all_site_data_cleaned.write.mode("overwrite").saveAsTable("temp_all_site_data_cleaned")
write_path = path.replace('table_name','temp_all_site_data_cleaned')
temp_all_site_data_cleaned.repartition(100).write.mode('overwrite').parquet(write_path)

# Assigning unique row number for each record for dedupeio algorithm and sha2 hash id to identify same record in multiple runs
temp_all_site_data_prep = spark.sql("""
select
    cast(row_number() over(order by coalesce(name,'NA') ,coalesce(city,'NA'), coalesce(state,'NA'), coalesce(zip,'NA'), coalesce(country,'NA'), coalesce(address,'NA')) as bigint) as uid,
    substr(sha2(concat(coalesce(name,'NA') ,coalesce(city,'NA'), coalesce(state,'NA'), coalesce(zip,'NA'), coalesce(country,'NA'), coalesce(address,'NA')),256),1,16) as hash_uid,
    concat_ws('\;',collect_set(data_src_nm)) as data_src_nm,
    concat_ws('\;',collect_set(ds_site_id)) as ds_site_id,
    name,
    city,
    state,
    zip,
    address,
    country
from temp_all_site_data_cleaned
group by name, city, state, zip, address, country
""")

# save data to hdfs
temp_all_site_data_prep.write.mode('overwrite').saveAsTable('temp_all_site_data_prep')
write_path = path.replace('table_name', 'temp_all_site_data_prep')
temp_all_site_data_prep.repartition(100).write.mode('overwrite').parquet(write_path)



