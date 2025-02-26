import csv
import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, col, lit
import pandas as pd
from pyspark.sql.types import *
import re
import os
import numpy

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

from CommonUtils import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
from pyspark.sql.functions import *

from pyspark.sql.types import *

sqlContext = SQLContext(spark)

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

site_name_address_standardization_path = bucket_path + '/uploads/name_address_standardization.csv'
site_name_address_standardization = spark.read.csv(site_name_address_standardization_path,
                                                   sep=',', header=True, inferSchema=True)
# site_name_address_standardization.registerTempTable('site_name_address_standardization')
site_name_address_standardization.write.mode('overwrite').saveAsTable("site_name_address_standardization")

path = bucket_path + '/applications/commons/temp/mastering/site/table_name/pt_data_dt=20240903/pt_cycle_id=2025011008122265198'

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/standard_country_mapping.csv".format(bucket_path=bucket_path))
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

standard_country_mapping_temp = spark.sql("""
select distinct * from (select   country, standard_country from standard_country_mapping 
union 
select standard_country, standard_country from standard_country_mapping)
""")
standard_country_mapping_temp.createOrReplaceTempView("standard_country_mapping_temp")

cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/zs_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

country_standardization = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct *
	from standard_country_mapping_temp )  cs
left join
		cluster_pta_mapping scpm
		on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
country_standardization.createOrReplaceTempView("country_standardization")

##########################################################CITY MAPPING AND DELTA################################################################################
city_delta_mapping = spark.read.format("com.crealytics.spark.excel").option("header", "true").load("{bucket_path}/uploads/CITY/city_delta/city_delta.xlsx".format(bucket_path = bucket_path))
city_delta_mapping.write.mode('overwrite').saveAsTable('city_delta_mapping')

city_standardization_prev = spark.read.format("com.crealytics.spark.excel").option("header", "true").load("{bucket_path}/uploads/CITY/city_standardization/city_standardization.xlsx".format(bucket_path = bucket_path))
city_standardization_prev.write.mode('overwrite').saveAsTable('city_standardization_prev')

city_threshold = spark.read.format("com.crealytics.spark.excel").option("header", "true").load("{bucket_path}/uploads/CITY/Temp_City_Mapping_Proposed_Holder/city_delta_ouput.xlsx".format(bucket_path = bucket_path))
city_threshold.createOrReplaceTempView('city_threshold')

country_zip_standardization = spark.read.format("com.crealytics.spark.excel").option("header", "true").load("{bucket_path}/uploads/country_zip_standardization.xlsx".format(bucket_path = bucket_path))
country_zip_standardization.write.mode('overwrite').saveAsTable('country_zip_standardization')


from datetime import datetime

##Moving mapping file in Archive folder with date
CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
s3_mapping_file_location = "{bucket_path}/uploads/CITY/city_standardization/city_standardization.xlsx".format(bucket_path = bucket_path)

prev_mapping_file_location = "{bucket_path}/uploads/CITY/city_standardization/Archive/".format(bucket_path = bucket_path)

# creating backup of existing city mapping file in archive folder
file_copy_command = "aws s3 cp {} {}".format(
    os.path.join(s3_mapping_file_location),
    os.path.join(prev_mapping_file_location, "city_standardization_" +CURRENT_DATE+ ".xlsx"))

print("Command to create backup of file on s3 - {}".format(file_copy_command))

file_copy_status = os.system(file_copy_command)
if file_copy_status != 0:
    raise Exception("Failed to create backup of city mapping file on s3")

## Mapping file to be updated with the updated city data
updated_city_standardization = spark.sql(""" 
select * from (
(select raw_city_name as city, standard_city from city_delta_mapping)
union
(select city, standard_city from city_standardization_prev)
union
(select raw_city_name as city, standard_city from city_threshold
where similarity >= 0.85))
""")
updated_city_standardization.write.mode('overwrite').saveAsTable('updated_city_standardization')

city_mapping_code_path = CommonConstants.AIRFLOW_CODE_PATH + '/city_standardization.xlsx'

if os.path.exists(city_mapping_code_path):
    os.system('rm ' + city_mapping_code_path)

# create new mapping file : union of new city mapping data and delta from BA
updated_city_standardization.toPandas().to_excel("city_standardization.xlsx", index=False)

file_delete_command = "aws s3 rm {} ".format(os.path.join(s3_mapping_file_location))
print("Command to delete file on s3 - {}".format(file_delete_command))
file_delete_status = os.system(file_delete_command)
if file_delete_status != 0:
   raise Exception("Failed to delete city mapping file on s3")

#COPY new mapping file on s3
os.system("aws s3 cp city_standardization.xlsx {bucket_path}/uploads/CITY/city_standardization/city_standardization.xlsx".format(bucket_path=bucket_path))


##Moving delta file in Archive folder with date
CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
s3_delta_file_location = "{bucket_path}/uploads/CITY/city_delta/city_delta.xlsx".format(bucket_path = bucket_path)

prev_delta_file_location = "{bucket_path}/uploads/CITY/city_delta/Archive/".format(bucket_path = bucket_path)

# creating backup of existing CITY delta file in archive folder
file_copy_command = "aws s3 cp {} {}".format(
    os.path.join(s3_delta_file_location),
    os.path.join(prev_delta_file_location, "city_delta_" +CURRENT_DATE+ ".xlsx"))

print("Command to create backup of file on s3 - {}".format(file_copy_command))

file_copy_status = os.system(file_copy_command)
if file_copy_status != 0:
    raise Exception("Failed to create backup of city delta file on s3")

#deleting the old delta file
file_delete_command = "aws s3 rm {} ".format(os.path.join(s3_delta_file_location))
file_delete_status = os.system(file_delete_command)
if file_delete_status != 0:
   raise Exception("Failed to delete city delta file on s3")

##Creating an empty delta file
schema = StructType([ \
    StructField("raw_city_name",StringType(),True), \
    StructField("standard_city",StringType(),True), \
  ])
city_delta = spark.createDataFrame(data=[],schema=schema)
city_delta.registerTempTable('city_delta')

#creating new delta file
city_delta.toPandas().to_excel("city_delta.xlsx", index=False)
os.system("aws s3 cp city_delta.xlsx {bucket_path}/uploads/CITY/city_delta/city_delta.xlsx".format(bucket_path=bucket_path))
####################################################################################################################################################################################

city_standardization_temp = spark.sql(""" 
select distinct * from (select city,standard_city  from updated_city_standardization
union
select standard_city,standard_city from updated_city_standardization)
""")
city_standardization_temp.createOrReplaceTempView('city_standardization_temp')

city_standardization_final = spark.sql("""
select
    trim(city) as city,
    max(standard_city) as standard_city
from
(
select
    regexp_replace(trim(lower(city)),"[^0-9A-Za-z_ -,']",'') as city,
    standard_city
from city_standardization_temp)
group by 1
""")

city_standardization_final.registerTempTable("city_standardization_final")

state_codes=spark.read.option("header","true").csv("{bucket_path}/uploads/state_codes.csv".format(bucket_path = bucket_path))
state_codes.registerTempTable("state_codes")

# Fetching name and address standardization table
site_name_address_standardization_temp_path = spark.sql("select * from site_name_address_standardization")
site_name_address_standardization_temp_path.registerTempTable('site_name_address_standardization_temp_path')

pandas_name_address_standardization_df = site_name_address_standardization_temp_path. \
    select('*').toPandas()

non_standard_value_list = pandas_name_address_standardization_df['NON_STANDARD_VALUE'].tolist()
standard_value_list = pandas_name_address_standardization_df['STANDARD_VALUE'].tolist()


def standardize_name_address(column_name):
    non_standard_value_list = pandas_name_address_standardization_df['NON_STANDARD_VALUE'].tolist()
    count = 0
    if column_name is None:
        return column_name
    for value in non_standard_value_list:
        if ' ' + value + ' ' in column_name:
            new_value = ' ' + standard_value_list[count] + ' '
            column_name = column_name.replace(' ' + value + ' ', new_value)
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

# AACT Raw data computation
temp_aact_site_data = spark.sql("""
select 
      'aact' as data_src_nm, 
      a.id as src_site_id, 
      a.name, 
      a.city, 
      a.state, 
      a.zip, 
      concat_ws(' ',NULLIF(a.city,''), NULLIF(a.state,''), NULLIF(a.country,'')) as address, 
      a.country
    from 
      (
        select 
          id, 
          name, 
          city, 
          state, 
          zip, 
          country
        from 
          zs_ctfo_datastore_staging_dev.aact_facilities aact_facility where name is not null
      ) a 
    where 
      a.id is not null 
    group by 1,2,3,4,5,6,7,8
""")
temp_aact_site_data.createOrReplaceTempView('temp_aact_site_data')

temp_tascan_site_data = spark.sql("""
select 
      'tascan' as data_src_nm, 
      a.site_id as src_site_id, 
      a.site_name as name, 
      a.site_location_city as city, 
      a.site_location_state as state, 
      a.site_location_postcode as zip, 
      concat_ws(' ',NULLIF(a.site_location_city,''), NULLIF(a.site_location_state,''), NULLIF(a.site_location_country,'')) as address, 
      a.site_location_country as country
    from 
      (
        select 
          site_id, 
          site_name, 
          site_location_city, 
          site_location_state,          
          site_location_postcode, 
          site_location_country
        from 
          zs_ctfo_datastore_app_commons_dev.tascan_site where site_name is not null
      ) a 
    where 
      a.site_id is not null 
    group by 1,2,3,4,5,6,7,8
""")
temp_tascan_site_data.registerTempTable('temp_tascan_site_data')

# Citeline Raw data Computation
temp_citeline_site_data = spark.sql("""
select
    'citeline' as data_src_nm,
    cite_org.src_site_id as src_site_id,
    cite_org.name as name,
    cite_org.city as city,
    cite_org.state as state,
    cite_org.zip as zip,
    cite_org.address as address,
    cite_org.country as country  
from
(
select
    'citeline' as data_src_nm,
    site_id as src_site_id,
    site_name as name,
    site_location_city as city,
    site_location_state as state,
    site_location_postcode as zip,
    concat_ws(' ',site_location_street,site_location_city,site_location_state,site_location_country) as address,
    site_location_country as country,
    new_site_trial_id
from zs_ctfo_datastore_staging_dev.citeline_organization
lateral view outer explode(split(site_trial_id,'\\\;'))one as new_site_trial_id)cite_org
group by 1,2,3,4,5,6,7,8
""")
temp_citeline_site_data.registerTempTable('temp_citeline_site_data')

# DQS Raw Data Computation
temp_dqs_exploded_disease = spark.sql("""select
    'ir' as data_src_nm,
    facility_golden_id as src_site_id,
    facility_name as name,
    facility_city as city,
    facility_state as state,
    facility_postal as zip,
    facility_address as address,
    facility_country as country,
    site_open_dt
from zs_ctfo_datastore_app_commons_dev.usl_ir
group by 1,2,3,4,5,6,7,8,9""")
temp_dqs_exploded_disease.registerTempTable('temp_dqs_exploded_disease')

temp_dqs_site_data_rnk = spark.sql("""
select data_src_nm,
src_site_id,
name,
city,
state,
zip,
address,
country,
row_number() over (partition by src_site_id order by site_open_dt desc) as rnk
from temp_dqs_exploded_disease""")
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
    country
from temp_dqs_site_data_rnk where rnk=1
""")
# temp_dqs_site_data.registerTempTable("temp_dqs_site_data")
temp_dqs_site_data.write.mode('overwrite').saveAsTable('temp_dqs_site_data')

# CTMS Raw Data Computation
temp_ctms_site_data = spark.sql("""
    select
    'ctms' as data_src_nm,
    usl_stdy_site_inv.src_site_id,
    usl_stdy_site_inv.site_name as name,
    usl_stdy_site_inv.site_city as city,
    usl_stdy_site_inv.site_state as state,
    usl_stdy_site_inv.site_zip as zip,
    usl_stdy_site_inv.site_address as address,
    usl_stdy_site_inv.site_country as country
    from zs_ctfo_datastore_app_commons_dev.usl_study_site_investigator usl_stdy_site_inv
where usl_stdy_site_inv.src_site_id is not null and site_name is not null
group by 1,2,3,4,5,6,7,8 """)
# temp_ctms_site_data.registerTempTable('temp_ctms_site_data')
temp_ctms_site_data.write.mode('overwrite').saveAsTable('temp_ctms_site_data')

# Union of all the data sources
temp_all_site_data_0 = spark.sql("""
select * from temp_aact_site_data
union
select * from temp_citeline_site_data
union
select * from temp_dqs_site_data
union
select * from temp_ctms_site_data
union
select * from temp_tascan_site_data
""")
# temp_all_site_data_0.registerTempTable('temp_all_site_data_0')

temp_all_site_data_0.write.mode('overwrite').saveAsTable('dedupe_all_site_data')

# -------------------------------------------------------------------------------------------------------------------------

# Initial cleaning of raw data
temp_all_site_data_1 = spark.sql("""
select distinct data_src_nm, 
src_site_id,
case when lower(trim(name)) in (' ','','null','na','none', null,'n/a') then null 
else lower(trim(name)) end as name ,
case when lower(trim(city)) in (' ','','null','na','none', null,'n/a') then null
else lower(trim(regexp_replace(get_ascii_value(city),"[^0-9A-Za-z ]",''))) end as city,
case when lower(trim(zip)) in (' ','','null','na','none', null,'n/a') then null
else lower(trim(regexp_replace(zip,'[^0-9A-Za-z-]',''))) end as zip,
case when lower(trim(address)) in (' ','','null','na','none', null,'n/a') then null
else lower(trim(address)) end as address,
case when lower(trim(country)) in (' ','','null','na','none', null,'n/a') then null
else lower(trim(country)) end as country
from default.dedupe_all_site_data""")

for col in temp_all_site_data_1.columns:
    temp_all_site_data_1 = temp_all_site_data_1.withColumn(col, regexp_replace(col, ' ', '<>')). \
        withColumn(col, regexp_replace(col, '><', '')). \
        withColumn(col, regexp_replace(col, '<>', ' ')) \
        .withColumn(col, regexp_replace(col, '[|]+', '@#@')). \
        withColumn(col, regexp_replace(col, '@#@ ', '@#@')). \
        withColumn(col, regexp_replace(col, '@#@', '|')). \
        withColumn(col, regexp_replace(col, '[|]+', '|')). \
        withColumn(col, regexp_replace(col, '[|]$', '')). \
        withColumn(col, regexp_replace(col, '^[|]', ''))

temp_all_site_data_1 = temp_all_site_data_1.dropDuplicates()
temp_all_site_data_1.registerTempTable('temp_all_site_data_1')
#temp_all_site_data.write.mode('overwrite').saveAsTable('temp_all_site_data')

## Zip Code stanardization based on the countries by reading mapping file from S3 ##
cnt=0
query="case "
records=country_zip_standardization.collect()
for row in records:
    cnt+=1
    query_country="when lower(trim(country))='{country}' then {logic}\n".format(country=row["country"],logic=row["logic"])
    query = query+query_country
    if(cnt==len(records)):
       query=query + "else zip end as zip"

query_to_execute="""
select distinct data_src_nm,
src_site_id,
name,
city,
{zip},
address,
country
from
temp_all_site_data_1
""".format(zip=query)

temp_all_site_data= spark.sql(query_to_execute)

temp_all_site_data.write.mode('overwrite').saveAsTable('temp_all_site_data')


spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table zs_ctfo_datastore_app_commons_dev.temp_all_site_data
PARTITION (
            pt_data_dt='20240903',
            pt_cycle_id='2025011008122265198'
            )
select
*
from
temp_all_site_data
""")
CommonUtils().copy_hdfs_to_s3("zs_ctfo_datastore_app_commons_dev.temp_all_site_data")

# CTMS IR Regex replace and cleaning of IR and CTMS
temp_all_site_data_cleaned_int_ctms_ir = spark.sql("""
select /* + broadcast(country_std) */
    data_src_nm,
    concat_ws('_',data_src_nm,src_site_id) as ds_site_id,
    name,
    lower(trim(get_ascii_value(coalesce(city_std.standard_city,all_data.city)))) as city,
    trim(case when lower(trim(zip)) in (' ','','null','na','none') then null
    else lower(regexp_replace(case
    when length(split(zip,'-')[0]) > 4 then split(zip,'-')[0]
    else zip end
    ,"[^0-9A-Za-z]",'')) end) as zip,
    address,
    lower(trim(get_ascii_value(coalesce(country_std.standard_country,all_data.country,'Other')))) as country,
    sha2(regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]',''),256) as name_hash_uid,
    sha2(regexp_replace(lower(trim(address)),'[^0-9A-Za-z_ ]',''),256) as addr_hash_uid
from default.temp_all_site_data all_data
left outer join city_standardization_final city_std
on regexp_replace(lower(trim(all_data.city)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(city_std.city)),"[^0-9A-Za-z_ -,']",'')
left outer join country_standardization country_std
on regexp_replace(lower(trim(all_data.country)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(country_std.country)),"[^0-9A-Za-z_ -,']",'')
where lower(trim(data_src_nm)) in ('ctms','ir') and 
lower(trim(name)) not like '%investigational site%'
    and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'site'
    and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'clinical research site'
    and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'research site'
    and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'research facility'
    and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%dummy%'
    and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') is not null
    and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') != ''
    and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not in ('null', '', 'na')
    and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site reference id%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigator%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%site reference%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%study site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%investigative site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%clinical investigative site%'
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
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') is not null
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') != ''
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not in ('null', '', 'na')
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
and is_numeric_type(regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'')) != 'true'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%many locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%multiple locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%various locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%recruiting locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%trial locations%'
group by 1,2,3,4,5,6,7,8,9
""")
temp_all_site_data_cleaned_int_ctms_ir.write.mode("overwrite").saveAsTable('temp_all_site_data_cleaned_int_ctms_ir')

# cleaning aact and citeline

temp_all_sit_data_cleaned_int_aact_citeline_tascan = spark.sql("""
select /* + broadcast(country_std) */
    data_src_nm,
    concat_ws('_',data_src_nm,src_site_id) as ds_site_id,
    name,
    lower(trim(get_ascii_value(coalesce(city_std.standard_city,all_data.city)))) as city,
    trim(case when lower(trim(zip)) in (' ','','null','na','none') then null
    else lower(regexp_replace(case
    when length(split(zip,'-')[0]) > 4 then split(zip,'-')[0]
    else zip end
    ,"[^0-9A-Za-z]",'')) end) as zip,
    address,
    lower(trim(get_ascii_value(coalesce(country_std.standard_country,all_data.country,'Other')))) as country,
   sha2(regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]',''),256) as name_hash_uid,
   sha2(regexp_replace(lower(trim(address)),'[^0-9A-Za-z_ ]',''),256) as addr_hash_uid
from default.temp_all_site_data all_data
left outer join city_standardization_final city_std
on regexp_replace(lower(trim(all_data.city)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(city_std.city)),"[^0-9A-Za-z_ -,']",'')
left outer join country_standardization country_std
on regexp_replace(lower(trim(all_data.country)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(country_std.country)),"[^0-9A-Za-z_ -,']",'')
where data_src_nm in ('aact','citeline','tascan') and 
lower(trim(name)) not like '%investigational site%'
and lower(trim(name)) not like '%clinical research site%'
and lower(trim(name)) not like '%research site%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like 'personal physicians'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%or speak with your personal physician%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%your personal physician%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,\.']",'') not  like '%local institution%'    
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
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%clinical investigative site%'
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
and regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'') not  like '%dr.%'
and regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'') not  like '% md%'
and is_numeric_type(regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'')) != 'true'
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') is not null
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') != ''
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not in ('null', '', 'na')
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%many locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%multiple locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%various locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%recruiting locations%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not  like '%trial locations%'
group by 1,2,3,4,5,6,7,8,9
""")
temp_all_sit_data_cleaned_int_aact_citeline_tascan.write.mode("overwrite").saveAsTable(
    'temp_all_sit_data_cleaned_int_aact_citeline_tascan')

# Union of Cleaned data (IR CTMS AACT CITELINE)
#Added standardized zip in address formation for AAC & Citeline
temp_all_site_data_cleaned_int = spark.sql("""
select * from temp_all_site_data_cleaned_int_ctms_ir
union
select distinct data_src_nm,
ds_site_id,
name,
city,
zip,
concat_ws(' ',NULLIF(address,''),NULLIF(zip,'')) as address,
country,
name_hash_uid,
addr_hash_uid
from temp_all_sit_data_cleaned_int_aact_citeline_tascan
""")

temp_all_site_data_cleaned_int.write.mode("overwrite").saveAsTable('temp_all_site_data_cleaned_int')

temp_all_site_data_cleaned_int2 = spark.sql("""
select
    data_src_nm,
    ds_site_id,
    case when lower(trim(name)) in (' ','','null','na','none', null) then null
    else standardize_name_address(lower(regexp_replace(get_ascii_value(name),"[^0-9A-Za-z ]",' '))) end as name,
    city,
    zip,
    case when lower(trim(address)) in (' ','','null','na','none', null) then null
    else standardize_name_address(lower(regexp_replace(get_ascii_value(address),"[^0-9A-Za-z ]",' '))) end as address,
    country
from
temp_all_site_data_cleaned_int
""")
temp_all_site_data_cleaned_int2.write.mode("overwrite").saveAsTable('temp_all_site_data_cleaned_int2')

temp_all_site_data_cleaned = spark.sql("""
select
    data_src_nm,
    ds_site_id,
    case when trim(substring_index(lower(trim(name)),' ',-1))=lower(trim(b.state)) then trim(trailing trim(substring_index(lower(trim(name)),' ',-1)) from name)
    else name end as name,
    city,
    zip,
    address,
    temp_all_site_data_cleaned_int2.country
from temp_all_site_data_cleaned_int2 left join state_codes b 
on trim(substring_index(lower(trim(name)),' ',-1))=lower(trim(b.state)) and lower(trim(temp_all_site_data_cleaned_int2.country))=lower(trim(b.country))
group by 1,2,3,4,5,6,7
""")
temp_all_site_data_cleaned.write.mode("overwrite").saveAsTable("temp_all_site_data_cleaned")

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table zs_ctfo_datastore_app_commons_dev.temp_all_site_data_cleaned
PARTITION (
            pt_data_dt='20240903',
            pt_cycle_id='2025011008122265198'
            )
select
*
from
  temp_all_site_data_cleaned
""")
CommonUtils().copy_hdfs_to_s3("zs_ctfo_datastore_app_commons_dev.temp_all_site_data_cleaned")

# Assigning unique row number for each record for dedupeio algorithm and sha2 hash id to identify same record in multiple runs
temp_all_site_data_cleaned_1 = temp_all_site_data_cleaned.sort("name", "city", "zip", "country", "address")
temp_all_site_data_cleaned_1.write.mode('overwrite').saveAsTable('temp_all_site_data_cleaned_1')

def remove_spaces(string):
    string_f = str(string)
    pattern = re.compile(r'\s+')
    return re.sub(pattern, ' ', string_f)


sqlContext.udf.register("remove_spaces", remove_spaces)


temp_all_site_data_cleaned_2 = spark.sql(""" select
        concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_src_nm),'')))) as data_src_nm,
        concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_site_id),'')))) as ds_site_id,
        remove_spaces(name) as name,
        remove_spaces(city) as city,
        remove_spaces(zip) as zip,
        remove_spaces(address) as address,
        remove_spaces(regexp_replace(country,"[^0-9A-Za-z, ()_-]","")) as country
    from (select data_src_nm,ds_site_id,lower(trim(name)) as name , lower(trim(city)) as city ,
    lower(trim(zip)) as zip,lower(trim(address)) as address,lower(trim(country)) as country from  temp_all_site_data_cleaned_1)
    group by 3,4,5,6,7
    """)

# save data to hdfs
temp_all_site_data_cleaned_2.write.mode('overwrite').saveAsTable('temp_all_site_data_cleaned_2')

temp_all_site_data_prep_2 = spark.sql("""select
cast(DENSE_RANK() over(order by lower(trim(coalesce(name,'NA'))) ,lower(trim(coalesce(city,'NA'))), lower(trim(coalesce(zip,'NA'))),
 lower(trim(coalesce(country,'NA')))) as bigint) as uid,
sha2(concat_ws('',lower(trim(coalesce(name,'NA'))) ,lower(trim(coalesce(city,'NA'))), lower(trim(coalesce(zip,'NA'))),
lower(trim(coalesce(country,'NA')))),256) as hash_uid,
        data_src_nm,
        ds_site_id,
         name,
         city,
        zip,
        address,
        country
    from temp_all_site_data_cleaned_2
    """)
temp_all_site_data_prep_2.registerTempTable('temp_all_site_data_prep_2')

temp_all_site_data_prep_3 = spark.sql("""select  uid,
 hash_uid,
 data_src_nms as data_src_nm,
        ds_site_ids as ds_site_id,
         name,
        city,
        CASE WHEN lower(trim(zip)) = "none" THEN null ELSE zip END AS zip,
        address,
        country
    from temp_all_site_data_prep_2
    lateral view outer explode (split(ds_site_id,"\;")) as ds_site_ids
    lateral view outer explode (split(data_src_nm,"\;")) as data_src_nms

    """)
temp_all_site_data_prep_3.write.mode('overwrite').saveAsTable('temp_all_site_data_prep_3')

temp_all_site_data_prep_aact = spark.sql("""select  uid,
 hash_uid,
 concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_src_nm),'')))) as data_src_nm,
        concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_site_id),'')))) as ds_site_id,
         name,
        city,
        zip,
        max(address) as address,
        country
    from temp_all_site_data_prep_3 where lower(trim(split(ds_site_id,'\_')[0]))=lower(trim(data_src_nm)) and lower(trim(data_src_nm))='aact'  group by 1,2,5,6,7,9
    """)

# save data to hdfs
temp_all_site_data_prep_aact.write.mode('overwrite').saveAsTable('temp_all_site_data_prep_aact')

temp_all_site_data_prep_tascan = spark.sql("""select  uid,
 hash_uid,
 concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_src_nm),'')))) as data_src_nm,
        concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_site_id),'')))) as ds_site_id,
         name,
        city,
        zip,
        max(address) as address,
        country
    from temp_all_site_data_prep_3 where lower(trim(split(ds_site_id,'\_')[0]))=lower(trim(data_src_nm)) and lower(trim(data_src_nm))='tascan'  group by 1,2,5,6,7,9
    """)

# save data to hdfs
temp_all_site_data_prep_tascan.write.mode('overwrite').saveAsTable('temp_all_site_data_prep_tascan')

temp_all_site_data_prep = spark.sql("""select  uid,
 hash_uid,
 concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_src_nm),'')))) as data_src_nm,
        concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_site_id),'')))) as ds_site_id,
         name,
        city,
        zip,
        max(address) as address,
        country
    from temp_all_site_data_prep_3 where lower(trim(split(ds_site_id,'\_')[0]))=lower(trim(data_src_nm)) and lower(trim(data_src_nm)) not in ('aact','tascan') group by 1,2,5,6,7,9
    """)

# save data to hdfs
temp_all_site_data_prep.write.mode('overwrite').saveAsTable('temp_all_site_data_prep')

temp_all_site_data_prep_aact_clustering = spark.sql("""select * from 
     temp_all_site_data_prep_aact where uid not in (select distinct uid from temp_all_site_data_prep)
    """)

# save data to hdfs
temp_all_site_data_prep_aact_clustering.write.mode('overwrite').saveAsTable('temp_all_site_data_prep_aact_clustering')

temp_all_site_data_prep_tascan_clustering = spark.sql("""select * from 
     temp_all_site_data_prep_tascan where uid not in (select distinct uid from temp_all_site_data_prep)
    """)

# save data to hdfs
temp_all_site_data_prep_tascan_clustering.write.mode('overwrite').saveAsTable('temp_all_site_data_prep_tascan_clustering')

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep_aact
PARTITION (
            pt_data_dt='20240903',
            pt_cycle_id='2025011008122265198'
            )
select
*
from
    temp_all_site_data_prep_aact
""")

spark.sql("""insert overwrite table zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep_tascan
PARTITION (
            pt_data_dt='20240903',
            pt_cycle_id='2025011008122265198'
            )
select
*
from
    temp_all_site_data_prep_tascan
""")

spark.sql("""insert overwrite table zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep
PARTITION (
            pt_data_dt='20240903',
            pt_cycle_id='2025011008122265198'
            )
select
*
from
    temp_all_site_data_prep
""")

spark.sql("""insert overwrite table zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep_3
PARTITION (
            pt_data_dt='20240903',
            pt_cycle_id='2025011008122265198'
            )
select
*
from
    temp_all_site_data_prep_3
""")

CommonUtils().copy_hdfs_to_s3("zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep_aact")
CommonUtils().copy_hdfs_to_s3("zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep_tascan")
CommonUtils().copy_hdfs_to_s3("zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep")
CommonUtils().copy_hdfs_to_s3("zs_ctfo_datastore_app_commons_dev.temp_all_site_data_prep_3")