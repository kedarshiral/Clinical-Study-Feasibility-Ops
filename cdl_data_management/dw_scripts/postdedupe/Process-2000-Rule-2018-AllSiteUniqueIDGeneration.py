import hashlib
import sys
import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sys.path.insert(1, CommonConstants.EMR_CODE_PATH + '/code')
import MySQLdb
import unidecode
import CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from pyspark.sql.functions import *
from ConfigUtility import JsonConfigUtility
from DedupeUtility import maintainGoldenID
from DedupeUtility import KMValidate
from CommonUtils import *
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, col, lit
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

sys.path.insert(1, CommonConstants.EMR_CODE_PATH + '/code')
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

sqlContext = SQLContext(spark)


def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)

path = bucket_path + '/applications/commons/temp/mastering/site/' \
                     'table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' +
                                  CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, 'mysql_db'])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

# only country standardization is used for xref

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
    "{bucket_path}/uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

country_standardization = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct *
	from standard_country_mapping_temp)  cs
left join
		cluster_pta_mapping scpm
		on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
country_standardization.createOrReplaceTempView("country_standardization")

temp_all_site_data = spark.sql(
    """ select * from $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_data """).registerTempTable(
    'temp_all_site_data')

# AACT data preparation for Site
temp_aact_site_data0 = spark.sql("""
select
'aact' as data_src_nm,
site.name as site_name,
site.city as site_city,
site.state as site_state,
site.zip as site_zip,
concat_ws(' ',NULLIF(site.city, ''),NULLIF(site.state, ''),NULLIF(site.country, ''),NULLIF(site.zip, '')) as site_address,
site.country as site_country,
cast(NULL as string) as  site_latitude,
cast(NULL as string) as site_longitude,
case when PHONE_EXTENSION is not null then (case when trim(PHONE_EXTENSION) like '+%' or trim(PHONE_EXTENSION) like ':+%' or trim(PHONE_EXTENSION) like '-%' then REGEXP_REPLACE(PHONE, 'ext', 'x') else 
concat_ws(' x',NULLIF(REGEXP_REPLACE(phone, 'ext [0-9]*', ''), ''),NULLIF(PHONE_EXTENSION, '')) end) else null end  as site_phone,
cast(NULL as string) as  site_email,
site.id as src_site_id,
cast(NULL as string)  as site_supporting_urls
from $$client_name_ctfo_datastore_staging_$$db_env.aact_facilities site 
left join (select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_facility_contacts where 
lower(trim(PHONE_EXTENSION)) REGEXP '^[^a-z]*$' ) f on lower(trim(site.id))=lower(trim(f.facility_id))
left join (select * from temp_all_site_data site_data where lower(trim(data_src_nm))='aact') a on 
lower(trim(site.id))=lower(trim(a.src_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_aact_site_data0.registerTempTable('temp_aact_site_data0')

# removing Tel:
temp_aact_site_data1 = spark.sql("""
select  data_src_nm,
site_name,
site_city,
site_state,
site_zip,
site_address,
site_country,
site_latitude,
site_longitude,
trim(replace(site_phone,'Tel:',''))  site_phone,
site_email,
src_site_id,
site_supporting_urls
from temp_aact_site_data0
""")
temp_aact_site_data1.registerTempTable('temp_aact_site_data1')

# removing Tel.
temp_aact_site_data = spark.sql("""
select  data_src_nm,
site_name,
site_city,
site_state,
site_zip,
site_address,
site_country,
site_latitude,
site_longitude,
trim(replace( site_phone,'Tel.','')) as site_phone,
site_email,
src_site_id,
site_supporting_urls
from temp_aact_site_data1 where site_name is not null 
""")
temp_aact_site_data.registerTempTable('temp_aact_site_data')

temp_tascan_site_data = spark.sql("""
select
    'tascan' as data_src_nm,
    site.site_name as site_name,
    site.site_location_city as site_city,
    site.site_location_state as site_state,
    site.site_location_postcode as site_zip,
    concat_ws(' ', NULLIF(site.site_location_address, ''), NULLIF(site.site_location_city, ''), 
    NULLIF(site.site_location_state, ''), NULLIF(site.site_location_country, ''), 
    NULLIF(site.site_location_postcode, '')) as  site_address,
    site.site_location_country as site_country,
    site.site_geo_latitude as site_latitude,
    site.site_geo_longitude as site_longitude,
    '' as site_phone,
    cast(NULL as string) as site_email,
    site.site_id as src_site_id,
    '' as site_supporting_urls
from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_site site
left join (select * from temp_all_site_data site_data where lower(trim(data_src_nm))='tascan') a on lower(trim(site.site_id))=lower(trim(a.src_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13""")
temp_tascan_site_data.registerTempTable('temp_tascan_site_data')
# Citeline data preparation for Site

temp_citeline_site_data = spark.sql("""
select
    'citeline' as data_src_nm,
    site.site_name as site_name,
    site.site_location_city as site_city,
    site.site_location_state as site_state,
    site.site_location_postcode as site_zip,
    concat_ws(' ', NULLIF(site.site_location_street, ''), NULLIF(site.site_location_city, ''), 
    NULLIF(site.site_location_state, ''), NULLIF(site.site_location_country, ''), 
    NULLIF(site.site_location_postcode, '')) as  site_address,
    site.site_location_country as site_country,
    site.site_geo_location_lat as site_latitude,
    site.site_geo_location_lon as site_longitude,
    site.site_phone_no as site_phone,
    cast(NULL as string) as site_email,
    site.site_id as src_site_id,
    site.site_supporting_urls
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_organization site
left join (select * from temp_all_site_data site_data where lower(trim(data_src_nm))='citeline') a on lower(trim(site.site_id))=lower(trim(a.src_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13""")
temp_citeline_site_data.registerTempTable('temp_citeline_site_data')

# DQS data preparation for Site

temp_dqs_site_data_1 = spark.sql("""
select
'ir' as data_src_nm,
site.facility_name as site_name,
site.facility_city as site_city,
site.facility_state as site_state,
site.facility_postal as site_zip,
concat_ws(' ',NULLIF(site.facility_address, ''), NULLIF(site.facility_city, ''),  NULLIF(site.facility_state, ''),
NULLIF(site.facility_country, ''),  NULLIF(site.facility_postal, '')) as site_address,
site.facility_country as site_country,
cast(NULL as string) as site_latitude,
cast(NULL as string) as site_longitude,
site.facility_phone as site_phone,
cast(NULL as string) as site_email,
site.facility_golden_id as src_site_id,
cast(NULL as string) as site_supporting_urls
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir site
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_dqs_site_data_1.registerTempTable('temp_dqs_site_data_1')

### cases seen data is duplicate, to pick one record rnk is needed
temp_dqs_site_data_2 = spark.sql("""
select
data_src_nm,
site_name,
site_city,
site_state,
site_zip,
site_address,
site_country,
site_latitude,
site_longitude,
site_phone,
site_email,
src_site_id,
site_supporting_urls,
row_number() over (partition by src_site_id order by null asc) as rnk
from temp_dqs_site_data_1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_dqs_site_data_2.registerTempTable('temp_dqs_site_data_2')

temp_dqs_site_data = spark.sql("""
select
    a.data_src_nm,
a.site_name,
a.site_city,
a.site_state,
a.site_zip,
a.site_address,
a.site_country,
a.site_latitude,
a.site_longitude,
a.site_phone,
a.site_email,
a.src_site_id,
a.site_supporting_urls
from temp_dqs_site_data_2 a 
left join
(select * from temp_all_site_data site_data where lower(trim(data_src_nm))='ir') b on lower(trim(a.src_site_id))=lower(trim(b.src_site_id)) 
 where a.rnk = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13""")
temp_dqs_site_data.registerTempTable('temp_dqs_site_data')

# CTMS data preparation for Site

temp_ctms_site_data = spark.sql("""
select
    a.source as data_src_nm,
    a.site_name,
    a.site_city,
    a.site_state,
    a.site_zip,
    a.site_address,
    a.site_country,
    cast(NULL as string) as site_latitude,
    cast(NULL as string) as site_longitude,
    a.site_phone,
    cast(NULL as string) as site_email,
    a.src_site_id,
    cast(NULL as string) as site_supporting_urls
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator a
left join 
(select * from temp_all_site_data site_data where lower(trim(data_src_nm))='ctms') b 
on lower(trim(a.src_site_id))=lower(trim(b.src_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_ctms_site_data.registerTempTable('temp_ctms_site_data')

# join data sources
temp_all_site_data_final = spark.sql("""
select data_src_nm,
case when lower(trim(site_name)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_name) end as site_name ,
case when lower(trim(site_city)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_city) end as site_city ,
case when lower(trim(site_state)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_state) end as site_state ,
case when lower(trim(site_zip)) in (' ','','null','na','none', null,'n/a') then null 
else trim(regexp_replace(site_zip,'[^0-9A-Za-z-]','')) end as site_zip ,
case when lower(trim(site_address)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_address) end as site_address ,
case when lower(trim(site_country)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_country) end as site_country ,
case when lower(trim(site_latitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_latitude) end as site_latitude ,
case when lower(trim(site_longitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_longitude) end as site_longitude ,
case when lower(trim(site_phone)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_phone) end as site_phone ,
case when lower(trim(site_email)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_email) end as site_email , 
src_site_id,
case when site_supporting_urls in (' ','','null','NA', NULL) then null 
else trim(site_supporting_urls) end as site_supporting_urls
from temp_aact_site_data
union
select data_src_nm,
case when lower(trim(site_name)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_name) end as site_name ,
case when lower(trim(site_city)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_city) end as site_city ,
case when lower(trim(site_state)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_state) end as site_state ,
case when lower(trim(site_zip)) in (' ','','null','na','none', null,'n/a') then null 
else trim(regexp_replace(site_zip,'[^0-9A-Za-z-]','')) end as site_zip ,
case when lower(trim(site_address)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_address) end as site_address ,
case when lower(trim(site_country)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_country) end as site_country ,
case when lower(trim(site_latitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_latitude) end as site_latitude ,
case when lower(trim(site_longitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_longitude) end as site_longitude ,
case when lower(trim(site_phone)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_phone) end as site_phone ,
case when lower(trim(site_email)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_email) end as site_email , 
src_site_id,
case when site_supporting_urls in (' ','','null','NA', NULL) then null 
else trim(site_supporting_urls) end as site_supporting_urls
from temp_citeline_site_data
union
select data_src_nm,
case when lower(trim(site_name)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_name) end as site_name ,
case when lower(trim(site_city)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_city) end as site_city ,
case when lower(trim(site_state)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_state) end as site_state ,
case when lower(trim(site_zip)) in (' ','','null','na','none', null,'n/a') then null 
else trim(regexp_replace(site_zip,'[^0-9A-Za-z-]','')) end as site_zip ,
case when lower(trim(site_address)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_address) end as site_address ,
case when lower(trim(site_country)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_country) end as site_country ,
case when lower(trim(site_latitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_latitude) end as site_latitude ,
case when lower(trim(site_longitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_longitude) end as site_longitude ,
case when lower(trim(site_phone)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_phone) end as site_phone ,
case when lower(trim(site_email)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_email) end as site_email , 
src_site_id,
case when site_supporting_urls in (' ','','null','NA', NULL) then null 
else trim(site_supporting_urls) end as site_supporting_urls
from temp_tascan_site_data
union
select data_src_nm,
case when lower(trim(site_name)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_name) end as site_name ,
case when lower(trim(site_city)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_city) end as site_city ,
case when lower(trim(site_state)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_state) end as site_state ,
case when lower(trim(site_zip)) in (' ','','null','na','none', null,'n/a') then null 
else trim(regexp_replace(site_zip,'[^0-9A-Za-z-]','')) end as site_zip ,
case when lower(trim(site_address)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_address) end as site_address ,
case when lower(trim(site_country)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_country) end as site_country ,
case when lower(trim(site_latitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_latitude) end as site_latitude ,
case when lower(trim(site_longitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_longitude) end as site_longitude ,
case when lower(trim(site_phone)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_phone) end as site_phone ,
case when lower(trim(site_email)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_email) end as site_email , 
src_site_id,
case when site_supporting_urls in (' ','','null','NA', NULL) then null 
else trim(site_supporting_urls) end as site_supporting_urls
from temp_dqs_site_data
union
select data_src_nm,
case when lower(trim(site_name)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_name) end as site_name ,
case when lower(trim(site_city)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_city) end as site_city ,
case when lower(trim(site_state)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_state) end as site_state ,
case when lower(trim(site_zip)) in (' ','','null','na','none', null,'n/a') then null 
else trim(regexp_replace(site_zip,'[^0-9A-Za-z-]','')) end as site_zip ,
case when lower(trim(site_address)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_address) end as site_address ,
case when lower(trim(site_country)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_country) end as site_country ,
case when lower(trim(site_latitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_latitude) end as site_latitude ,
case when lower(trim(site_longitude)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_longitude) end as site_longitude ,
case when lower(trim(site_phone)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_phone) end as site_phone ,
case when lower(trim(site_email)) in (' ','','null','na','none', null,'n/a') then null 
else trim(site_email) end as site_email , 
src_site_id,
case when site_supporting_urls in (' ','','null','NA', NULL) then null 
else trim(site_supporting_urls) end as site_supporting_urls
from temp_ctms_site_data where src_site_id is not null
""")
temp_all_site_data_final.write.mode('overwrite').saveAsTable('temp_all_site_data_final')

write_path = path.replace('table_name', 'temp_all_site_data_final')
temp_all_site_data_final.repartition(100).write.mode('overwrite').parquet(write_path)

cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 1000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)
temp_all_site_dedupe_results_final_0 = spark.read.parquet(
    "{bucket_path}/applications/commons/temp/mastering/site/temp_all_site_dedupe_results_final/pt_data_dt={}/pt_cycle_id={}/".format(
        latest_stable_data_date, latest_stable_cycle_id, bucket_path=bucket_path))
temp_all_site_dedupe_results_final_0.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_final_0')

# Modified code to use KMValidate function for incorporating KM input

# Writing base table on HDFS to make it available for the function to fetch data
temp_all_site_dedupe_results_final_1 = spark.sql("""
select * from
temp_all_site_dedupe_results_final_0
""")

temp_all_site_dedupe_results_final_1.write.mode('overwrite'). \
    saveAsTable('temp_all_site_dedupe_results_final_1')

final_table = KMValidate('temp_all_site_dedupe_results_final_1', 'site', '$$data_dt', '$$cycle_id', '$$s3_env')

cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 2000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)
prev_xref_src_site = spark.read.parquet(
    "{bucket_path}/applications/commons/dimensions/xref_src_site/pt_data_dt={}/pt_cycle_id={}/".format(
        latest_stable_data_date, latest_stable_cycle_id, bucket_path=bucket_path))
prev_xref_src_site.write.mode('overwrite').saveAsTable('prev_xref_src_site')

# Initialize Spark session
# spark = SparkSession.builder \
#     .appName("Create Empty DataFrame") \
#     .getOrCreate()
#
# # Create the schema based on the existing DataFrame xref_src_inv1
# schema = StructType([
#     StructField("data_src_nm", StringType(), True),
#     StructField("src_site_id", StringType(), True),
#     StructField("ctfo_site_id", StringType(), True),
#     StructField("site_name", StringType(), True),
#     StructField("site_city", StringType(), True),
#     StructField("site_state", StringType(), True),
#     StructField("site_zip", StringType(), True),
#     StructField("site_country", StringType(), True),
#     StructField("site_address", StringType(), True),
#     StructField("site_supporting_urls", StringType(), True),
#     StructField("therapeutic_area", StringType(), True)
# ])
#
# # Create an empty DataFrame with the specified schema
# prev_xref_src_site = spark.createDataFrame([], schema)


# Save the empty DataFrame as a table if needed
#prev_xref_src_site.write.mode('overwrite').saveAsTable('prev_xref_src_site')



'''
temp_site_clusterid = spark.sql("""
select
    concat("ctfo_site_",row_number() over(order by null)) as site_id,
    temp.final_cluster_id as cluster_id from (select distinct final_cluster_id from """ + final_table + """ where round(final_score,2) >=$$threshold) 
temp""")


temp_site_clusterid.write.mode('overwrite').saveAsTable('temp_site_clusterid')
write_path = path.replace('table_name', 'temp_site_clusterid')
temp_site_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)
'''

temp_site_clusterid = spark.sql("""
        select
            a.final_cluster_id as cluster_id,
            concat("ctfo_site_", row_number() over(order by null) + coalesce(temp.max_id,0)) as site_id
        from
        (select distinct final_cluster_id from """ + final_table + """ where round(final_score,2) >=$$threshold) a cross join
        (select max(cast(split(ctfo_site_id,'_')[2] as bigint)) as max_id from prev_xref_src_site) temp""")

temp_site_clusterid.write.mode('overwrite').saveAsTable('temp_site_clusterid')
write_path = path.replace('table_name', 'temp_site_clusterid')
temp_site_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)

# selecting only those records where score>=$$threshold
temp_xref_site_1 = spark.sql("""
select
    site.uid,
    site.hash_uid,
    site.data_src_nm,
    site.ds_site_id,
    ctoid.site_id,
    site.name,
    site.city,
    site.zip,
    site.address,
    site.country,
    site.score,
    site.final_KM_comment as final_KM_comment
from
    (select distinct uid, final_cluster_id as cluster_id, hash_uid, final_score as score, name,
    city, zip,
        address, country,ds_site_id, data_src_nm,final_KM_comment
    from """ + final_table + """ where round(coalesce(final_score,0),2) >= $$threshold) site
inner join
temp_site_clusterid ctoid
on lower(trim(ctoid.cluster_id))=lower(trim(site.cluster_id))
""")
temp_xref_site_1.write.mode('overwrite').saveAsTable('temp_xref_site_1')
write_path = path.replace('table_name', 'temp_xref_site_1')
temp_xref_site_1.repartition(100).write.mode('overwrite').parquet(write_path)

# for some records whose score>=$$threshold and no cluster id after post dedupe logic assign
#  them unique golden_id=max(site_id)+row_number

temp_xref_site_2 = spark.sql("""
select /*+ broadcast(temp) */
    site.uid,
    site.hash_uid,
    site.data_src_nm,
    site.ds_site_id,
    concat("ctfo_site_", row_number() over(order by null) + coalesce(temp.max_id,0)) as site_id,
    site.name,
    site.city,
    site.zip,
    site.address,
    site.country,
    site.score,
    site.final_KM_comment as final_KM_comment
from
    (select distinct uid,final_cluster_id as cluster_id, hash_uid, final_score as score, name, city,
     zip,
        address, country, ds_site_id, data_src_nm,final_KM_comment
    from """ + final_table + """ where round(coalesce(final_score,0),2) < $$threshold) site
cross join
    (select max(cast(split(site_id,"site_")[1] as bigint)) as max_id from temp_xref_site_1) temp
""")
temp_xref_site_2.write.mode('overwrite').saveAsTable('temp_xref_site_2')
write_path = path.replace('table_name', 'temp_xref_site_2')
temp_xref_site_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_site_final_0 = spark.sql("""
select *
from default.temp_xref_site_1
union
select *
from default.temp_xref_site_2
""")
temp_xref_site_final_0 = temp_xref_site_final_0.dropDuplicates()
temp_xref_site_final_0.write.mode('overwrite').saveAsTable('temp_xref_site_final_0')

# Calling maintainGoldenID function to preserve IDs across runs

# N for new ctfo_id

if '$$Prev_Data' == 'N':
    temp_xref_site = spark.sql("""
    select *
    from temp_xref_site_final_0
    """)
else:
    final_table = maintainGoldenID('temp_xref_site_final_0', 'site', '$$process_id', '$$Prev_Data', '$$s3_env')
    temp_xref_site = spark.sql("""
    select
        base.uid,
        base.hash_uid,
        base.data_src_nm,
        base.ds_site_id as ds_site_id,
        case when (lower(trim(final_KM_comment)) is null or lower(trim(final_KM_comment))  ='' or lower(trim(final_KM_comment))  ='null') then func_opt.ctfo_site_id
        else base.site_id end as site_id,
        base.name,
        base.city,
        base.zip,
        base.country,
        base.address,
        base.score
    from
    temp_xref_site_final_0 base
    left outer join
    """ + final_table + """ func_opt
    on lower(trim(base.hash_uid)) = lower(trim(func_opt.hash_uid))
    """)
temp_xref_site = temp_xref_site.dropDuplicates()
temp_xref_site.write.mode('overwrite').saveAsTable('temp_xref_site')
write_path = path.replace('table_name', 'temp_xref_site')
temp_xref_site.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_site1 = spark.sql("""
select e.uid, 
e.hash_uid,
e.data_src_nm,
f.ds_site_id,
e.site_id, 
e.name,
e.city,
e.zip,
e.country,
e.address,
e.score
from temp_xref_site e left outer join
(select distinct hash_uid, concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_site_ids),'')),true)) AS ds_site_id from temp_xref_site
lateral view outer explode(split(ds_site_id,"\;"))one as ds_site_ids group by 1) f
on lower(trim(e.hash_uid))=lower(trim(f.hash_uid))
""")
temp_xref_site1.write.mode('overwrite').saveAsTable('temp_xref_site1')

xref_src_temp_site1 = spark.sql("""select a.*, b.cnt_site_id from temp_xref_site1 a left join 
(select distinct 
hash_uid, count(distinct site_id) as cnt_site_id from temp_xref_site1 group by 1 having count(distinct site_id)>1) b
on lower(trim(a.hash_uid))=lower(trim(b.hash_uid))
""")
xref_src_temp_site1.write.mode('overwrite').saveAsTable("xref_src_temp_site1")

xref_src_temp1_site1 = spark.sql("""select c.*, d.cnt_hash from xref_src_temp_site1 c left join
(select distinct site_id, count(distinct hash_uid) cnt_hash from temp_xref_site1 where site_id in (select site_id from xref_src_temp_site1)
group by 1) d
on c.site_id=d.site_id
""")
xref_src_temp1_site1.write.mode('overwrite').saveAsTable("xref_src_temp1_site1")

xref_src_site_abc1 = spark.sql("""
select data_src_nm, ds_site_id, hash_uid, uid, score,
site_id,name,city,zip, country, address from
(select *, row_number() over (partition by hash_uid order by cnt_hash desc, site_id desc) as rnk
from xref_src_temp1_site1) where rnk=1""")
xref_src_site_abc1.write.mode('overwrite').saveAsTable('xref_src_site_abc1')

temp_xref_src_site_1 = spark.sql("""
select
    uid,
    hash_uid,
    split(datasource_site_id,"_")[0] as data_src_nm,
    SUBSTRING(datasource_site_id,(instr(datasource_site_id, '_')+1)) as src_site_id,
    site_id,
    name,
    city,
    zip,
    country,
    address,
    score
from xref_src_site_abc1
lateral view outer explode (split(ds_site_id,"\;")) as datasource_site_id
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_xref_src_site_1 = temp_xref_src_site_1.dropDuplicates()
temp_xref_src_site_1.write.mode('overwrite').saveAsTable('temp_xref_src_site_1')
write_path = path.replace('table_name', 'temp_xref_src_site_1')
temp_xref_src_site_1.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_src_site_1.registerTempTable("temp_xref_src_site_1")

temp_xref_src_site_temp = spark.sql("""
select
a.uid,
    a.hash_uid,
    b.data_src_nm,
    b.src_site_id,
    a.site_id,
    a.name,
    a.city,
    a.zip,
    a.country,
    a.address,
    a.score
from temp_xref_src_site_1 a
inner join
(select
src_site_id,data_src_nm
from  temp_xref_src_site_1 group by 1,2 having count(distinct uid)<2) b 
on lower(trim(a.src_site_id))=lower(trim(b.src_site_id)) AND lower(trim(a.data_src_nm))=lower(trim(b.data_src_nm))
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_xref_src_site_temp = temp_xref_src_site_temp.dropDuplicates()
temp_xref_src_site_temp.write.mode('overwrite'). \
    saveAsTable('temp_xref_src_site_temp')
# temp_xref_src_site_temp.registerTempTable("temp_xref_src_site_temp")

temp_xref_src_site_2 = spark.sql("""
select uid,
    hash_uid,
    data_src_nm,
    src_site_id,
    site_id,
    name,
    city,
    zip,
    country,
    address,
    score
from
temp_xref_src_site_temp
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_xref_src_site_2 = temp_xref_src_site_2.dropDuplicates()
# temp_xref_src_site_2.registerTempTable("temp_xref_src_site_2")
temp_xref_src_site_2.write.mode('overwrite').saveAsTable('temp_xref_src_site_2')

temp_xref_src_site_data_src_rnk = spark.sql("""
select temp_xref_src_site_1.*,
case when lower(trim(data_src_nm)) like '%ctms%' then 1
when lower(trim(data_src_nm)) like '%ir%' then 2
when lower(trim(data_src_nm)) like '%citeline%' then 3
when lower(trim(data_src_nm)) like '%aact%' then 5
when lower(trim(data_src_nm)) like '%tascan%' then 4
else 6 end as datasource_rnk from  temp_xref_src_site_1""")
temp_xref_src_site_data_src_rnk.write.mode('overwrite'). \
    saveAsTable('temp_xref_src_site_data_src_rnk')

# temp_xref_src_site_data_src_rnk.registerTempTable("temp_xref_src_site_data_src_rnk")


# Giving rank to records having multiple distinct UID
temp_xref_src_site_3 = spark.sql("""
select uid,
    hash_uid,
    data_src_nm,
    src_site_id,
    site_id,
    name,
    city,
    zip,
    country,
    address,
    score,
datasource_rnk,
ROW_NUMBER() over (partition by src_site_id,data_src_nm order by datasource_rnk asc) as rnk
from
default.temp_xref_src_site_data_src_rnk where src_site_id in
(select src_site_id from (select
src_site_id,data_src_nm
from  default.temp_xref_src_site_data_src_rnk group by 1,2 having count(distinct uid)>1))
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_xref_src_site_3 = temp_xref_src_site_3.dropDuplicates()
temp_xref_src_site_3.write.mode('overwrite').saveAsTable('temp_xref_src_site_3')
write_path = path.replace('table_name', 'temp_xref_src_site_3')
temp_xref_src_site_3.repartition(100).write.mode('overwrite').parquet(write_path)
# temp_xref_src_site_3.registerTempTable("temp_xref_src_site_3")


temp_xref_src_site_4 = spark.sql("""
select
uid,
hash_uid,
data_src_nm,
src_site_id,
site_id,
name,
city,
zip,
country,
address,
score
from
temp_xref_src_site_3  where rnk = 1
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_xref_src_site_4 = temp_xref_src_site_4.dropDuplicates()
temp_xref_src_site_4.write.mode('overwrite').saveAsTable('temp_xref_src_site_4')
write_path = path.replace('table_name', 'temp_xref_src_site_4')
temp_xref_src_site_4.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_src_site_4.registerTempTable("temp_xref_src_site_4")


# Union of records with unique UID and prec records having distinct multi. UID

temp_xref_src_site_5 = spark.sql("""
select
* from temp_xref_src_site_4
union
select * from temp_xref_src_site_2
""")
temp_xref_src_site_5 = temp_xref_src_site_5.dropDuplicates()
temp_xref_src_site_5.write.mode('overwrite').saveAsTable('temp_xref_src_site_5')
write_path = path.replace('table_name', 'temp_xref_src_site_5')
temp_xref_src_site_5.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_src_site_5.registerTempTable("temp_xref_src_site_5")

xref_src_site_pre_temp = spark.sql("""
select * from
(select
xref_site_1.uid,
xref_site_1.hash_uid,
    xref_site_1.data_src_nm,
    xref_site_1.src_site_id,
    xref_site_1.site_id as ctfo_site_id,
    xref_site_1.score,
    site_final.site_name,
    case when site_final.site_city in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_city end as site_city,
    case when site_final.site_state in ('null', '', 'na', 'NA', 'N/A','Not Known')
        then null else site_final.site_state end as site_state,
    case when site_final.site_zip in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_zip end as site_zip,
    case when site_final.site_country in ('null', '', 'na', 'NA', 'N/A') or site_final.site_country is null 
        then null else trim(get_ascii_value(coalesce(country_std.standard_country,site_final.site_country))) end as site_country,
    case when trim(site_final.site_address) in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_address end as site_address,
    site_final.site_supporting_urls
from temp_xref_src_site_5 xref_site_1
left outer join (select ss.*,row_number() over 
(partition by site_country,site_state,site_city,site_zip,src_site_id order by site_address asc ) 
as site_rnk from (select * from temp_all_site_data_final) ss)  site_final
on lower(trim(xref_site_1.data_src_nm)) = lower(trim(site_final.data_src_nm))
and lower(trim(xref_site_1.src_site_id)) = lower(trim(site_final.src_site_id)) 
left outer join country_standardization country_std
on regexp_replace(lower(trim(site_final.site_country)),"[^0-9A-Za-z_ -,']",'') = regexp_replace(lower(trim(country_std.country)),"[^0-9A-Za-z_ -,']",'')
where site_final.site_rnk=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
""")
for col in xref_src_site_pre_temp.columns:
    xref_src_site_pre_temp = xref_src_site_pre_temp.withColumn(col, regexp_replace(col, ' ', '<>')). \
        withColumn(col, regexp_replace(col, '><', '')). \
        withColumn(col, regexp_replace(col, '<>', ' ')) \
        .withColumn(col, regexp_replace(col, '[|]+', '@#@')). \
        withColumn(col, regexp_replace(col, '@#@ ', '@#@')). \
        withColumn(col, regexp_replace(col, '@#@', '|')). \
        withColumn(col, regexp_replace(col, '[|]+', '|')). \
        withColumn(col, regexp_replace(col, '[|]$', '')). \
        withColumn(col, regexp_replace(col, '^[|]', ''))

xref_src_site_pre_temp = xref_src_site_pre_temp.dropDuplicates()
# xref_src_site_pre.registerTempTable('xref_src_site_pre')
# xref_src_site_pre = xref_src_site_pre.drop('score')
xref_src_site_pre_temp.write.mode('overwrite').saveAsTable('xref_src_site_pre_temp')

xref_src_site_pre = spark.sql("""
select cast(uid as bigint) as uid,
hash_uid,
data_src_nm,
src_site_id,
ctfo_site_id,
cast(score as double) as score,
site_name,
site_city,
site_state,
site_zip,
site_country,
site_address,
site_supporting_urls from xref_src_site_pre_temp""")
xref_src_site_pre.registerTempTable('xref_src_site_pre')

xref_src_site_pre2 = spark.sql("""
select
uid,
hash_uid,
data_src_nm,
src_site_id,
ctfo_site_id,
trim(site_name) as site_name,
trim(site_city) as site_city,
trim(site_state) as site_state,
trim(site_zip) as site_zip,
initcap(trim(site_country)) as site_country,
trim(site_address) as site_address,
trim(site_supporting_urls) as site_supporting_urls,
score from
(
select
uid,
hash_uid,
data_src_nm,
src_site_id,
ctfo_site_id,
site_name,
site_city,
site_state,
site_zip,
 site_country,
site_address,
site_supporting_urls,
score,
row_number() over (partition by src_site_id,data_src_nm order by null asc) as rnk
from xref_src_site_pre) a where a.rnk=1
""")
xref_src_site_pre2.write.mode('overwrite').saveAsTable('xref_src_site_pre2')

xref_src_site_precedence_int = spark.sql("""
select * from default.xref_src_site_pre2
""")
xref_src_site_precedence_int = xref_src_site_precedence_int.dropDuplicates()
xref_src_site_precedence_int.write.mode('overwrite').saveAsTable('xref_src_site_precedence_int')

commons_path = bucket_path + '/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
write_path = commons_path.replace('table_name', 'xref_src_site_precedence_int')
xref_src_site_precedence_int.repartition(100).write.mode('overwrite').parquet(write_path)

xref_src_site = spark.sql("""
select
data_src_nm,
src_site_id,
ctfo_site_id,
site_name,
site_city,
site_state,
site_zip,
site_country,
site_address,
site_supporting_urls from
default.xref_src_site_pre2
""")
xref_src_site = xref_src_site.dropDuplicates()
xref_src_site.write.mode('overwrite').saveAsTable('xref_src_site')

###Have to update the DDL
spark.sql("""
INSERT OVERWRITE TABLE $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site
PARTITION (
   pt_data_dt='$$data_dt',
   pt_cycle_id='$$cycle_id'
   )
SELECT
 *
FROM default.xref_src_site
""")

CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site')