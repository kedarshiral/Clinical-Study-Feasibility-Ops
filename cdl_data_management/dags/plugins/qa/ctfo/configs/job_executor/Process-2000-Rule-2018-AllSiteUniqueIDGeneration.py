


################################# Module Information ######################################
#  Module Name         : Investigator records combine
#  Purpose             : This will execute queries for creating xred table
#  Pre-requisites      : Source table required:
#  Last changed on     : 16-06-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# Prepare data for xref tables
############################################################################################
import hashlib
import sys
import pyspark

sys.path.insert(1, '/clinical_design_center/data_management/sanofi_ctfo/code')
import MySQLdb
import CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from pyspark.sql.functions import *
from ConfigUtility import JsonConfigUtility
from DedupeUtility import maintainGoldenID
from DedupeUtility import KMValidate
from CommonUtils import *

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')


path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
     'commons/temp/mastering/site/' \
     'table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH+'/'+
                                  CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, 'mysql_db'])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

#AACT data preparation for Site
temp_aact_site_data = spark.sql("""
select
    'aact' as data_src_nm,
        site.name as site_name,
    site.city as site_city,
    site.state as site_state,
    site.zip as site_zip,
    concat(coalesce(site.city, ''), ' ', coalesce(site.state, ''), ' ', coalesce(site.country, '')
    , ' ',
        coalesce(site.zip, '')) as site_address,
    site.country as site_country,
        null as site_latitude,
        null as site_longitude,
        null as site_phone,
        '' as site_email,
        site.id as src_site_id,
        null as site_supporting_urls
from sanofi_ctfo_datastore_staging_$$db_env.aact_facilities site
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_aact_site_data.registerTempTable('temp_aact_site_data')

#Citeline data preparation for Site

temp_citeline_site_data = spark.sql("""
select
    'citeline' as data_src_nm,
        site.site_name as site_name,
    site.site_location_city as site_city,
    site.site_location_state as site_state,
    site.site_location_postcode as site_zip,
    concat(coalesce(site.site_location_street, ''), ' ', coalesce(site.site_location_city, ''), ' ',
        coalesce(site.site_location_state, ''), ' ', coalesce(site.site_location_country, ''), ' ',
        coalesce(site.site_location_postcode, '')) as site_address,
    site.site_location_country as site_country,
        site.site_geo_location_lat as site_latitude,
        site.site_geo_location_lon as site_longitude,
        site.site_phone_no as site_phone,
        '' as site_email,
        site.site_id as src_site_id,
        site.site_supporting_urls
from sanofi_ctfo_datastore_staging_$$db_env.citeline_organization site
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_citeline_site_data.registerTempTable('temp_citeline_site_data')

#DQS data preparation for Site

temp_dqs_site_data_1 = spark.sql("""
select
    'ir' as data_src_nm,
        site.facility_name as site_name,
    site.city as site_city,
    site.state as site_state,
    site.postal as site_zip,
    concat(coalesce(site.facility_address, ''), ' ', coalesce(site.city, ''), ' ', coalesce(site.state, ''),
     ' ',
        coalesce(site.country, ''), ' ', coalesce(site.postal, '')) as site_address,
    site.country as site_country,
        null as site_latitude,
        null as site_longitude,
        site.phone as site_phone,
        '' as site_email,
        site.facility_golden_id as src_site_id,
        null as site_supporting_urls
from sanofi_ctfo_datastore_staging_$$db_env.dqs_study site
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_dqs_site_data_1.registerTempTable('temp_dqs_site_data_1')

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

temp_dqs_site_data= spark.sql("""
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
 site_supporting_urls

from temp_dqs_site_data_2 where rnk = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_dqs_site_data.registerTempTable('temp_dqs_site_data')



#CTMS data preparation for Site

temp_ctms_site_data = spark.sql("""
select
    'ctms' as data_src_nm,
     b.CENTER_NAME as site_name,
    a.TOWN_CITY as site_city,
    a.STATE_PROVINCE_COUNTY as site_state,
    a.POSTAL_ZIP_CODE as site_zip,
    case when  a.ADDRESS_LINE_1 = a.ADDRESS_LINE_2 then a.ADDRESS_LINE_1
        else concat(a.ADDRESS_LINE_1,' ',a.ADDRESS_LINE_2) end as site_address,
       a.COUNTRY site_country,
        null as site_latitude,
        null as site_longitude,
        '' as site_phone,
        '' as site_email,
        b.CENTER_ID as src_site_id,
        null as site_supporting_urls
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site b
left outer join  sanofi_ctfo_datastore_staging_$$db_env.ctms_centre_address a
on lower(trim(b.CENTER_ID)) = lower(trim(a.CENTER_ID))
where trim(lower(a.primary_address))='yes'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_ctms_site_data.registerTempTable('temp_ctms_site_data')


# join data sources
temp_all_site_data_final = spark.sql("""
select * from temp_aact_site_data
union
select * from temp_citeline_site_data
union
select * from temp_dqs_site_data
union
select * from temp_ctms_site_data
""")
temp_all_site_data_final.write.mode('overwrite').saveAsTable('temp_all_site_data_final')
write_path = path.replace('table_name', 'temp_all_site_data_final')
temp_all_site_data_final.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_site_dedupe_results_final = spark.sql("""
select *
from
sanofi_ctfo_datastore_app_commons_$$db_env.temp_all_site_dedupe_results_final
""")
temp_all_site_dedupe_results_final.write.mode('overwrite').\
    saveAsTable('temp_all_site_dedupe_results_final')

final_table = KMValidate('temp_all_site_dedupe_results_final', 'site', '$$data_dt', '$$cycle_id', '$$s3_env')


# generation of golden id for records whose score>=$$threshold
temp_site_clusterid = spark.sql("""
select
    concat("ctfo_site_",row_number() over(order by null)) as site_id,
    temp.final_cluster_id as cluster_id
from
(select distinct final_cluster_id from """+final_table+
                                """ where round(final_score,2) >=$$threshold) temp""")
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
    site.state,
    site.zip,
    site.address,
    site.country,
    site.score
from
    (select distinct uid, final_cluster_id as cluster_id, hash_uid, final_score as score, name,
    city, state, zip,
        address, country, ds_site_id, data_src_nm
    from """+ final_table+""" where round(coalesce(final_score,0),2) >= $$threshold) site
inner join
temp_site_clusterid ctoid
on ctoid.cluster_id=site.cluster_id
""")
temp_xref_site_1.registerTempTable('temp_xref_site_1')
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
    site.state,
    site.zip,
    site.address,
    site.country,
    site.score
from
    (select distinct uid,final_cluster_id as cluster_id, hash_uid, final_score as score, name, city,
     state, zip,
        address, country, ds_site_id, data_src_nm
    from """+ final_table+""" where round(coalesce(final_score,0),2) < $$threshold) site
cross join
    (select max(cast(split(site_id,"site_")[1] as bigint)) as max_id from temp_xref_site_1) temp
""")
temp_xref_site_2.registerTempTable('temp_xref_site_2')
write_path = path.replace('table_name', 'temp_xref_site_2')
temp_xref_site_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_site_final_0 = spark.sql("""
select *
from temp_xref_site_1
union
select *
from temp_xref_site_2
""")
temp_xref_site_final_0 = temp_xref_site_final_0.dropDuplicates()
temp_xref_site_final_0.write.mode('overwrite').saveAsTable('temp_xref_site_final_0')

# Calling maintainGoldenID function to preserve IDs across runs
final_table = maintainGoldenID('temp_xref_site_final_0', 'site', '$$process_id', '$$Prev_Data', '$$s3_env')


if final_table == 'temp_xref_site_final_0':
    temp_xref_site = spark.sql("""
    select *
    from temp_xref_site_final_0
    """)
else:
    temp_xref_site = spark.sql("""
    select
        base.uid,
        base.hash_uid,
        base.data_src_nm,
        base.ds_site_id as ds_site_id,
        func_opt.ctfo_site_id as site_id,
        base.name,
        base.city,
        base.state,
        base.zip,
        base.country,
        base.address,
        base.score
    from
    temp_xref_site_final_0 base
    left outer join
    """+final_table+""" func_opt
    on base.hash_uid = func_opt.hash_uid
    """)
temp_xref_site = temp_xref_site.dropDuplicates()
temp_xref_site.write.mode('overwrite').saveAsTable('temp_xref_site')
write_path = path.replace('table_name', 'temp_xref_site')
temp_xref_site.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_src_site_1 = spark.sql("""
select
    uid,
    hash_uid,
    split(datasource_site_id,"_")[0] as data_src_nm,
    SUBSTRING(datasource_site_id,(instr(datasource_site_id, '_')+1)) as src_site_id,
    site_id,
    name,
    city,
    state,
    zip,
    country,
    address,
    score
from temp_xref_site temp_xref_site
lateral view explode (split(ds_site_id,"\;")) as datasource_site_id
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_xref_src_site_1 = temp_xref_src_site_1.dropDuplicates()
temp_xref_src_site_1.write.mode('overwrite').saveAsTable('temp_xref_src_site_1')
write_path = path.replace('table_name', 'temp_xref_src_site_1')
temp_xref_src_site_1.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_src_site_1.registerTempTable("temp_xref_src_site_1")

temp_xref_src_site_temp = spark.sql("""
select
a.uid,
    a.hash_uid,
    b.data_src_nm,
    b.src_site_id,
    a.site_id,
    a.name,
    a.city,
    a.state,
    a.zip,
    a.country,
    a.address,
    a.score
from temp_xref_src_site_1 a
inner join
(select
src_site_id,data_src_nm
from  temp_xref_src_site_1 group by 1,2 having count(distinct uid)<2) b on a.src_site_id=b.src_site_id AND a.data_src_nm=b.data_src_nm
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_xref_src_site_temp = temp_xref_src_site_temp.dropDuplicates()
temp_xref_src_site_temp.write.mode('overwrite').\
    saveAsTable('temp_xref_src_site_temp')
#temp_xref_src_site_temp.registerTempTable("temp_xref_src_site_temp")

temp_xref_src_site_2 = spark.sql("""
select uid,
    hash_uid,
    data_src_nm,
    src_site_id,
    site_id,
    name,
    city,
    state,
    zip,
    country,
    address,
    score
from
temp_xref_src_site_temp
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_xref_src_site_2 = temp_xref_src_site_2.dropDuplicates()
#temp_xref_src_site_2.registerTempTable("temp_xref_src_site_2")
temp_xref_src_site_2.write.mode('overwrite').\
    saveAsTable('temp_xref_src_site_2')




temp_xref_src_site_data_src_rnk = spark.sql ("""
select temp_xref_src_site_1.*,
        case when data_src_nm like '%ctms%' then 1
        when data_src_nm like '%ir%' then 2
        when data_src_nm like '%citeline%' then 3
    when data_src_nm like '%aact%' then 4
else 5 end as datasource_rnk from  temp_xref_src_site_1""")
temp_xref_src_site_data_src_rnk.write.mode('overwrite').\
    saveAsTable('temp_xref_src_site_data_src_rnk')

#temp_xref_src_site_data_src_rnk.registerTempTable("temp_xref_src_site_data_src_rnk")


# Giving rank to records having multiple distinct UID
temp_xref_src_site_3= spark.sql("""
select uid,
    hash_uid,
    data_src_nm,
    src_site_id,
    site_id,
    name,
    city,
    state,
    zip,
    country,
    address,
    score,
datasource_rnk,
ROW_NUMBER() over (partition by src_site_id order by datasource_rnk asc) as rnk
from
temp_xref_src_site_data_src_rnk where src_site_id in
(select src_site_id from (select
src_site_id,data_src_nm
from  temp_xref_src_site_data_src_rnk group by 1,2 having count(distinct uid)>1))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_xref_src_site_3 = temp_xref_src_site_3.dropDuplicates()
temp_xref_src_site_3.write.mode('overwrite').saveAsTable('temp_xref_src_site_3')
write_path = path.replace('table_name', 'temp_xref_src_site_3')
temp_xref_src_site_3.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_src_site_3.registerTempTable("temp_xref_src_site_3")



temp_xref_src_site_4 = spark.sql("""
select
uid,
hash_uid,
data_src_nm,
src_site_id,
site_id,
name,
city,
state,
zip,
country,
address,
score
from
temp_xref_src_site_3  where rnk = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_xref_src_site_4 = temp_xref_src_site_4.dropDuplicates()
temp_xref_src_site_4.write.mode('overwrite').saveAsTable('temp_xref_src_site_4')
write_path = path.replace('table_name', 'temp_xref_src_site_4')
temp_xref_src_site_4.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_src_site_4.registerTempTable("temp_xref_src_site_4")


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

#temp_xref_src_site_5.registerTempTable("temp_xref_src_site_5")







'''
############################################
temp_xref_src_site_2 = spark.sql("""
select uid,
hash_uid,
data_src_nm,
src_site_id,
site_id,
name,
city,
state,
zip,
country,
address,
score,
ROW_NUMBER() over (partition by site_id,data_src_nm order by score desc) as rnk
from temp_xref_src_site_1
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_xref_src_site_2 = temp_xref_src_site_2.dropDuplicates()
temp_xref_src_site_2.write.mode('overwrite').saveAsTable('temp_xref_src_site_2')
write_path = path.replace('table_name', 'temp_xref_src_site_2')
temp_xref_src_site_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_src_site_3 = spark.sql("""
select
trim(concat_ws('\|',collect_set(src_site_id))) as collect_src_site_id,
site_id
from temp_xref_src_site_2
group by 2
""")
temp_xref_src_site_3 = temp_xref_src_site_3.dropDuplicates()
temp_xref_src_site_3.write.mode('overwrite').saveAsTable('temp_xref_src_site_3')
write_path = path.replace('table_name', 'temp_xref_src_site_3')
temp_xref_src_site_3.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_src_site_4 = spark.sql("""
select a.uid,
a.hash_uid,
b.collect_src_site_id,
a.data_src_nm,
a.src_site_id,
a.site_id,
a.name,
a.city,
a.state,
a.zip,
a.country,
a.address,
a.score
from
temp_xref_src_site_2 a
inner join
temp_xref_src_site_3 b
on a.site_id = b.site_id where a.rnk = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_xref_src_site_4 = temp_xref_src_site_4.dropDuplicates()
temp_xref_src_site_4.write.mode('overwrite').saveAsTable('temp_xref_src_site_4')
write_path = path.replace('table_name', 'temp_xref_src_site_4')
temp_xref_src_site_4.repartition(100).write.mode('overwrite').parquet(write_path)
###################
'''

#Joining golden IDS with site data
xref_src_site_pre = spark.sql("""
select * from
(select
    xref_site_1.data_src_nm,
    xref_site_1.src_site_id,
    xref_site_1.site_id as ctfo_site_id,
    xref_site_1.score,
    site_final.site_name,
    case when site_final.site_city in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_city end as site_city,
    case when site_final.site_state in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_state end as site_state,
    case when site_final.site_zip in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_zip end as site_zip,
    case when site_final.site_country in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_country end as site_country,
    case when trim(site_final.site_address) in ('null', '', 'na', 'NA', 'N/A')
        then null else site_final.site_address end as site_address,
    site_final.site_supporting_urls
from temp_xref_src_site_5 xref_site_1
left outer join (select ss.*,row_number() over (partition by site_country,site_state,site_city,site_zip,src_site_id order by site_address asc ) as site_rnk from (select * from temp_all_site_data_final) ss)  site_final
on xref_site_1.data_src_nm = site_final.data_src_nm
and xref_site_1.src_site_id = site_final.src_site_id
where site_final.site_rnk=1
group by 1,2,3,4,5,6,7,8,9,10,11
) where site_name not like '% M.D%' and site_name not like '%DMD%' and site_name not like '%D.M.D%'
and  regexp_replace(trim(site_name),'[^0-9A-Za-z_@| ]','' ) <> 'MD'
or (site_name like '%Office%' and site_name like '%University%' and site_name like '%Hospital%'
and site_name like '%MedSpa%' and site_name like '%Health%' and site_name like '%Practice%'
and site_name like '%Institute%' and site_name like '%Centre%' and site_name like '%Center%')
""")
for col in xref_src_site_pre.columns:
    xref_src_site_pre = xref_src_site_pre.withColumn(col, regexp_replace(col, ' ', '<>')).\
       withColumn(col, regexp_replace(col, '><', '')).\
       withColumn(col, regexp_replace(col, '<>', ' '))\
       .withColumn(col, regexp_replace(col, '[|]+', '@#@')).\
       withColumn(col, regexp_replace(col, '@#@ ', '@#@')).\
       withColumn(col, regexp_replace(col, '@#@', '|')).\
       withColumn(col, regexp_replace(col, '[|]+', '|')).\
       withColumn(col, regexp_replace(col, '[|]$', '')).\
       withColumn(col, regexp_replace(col, '^[|]', ''))

xref_src_site_pre = xref_src_site_pre.dropDuplicates()
#xref_src_site_pre.registerTempTable('xref_src_site_pre')
#xref_src_site_pre = xref_src_site_pre.drop('score')
xref_src_site_pre.write.mode('overwrite').saveAsTable('xref_src_site_pre')

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
site_supporting_urls,
score from
( select
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
xref_src_site = xref_src_site.dropDuplicates()
xref_src_site.registerTempTable('xref_src_site')

xref_src_site_precedence_int=spark.sql("""select * from xref_src_site""")
xref_src_site_precedence_int.write.mode('overwrite').saveAsTable('xref_src_site_precedence_int')
xref_src_site = xref_src_site.drop('score')
xref_src_site.write.mode('overwrite').saveAsTable('xref_src_site')
write_path = path.replace('table_name', 'xref_src_site')
xref_src_site.repartition(100).write.mode('overwrite').parquet(write_path)



spark.sql("""
INSERT OVERWRITE TABLE sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site PARTITION (
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id'
    )
SELECT *
FROM default.xref_src_site
""")

CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site')


