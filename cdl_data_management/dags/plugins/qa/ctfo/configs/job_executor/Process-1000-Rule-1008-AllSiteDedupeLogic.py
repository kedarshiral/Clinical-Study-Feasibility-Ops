


################################# Module Information ######################################
#  Module Name         : Site Post Dedupe Logic
#  Purpose             : This will combine clustered records
#  Pre-requisites      : L1 source table required: temp_all_site_match_score,temp_all_site_data_prep
#  Last changed on     : 22-03-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################
import sys
from CommonUtils import CommonUtils

sys.path.insert(1, '/app/clinical_design_center/data_management/sanofi_ctfo/configs/job_executor')
from DedupeUtility import KMValidate

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/site/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

temp_all_site_match_score = spark.sql(
    """
    select * from temp_all_site_match_score
    """)

write_path = path.replace('table_name', 'temp_all_site_match_score')
temp_all_site_match_score.repartition(100).write.mode('overwrite').parquet(write_path)

# Get the grain columns and dedupe output columns
temp_all_site_dedupe_results = spark.sql("""
select
    site_score.cluster_id,
    site_score.score,
    site_prep.uid,
    site_prep.hash_uid,
    site_prep.data_src_nm,
    site_prep.ds_site_id,
    site_prep.name,
    site_prep.city,
    site_prep.state,
    site_prep.zip,
    site_prep.address,
    site_prep.country
from temp_all_site_data_prep site_prep
left outer join
(select
    cluster_id,
    uid,
    score
from temp_all_site_match_score group by 1,2,3) site_score
on site_prep.uid=site_score.uid
""")

temp_all_site_dedupe_results.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results')
temp_all_site_dedupe_results.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule to assign a cluster for exact match on  zip , country and first 8 letters of
#  site name for un clustered records and score less than 0.5
## club dssite and data_src nm
temp_all_site_dedupe_results_1 = spark.sql("""
select
    site_dedupe_1.rule_id,
    coalesce(site_dedupe_1.cluster_id, site_dedupe.cluster_id) as cluster_id,
    coalesce(site_dedupe_1.score, site_dedupe.score) as score,
    site_dedupe.uid,
    site_dedupe.hash_uid,
    site_dedupe.data_src_nm,
    site_dedupe.ds_site_id,
    site_dedupe.name,
    site_dedupe.city,
    site_dedupe.state,
    site_dedupe.zip,
    site_dedupe.address,
    site_dedupe.country
from temp_all_site_dedupe_results site_dedupe
left outer join (select
 'Rule_01' as rule_id,
 uid,
 cluster_id,
 score
from
(select concat_ws("\;",collect_set(uid)) as new_uid ,size(collect_set(uid)) cnt,  name, zip,
country, concat(country,'_',coalesce(zip,'NA'),'_', name) as cluster_id, 0.80 as score
from (select uid, substring(regexp_replace(trim(lower(name)),"[^0-9A-Za-z]",""), 1, 8) as name ,
 coalesce(regexp_replace(lower(zip),"[^0-9]",""),LAG(regexp_replace(lower(zip),"[^0-9]",""),1)
  OVER (ORDER BY name,city, zip desc)) as zip, country from temp_all_site_dedupe_results where
   round(cast(coalesce(score,0.0) as double),2) < 0.50 or trim(lower(score)) = 'null' )
   all_site_dedupe_results
group by 3,4,5
having cnt>1) all_site_dedupe_results lateral view explode (split(new_uid,"\;"))one as uid)
 site_dedupe_1
on site_dedupe.uid = site_dedupe_1.uid
""")
temp_all_site_dedupe_results_1.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_1')

write_path = path.replace('table_name', 'temp_all_site_dedupe_results_1')
temp_all_site_dedupe_results_1.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule to decluster records with different city and zip withing the single cluster
temp_all_site_dedupe_results_2 = spark.sql("""
select
    coalesce(site_dedupe_2.rule_id,site_dedupe_1.rule_id) as rule_id,
    coalesce(site_dedupe_2.cluster_id, site_dedupe_1.cluster_id) as cluster_id,
    coalesce(site_dedupe_2.score, site_dedupe_1.score) as score,
    site_dedupe_1.uid,
    site_dedupe_1.hash_uid,
    site_dedupe_1.data_src_nm,
    site_dedupe_1.ds_site_id,
    site_dedupe_1.name,
    site_dedupe_1.city,
    site_dedupe_1.state,
    site_dedupe_1.zip,
    site_dedupe_1.address,
    site_dedupe_1.country
from temp_all_site_dedupe_results_1 site_dedupe_1
left outer join (select distinct
    'Rule_02' as rule_id,
    uid,
    uid as cluster_id,
    0 as score
from(select site_dedupe_results_1.cluster_id,site_dedupe_results_1.name,
site_dedupe_results_1.city,site_dedupe_results_1.uid from
temp_all_site_dedupe_results_1 site_dedupe_results_1
inner join temp_all_site_dedupe_results_1 site_dedupe_results_2
on site_dedupe_results_1.cluster_id = site_dedupe_results_2.cluster_id
and coalesce(site_dedupe_results_1.zip,'NA') <> coalesce(site_dedupe_results_2.zip,'test')
and site_dedupe_results_1.city <> site_dedupe_results_2.city
where round(cast(coalesce(site_dedupe_results_1.score,0.0) as double),2) >= 0.50)) site_dedupe_2
on site_dedupe_1.uid = site_dedupe_2.uid
""")
temp_all_site_dedupe_results_2.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_2')

write_path = path.replace('table_name', 'temp_all_site_dedupe_results_2')
temp_all_site_dedupe_results_2.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule decluster the records having different countries
temp_all_site_dedupe_results_3 = spark.sql("""
select
    coalesce(site_dedupe_2.rule_id,site_dedupe_1.rule_id) as rule_id,
    coalesce(site_dedupe_2.cluster_id, site_dedupe_1.cluster_id) as cluster_id,
    coalesce(site_dedupe_2.score, site_dedupe_1.score) as score,
    site_dedupe_1.uid,
    site_dedupe_1.hash_uid,
    site_dedupe_1.data_src_nm,
    site_dedupe_1.ds_site_id,
    site_dedupe_1.name,
    site_dedupe_1.city,
    site_dedupe_1.state,
    site_dedupe_1.zip,
    site_dedupe_1.address,
    site_dedupe_1.country
from temp_all_site_dedupe_results_2 site_dedupe_1
left outer join (
select distinct
    'Rule_03' as rule_id,
    uid,
    uid as cluster_id,
    0 as score
from (
select
    site_dedupe_results_1.cluster_id,
    site_dedupe_results_1.name,
    site_dedupe_results_1.country,
    site_dedupe_results_1.uid
from  temp_all_site_dedupe_results_2 site_dedupe_results_1
inner join temp_all_site_dedupe_results_2 site_dedupe_results_2
on site_dedupe_results_1.cluster_id = site_dedupe_results_2.cluster_id
and site_dedupe_results_1.country <> site_dedupe_results_2.country
where round(cast(coalesce(site_dedupe_results_1.score,0.0) as double),2) >= 0.50)) site_dedupe_2
on site_dedupe_1.uid = site_dedupe_2.uid
""")

temp_all_site_dedupe_results_3.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_3')
write_path = path.replace('table_name','temp_all_site_dedupe_results_3')
temp_all_site_dedupe_results_3.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule cluster the records having same name, city and zip
temp_all_site_dedupe_results_4 = spark.sql("""
select
    site_dedupe_1.rule_id,
    coalesce(site_dedupe_1.cluster_id, site_dedupe.cluster_id) as cluster_id,
    coalesce(site_dedupe_1.score, site_dedupe.score) as score,
    site_dedupe.uid,
    site_dedupe.hash_uid,
    site_dedupe.data_src_nm,
    site_dedupe.ds_site_id,
    site_dedupe.name,
    site_dedupe.city,
    site_dedupe.state,
    site_dedupe.zip,
    site_dedupe.address,
    site_dedupe.country
from temp_all_site_dedupe_results_3  site_dedupe
left outer join (
select
    'Rule_04' as rule_id,
    uid,
    cluster_id,
    score
from (
select
    name,
    zip,
    city,
    concat_ws("\;",collect_set(uid)) as new_uid ,
    size(collect_set(uid)) cnt,
    max(coalesce(cluster_id,concat(name,'_',city,'_',zip))) as cluster_id,
    0.80 as score
from (
select
    uid,cluster_id,
    regexp_replace(trim(lower(name)),"[^0-9A-Za-z]","") as name ,
        regexp_replace(lower(coalesce(zip,'NA')),"[^0-9]","")  as zip,
    regexp_replace(trim(lower(city)),"[^0-9A-Za-z]","") as city
from temp_all_site_dedupe_results_3) all_site_dedupe_results
group by 1,2,3
having cnt > 1) all_site_dedupe_results lateral view explode (split(new_uid,"\;"))one as uid
) site_dedupe_1
on trim(site_dedupe.uid) = trim(site_dedupe_1.uid)
""")

temp_all_site_dedupe_results_4.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_4')
write_path = path.replace('table_name','temp_all_site_dedupe_results_4')
temp_all_site_dedupe_results_4.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule cluster the records having same name, city and only one zip
temp_all_site_dedupe_results_5 = spark.sql("""
select
    site_dedupe_1.rule_id,
    coalesce(site_dedupe_1.cluster_id, site_dedupe.cluster_id) as cluster_id,
    coalesce(site_dedupe_1.score, site_dedupe.score) as score,
    site_dedupe.uid,
    site_dedupe.hash_uid,
    site_dedupe.data_src_nm,
    site_dedupe.ds_site_id,
    site_dedupe.name,
    site_dedupe.city,
    site_dedupe.state,
    site_dedupe.zip,
    site_dedupe.address,
    site_dedupe.country
from temp_all_site_dedupe_results_4  site_dedupe
left outer join (
select
    'Rule_05' as rule_id,
    uid,
    cluster_id,
    score
from (
select
cluster_record.name,
cluster_record.city,
concat_ws("\;",collect_set(results_4.uid)) as new_uid ,
max(coalesce(results_4.cluster_id,concat(cluster_record.name,'_',cluster_record.city))) as cluster_id,
0.80 as score
from
(select
        name,
    city,
    count(distinct cluster_id) as cnt_cluster,
        count(distinct zip) as cnt_zip
from (
select
    uid,coalesce(cluster_id,'NA') as cluster_id,
    regexp_replace(trim(lower(name)),"[^0-9A-Za-z]","") as name ,
        regexp_replace(lower(zip),"[^0-9]","")  as zip,
    regexp_replace(trim(lower(city)),"[^0-9A-Za-z]","") as city
from temp_all_site_dedupe_results_4) results_4
group by 1,2 having cnt_cluster > 1 and cnt_zip = 1) cluster_record
inner join (select
    uid,cluster_id ,
    regexp_replace(trim(lower(name)),"[^0-9A-Za-z]","") as name ,
        regexp_replace(lower(zip),"[^0-9]","")  as zip,
    regexp_replace(trim(lower(city)),"[^0-9A-Za-z]","") as city
from temp_all_site_dedupe_results_4) results_4
on cluster_record.name = results_4.name
and cluster_record.city = results_4.city
group by 1,2
) all_site_dedupe_results lateral view explode (split(new_uid,"\;"))one as uid
) site_dedupe_1
on trim(site_dedupe.uid) = trim(site_dedupe_1.uid)
""")

temp_all_site_dedupe_results_5.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_5')
write_path = path.replace('table_name','temp_all_site_dedupe_results_5')
temp_all_site_dedupe_results_5.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule to decluster the records with same data_src_nm
temp_all_site_dedupe_results_6 = spark.sql("""
select
    coalesce(site_dedupe_2.rule_id,site_dedupe_1.rule_id) as rule_id,
    coalesce(site_dedupe_2.cluster_id, site_dedupe_1.cluster_id) as cluster_id,
    coalesce(site_dedupe_2.score, site_dedupe_1.score) as score,
    site_dedupe_1.uid,
    site_dedupe_1.hash_uid,
    site_dedupe_1.data_src_nm,
    site_dedupe_1.ds_site_id,
    site_dedupe_1.name,
    site_dedupe_1.city,
    site_dedupe_1.state,
    site_dedupe_1.zip,
    site_dedupe_1.address,
    site_dedupe_1.country
from temp_all_site_dedupe_results_5 site_dedupe_1
left outer join (
select distinct
    'Rule_06' as rule_id,
    uid,
    uid as cluster_id,
    case when rnk = 1 then 0.80
             when rnk <>1 then 0.50
                 end as score
from (
select site_dedupe.cluster_id,
site_dedupe.name,
site_dedupe.country,
site_dedupe.uid,
site_dedupe.score,
rank() over (partition by site_dedupe.data_src_nm order by site_dedupe.score desc ) as rnk
from
(
select
    site_dedupe_results_1.cluster_id,
    site_dedupe_results_1.name,
    site_dedupe_results_1.country,
    site_dedupe_results_1.uid,
    site_dedupe_results_1.data_src_nm,
    site_dedupe_results_1.score
from  temp_all_site_dedupe_results_5  site_dedupe_results_1
inner join temp_all_site_dedupe_results_5 site_dedupe_results_2
on site_dedupe_results_1.cluster_id = site_dedupe_results_2.cluster_id
and site_dedupe_results_1.data_src_nm = site_dedupe_results_2.data_src_nm
where site_dedupe_results_1.data_src_nm in ('ctms','ir')) site_dedupe) where rnk=1 ) site_dedupe_2
on site_dedupe_1.uid = site_dedupe_2.uid
""")
temp_all_site_dedupe_results_6.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_6')
write_path = path.replace('table_name','temp_all_site_dedupe_results_6')
temp_all_site_dedupe_results_6.repartition(100).write.mode('overwrite').parquet(write_path)

# Integer cluster id
temp_site_clusterid = spark.sql("""
select
    cluster_id,
    row_number() over(order by null) as int_final_cluster_id
from temp_all_site_dedupe_results_6
where cluster_id is not null
group by 1
having count(*)>1
""")
temp_site_clusterid.registerTempTable('temp_site_clusterid')

temp_all_site_dedupe_results_final_0 = spark.sql("""
select /*+ broadcast(temp_site_clusterid) */
        rule_id,
    cast(temp_site_clusterid.int_final_cluster_id as string) as cluster_id,
    case when int_final_cluster_id is not null then site_dedupe_2.score else null end as score,
    site_dedupe_2.uid,
    site_dedupe_2.hash_uid,
    site_dedupe_2.data_src_nm,
    site_dedupe_2.ds_site_id,
    site_dedupe_2.name,
    site_dedupe_2.city,
    site_dedupe_2.state,
    site_dedupe_2.zip,
    site_dedupe_2.address,
    site_dedupe_2.country
from temp_all_site_dedupe_results_6 site_dedupe_2
left outer join temp_site_clusterid temp_site_clusterid
on site_dedupe_2.cluster_id = temp_site_clusterid.cluster_id
""")

temp_all_site_dedupe_results_final_0.write.mode('overwrite').saveAsTable(
    'temp_all_site_dedupe_results_final_0')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_final_0')
temp_all_site_dedupe_results_final_0.repartition(100).write.mode('overwrite').parquet(write_path)

final_table = KMValidate('temp_all_site_dedupe_results_final_0', 'site', '$$data_dt','$$cycle_id','$$s3_env')

temp_all_site_dedupe_results_final = spark.sql("""
select distinct
        rule_id,
        final_cluster_id as cluster_id,
    final_score as score,
    uid,
    hash_uid,
    data_src_nm,
    ds_site_id,
    name,
    city,
    state,
    zip,
    address,
    country,
    final_KM_comment as KM_comment
from """ + final_table + """
""")
temp_all_site_dedupe_results_final.registerTempTable('temp_all_site_dedupe_results_final')
spark.sql("""
    INSERT OVERWRITE TABLE
    sanofi_ctfo_datastore_app_commons_$$db_env.temp_all_site_dedupe_results_final PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_site_dedupe_results_final
    """)
# Push to s3

write_path = path.replace('table_name', 'temp_all_site_dedupe_results_final')
temp_all_site_dedupe_results_final.repartition(100).write.mode('overwrite').parquet(write_path)

#CommonUtils().copy_hdfs_to_s3('ctfo_internal_datastore_app_commons_$$db_env.temp_all_site_dedupe_results_final')

# csv file for km validation
temp_all_site_dedupe_results_final.repartition(1).write.mode('overwrite').format('csv').option(
    'header', 'true').option('delimiter', ',').option('quoteAll', 'true').save(
        's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/'
        'temp/km_validation/km_inputs/site/$$data_dt/')






