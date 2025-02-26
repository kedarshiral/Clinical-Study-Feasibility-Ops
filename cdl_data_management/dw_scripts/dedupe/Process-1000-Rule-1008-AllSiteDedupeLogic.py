import sys
from CommonUtils import CommonUtils
from DedupeUtility import KMValidate
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

sys.path.insert(1, CommonConstants.EMR_CODE_PATH + '/configs/job_executor')

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = bucket_path + '/applications/commons/temp/mastering/site/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

temp_all_site_match_score = spark.sql(
    """
    select * from temp_all_site_match_score
    """)
temp_all_site_match_score.registerTempTable('temp_all_site_match_score')
#write_path = path.replace('table_name', 'temp_all_site_match_score')
#temp_all_site_match_score.repartition(100).write.mode('overwrite').parquet(write_path)

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_match_score PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_site_match_score
    """)
# # Push to s3


#
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_match_score')

# Get the grain columns and dedupe output columns
temp_all_site_dedupe_results_combined = spark.sql("""
select
        site_score.cluster_id,
        site_score.score,
        site_prep.uid,
        site_prep.hash_uid,
        site_prep.data_src_nm,
        site_prep.ds_site_id,
        site_prep.name,
        site_prep.city,
        site_prep.zip,
        site_prep.address,
        site_prep.country
from temp_all_site_data_prep_combined site_prep
left  join
(select
    cluster_id,
    uid,
    score
from temp_all_site_match_score group by 1,2,3) site_score
on trim(site_prep.uid)=trim(site_score.uid)
""")

temp_all_site_dedupe_results_combined.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_combined')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_combined')
temp_all_site_dedupe_results_combined.repartition(100).write.mode('overwrite').parquet(write_path)

a=spark.sql(""" select a.*,b.cluster_id as final_cluster_id from temp_all_site_dedupe_results_aact_null a inner join 
(select * from temp_all_site_dedupe_results_combined where data_src_nm='aact') b on a.uid=b.uid """)
a.registerTempTable('a')

aact_merge=spark.sql(""" select a.final_cluster_id,b.* from  a left join temp_all_site_dedupe_results_aact_null b on a.cluster_id=b.cluster_id where a.cluster_id is not null
union
select a.final_cluster_id,b.* from  a left join temp_all_site_dedupe_results_aact_null b on a.uid=b.uid where a.cluster_id is null

""" )
aact_merge.registerTempTable('aact_merge')

aact_merge_final=spark.sql(""" select coalesce(final_cluster_id,final_cluster_id_null) as final_cluster_id,cluster_id,score,uid,hash_uid,data_src_nm,ds_site_id,name,city,zip,address,country from (
select *,case when final_cluster_id is null and cluster_id is not null then concat(cluster_id,'_new') end as final_cluster_id_null from aact_merge  ) """)

aact_merge_final.registerTempTable('aact_merge_final')

# -----------------------------------------------------------------
# for TASCAN
ta=spark.sql(""" select a.*,b.cluster_id as final_cluster_id from temp_all_site_dedupe_results_tascan_null a inner join 
(select * from temp_all_site_dedupe_results_combined where data_src_nm='tascan') b on a.uid=b.uid """)
ta.registerTempTable('ta')

tascan_merge=spark.sql(""" select ta.final_cluster_id,b.* from  ta left join temp_all_site_dedupe_results_tascan_null b on ta.cluster_id=b.cluster_id where ta.cluster_id is not null
union
select ta.final_cluster_id,b.* from ta left join temp_all_site_dedupe_results_tascan_null b on ta.uid=b.uid where ta.cluster_id is null

""" )
tascan_merge.registerTempTable('tascan_merge')

tascan_merge_final=spark.sql(""" select coalesce(final_cluster_id,final_cluster_id_null) as final_cluster_id,cluster_id,score,uid,hash_uid,data_src_nm,ds_site_id,name,city,zip,address,country from (
select *,case when final_cluster_id is null and cluster_id is not null then concat(cluster_id,'_new') end as final_cluster_id_null from tascan_merge  ) """)

tascan_merge_final.registerTempTable('tascan_merge_final')

# -----------------------------------------------------------------
temp_all_site_dedupe_final_temp=spark.sql(""" 
select final_cluster_id as cluster_id,score,uid,hash_uid,data_src_nm,ds_site_id,name,city,zip,address,country
from aact_merge_final
union
select final_cluster_id as cluster_id,score,uid,hash_uid,data_src_nm,ds_site_id,name,city,zip,address,country
from tascan_merge_final
union
select * from temp_all_site_dedupe_results_combined """)
temp_all_site_dedupe_final_temp.registerTempTable('temp_all_site_dedupe_final_temp')

temp_all_site_dedupe_results=spark.sql(""" select cluster_id,score,uid,hash_uid,data_src_nm,ds_site_id,name,city,zip,address,country from  (select  cluster_id,score,uid,hash_uid,data_src_nm,ds_site_id,name,city,zip,address,country, row_number() 
over(partition by uid order by score desc nulls last) as rnk from temp_all_site_dedupe_final_temp) where rnk=1 """)
temp_all_site_dedupe_results.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results')

temp_all_site_dedupe_results.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results')
temp_all_site_dedupe_results.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule to cluster recordscwith exact match on 15 letters of name, zip ,city, country
'''
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
        site_dedupe.zip,
        site_dedupe.address,
        site_dedupe.country
    from temp_all_site_dedupe_results  site_dedupe
    left outer join (
        select
            'Rule_01' as rule_id,
            uid,
            cluster_id,
            score from ( 
    select
        name,
        zip,
        city,
        country,
                size(collect_set(uid)) cnt,
        concat_ws("\;",collect_set(uid)) as new_uid ,
        concat_ws('_',coalesce(name,'na'),coalesce(city,'na'),coalesce(zip,'na'),coalesce(country,'na')) as cluster_id,
                0.80 as score from
    (select
        uid,cluster_id,
        substring(regexp_replace(trim(lower(name)),"[^0-9A-Za-z]",""), 1, 15) as name ,
            regexp_replace(lower(coalesce(zip,'NA')),"[^0-9]","")  as zip,
        regexp_replace(trim(lower(city)),"[^0-9A-Za-z]","") as city,
                    regexp_replace(trim(lower(country)),"[^0-9A-Za-z]","")  as country 
    from temp_all_site_dedupe_results group by 1,2,3,4,5,6)
    all_site_dedupe_results 
     group by 1,2,3,4 having cnt>1) site_dedupe_temp lateral view outer explode (split(new_uid,"\;"))one as uid)

    site_dedupe_1
    on trim(site_dedupe.uid) = trim(site_dedupe_1.uid)
""")
temp_all_site_dedupe_results_1.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_1')

write_path = path.replace('table_name', 'temp_all_site_dedupe_results_1')
temp_all_site_dedupe_results_1.repartition(100).write.mode('overwrite').parquet(write_path)
'''

# Applied a rule decluster the records having different zip in same cluster
temp_all_site_dedupe_results_2_temp = spark.sql("""
select
    site_dedupe_2.rule_id as rule_id,
    coalesce(site_dedupe_2.cluster_id, site_dedupe_1.cluster_id) as cluster_id,
    coalesce(site_dedupe_2.score, site_dedupe_1.score) as score,
    site_dedupe_1.uid,
    site_dedupe_1.hash_uid,
    site_dedupe_1.data_src_nm,
    site_dedupe_1.ds_site_id,
    site_dedupe_1.name,
    site_dedupe_1.city,
    site_dedupe_1.zip,
    site_dedupe_1.address,
    site_dedupe_1.country
from temp_all_site_dedupe_results site_dedupe_1
left outer join (
select distinct
    'Rule_02' as rule_id,
    uid,
    concat(country,'_',cluster_id,'_',zip) as cluster_id,
    0.8 as score
from(
select 
	site_dedupe_results_1.cluster_id,
	case when site_dedupe_results_1.zip like '0%' then REGEXP_REPLACE(site_dedupe_results_1.zip, '^0+', '') else site_dedupe_results_1.zip end as zip ,
	site_dedupe_results_1.country,
	site_dedupe_results_1.uid 
from temp_all_site_dedupe_results site_dedupe_results_1
inner join (select cluster_id, country,zip from  temp_all_site_dedupe_results group by 1,2,3) site_dedupe_results_2
on lower(trim(site_dedupe_results_1.cluster_id)) = lower(trim(site_dedupe_results_2.cluster_id))
and lower(trim(site_dedupe_results_1.country)) = lower(trim(site_dedupe_results_2.country))
and lower(trim(REGEXP_REPLACE(site_dedupe_results_1.zip, '^0+', ''))) <>lower(trim(REGEXP_REPLACE(site_dedupe_results_2.zip, '^0+', ''))))) site_dedupe_2
on lower(trim(site_dedupe_1.uid)) = lower(trim(site_dedupe_2.uid))
""")
temp_all_site_dedupe_results_2_temp.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_2_temp')
#write_path = path.replace('table_name', 'temp_all_site_dedupe_results_2')
#temp_all_site_dedupe_results_2.repartition(100).write.mode('overwrite').parquet(write_path)

cluster_zip_combination = spark.sql("""SELECT distinct tmp.cluster_id,b.uid,concat(b.country,'_',tmp.cluster_id,'_',REGEXP_REPLACE(tmp.zip, '^0+', '')) as final_cluster_id,0.8 as score,'Rule_02' as rule_id
    FROM (
        SELECT cluster_id, zip, COUNT(*) AS occurrence,
               ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC) AS row_num
        FROM temp_all_site_dedupe_results
        GROUP BY cluster_id, zip
    ) tmp inner join (select distinct country,cluster_id,uid from temp_all_site_dedupe_results where zip is null and cluster_id in 
(select cluster_id from temp_all_site_dedupe_results group by 1 having count(distinct REGEXP_REPLACE(zip, '^0+', ''))>1))  b on lower(trim(tmp.cluster_id))=lower(trim(b.cluster_id))
    WHERE row_num = 1 """)

cluster_zip_combination.write.mode('overwrite').saveAsTable('cluster_zip_combination')

temp_all_site_dedupe_results_2=spark.sql(""" 
select coalesce(b.rule_id,a.rule_id) as rule_id,
    coalesce(b.final_cluster_id,a.cluster_id) as cluster_id,
    coalesce(b.score,a.score) as score,
    a.uid,
    a.hash_uid,
    a.data_src_nm,
    a.ds_site_id,
    a.name,
    a.city,
    a.zip,
    a.address,
    a.country from temp_all_site_dedupe_results_2_temp a left join cluster_zip_combination b on
    lower(trim(a.cluster_id))= lower(trim(b.cluster_id)) and lower(trim(a.uid))= lower(trim(b.uid))""")


temp_all_site_dedupe_results_2.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_2')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_2')
temp_all_site_dedupe_results_2.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule decluster the records having different country in same cluster
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
    site_dedupe_1.zip,
    site_dedupe_1.address,
    site_dedupe_1.country
from temp_all_site_dedupe_results_2 site_dedupe_1
left outer join (
select distinct
    'Rule_03' as rule_id,
    uid,
    concat(country,'_',cluster_id) as cluster_id,
    0.8 as score
from(
select 
	site_dedupe_results_1.cluster_id,
	site_dedupe_results_1.name,
	site_dedupe_results_1.country,
	site_dedupe_results_1.uid 
from temp_all_site_dedupe_results_2 site_dedupe_results_1
inner join (select cluster_id, country from  temp_all_site_dedupe_results_2 group by 1,2) site_dedupe_results_2
on lower(trim(site_dedupe_results_1.cluster_id)) = lower(trim(site_dedupe_results_2.cluster_id))
and lower(trim(site_dedupe_results_1.country)) <> lower(trim(site_dedupe_results_2.country)))) site_dedupe_2
on lower(trim(site_dedupe_1.uid)) = lower(trim(site_dedupe_2.uid))
""")
temp_all_site_dedupe_results_3.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_3')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_3')
temp_all_site_dedupe_results_3.repartition(100).write.mode('overwrite').parquet(write_path)


temp = spark.sql(
    """select *, hash_uid as new_cluster_id from temp_all_site_dedupe_results_3 where cluster_id is null""")
temp.registerTempTable("temp")

temp_site_1 = spark.sql("""
select distinct 
a.rule_id,
coalesce(temp.new_cluster_id,a.cluster_id) as cluster_id,
a.score,
a.uid,
a.hash_uid,
a.data_src_nm,
a.ds_site_id,
a.name,
a.city,
a.zip,
a.address,
a.country
from temp_all_site_dedupe_results_3 a left join temp on lower(trim(a.uid))=lower(trim(temp.uid)) 
where round(cast(coalesce(a.score,0.0) as double),2) < 0.80 or a.score is null""")
temp_site_1.registerTempTable('temp_site_1')

temp_all_site_dedupe_results_4_int = spark.sql("""select
rule_id,
cluster_id,
score,
uid,
hash_uid,
split(datasource_site_id,"_")[0] as data_src_nm,
datasource_site_id as ds_site_id,
name,
city,
zip,
address,
country
from  temp_site_1  
lateral view outer explode (split(ds_site_id,"\;")) as datasource_site_id""")
# temp_all_site_dedupe_results_4_int.createOrReplaceTempView('temp_all_site_dedupe_results_4_int')
temp_all_site_dedupe_results_4_int.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_4_int')

# collecting the sites which are clustered in dataprep

temp_all_site_dedupe_results_3ex_int = spark.sql("""select 
A.rule_id,
A.cluster_id,
A.score,
A.uid,
A.hash_uid,
A.data_src_nm,
A.ds_site_id,
A.name,
A.city,
A.zip,
A.address,
A.country,
B.ds_site_id as prep_ds_site_id,B.data_src_nm as prep_data_src_nm from 
(select
rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country
from temp_all_site_dedupe_results_4_int ) A
left join 
(select distinct  uid, ds_site_id,data_src_nm from  default.temp_all_site_dedupe_results)
 B on trim(B.uid)= trim(A.uid) 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
""")
# temp_all_site_dedupe_results_3ex_int.createOrReplaceTempView('temp_all_site_dedupe_results_3ex_int')
temp_all_site_dedupe_results_3ex_int.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_3ex_int')

# collecting the records which are clustered in dataprep
temp_all_site_dedupe_results_3ex_int1 = spark.sql("""select rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country, 
prep_ds_site_id, 
dense_rank() over(partition by  cluster_id order by uid) as rnk  from temp_all_site_dedupe_results_3ex_int where prep_data_src_nm like '%;%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13""")
# temp_all_site_dedupe_results_3ex_int1.registerTempTable('temp_all_site_dedupe_results_3ex_int1')
temp_all_site_dedupe_results_3ex_int1.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_3ex_int1')
temp_all_site_dedupe_results_3ex_int1.repartition(100).write.mode('overwrite').parquet(write_path)


temp_all_site_dedupe_results_3ex_int2 = spark.sql("""select a.rule_id,
a.cluster_id,
a.score,
a.uid,
a.hash_uid,
a.data_src_nm,
a.ds_site_id,
a.name,
a.city,
a.zip,
a.address,
a.country, 
a.prep_ds_site_id, 
dense_rank() over(partition by  a.cluster_id, a.data_src_nm order by a.uid) as rnk  from temp_all_site_dedupe_results_3ex_int a left join temp_all_site_dedupe_results_3ex_int1 b
on lower(trim(a.cluster_id))=lower(trim(b.cluster_id))
where b.cluster_id is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13""")
# temp_all_site_dedupe_results_3ex_int1.registerTempTable('temp_all_site_dedupe_results_3ex_int1')
temp_all_site_dedupe_results_3ex_int2.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_3ex_int2')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_3ex_int2')
temp_all_site_dedupe_results_3ex_int2.repartition(100).write.mode('overwrite').parquet(write_path)


temp_2_site = spark.sql("""select ds_site_id from temp_all_site_dedupe_results_3ex_int 
minus
(select ds_site_id from temp_all_site_dedupe_results_3ex_int1
union
select ds_site_id from temp_all_site_dedupe_results_3ex_int2)
""")
temp_2_site.registerTempTable("temp_2_site")

# union records which are clustered and not clustered
temp_all_site_dedupe_results_3ex_int3 = spark.sql(""" select * from temp_all_site_dedupe_results_3ex_int1
union
select A.rule_id,
A.cluster_id,
A.score,
A.uid,
A.hash_uid,
A.data_src_nm,
A.ds_site_id,
A.name,
A.city,
A.zip,
A.address,
A.country, 
A.prep_ds_site_id, 
null as rnk from temp_all_site_dedupe_results_3ex_int A
where ds_site_id in (select ds_site_id from temp_2_site)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
""")
# temp_all_site_dedupe_results_3ex_int3.registerTempTable('temp_all_site_dedupe_results_3ex_int3')
temp_all_site_dedupe_results_3ex_int3.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_3ex_int3')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_3ex_int3')
temp_all_site_dedupe_results_3ex_int3.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_site_dedupe_results_6 = spark.sql("""
select 'Rule_06' as rule_id,
case when rnk =1 or (data_Src_nm = 'aact' and rnk is null)  then cluster_id
     when rnk<>1  then concat_ws('|',cluster_id,rnk) 
     when  (data_Src_nm <> 'aact' and rnk is null) then concat_ws('|',cluster_id,hash_uid) end as cluster_id,
case when  rnk =1 or (data_Src_nm = 'aact' and rnk is null)  then 0.95
when rnk<>1  then 0.95 
when (data_Src_nm <> 'aact' and rnk is null) then null end as score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country from temp_all_site_dedupe_results_3ex_int3
union
select rule_id,
case when rnk =1 or trim(data_src_nm) = 'aact'  then cluster_id
when rnk<>1 and trim(data_src_nm) != 'aact'  then concat_ws('|',cluster_id,rnk)  end as cluster_id,
case when  rnk =1 or trim(data_src_nm) = 'aact'  then 0.95
when rnk<>1 and trim(data_src_nm) != 'aact'  then null end as score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country from temp_all_site_dedupe_results_3ex_int2
group by 1,2,3,4,5,6,7,8,9,10,11,12
union
select rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country
from temp_all_site_dedupe_results_3
where round(cast(coalesce(score,0.0) as double),2) >= 0.80""")
# temp_all_site_dedupe_results_6.registerTempTable('temp_all_site_dedupe_results_6')
temp_all_site_dedupe_results_6.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_6')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_6')
temp_all_site_dedupe_results_6.repartition(100).write.mode('overwrite').parquet(write_path)


#same uid for multiple records, assign 0.95 score to the records
temp_common_uid = spark.sql("""
select distinct uid from 
(select uid, count(*) from temp_all_site_dedupe_results_6 where score is null  group by 1 having  count(*)>1 )""")
temp_common_uid.registerTempTable('temp_common_uid')


temp_all_site_dedupe_results_7_temp = spark.sql("""select rule_id, cluster_id, 0.95 as score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country from temp_all_site_dedupe_results_6 where uid in (select * from temp_common_uid) """)
temp_all_site_dedupe_results_7_temp.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_7_temp')


temp_all_site_dedupe_results_7_ex = spark.sql("""select * from temp_all_site_dedupe_results_6 where uid not in (select * from temp_common_uid) union
select * from temp_all_site_dedupe_results_7_temp  """)
temp_all_site_dedupe_results_7_ex.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_7_ex')

temp_hash=spark.sql("""select hash_uid from temp_all_site_dedupe_results_7_ex group by 1 having count(*)>1""")
temp_hash.registerTempTable("temp_hash")

temp_all_site_dedupe_results_7=spark.sql("""select rule_id,
cluster_id,
0.90 as score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country
from temp_all_site_dedupe_results_7_ex where hash_uid in (select hash_uid from temp_hash)
union
select rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_site_id,
name,
city,
zip,
address,
country
from temp_all_site_dedupe_results_7_ex where hash_uid not in (select hash_uid from temp_hash)""")
temp_all_site_dedupe_results_7.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_7')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_7')
temp_all_site_dedupe_results_7.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_site_uni = spark.sql("""
select 
b.rule_id,
b.cluster_id,
b.score,
b.uid,
b.hash_uid,
b.data_src_nm,
b.ds_site_id,
b.name,
b.city,
b.zip,
b.address,
b.country from
(select 
a.rule_id,
a.cluster_id,
a.score,
a.uid,
a.hash_uid,
a.data_src_nm,
a.ds_site_id,
a.name,
a.city,
a.zip,
a.address,
a.country, row_number() over (partition by ds_site_id order by score desc nulls last, cluster_id desc nulls last, uid desc nulls last, hash_uid desc nulls last) as rnk1
from (select
*
from temp_all_site_dedupe_results_7 temp) a ) b where rnk1=1
""")
temp_all_site_uni = temp_all_site_uni.dropDuplicates()
temp_all_site_uni.write.mode('overwrite').saveAsTable('temp_all_site_uni')
write_path = path.replace('table_name', 'temp_all_site_uni')
temp_all_site_uni.repartition(100).write.mode('overwrite').parquet(write_path)

# Integer cluster id
temp_site_clusterid = spark.sql("""
select
    cluster_id,
    row_number() over(order by null) as int_final_cluster_id
from temp_all_site_uni
where cluster_id is not null
group by 1
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
    site_dedupe_2.zip,
    site_dedupe_2.address,
    site_dedupe_2.country
    from temp_all_site_uni site_dedupe_2
    left outer join temp_site_clusterid temp_site_clusterid
    on lower(trim(site_dedupe_2.cluster_id)) = lower(trim(temp_site_clusterid.cluster_id))
""")

temp_all_site_dedupe_results_final_0.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_final_0')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_final_0')
temp_all_site_dedupe_results_final_0.repartition(100).write.mode('overwrite').parquet(write_path)


temp_all_site_dedupe_results_final = spark.sql("""
select distinct
    rule_id,
    cluster_id,
    score,
    uid,
    hash_uid,
    data_src_nm,
    ds_site_id,
    name,
    city,
    zip,
    address,
    country,cast(Null as string) as KM_comment
from temp_all_site_dedupe_results_final_0
""")
temp_all_site_dedupe_results_final.registerTempTable('temp_all_site_dedupe_results_final')
temp_all_site_dedupe_results_final.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_final')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_final')

#write csv
csv_path = bucket_path + '/applications/commons/temp/km_validation/Dedupe_output/site/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
csv_write_path = csv_path.replace('table_name', 'temp_all_site_dedupe_results_final')
temp_all_site_dedupe_results_final.repartition(1).write.mode('overwrite').format('csv').option(
    'header', 'true').option('delimiter', ',').option('quoteAll', 'true').save(csv_write_path)


spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_dedupe_results_final PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_site_dedupe_results_final
    """)
# # Push to s3


#
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_dedupe_results_final')

# csv file for km validation
#temp_all_site_dedupe_results_final.repartition(1).write.mode('overwrite').format('csv').option(
#    'header', 'true').option('delimiter', ',').option('quoteAll', 'true').save(
#    '{bucket_path}/applications/commons/'
#    'temp/km_validation/km_inputs/site/$$data_dt/'.format(bucket_path=bucket_path))
