################################# Module Information ######################################
#  Module Name         : Trial Post dedupe logic
#  Purpose             : This will execute business rules on dedupe output data
#  Pre-requisites      : Source table required: temp_all_trial_match_score, temp_trial_data_common_nct_id_result
#  Last changed on     : 10-04-2023
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Dedupe data results stores on S3
# 2. Apply busniess rules and prepare data for KM
############################################################################################
import sys
from CommonUtils import CommonUtils

import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

sys.path.insert(1, CommonConstants.EMR_CODE_PATH + '/configs/job_executor')
from DedupeUtility import KMValidate

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = bucket_path + '/applications/commons/temp/mastering/trial/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

temp_all_trial_match_score = spark.sql("""
select * from temp_all_trial_match_score
""")
temp_all_trial_match_score.registerTempTable('temp_all_trial_match_score')
# write_path = path.replace('table_name', 'temp_all_trial_match_score')
# temp_all_trial_match_score.repartition(100).write.mode('overwrite').parquet(write_path)

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_match_score PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_trial_match_score
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_match_score')

# Fetching dedupe grain columns against dedupe output
temp_all_trial_dedupe_results = spark.sql("""
select
dedupe_res.cluster_id,
dedupe_res.score,
data_prep.uid,
data_prep.hash_uid,
data_prep.data_src_nm,
data_prep.ds_trial_id,
data_prep.trial_title,
data_prep.trial_phase,
data_prep.trial_drug,
data_prep.trial_indication
from temp_all_trial_data_prep data_prep
left outer join temp_all_trial_match_score dedupe_res
on lower(trim(data_prep.uid)) = lower(trim(dedupe_res.uid))
group by 1,2,3,4,5,6,7,8,9,10
""")
# temp_all_trial_dedupe_results.registerTempTable('temp_all_trial_dedupe_results')


temp_all_trial_dedupe_results.write.mode('overwrite').saveAsTable('temp_all_trial_dedupe_results')

# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results')
temp_all_trial_dedupe_results.repartition(100).write.mode('overwrite').parquet(write_path)

# Pulled in NCT ID from temp_all_trial_data_cleaned_obs_remove
data_prep_nct_id = spark.sql("""
SELECT
    a.cluster_id, a.score, a.uid, a.hash_uid, 
	CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(NULLIF(TRIM(exploded_data_src_nm), '')), true)) AS data_src_nm,
	a.trial_title,a.trial_phase, a.trial_drug, a.trial_indication,
	CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(NULLIF(TRIM(exploded_trial_id_raw), '')), true)) AS ds_trial_id,
    case when lower(b.protocol_ids) like 'nct%' then b.protocol_ids else 'None' end as nct_id
FROM
         (SELECT
              a.*,
              exploded1.exploded_trial_id_raw, SPLIT(exploded1.exploded_trial_id_raw, '_')[0] AS exploded_data_src_nm
          FROM
              temp_all_trial_dedupe_results a
          LATERAL VIEW OUTER EXPLODE(SPLIT(a.ds_trial_id, ';')) exploded1 AS exploded_trial_id_raw
         ) a

LEFT JOIN
    (select * from temp_all_trial_data_cleaned_obs_remove where lower(trim(protocol_ids)) like 'nct%') b
ON
    a.exploded_trial_id_raw = b.src_trial_id
group by 1,2,3,4,6,7,8,9,11
""")
data_prep_nct_id.registerTempTable('data_prep_nct_id')

# Logic for declustering records having length(title)<= 10
temp_all_trial_dedupe_results_1 = spark.sql("""
select
rule_df.rule_id,
coalesce(rule_df.cluster_id, base.cluster_id)  as cluster_id,
coalesce(rule_df.score, base.score) as score,
base.uid,
base.hash_uid,
base.data_src_nm,
base.ds_trial_id,
base.trial_title,
base.trial_phase,
base.trial_drug,
base.trial_indication,base.nct_id
from data_prep_nct_id base
left outer join
(
select
'Rule_01' as rule_id,
'unclustered_id' as cluster_id,
0 as score,
uid,
hash_uid,
data_src_nm,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication
from data_prep_nct_id where length(trial_title) <= 10 ) rule_df
on lower(trim(base.uid)) = lower(trim(rule_df.uid))
""")
temp_all_trial_dedupe_results_1.registerTempTable('temp_all_trial_dedupe_results_1')

write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_1')
temp_all_trial_dedupe_results_1.repartition(100).write.mode('overwrite').parquet(write_path)


# Logic to apply nct_id as cluster_id so that same nct_id records can be clustered together
nct_as_cluster_id = spark.sql("""select a.*,nct_id as test_clusterid from temp_all_trial_dedupe_results_1 a""")
nct_as_cluster_id.registerTempTable('nct_as_cluster_id')

# Case1: Assigned nct_id as cluster id where nct_id is not null.
# Case2: In case of null nct_id, assigned nct_id on the basis of same hash_uid and cluster_id combination (precedence by score).
# Case3: In case of no match found for hash_uid and cluster_id combination then assign nct_id of same cluster_id record.

assign_nct_id1 = spark.sql("""select case when a.test_clusterid!='None' or b.test_clusterid is not null or c.test_clusterid is not null then 'Rule_2' else a.rule_id end as rule_id,
a.cluster_id,
case when a.test_clusterid!='None' or b.test_clusterid is not null or c.test_clusterid is not null then 0.9 else a.score end as score,
uid,
a.hash_uid,
data_src_nm,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
nct_id,
case when a.test_clusterid!='None' then a.test_clusterid when b.test_clusterid is not null then b.test_clusterid when c.test_clusterid is not null then c.test_clusterid else a.cluster_id
end as test_clusterid 
from nct_as_cluster_id a

left outer join 

(select a.cluster_id,a.hash_uid,a.test_clusterid from 
(select cluster_id,hash_uid,test_clusterid,row_number() over (partition by cluster_id,hash_uid order by score desc nulls last) as rnk from nct_as_cluster_id where test_clusterid!='None') a
where a.rnk=1) b
on lower(trim(a.cluster_id))=lower(trim(b.cluster_id)) and lower(trim(a.hash_uid))=lower(trim(b.hash_uid))

left outer join

(select b.cluster_id,b.test_clusterid from 
(select cluster_id,test_clusterid,row_number() over (partition by cluster_id order by score desc nulls last) as rnk from nct_as_cluster_id where test_clusterid!='None') b
where b.rnk=1) c
on lower(trim(a.cluster_id))=lower(trim(c.cluster_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13""")
# test201.registerTempTable('test201')
assign_nct_id1.write.mode('overwrite').saveAsTable('assign_nct_id1')

assign_nct_id_temp= spark.sql("""select hash_uid, test_clusterid from (select hash_uid,test_clusterid,row_number() over 
(partition by hash_uid order by test_clusterid desc nulls last) as rnk from  assign_nct_id1) a where a.rnk=1""")
assign_nct_id_temp.write.mode('overwrite').saveAsTable('assign_nct_id_temp')

assign_nct_id1_final = spark.sql("""select coalesce(a.rule_id, b.rule_id) as rule_id,cluster_id,coalesce(a.score,b.score) as score,uid, a.hash_uid, 
data_src_nm,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
nct_id,
coalesce(a.test_clusterid,b.test_clusterid) as test_clusterid  from assign_nct_id1 a
 left join (select test.*,'Rule_2' as rule_id, 0.9 as score from assign_nct_id_temp test where test_clusterid is not null) b
 on a.hash_uid=b.hash_uid""")
assign_nct_id1_final.write.mode('overwrite').saveAsTable('assign_nct_id1_final')

#Exploading ds_trial_id
exploaded_id = spark.sql("""
select
a.*,exp_ds_trial_id
from
assign_nct_id1_final a
lateral view outer explode(split(ds_trial_id,'\\\;'))one as exp_ds_trial_id
""")
exploaded_id.registerTempTable('exploaded_id')


# Handled multiple nct_id on one src_trial_id issue by assigning random nct_id for non aact trials and src_trial_id for aact trial.
assign_nct_id2 = spark.sql(""" select 
a.exp_ds_trial_id,a.test_clusterid from 
(select exp_ds_trial_id,test_clusterid,
row_number() over (partition by exp_ds_trial_id order by score desc nulls last) as rnk 
from exploaded_id where test_clusterid!='None' and exp_ds_trial_id not like '%aact%') a where rnk=1
union
select distinct exp_ds_trial_id,SPLIT(exp_ds_trial_id, '_')[1] AS test_clusterid from exploaded_id  
where test_clusterid!='None' and exp_ds_trial_id like '%aact%'

""")
assign_nct_id2.write.mode('overwrite').saveAsTable('assign_nct_id2')

# Applied coalesce to fetch nct_id
assign_nct_id3 = spark.sql("""select rule_id,
case when coalesce(b.test_clusterid,a.test_clusterid)='None' then null else coalesce(b.test_clusterid,a.test_clusterid) end as test_cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_trial_id,
a.exp_ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
nct_id  from exploaded_id a
left join assign_nct_id2 b on a.exp_ds_trial_id=b.exp_ds_trial_id
""")
assign_nct_id3 = assign_nct_id3.dropDuplicates()
assign_nct_id3.registerTempTable('assign_nct_id3')

#Declustering
assign_nct_id4 = spark.sql("""
WITH nct_agg AS (
    SELECT
        test_cluster_id,
        hash_uid,
        COLLECT_SET(nct_id) AS nct_ids
    FROM assign_nct_id3
    GROUP BY test_cluster_id, hash_uid
),
nct_none_check AS (
    SELECT
        test_cluster_id,
        hash_uid,
        nct_ids,
        CASE WHEN size(nct_ids) = 1 AND nct_ids[0] = 'None' THEN true ELSE false END AS all_none
    FROM nct_agg
),
hash_uid_count AS (
    SELECT
        test_cluster_id,
        COUNT(DISTINCT hash_uid) AS distinct_hash_uid_count
    FROM assign_nct_id3
    GROUP BY test_cluster_id
),
joined AS (
    SELECT
        t.*,
        nct.all_none,
        hc.distinct_hash_uid_count
    FROM assign_nct_id3 t
    LEFT JOIN nct_none_check nct
    ON t.test_cluster_id = nct.test_cluster_id AND t.hash_uid = nct.hash_uid
    LEFT JOIN hash_uid_count hc
    ON t.test_cluster_id = hc.test_cluster_id
)
SELECT
    rule_id,
    CASE
        WHEN (distinct_hash_uid_count > 1 AND all_none=true) THEN NULL
        ELSE test_cluster_id
    END AS final_cluster_id,
    score,
    uid,
    hash_uid,
    data_src_nm,
    ds_trial_id,
    exp_ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication,
    nct_id
FROM joined
""")
assign_nct_id4.write.mode('overwrite').saveAsTable('assign_nct_id4')

assign_nct_id4.registerTempTable('assign_nct_id4')

# concatinating ds_trial_id and selecting only required columns
assign_nct_id_final = spark.sql("""
SELECT 
    rule_id, 
    final_cluster_id as cluster_id, 
    score,
    uid, 
    hash_uid, 
    data_src_nm, 
    CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(NULLIF(TRIM(exp_ds_trial_id), '')), true)) AS ds_trial_id,
    trial_title, 
    trial_phase, 
    trial_drug, 
    trial_indication
FROM 
    assign_nct_id4
GROUP BY 1,2,3,4,5,6,8,9,10,11
""")
assign_nct_id_final.write.mode('overwrite').saveAsTable('assign_nct_id_final')


# Assigning intger id to cluster id
temp_trial_cluterid = spark.sql("""
select
lower(trim(cluster_id)) as cluster_id,
row_number() over(order by null) as int_final_cluster_id
from assign_nct_id_final
where cluster_id is not null
group by lower(trim(cluster_id))
""")
# temp_trial_cluterid.registerTempTable('temp_trial_cluterid')
temp_trial_cluterid.write.mode('overwrite').saveAsTable('temp_trial_cluterid')

temp_all_trial_dedupe_results_2 = spark.sql("""
select /*+ broadcast(b) */
rule_id,
cast(b.int_final_cluster_id as string) as cluster_id,
case when int_final_cluster_id is not null then results_1.score else null end as score,
results_1.uid,
results_1.hash_uid,
results_1.data_src_nm,
results_1.ds_trial_id,
results_1.trial_title,
results_1.trial_phase,
results_1.trial_drug,
results_1.trial_indication
from assign_nct_id_final results_1
left outer join temp_trial_cluterid b
on lower(trim(results_1.cluster_id)) = lower(trim(b.cluster_id))
""")

temp_all_trial_dedupe_results_2.write.mode('overwrite') \
    .saveAsTable('temp_all_trial_dedupe_results_2')
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_2')
temp_all_trial_dedupe_results_2.repartition(100).write.mode('overwrite').parquet(write_path)

# Giving cluster id to null cluster i=]\

# dedupe_results_src_exp = spark.sql("""
# select
# hash_uid,
# cluster_id,
# score,
# exp_data_src
# from
# temp_all_trial_dedupe_results_2
# lateral view outer explode(split(data_src_nm,'\\\;'))one as exp_data_src
# where cluster_id is not null
# """)
##dedupe_results_src_exp.registerTempTable("dedupe_results_src_exp")
# dedupe_results_src_exp.write.mode('overwrite').saveAsTable('dedupe_results_src_exp')
#
# temp_trial=spark.sql("""
# select *, hash_uid as new_cluster_id from temp_all_trial_dedupe_results_2 where cluster_id is null
# """)
# temp_trial.registerTempTable("temp_trial")
#
# temp_trial_1=spark.sql("""
# select distinct
# a.rule_id,
# coalesce(temp.new_cluster_id,a.cluster_id) as cluster_id,
# a.score,
# a.uid,
# a.hash_uid,
# a.data_src_nm,
# a.ds_trial_id,
# a.trial_title,
# a.trial_phase,
# a.trial_drug,
# a.trial_indication
# from temp_all_trial_dedupe_results_2 a left join temp_trial temp on lower(trim(a.uid))=lower(trim(temp.uid))
# where round(cast(coalesce(a.score,0.0) as double),2) < 0.80 or a.score is null
# """)
##temp_trial_1.registerTempTable("temp_trial_1")
# temp_trial_1.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_trial_1')
#


# temp_all_trial_dedupe_results_4_int=spark.sql("""select
# rule_id,
# cluster_id,
# score,
# uid,
# hash_uid,
# split(datasource_trial_id,"_")[0] as data_src_nm,
# datasource_trial_id as ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication
# from  temp_trial_1
# lateral view outer explode (split(ds_trial_id,"\;")) as datasource_trial_id""")
##temp_all_trial_dedupe_results_4_int.createOrReplaceTempView('temp_all_trial_dedupe_results_4_int')
# temp_all_trial_dedupe_results_4_int.write.mode('overwrite').saveAsTable('temp_all_trial_dedupe_results_4_int')
#
##collecting the trial which are clustered in dataprep
#
# temp_all_trial_dedupe_results_3ex_int=spark.sql("""select
# A.rule_id,
# A.cluster_id,
# A.score,
# A.uid,
# A.hash_uid,
# A.data_src_nm,
# A.ds_trial_id,
# A.trial_title,
# A.trial_phase,
# A.trial_drug,
# A.trial_indication,B.ds_trial_id as prep_ds_trial_id,B.data_src_nm as prep_data_src_nm from
# (select
# rule_id,
# cluster_id,
# score,
# uid,
# hash_uid,
# data_src_nm,
# ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication
# from temp_all_trial_dedupe_results_4_int ) A
# left join
# (select distinct  uid, ds_trial_id, data_src_nm from  temp_all_trial_data_prep)
# B on trim(B.uid)= trim(A.uid)
# group by 1,2,3,4,5,6,7,8,9,10,11,12,13
# """)
##temp_all_trial_dedupe_results_3ex_int.createOrReplaceTempView('temp_all_trial_dedupe_results_3ex_int')
# temp_all_trial_dedupe_results_3ex_int.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_trial_dedupe_results_3ex_int')
#
##collecting the records which are clustered in dataprep
# temp_all_trial_dedupe_results_3ex_int1 = spark.sql("""select rule_id,
# cluster_id,
# score,
# uid,
# hash_uid,
# data_src_nm,
# ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication, prep_ds_trial_id, dense_rank() over(partition by  cluster_id order by uid) as rnk  from temp_all_trial_dedupe_results_3ex_int where prep_data_src_nm like '%;%'
# group by 1,2,3,4,5,6,7,8,9,10,11,12""")
##temp_all_trial_dedupe_results_3ex_int1.registerTempTable('temp_all_trial_dedupe_results_3ex_int1')
# temp_all_trial_dedupe_results_3ex_int1.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_trial_dedupe_results_3ex_int1')
#
# temp_all_trial_dedupe_results_3ex_int2 = spark.sql("""select a.rule_id,
# a.cluster_id,
# a.score,
# a.uid,
# a.hash_uid,
# a.data_src_nm,
# a.ds_trial_id,
# a.trial_title,
# a.trial_phase,
# a.trial_drug,
# a.trial_indication, a.prep_ds_trial_id, dense_rank() over(partition by  a.cluster_id, a.data_src_nm order by a.uid) as rnk  from temp_all_trial_dedupe_results_3ex_int a left join temp_all_trial_dedupe_results_3ex_int1 b
# on lower(trim(a.cluster_id))=lower(trim(b.cluster_id))
# where b.cluster_id is null
# group by 1,2,3,4,5,6,7,8,9,10,11,12""")
##temp_all_trial_dedupe_results_3ex_int2.registerTempTable('temp_all_trial_dedupe_results_3ex_int2')
# temp_all_trial_dedupe_results_3ex_int2.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_trial_dedupe_results_3ex_int2')
#
# temp_2_trial=spark.sql("""select ds_trial_id from temp_all_trial_dedupe_results_3ex_int
# minus
# (select ds_trial_id from temp_all_trial_dedupe_results_3ex_int1
# union
# select ds_trial_id from temp_all_trial_dedupe_results_3ex_int2)
# """)
# temp_2_trial.registerTempTable("temp_2_trial")
#
##union records which are clustered and not clustered
# temp_all_trial_dedupe_results_3ex_int3 = spark.sql(""" select * from temp_all_trial_dedupe_results_3ex_int1
# union
# select A.rule_id,
# A.cluster_id,
# A.score,
# A.uid,
# A.hash_uid,
# A.data_src_nm,
# A.ds_trial_id,
# A.trial_title,
# A.trial_phase,
# A.trial_drug,
# A.trial_indication, A.prep_ds_trial_id, null as rnk from temp_all_trial_dedupe_results_3ex_int A where ds_trial_id in (select ds_trial_id from temp_2_trial)
# group by 1,2,3,4,5,6,7,8,9,10,11,12
# """)
##temp_all_trial_dedupe_results_3ex_int3.registerTempTable('temp_all_trial_dedupe_results_3ex_int3')
# temp_all_trial_dedupe_results_3ex_int3.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_trial_dedupe_results_3ex_int3')
#
# temp_all_trial_dedupe_results_3_ex = spark.sql("""
# select rule_id,
# case when rnk =1 or (data_Src_nm = 'aact' and rnk is null)  then cluster_id
#     when rnk<>1  then concat_ws('|',cluster_id,rnk)
#     when  (data_Src_nm <> 'aact' and rnk is null) then concat_ws('|',cluster_id,hash_uid) end as cluster_id,
# case when  rnk =1 or (data_Src_nm = 'aact' and rnk is null)  then 0.95
# when rnk<>1  then 0.95
# when (data_Src_nm <> 'aact' and rnk is null) then null end as score,
# uid,
# hash_uid,
# data_src_nm,
# ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication from temp_all_trial_dedupe_results_3ex_int3
# group by 1,2,3,4,5,6,7,8,9,10,11
# union
# select rule_id,
# case when rnk =1 or trim(data_src_nm) = 'aact'  then cluster_id
# when rnk<>1 and trim(data_src_nm) != 'aact'  then concat_ws('|',cluster_id,rnk)  end as cluster_id,
# case when  rnk =1 or trim(data_src_nm) = 'aact'  then 0.95
# when rnk<>1 and trim(data_src_nm) != 'aact'  then null end as score,
# uid,
# hash_uid,
# data_src_nm,
# ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication from temp_all_trial_dedupe_results_3ex_int2
# union
# select rule_id,
# cluster_id,
# score,
# uid,
# hash_uid,
# data_src_nm,
# ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication from temp_all_trial_dedupe_results_2
# where round(cast(coalesce(score,0.0) as double),2) >= 0.80
# """)
# temp_all_trial_dedupe_results_3_ex.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_trial_dedupe_results_3_ex')
#
# temp_hash=spark.sql("""select hash_uid from temp_all_trial_dedupe_results_3_ex group by 1 having count(*)>1""")
# temp_hash.registerTempTable("temp_hash")
#
# temp_all_trial_dedupe_results_3=spark.sql("""select rule_id,
# cluster_id,
# 0.90 as score,
# uid,
# hash_uid,
# data_src_nm,
# ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication
# from temp_all_trial_dedupe_results_3_ex where hash_uid in (select hash_uid from temp_hash)
# union
# select rule_id,
# cluster_id,
# score,
# uid,
# hash_uid,
# data_src_nm,
# ds_trial_id,
# trial_title,
# trial_phase,
# trial_drug,
# trial_indication
# from temp_all_trial_dedupe_results_3_ex where hash_uid not in (select hash_uid from temp_hash)""")
##temp_all_trial_dedupe_results_3.registerTempTable('temp_all_trial_dedupe_results_3')
# temp_all_trial_dedupe_results_3.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_trial_dedupe_results_3')
# write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_3')
# temp_all_trial_dedupe_results_3.repartition(100).write.mode('overwrite').parquet(write_path)
#
#
# temp_all_trial_uni = spark.sql("""
# select
# b.rule_id,
# b.cluster_id,
# b.score,
# b.uid,
# b.hash_uid,
# b.data_src_nm,
# b.ds_trial_id,
# b.trial_title,
# b.trial_phase,
# b.trial_drug,
# b.trial_indication from
# (select
# a.rule_id,
# a.cluster_id,
# a.score,
# a.uid,
# a.hash_uid,
# a.data_src_nm,
# a.ds_trial_id,
# a.trial_title,
# a.trial_phase,
# a.trial_drug,
# a.trial_indication, row_number() over (partition by ds_trial_id order by score desc nulls last, cluster_id desc nulls last, uid desc nulls last, hash_uid desc nulls last) as rnk1
# from (select
# *
# from temp_all_trial_dedupe_results_3 temp) a ) b where rnk1=1
# """)
# temp_all_trial_uni.dropDuplicates()
# temp_all_trial_uni.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_trial_uni')
# write_path = path.replace('table_name', 'temp_all_trial_uni')
# temp_all_trial_uni.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_trial_dedupe_results_final = spark.sql("""
select distinct
rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
cast(null as string) as KM_comment
from
temp_all_trial_dedupe_results_2
""")
temp_all_trial_dedupe_results_final.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(
    'temp_all_trial_dedupe_results_final')
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_final')
# temp_all_trial_dedupe_results_union_final.repartition(100).write.mode('overwrite').parquet(write_path)

# write csv
csv_path = bucket_path + '/applications/commons/temp/km_validation/Dedupe_output/trial/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
csv_write_path = csv_path.replace('table_name', 'temp_all_trial_dedupe_results_final')
temp_all_trial_dedupe_results_final.repartition(1).write.mode('overwrite').format('csv').option(
    'header', 'true').option('delimiter', ',').option('quoteAll', 'true').save(csv_write_path)

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_dedupe_results_final PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_trial_dedupe_results_final
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_dedupe_results_final')

# KM output on S3
# temp_all_trial_dedupe_results_union_final.repartition(1).write.mode('overwrite').format('csv')\
# .option('header', 'true').option('delimiter', ',').option('quoteAll', 'true')\
# .save('{bucket_path}/applications/commons/temp/km_validation/km_inputs/trial/$$data_dt/'.format(bucket_path=bucket_path))