
################################# Module Information ######################################
#  Module Name         : Trial Post dedupe logic
#  Purpose             : This will execute business rules on dedupe output data
#  Pre-requisites      : Source table required: temp_all_trial_match_score, temp_trial_data_common_nct_id_result
#  Last changed on     : 22-03-2021
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

sys.path.insert(1, '/app/clinical_design_center/data_management/sanofi_ctfo/code')
from DedupeUtility import KMValidate
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/trial/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

temp_all_trial_match_score = spark.sql("""
select * from temp_all_trial_match_score
""")

write_path = path.replace('table_name', 'temp_all_trial_match_score')
temp_all_trial_match_score.repartition(100).write.mode('overwrite').parquet(write_path)

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
on data_prep.uid = dedupe_res.uid
group by 1,2,3,4,5,6,7,8,9,10
""")

temp_all_trial_dedupe_results.write.mode('overwrite').saveAsTable('temp_all_trial_dedupe_results')

# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results')
temp_all_trial_dedupe_results.repartition(100).write.mode('overwrite').parquet(write_path)

# Code to resolve OTHER phase records getting mapped to multiple clusters

multiple_clustered_rec = spark.sql("""
select trial_phase, ds_trial_id, count(distinct cluster_id) as count
from
temp_all_trial_dedupe_results
group by 1,2
having count(distinct cluster_id) > 1
""")

multiple_clustered_rec.registerTempTable('multiple_clustered_rec')
multiple_clustered_rec.write.mode('overwrite').saveAsTable('multiple_clustered_rec')


#Multiple CTMS studies get merged with external data source(example-DQS). Assign cluster for 2 records,
#1 CTMS record with greater score should get clustered with 1 record from DQS with greater score. If same score, then pick anyone
multiple_clustered_rec_map = spark.sql("""
select
    base.cluster_id,
    base.score,
    base.uid,
    base.hash_uid,
    base.data_src_nm,
    base.ds_trial_id,
    base.trial_title,
    base.trial_phase,
    base.trial_drug,
    base.trial_indication,
    Rank () over (partition by base.trial_phase,base.ds_trial_id order by base.score desc) score_rnk
from
temp_all_trial_dedupe_results base
inner join
multiple_clustered_rec temp
on base.trial_phase = temp.trial_phase and base.ds_trial_id = temp.ds_trial_id
having score_rnk = 1
""")

multiple_clustered_rec_map.registerTempTable('multiple_clustered_rec_map')
multiple_clustered_rec_map.write.mode('overwrite').saveAsTable('multiple_clustered_rec_map')


multiple_clustered_rec_map_max = spark.sql("""
select
    max(cluster_id) as cluster_id,
    score,
    uid,
    hash_uid,
    data_src_nm,
    ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from
multiple_clustered_rec_map
group by score,uid,hash_uid,data_src_nm,ds_trial_id,trial_title,trial_phase,trial_drug,trial_indication
""")

multiple_clustered_rec_map_max.write.mode('overwrite').saveAsTable('multiple_clustered_rec_map_max')

temp_all_trial_dedupe_results_union = spark.sql("""
select
    base.cluster_id,
    base.score,
    base.uid,
    base.hash_uid,
    base.data_src_nm,
    base.ds_trial_id,
    base.trial_title,
    base.trial_phase,
    base.trial_drug,
    base.trial_indication
from
temp_all_trial_dedupe_results base
left outer join
multiple_clustered_rec temp
on base.trial_phase = temp.trial_phase and base.ds_trial_id = temp.ds_trial_id
where temp.ds_trial_id is null

union

select
    cluster_id,
    score,
    uid,
    hash_uid,
    data_src_nm,
    ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from
multiple_clustered_rec_map_max
""")

temp_all_trial_dedupe_results_union.write.mode('overwrite').\
    saveAsTable('temp_all_trial_dedupe_results_union')


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
    base.trial_indication
from temp_all_trial_dedupe_results_union base
left outer join
(
select
    'Rule_01' as rule_id,
    'unclusterd_id' as cluster_id,
    0 as score,
    uid,
    hash_uid,
    data_src_nm,
    ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from temp_all_trial_dedupe_results_union where length(trial_title) <= 10 and trim(lower(cluster_id))
 not like 'common_nct%') rule_df
on base.uid = rule_df.uid
""")

temp_all_trial_dedupe_results_1.write.mode('overwrite')\
    .saveAsTable('temp_all_trial_dedupe_results_1')

# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_1')
temp_all_trial_dedupe_results_1.repartition(100).write.mode('overwrite').parquet(write_path)

# Assigning intger id to cluster id
temp_trial_cluterid = spark.sql("""
select
    cluster_id,
    row_number() over(order by null) as int_final_cluster_id
from temp_all_trial_dedupe_results_1
where cluster_id is not null
group by cluster_id having count(*)>1
""")
temp_trial_cluterid.registerTempTable('temp_trial_cluterid')
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
from temp_all_trial_dedupe_results_1 results_1
left outer join temp_trial_cluterid b
on results_1.cluster_id = b.cluster_id
""")


temp_all_trial_dedupe_results_2.write.mode('overwrite')\
    .saveAsTable('temp_all_trial_dedupe_results_2')
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_2')
temp_all_trial_dedupe_results_2.repartition(100).write.mode('overwrite').parquet(write_path)



# De clustering records within a cluster having records of multiple registries clubbed together
#De-cluster studies from same data source with different source ID's.

dedupe_results_exp = spark.sql("""
select
        hash_uid,
        cluster_id,
        score,
        ds_trial_id,
        src_trial_ids as ds_trial_id_1
from
        temp_all_trial_dedupe_results_2
lateral view explode(split(ds_trial_id,'\\\;'))one as src_trial_ids
where cluster_id is not null
""")
dedupe_results_exp.registerTempTable("dedupe_results_exp")
dedupe_results_exp.write.mode('overwrite').saveAsTable('dedupe_results_exp')




src_protocol_map = spark.sql("""
select
        hash_uid,
        cluster_id,
        score,
        ds_trial_id,
        ds_trial_id_1,
        case when src_trial_id is null or src_trial_id = '' then 'NA' else src_trial_id end
        as protocol_ids
from
        dedupe_results_exp exp
left outer join
        temp_all_trial_data_cleaned_obs_remove obs
on trim(exp.ds_trial_id_1) = trim(obs.src_trial_id)
group by 1,2,3,4,5,6
""")

src_protocol_map.registerTempTable("src_protocol_map")
src_protocol_map.write.mode('overwrite').saveAsTable('src_protocol_map')


src_protocol_map_final = spark.sql("""
select
        hash_uid,
        cluster_id,
        score,
        ds_trial_id,
        protocol_ids,
        case when trim(lower(protocol_ids)) like 'nct%' then 'nct'
        when trim(lower(protocol_ids)) like 'eudract%' then 'eudract'
        when trim(lower(protocol_ids)) like 'euctr%' then 'euctr'
        when trim(lower(protocol_ids)) like 'jprn%' then 'jprn'
        when trim(lower(protocol_ids)) like 'chictr%'  then 'chictr'
        when trim(lower(protocol_ids)) like 'irct%' then 'irct'
        when trim(lower(protocol_ids)) like 'ctri%' then 'ctri'
        when trim(lower(protocol_ids)) like 'actrn%' then 'actrn'
        when trim(lower(protocol_ids)) like 'isrctn%' then 'isrctn'
        when trim(lower(protocol_ids)) like 'drks%' then 'drks'
        when trim(lower(protocol_ids)) like 'ntr%' then 'ntr'
        when trim(lower(protocol_ids)) like 'kct%' then 'kct'
        when trim(lower(protocol_ids)) like 'tctr%' then 'tctr'
        when trim(lower(protocol_ids)) like 'rbr-%' then 'rbr'
        when trim(lower(protocol_ids)) like 'pactr%' then 'pactr'
        when trim(lower(protocol_ids)) like 'per%' then 'per'
        when trim(lower(protocol_ids)) like 'slctr%' then 'slctr'
        when trim(lower(protocol_ids)) like 'supergen%' then 'supergen'
        when trim(lower(protocol_ids)) like 'rpcec%' then 'rpcec'
        when trim(lower(protocol_ids)) like 'lbctr%' then 'lbctr'
        when trim(lower(protocol_ids)) like 'ctms%' then 'ctms'
        when trim(lower(protocol_ids)) like 'ir%' then 'ir'
        when trim(lower(protocol_ids)) like 'na' then 'other'

        else null end as src_reg
from
(select
        hash_uid,
        cluster_id,
        score,
        ds_trial_id,
        max(protocol_ids) as protocol_ids
from
src_protocol_map
group by 1,2,3,4)
""")

src_protocol_map_final.write.mode('overwrite').saveAsTable("src_protocol_map_final")

reg_cluster_count = spark.sql("""
select
        cluster_id,
max(final_count) as final_count,
count(distinct src_reg) as reg_count
from
(select
        cluster_id,
        src_reg,
        case when src_reg = 'other' then 1 else count end as final_count
from
(
select
        cluster_id,
        src_reg,
        count(*) as count
from
src_protocol_map_final
group by 1,2))
group by 1
""")

reg_cluster_count.write.mode('overwrite').saveAsTable("reg_cluster_count")

src_protocol_map_final1 = spark.sql("""
select
        *,
        case when src_reg = 'other' then 1 else rnk end as final_rnk
from
(select
        base.hash_uid,
        base.cluster_id,
        base.score,
        base.ds_trial_id,
        base.protocol_ids,
        base.src_reg,
        rank() over(partition by base.cluster_id, base.src_reg order by base.score desc, base.hash_uid)
        as rnk,
        case when src_reg = 'other' then 999999 else reg.reg_count end as reg_count
from src_protocol_map_final base
inner join
reg_cluster_count reg
on base.cluster_id = reg.cluster_id)
""")

src_protocol_map_final1.registerTempTable("src_protocol_map_final1")
src_protocol_map_final1.write.mode('overwrite').saveAsTable('src_protocol_map_final1')


# Taking max scores records from each registry for cluster having multiple registries

temp_all_trial_dedupe_results_3 = spark.sql("""
select
        case when cluster.hash_uid is null then 'Rule_03' else rule_id end as rule_id,
    case when cluster.hash_uid is null then null else results_2.cluster_id end as cluster_id,
        case when cluster.hash_uid is null then null else results_2.score end as score,
    results_2.uid,
    results_2.hash_uid,
    results_2.data_src_nm,
        results_2.ds_trial_id,
    results_2.trial_title,
    results_2.trial_phase,
    results_2.trial_drug,
    results_2.trial_indication
from
temp_all_trial_dedupe_results_2 results_2
left outer join
(select distinct hash_uid from src_protocol_map_final1 where final_rnk = 1 and reg_count > 1)
cluster
on results_2.hash_uid = cluster.hash_uid
""")

temp_all_trial_dedupe_results_3.registerTempTable('temp_all_trial_dedupe_results_3')

temp_all_trial_dedupe_results_3.write.mode('overwrite')\
    .saveAsTable('temp_all_trial_dedupe_results_3')
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_3')
temp_all_trial_dedupe_results_3.repartition(100).write.mode('overwrite').parquet(write_path)

dedupe_results_src_exp = spark.sql("""
select
        hash_uid,
        cluster_id,
        score,
        exp_data_src
from
        temp_all_trial_dedupe_results_3
lateral view explode(split(data_src_nm,'\\\;'))one as exp_data_src
where cluster_id is not null
""")
dedupe_results_src_exp.registerTempTable("dedupe_results_src_exp")
dedupe_results_src_exp.write.mode('overwrite').saveAsTable('dedupe_results_src_exp')


cluster_ds_count = spark.sql("""
select
        base.*,
        ds.ds_cnt
from
dedupe_results_src_exp base
left outer join
(select
        cluster_id,
        exp_data_src,
        count(*) as ds_cnt
from
        dedupe_results_src_exp
group by 1,2) ds
on base.cluster_id = ds.cluster_id and base.exp_data_src = ds.exp_data_src
""")

cluster_ds_count.write.mode("overwrite").saveAsTable("cluster_ds_count")

declstr_multi_ds = spark.sql("""
select *
from
(select *,
        rank() over(partition by cluster_id order by score desc, hash_uid desc) as rnk
from
(select *
from cluster_ds_count where ds_cnt > 1))
where rnk > 1
""")

declstr_multi_ds.registerTempTable("declstr_multi_ds")
declstr_multi_ds.write.mode('overwrite').saveAsTable('declstr_multi_ds')


temp_all_trial_dedupe_results_4int = spark.sql("""
select
        case when cluster.hash_uid is not null then 'Rule_04' else rule_id end as rule_id,
    case when cluster.hash_uid is not null then null else results_3.cluster_id end as cluster_id,
        case when cluster.hash_uid is not null then null else results_3.score end as score,
    results_3.uid,
    results_3.hash_uid,
    results_3.data_src_nm,
        results_3.ds_trial_id,
    results_3.trial_title,
    results_3.trial_phase,
    results_3.trial_drug,
    results_3.trial_indication
from
temp_all_trial_dedupe_results_3 results_3
left outer join
(select distinct hash_uid from declstr_multi_ds ) cluster
on results_3.hash_uid = cluster.hash_uid
""")

temp_all_trial_dedupe_results_4int.registerTempTable("temp_all_trial_dedupe_results_4int")
temp_all_trial_dedupe_results_4int.write.mode('overwrite').saveAsTable('temp_all_trial_dedupe_results_4int')


single_rec_cluster = spark.sql("""
select
        cluster_id,
        count(distinct hash_uid) as count
from
temp_all_trial_dedupe_results_4int
group by 1
having count(distinct hash_uid) = 1
""")

single_rec_cluster.registerTempTable("single_rec_cluster")
single_rec_cluster.write.mode('overwrite').saveAsTable('single_rec_cluster')


temp_all_trial_dedupe_results_4 = spark.sql("""
select
        case when cluster.cluster_id is not null then 'Rule_04' else rule_id end as rule_id,
    case when cluster.cluster_id is not null then null else results_3.cluster_id end as cluster_id,
        case when cluster.cluster_id is not null then null else results_3.score end as score,
    results_3.uid,
    results_3.hash_uid,
    results_3.data_src_nm,
        results_3.ds_trial_id,
    results_3.trial_title,
    results_3.trial_phase,
    results_3.trial_drug,
    results_3.trial_indication
from
temp_all_trial_dedupe_results_4int results_3
left outer join
(select distinct cluster_id from single_rec_cluster ) cluster
on results_3.cluster_id = cluster.cluster_id
""")

temp_all_trial_dedupe_results_4.write.mode('overwrite')\
    .saveAsTable('temp_all_trial_dedupe_results_4')
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_4')
temp_all_trial_dedupe_results_4.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_trial_dedupe_results_5 = spark.sql("""
select
    coalesce(trial_dedupe_2.rule_id,trial_dedupe_1.rule_id) as rule_id,
    coalesce(trial_dedupe_2.cluster_id, trial_dedupe_1.cluster_id) as cluster_id,
    coalesce(trial_dedupe_2.score, trial_dedupe_1.score) as score,
    trial_dedupe_1.uid,
    trial_dedupe_1.hash_uid,
    trial_dedupe_1.data_src_nm,
    trial_dedupe_1.ds_trial_id,
    trial_dedupe_1.trial_title,
    trial_dedupe_1.trial_phase,
    trial_dedupe_1.trial_drug,
    trial_dedupe_1.trial_indication
from temp_all_trial_dedupe_results_4 trial_dedupe_1
left outer join (
select distinct
    'Rule_05' as rule_id,
    uid,
    cluster_id as cluster_id,
    case when rnk = 1 then score
             when rnk <>1 then 0.50
                 end as score
from (
select trial_dedupe.cluster_id,
trial_dedupe.uid,
trial_dedupe.score,
row_number() over (partition by trial_dedupe.cluster_id order by trial_dedupe.score desc ) as rnk
from
(
select
    trial_dedupe_results_1.cluster_id,
    trial_dedupe_results_1.uid,
    trial_dedupe_results_1.data_src_nm,
    trial_dedupe_results_1.score
from  temp_all_trial_dedupe_results_4  trial_dedupe_results_1
) trial_dedupe) where rnk=1  ) trial_dedupe_2
on trial_dedupe_1.uid = trial_dedupe_2.uid
""")
temp_all_trial_dedupe_results_5.write.mode('overwrite')\
    .saveAsTable('temp_all_trial_dedupe_results_5')
write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_5')
temp_all_trial_dedupe_results_5.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_trial_uni= spark.sql("""
select
    a.rule_id,
    a.cluster_id,
    a.score,
    a.uid,
    a.hash_uid,
    a.data_src_nm,
    a.ds_trial_id,
    a.trial_title,
    a.trial_phase,
    a.trial_drug,
    a.trial_indication
 from (select
    temp.rule_id,
    temp.cluster_id,
    temp.score,
    temp.uid,
    temp.hash_uid,
    temp.data_src_nm,
    temp.ds_trial_id,
    temp.trial_title,
    temp.trial_phase,
    temp.trial_drug,
    temp.trial_indication,
    row_number() over(partition by temp.hash_uid order by temp.score desc ) as rnk
from default.temp_all_trial_dedupe_results_5 temp) a
where a.rnk=1
""")
temp_all_trial_uni.dropDuplicates()

temp_all_trial_uni.write.mode('overwrite')\
    .saveAsTable('temp_all_trial_uni')
temp_all_trial_uni.repartition(100).write.mode('overwrite').parquet(write_path)




final_table = KMValidate('temp_all_trial_uni', 'trial', '$$data_dt','$$cycle_id','$$s3_env')

temp_all_trial_dedupe_results_union_final = spark.sql("""
select distinct
    rule_id,
    final_cluster_id as cluster_id,
    final_score as score,
    uid,
    hash_uid,
    data_src_nm,
    ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication,
    final_KM_comment as KM_comment
from
"""+final_table+"""
""")
temp_all_trial_dedupe_results_union_final\
    .registerTempTable('temp_all_trial_dedupe_results_union_final')
spark.sql("""
insert overwrite table
sanofi_ctfo_datastore_app_commons_$$db_env.temp_all_trial_dedupe_results_final PARTITION(
pt_data_dt='$$data_dt',
pt_cycle_id='$$cycle_id'
)
select *
from temp_all_trial_dedupe_results_union_final
""")

write_path = path.replace('table_name', 'temp_all_trial_dedupe_results_final')
temp_all_trial_dedupe_results_union_final.repartition(100).write.mode('overwrite').parquet(write_path)

#CommonUtils().copy_hdfs_to_s3('ctfo_internal_datastore_app_commons_$$db_env.temp_all_trial_dedupe_results_final')

# KM output on S3
temp_all_trial_dedupe_results_union_final.repartition(1).write.mode('overwrite').format('csv')\
    .option('header', 'true').option('delimiter', ',').option('quoteAll', 'true')\
    .save('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/'
          'applications/commons/temp/km_validation/km_inputs/trial/$$data_dt/')




