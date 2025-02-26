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

temp_all_site_match_score_aact = spark.sql(
    """
    select * from temp_all_site_match_score_aact
    """)
temp_all_site_match_score_aact.registerTempTable('temp_all_site_match_score_aact')


spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_match_score_aact PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_site_match_score_aact
    """)

CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_match_score_aact')

# This dataframe will hold each record of AACT and will be utilized in combining the results of AACT golden records, CTMS,IR,Citeline
temp_all_site_dedupe_results_aact = spark.sql("""
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
from temp_all_site_data_prep_aact site_prep
left  join
(select
    cluster_id,
    uid,
    score
from temp_all_site_match_score_aact group by 1,2,3) site_score
on trim(site_prep.uid)=trim(site_score.uid)
""")

temp_all_site_dedupe_results_aact.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_aact')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_aact')
temp_all_site_dedupe_results_aact.repartition(100).write.mode('overwrite').parquet(write_path)

#Assigning null to cluster ids where score less than 0.5 based on the sample data analysed
temp_all_site_dedupe_results_aact_null=spark.sql("""
select
        case when score<0.5 then null else cluster_id end as cluster_id,
        score,
        uid,
        hash_uid,
        data_src_nm,
        ds_site_id,
        name,
        city,
        zip,
        address,
        country from temp_all_site_dedupe_results_aact

""")
temp_all_site_dedupe_results_aact_null.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_aact_null')

temp_aact_golden_record= spark.sql("""
select uid,
 hash_uid,
 data_src_nm,
 ds_site_id,
 name,
 city,
 zip,
 address,
 country from (select uid,
 hash_uid,
 data_src_nm,
 ds_site_id,
 name,
 city,
 zip,
 address,
 country,row_number()over(partition by cluster_id order by zip nulls last, score desc) as rnk
from temp_all_site_dedupe_results_aact_null where cluster_id is not null) where rnk=1
union
select
 uid,
 hash_uid,
 data_src_nm,
 ds_site_id,
 name,
 city,
 zip,
 address,
 country from temp_all_site_dedupe_results_aact_null where cluster_id is null
 """)

temp_aact_golden_record.registerTempTable('temp_aact_golden_record')

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_aact_golden_record PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_aact_golden_record
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_aact_golden_record')

# -----------------------------------------------------------------
# TASCAN
temp_all_site_match_score_tascan = spark.sql(
    """
    select * from temp_all_site_match_score_tascan
    """)
temp_all_site_match_score_tascan.registerTempTable('temp_all_site_match_score_tascan')


spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_match_score_tascan PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_site_match_score_tascan
    """)

CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_match_score_tascan')

# This dataframe will hold each record of AACT and will be utilized in combining the results of AACT golden records, CTMS,IR,Citeline
temp_all_site_dedupe_results_tascan = spark.sql("""
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
from temp_all_site_data_prep_tascan site_prep
left  join
(select
    cluster_id,
    uid,
    score
from temp_all_site_match_score_tascan group by 1,2,3) site_score
on trim(site_prep.uid)=trim(site_score.uid)
""")

temp_all_site_dedupe_results_tascan.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_tascan')
write_path = path.replace('table_name', 'temp_all_site_dedupe_results_tascan')
temp_all_site_dedupe_results_tascan.repartition(100).write.mode('overwrite').parquet(write_path)

#Assigning null to cluster ids where score less than 0.5 based on the sample data analysed
temp_all_site_dedupe_results_tascan_null=spark.sql("""
select
        case when score<0.5 then null else cluster_id end as cluster_id,
        score,
        uid,
        hash_uid,
        data_src_nm,
        ds_site_id,
        name,
        city,
        zip,
        address,
        country from temp_all_site_dedupe_results_tascan

""")
temp_all_site_dedupe_results_tascan_null.write.mode('overwrite').saveAsTable('temp_all_site_dedupe_results_tascan_null')

temp_tascan_golden_record= spark.sql("""
select uid,
 hash_uid,
 data_src_nm,
 ds_site_id,
 name,
 city,
 zip,
 address,
 country from (select uid,
 hash_uid,
 data_src_nm,
 ds_site_id,
 name,
 city,
 zip,
 address,
 country,row_number()over(partition by cluster_id order by zip nulls last, score desc) as rnk
from temp_all_site_dedupe_results_tascan_null where cluster_id is not null) where rnk=1
union
select
 uid,
 hash_uid,
 data_src_nm,
 ds_site_id,
 name,
 city,
 zip,
 address,
 country from temp_all_site_dedupe_results_tascan_null where cluster_id is null
 """)

temp_tascan_golden_record.registerTempTable('temp_tascan_golden_record')

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_tascan_golden_record PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_tascan_golden_record
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_tascan_golden_record')
# ----------------------------------------------------------------



temp_all_site_data_prep_combined_temp=spark.sql("""
select uid,
 hash_uid,
 data_src_nms as data_src_nm,
 ds_site_ids as ds_site_id,
 name,
 city,
 zip,
 address,
 country
 from temp_all_site_data_prep
 lateral view outer explode (split(ds_site_id,"\;")) as ds_site_ids
 lateral view outer explode (split(data_src_nm,"\;")) as data_src_nms
 
 union
 
 select uid,
 hash_uid,
 data_src_nms as data_src_nm,
 ds_site_ids as ds_site_id,
 name,
 city,
 zip,
 address,
 country
 from temp_aact_golden_record
 lateral view outer explode (split(ds_site_id,"\;")) as ds_site_ids
 lateral view outer explode (split(data_src_nm,"\;")) as data_src_nms
 
  union
 
 select uid,
 hash_uid,
 data_src_nms as data_src_nm,
 ds_site_ids as ds_site_id,
 name,
 city,
 zip,
 address,
 country
 from temp_tascan_golden_record
 lateral view outer explode (split(ds_site_id,"\;")) as ds_site_ids
 lateral view outer explode (split(data_src_nm,"\;")) as data_src_nms
 
""")
temp_all_site_data_prep_combined_temp.registerTempTable('temp_all_site_data_prep_combined_temp')

temp_all_site_data_prep_combined = spark.sql("""select  uid,
 hash_uid,
 concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_src_nm),'')))) as data_src_nm,
        concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_site_id),'')))) as ds_site_id,
         name,
        city,
        zip,
        max(address) as address,
        country
    from temp_all_site_data_prep_combined_temp group by 1,2,5,6,7,9
    """)

# save data to hdfs
temp_all_site_data_prep_combined.write.mode('overwrite').saveAsTable('temp_all_site_data_prep_combined')

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_data_prep_combined
PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
select
*
from
    temp_all_site_data_prep_combined
""")

CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_site_data_prep_combined")
