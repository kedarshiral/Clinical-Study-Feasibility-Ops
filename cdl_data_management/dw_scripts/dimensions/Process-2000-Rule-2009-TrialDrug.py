################################# Module Information ######################################
#  Module Name         : r_trial_drug
#  Purpose             : This will create target table r_trial_drug
#  Pre-requisites      : Source table required: temp_d_drug, xref_src_trial
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from source table
# 2. Pass through the source tables on key columns to create final target table
############################################################################################
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')

path = "{bucket_path}/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id".format(bucket_path=bucket_path)

# Fetching xref_src_trial data
xref_src_trial = spark.sql("""
select
    ctfo_trial_id,
    src_trial_id,
    data_src_nm
from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
""")
xref_src_trial.registerTempTable('xref_src_trial')

r_trial_drug = spark.sql("""
select /*+ broadcast(temp_d_drug) */
    xref_src_trial.ctfo_trial_id,
    temp_d_drug.drug_id
from xref_src_trial
left outer join
   (select trim(id) as drug_id ,src as data_src_nm,trim(replace(trial_id, concat(src, '_'), '')) as trial_id
    from
    (select trim(drug_id) as id, trim(data_src) as src, trim(trial_id) as  trialid from temp_d_drug
     lateral view posexplode(split(src_trial_id,'\;'))one as pos1,trial_id lateral view posexplode
     (split(data_src_nm,'\;'))two as pos2,data_src where pos1=pos2
      group by 1,2,3) a
lateral view outer explode (split(trialid,'\\\|')) trial as trial_id
group by 1,2,3) temp_d_drug
on lower(trim(xref_src_trial.src_trial_id)) = lower(trim(temp_d_drug.trial_id))
and lower(trim(xref_src_trial.data_src_nm)) = lower(trim(temp_d_drug.data_src_nm))
where xref_src_trial.ctfo_trial_id is not null and xref_src_trial.src_trial_id is not null
and temp_d_drug.drug_id is not null
group by 1,2
""")
r_trial_drug.registerTempTable('r_trial_drug')

# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_drug
partition(
        pt_data_dt='$$data_dt',
        pt_cycle_id='$$cycle_id')
        select * from r_trial_drug
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_drug')