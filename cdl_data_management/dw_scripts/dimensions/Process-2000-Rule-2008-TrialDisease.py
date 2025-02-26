################################# Module Information ######################################
#  Module Name         : r_trial_disease
#  Purpose             : This will create target table r_trial_disease
#  Pre-requisites      : L1 and L2 source table required: xref_src_trial, d_disease
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

r_trial_disease = spark.sql("""
select
        A.ctfo_trial_id,
        B.disease_id
from temp_d_disease A
left join $$client_name_ctfo_datastore_app_commons_$$db_env.d_disease B
on lower(trim(A.disease_nm)) = lower(trim(B.disease_nm))
and lower(trim(A.therapeutic_area)) = lower(trim(B.therapeutic_area))
where A.ctfo_trial_id is not null
and B.disease_id is not null
group by 1,2
""")
r_trial_disease.createOrReplaceTempView('r_trial_disease')

# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_disease
 partition(
        pt_data_dt='$$data_dt',
        pt_cycle_id='$$cycle_id')
select *, 'ctfo' as project_flag from r_trial_disease
""")

# Closing spark context
try:
    print('losing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_disease')