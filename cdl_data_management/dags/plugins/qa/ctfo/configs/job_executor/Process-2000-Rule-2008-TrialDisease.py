
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

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/dimensions/table_name/' \
       'pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

r_trial_disease = spark.sql("""
select
        A.ctfo_trial_id,
        B.disease_id
from temp_d_disease A
left join sanofi_ctfo_datastore_app_commons_$$db_env.d_disease B
on lower(trim(A.disease_nm)) = lower(trim(B.disease_nm))
and lower(trim(A.therapeutic_area)) = lower(trim(B.therapeutic_area))
where A.ctfo_trial_id is not null
and B.disease_id is not null
group by 1,2
""")
r_trial_disease.createOrReplaceTempView('r_trial_disease')

# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_disease
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
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_disease')






