# ################################ Module Information ######################################
#  Module Name         : Investigator Site Study Fact Table
#  Purpose             : This will create below fact table for all KPI calculation
#                           a. f_trial_investigator
#  Pre-requisites      : Source table required:
#                           a. $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site
#                           b. $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site
#                           c. $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
#                           d. $$client_name_ctfo_datastore_staging_$$db_env.dqs_study
#                           e. country mapping file
#                           f. $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial
#  Last changed on     : 13-01-2022
#  Last changed by     : Vicky Soni
#  Reason for change   : Added Site Grain
#  Return Values       : NA
# ###########################################################################################

# ################################## High level Process #####################################
# 1. Create required table and stores data on HDFS
# ###########################################################################################
# ###########################################################################################
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from CommonUtils import CommonUtils
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import initcap
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version","2")

path = bucket_path + "/applications/commons/temp/kpi_output_dimension/table_name/" \
                     "pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"
                   
f_investigator_trial_site_final = spark.sql("""
select 
ctfo_trial_id,
ctfo_investigator_id,
ctfo_site_id,
last_subject_in_dt,
ready_to_enroll_dt,
CASE
         WHEN cast(screen_fail_rate as double) < 0 THEN 0
         WHEN cast(screen_fail_rate as double)>100 then 100
         ELSE cast(screen_fail_rate as double)
       END        AS screen_fail_rate,
CASE
         WHEN cast(randomization_rate as double) < 0 THEN 0
         ELSE cast(randomization_rate as double)
       END        AS randomization_rate,
CASE
         WHEN cast(total_recruitment_months as double) < 0 THEN 0
         ELSE cast(total_recruitment_months as double)
       END        AS total_recruitment_months,
last_subject_last_visit_dt,
patients_enrolled,
patients_screened
from inv_metric_engine_KPI_final
""")
f_investigator_trial_site_final = f_investigator_trial_site_final.dropDuplicates()
f_investigator_trial_site_final.createOrReplaceTempView('f_investigator_trial_site_final')
f_investigator_trial_site_final.write.mode("overwrite").saveAsTable("f_investigator_trial_site_final")


# Write Final Table f_investigator_trial_site to S3
write_path = path.replace("table_name", "f_investigator_trial_site_final")
f_investigator_trial_site_final.write.format("parquet").mode("overwrite").save(path=write_path)

# Write to HDFS
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.f_investigator_site_study_details
partition(pt_data_dt,pt_cycle_id)
select *,"$$data_dt" as pt_data_dt,
    "$$cycle_id" as pt_cycle_id
from f_investigator_trial_site_final 
""")



if f_investigator_trial_site_final.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_investigator_trial_site_final as zero records are present.")
else :
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env."
                              "f_investigator_site_study_details")
# Closing spark context
try:
    print("Closing spark context")
except:
    print("Error while closing spark context")
