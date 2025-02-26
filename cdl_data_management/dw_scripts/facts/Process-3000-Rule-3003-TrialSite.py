# ################################ Module Information ######################################
#  Module Name         : Site Study Fact Table
#  Purpose             : This will create below fact table for all KPI calculation
#                           a. f_trial_site
#  Pre-requisites      : Source table required:
#                           a. $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site
#                           b. $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site
#                           c. $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
#                           o. country mapping file
#                           p. $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial
#                           q. country timelines file
#  Last changed on     : 01-12-2022
#  Last changed by     : Vicky Soni
#  Reason for change   : Aggregated the KPI after USL change
#  Return Values       : NA
# ###########################################################################################


# ################################## High level Process #####################################
# 1. Create required table and stores data on HDFS
# ###########################################################################################
# ###########################################################################################


# trialsite


import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from CommonUtils import CommonUtils
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import initcap

import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

spark.conf.set("spark.sql.crossJoin.enabled", "true")

path = bucket_path + "/applications/commons/temp/kpi_output_dimension/table_name/" \
                     "pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

# Read Temp Table f_trial_site and apply remaining column Prec
f_trial_site_final_temp = spark.sql("""
select *
from site_metric_engine_KPI_final
""")
f_trial_site_final_temp = f_trial_site_final_temp.dropDuplicates()
f_trial_site_final_temp.createOrReplaceTempView('f_trial_site_final_temp')

f_trial_site_final = spark.sql("""
select 
ctfo_trial_id,
ctfo_site_id,
first_subject_randomized_dt as first_subject_in_dt,
last_subject_randomized_dt as last_subject_in_dt,
site_ready_to_enroll_dt as ready_to_enroll_dt,
CASE
  WHEN cast(screen_fail_rate as double) < 0 THEN 0
  WHEN cast(screen_fail_rate as double)>100 then 100
  ELSE cast(screen_fail_rate as double)
    END        AS screen_fail_rate,
CASE
  WHEN cast(site_startup_time as double) < 0 THEN 0
  ELSE cast(site_startup_time as double)
    END        AS average_startup_time,
CASE
WHEN cast(randomization_rate as double) < 0 THEN 0
ELSE cast(randomization_rate as double)
    END        AS randomization_rate,
CASE
  WHEN cast(lost_opportunity_time as double) < 0 THEN 0
  ELSE cast(lost_opportunity_time as double)
    END        AS lost_opportunity_time,
patients_enrolled,
CASE
  WHEN cast(total_recruitment_months as double) < 0 THEN 0
  ELSE cast(total_recruitment_months as double)
    END        AS total_recruitment_months,
trial_site_status as study_status,
last_subject_last_visit_dt,
patients_screened
from
f_trial_site_final_temp
""")
f_trial_site_final = f_trial_site_final.dropDuplicates()
f_trial_site_final.createOrReplaceTempView('f_trial_site_final')
f_trial_site_final.write.mode("overwrite").saveAsTable("f_trial_site_final")

# Write Final Table f_trial_site to S3
write_path = path.replace("table_name", "f_trial_site_final")
f_trial_site_final.write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.f_trial_site
partition(pt_data_dt,pt_cycle_id)
select
    ctfo_trial_id,
    ctfo_site_id,
    first_subject_in_dt,
    last_subject_in_dt,
    ready_to_enroll_dt,
    screen_fail_rate,
    average_startup_time,
    '' as enrollment_rate,
    randomization_rate,
    lost_opportunity_time,
    patients_enrolled,
    total_recruitment_months,
    study_status,
    last_subject_last_visit_dt,
    patients_screened,
    "$$data_dt" as pt_data_dt,
    "$$cycle_id" as pt_cycle_id
from
    f_trial_site_final
""")

# f_trial_site.write.format("parquet").mode("overwrite").save(path=write_path)
if f_trial_site_final.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_trial_site_final as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.f_trial_site")

# Closing spark context
try:
    print("Closing spark context")
except:
    print("Error while closing spark context")