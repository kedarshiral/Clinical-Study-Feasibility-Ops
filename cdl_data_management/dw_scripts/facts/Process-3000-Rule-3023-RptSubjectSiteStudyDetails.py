################################# Module Information ######################################
#  Module Name         : Site reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_subject_site_study_details
#  Pre-requisites      : Source table required:
#  Last changed on     : 26-08-2024
#  Last changed by     : Kashish
#  Return Values       : NA
############################################################################################

from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from CommonUtils import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])


path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
                         "kpi_output_dimension/table_name"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")



trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load("{bucket_path}/uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")


# Step 1: Join usl_subject with xref_src_trial and xref_src_site
usl_with_xref = spark.sql("""
SELECT 
    usl.src_trial_id,
    usl.src_site_id,
    usl.src_subject_id,
    usl.subject_status,
    usl.ENROLLMENT_DATE,
    usl.COHORT_FULL_NAME,
    xref_trial.ctfo_trial_id AS trial_id,
    xref_site.ctfo_site_id AS site_id
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.usl_subject usl
LEFT JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial xref_trial
    ON lower(trim(usl.src_trial_id)) = lower(trim(xref_trial.src_trial_id))
LEFT JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site xref_site
    ON lower(trim(usl.src_site_id)) = lower(trim(xref_site.src_site_id))
""")
usl_with_xref.registerTempTable("usl_with_xref")

# Step 2: Map Subject Status
subject_status_mapped = spark.sql("""
SELECT 
    s.src_trial_id,
    s.src_site_id,
    s.src_subject_id,
    s.ENROLLMENT_DATE,
    s.COHORT_FULL_NAME,
    s.trial_id,
    s.site_id,
    coalesce(h.status, s.subject_status) as subject_status
FROM usl_with_xref s
LEFT JOIN trial_status_mapping h
    ON lower(trim(s.subject_status)) = lower(trim(h.raw_status))
""")
subject_status_mapped.registerTempTable("subject_status_mapped")

d_site_2 = spark.sql("""
SELECT distinct
ctfo_site_id,
site_name,
site_address,
site_city,
site_state,
site_country,
scpm.$$client_name_cluster as region,
site_zip,
geo_cd,
site_supporting_urls
from $$client_name_ctfo_datastore_app_commons_$$db_env.d_site a
left join cluster_pta_mapping scpm on lower(trim(a.site_country))=lower(trim(scpm.standard_country))
""")
d_site_2 = d_site_2.dropDuplicates()
d_site_2.createOrReplaceTempView("d_site_2")

# Step 3: Final Join with Site and Trial Data
f_rpt_subject_site_study_details = spark.sql("""
SELECT
    TRIM(s.trial_id) as trial_id,
    TRIM(s.src_trial_id) as src_trial_id,
    TRIM(s.site_id) as site_id,
    TRIM(s.src_site_id) as src_site_id,
    TRIM(s.src_subject_id) as subject_id,
    site.site_country,
    site.region as site_region,
    s.subject_status,
    s.ENROLLMENT_DATE as enrollment_date,
    trial.trial_start_dt,
    s.COHORT_FULL_NAME as cohort_name
FROM subject_status_mapped s
LEFT JOIN d_site_2 site
    ON lower(trim(s.site_id)) = lower(trim(site.ctfo_site_id))
LEFT JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial trial
    ON lower(trim(s.trial_id)) = lower(trim(trial.ctfo_trial_id))
""")
f_rpt_subject_site_study_details = f_rpt_subject_site_study_details.dropDuplicates()
f_rpt_subject_site_study_details.write.mode("overwrite").saveAsTable("f_rpt_subject_site_study_details")


write_path_csv = path_csv.replace("table_name", "f_rpt_subject_site_study_details")
write_path_csv = write_path_csv + "_csv/"
f_rpt_subject_site_study_details.coalesce(1).write.format("csv").mode("overwrite").save(path=write_path_csv)

write_path = path.replace("table_name", "f_rpt_subject_site_study_details")
f_rpt_subject_site_study_details.write.format("parquet").mode("overwrite").save(path=write_path)


spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_subject_site_study_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_subject_site_study_details
""")

if f_rpt_subject_site_study_details.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_subject_site_study_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_subject_site_study_details")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
