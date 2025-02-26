


################################# Module Information ######################################
#  Module Name         : Patient Details reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_patient_details
#
#  Pre-requisites      : Source table required: DRG, Data Monitor, Globocan
#  Last changed on     : 04-14-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Create required table and stores data on HDFS
############################################################################################
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf
from CommonUtils import *

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

data_dt = "$$data_dt"
cycle_id = "$$cycle_id"

path = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
       "commons/temp/" \
       "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
           "commons/temp/" \
           "kpi_output_dimension/table_name"

# for standard country names and region
# geo_mapping = spark.read.format("csv").option("header", "true")\
#     .load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/"
#           "country_standardization.csv")
# geo_mapping.createOrReplaceTempView("country_mapping")
# geo_mapping.write.mode('overwrite').saveAsTable('country_mapping')

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/standard_country_mapping.csv")
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

sanofi_cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/sanofi_cluster_pta_mapping.csv")
sanofi_cluster_pta_mapping.createOrReplaceTempView("sanofi_cluster_pta_mapping")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Others') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.sanofi_cluster,scpm.sanofi_csu,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country, country
    from standard_country_mapping )  cs
left join
        sanofi_cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

ta_mapping = spark.read.format('csv').option("header", "true"). \
    option("delimiter", ","). \
    load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/TA_MAPPING/"
         "ta_mapping.csv")
ta_mapping.registerTempTable('ta_mapping')

disease_mapping_df = spark.read.format('csv').option("header", "true"). \
    option("delimiter", ","). \
    load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/"
    "disease_mapping.csv")
disease_mapping_df.registerTempTable('disease_mapping_full')

temp_disease_mapping_df = spark.sql("""
SELECT
disease, standard_disease
FROM disease_mapping_full
GROUP BY 1,2
""")
temp_disease_mapping_df.createOrReplaceTempView("disease_mapping")

ihme_gbd1=spark.sql("""
SELECT
    "ihme" as data_source,
    location as  country,
    "All" as state,
    Age as min_age,
    Age as max_age,
    sex as gender,
    Year as year,
    cause as disease,
    '' as incidence,
    val as prevalence,
    Age as age
    FROM sanofi_ctfo_datastore_staging_$$db_env.ihme_gbd

""")
ihme_gbd1.createOrReplaceTempView("ihme_gbd1")
temp_patient_details_df = spark.sql("""
select * from ihme_gbd1
""")
temp_patient_details_df=temp_patient_details_df.dropDuplicates()
temp_patient_details_df.write.mode("overwrite").saveAsTable("temp_patient_details")
#temp_patient_details_df.createOrReplaceTempView('temp_patient_details')

spark.sql("""SELECT
t.country as country, map_1.standard_country as std_country,map_1.sanofi_cluster as sanofi_cluster, t.age, t.min_age, t.max_age, t.gender, t.year, t.data_source,
 t.incidence, t.prevalence, "all" as state,
CASE WHEN data_source = "ihme" THEN regexp_replace(t.disease, '[+]', ' ') ELSE t.disease END AS disease,
map_1.region
FROM temp_patient_details t
LEFT JOIN country_mapping map_1 on LOWER(TRIM(map_1.standard_country)) = LOWER(TRIM(t.country)) AND map_1.standard_country is NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

""").createOrReplaceTempView('temp_patient_details2')

spark.sql("""SELECT
t.std_country, t.sanofi_cluster, t.age, t.min_age, t.max_age,  t.gender, t.year, t.data_source, t.incidence, t.prevalence, t.state, t.region,
t.disease as disease_new, coalesce(x.standard_disease, 'Other') as standard_disease
FROM temp_patient_details2 t
LEFT OUTER JOIN disease_mapping x on LOWER(TRIM(x.disease)) = LOWER(TRIM(t.disease))
""").createOrReplaceTempView('temp_patient_details3')

f_rpt_patient_details_df = spark.sql("""SELECT
t.data_source, t.std_country as country, t.sanofi_cluster, t.state, t.min_age, t.max_age,  t.gender, t.year, t.standard_disease as disease,
        coalesce(a.therapeutic_area, 'Other')
        AS therapy_area,
t.region, t.prevalence, t.incidence, t.age
FROM temp_patient_details3 t
LEFT OUTER JOIN ta_mapping a on LOWER(TRIM(a.disease_name)) = LOWER(TRIM(t.standard_disease))
""")
# f_rpt_patient_details_df.createOrReplaceTempView('f_rpt_patient_details')
f_rpt_patient_details_df = f_rpt_patient_details_df.dropDuplicates()
f_rpt_patient_details_df.write.mode('overwrite').saveAsTable('f_rpt_patient_details')

write_path = path.replace("table_name", "f_rpt_patient_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_patient_details")
write_path_csv = write_path_csv + "_csv/"
# f_rpt_patient_details_df.write.format("parquet").mode("overwrite").save(path=write_path)


f_rpt_patient_details_df.coalesce(1).write.option("header", "false").option("sep", "|").option('quote', '"') \
    .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$env.f_rpt_patient_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   f_rpt_patient_details
""")

if f_rpt_patient_details_df.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_patient_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$env.f_rpt_patient_details")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")






