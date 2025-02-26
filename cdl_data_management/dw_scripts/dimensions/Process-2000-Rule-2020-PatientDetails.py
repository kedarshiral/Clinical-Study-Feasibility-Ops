################################# Module Information ######################################
#  Module Name         : Patient Details dimension table
#  Purpose             : This will create below dimension layer table
#                           a. d_patient_details
#
#  Pre-requisites      : Source table required: DRG, Data Monitor, Globocan, IHME
#  Last changed on     : 09-23-2022
#  Last changed by     : Shivangi
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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import initcap

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# age_temp_path = bucket_path + '/uploads/age_mapping_latest.csv'
# age_standardization = spark.read.csv(age_temp_path, sep=',', header=True, inferSchema=True)
# age_standardization.createOrReplaceTempView('age_standardization')

gender_temp_path = bucket_path + '/uploads/gender_mapping.csv'
gender_standardization = spark.read.csv(gender_temp_path, sep=',', header=True, inferSchema=True)
gender_standardization.createOrReplaceTempView('gender_standardization')

data_dt = "$$data_dt"
cycle_id = "$$cycle_id"

path = bucket_path + "/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

# for standard country names and region
# geo_mapping = spark.read.format("csv").option("header", "true")\
#     .load("s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/uploads/"
#           "country_standardization.csv")
# geo_mapping.createOrReplaceTempView("country_mapping")
# geo_mapping.write.mode('overwrite').saveAsTable('country_mapping')

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/"
    "uploads/standard_country_mapping.csv".format(bucket_path=bucket_path))
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

customSchema = StructType([
    StructField("standard_country", StringType(), True),
    StructField("iso2_code", StringType(), True),
    StructField("iso3_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("$$client_name_cluster", StringType(), True),
    StructField("$$client_name_cluster_code", StringType(), True),
    StructField("$$client_name_csu", StringType(), True),
    StructField("post_trial_flag", StringType(), True),
    StructField("post_trial_details", StringType(), True)])

cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/"
    "uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.$$client_name_cluster_code as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country, country
    from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

# ta_mapping = spark.read.format('csv').option("header", "true"). \
#    option("delimiter", ","). \
#    load("{bucket_path}/uploads/TA_MAPPING/"
#         "ta_mapping.csv".format(bucket_path=bucket_path))
ta_mapping = spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_to_ta_mapping""")
ta_mapping.registerTempTable('ta_mapping')

# disease_mapping_df = spark.read.format('csv').option("header", "true"). \
#    load(
#    "{bucket_path}/uploads/FINAL_DISEASE_MAPPING/"
#    "disease_mapping.csv".format(bucket_path=bucket_path))
disease_mapping_df = spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping""")
disease_mapping_df.registerTempTable('disease_mapping_full')



temp_disease_mapping_df = spark.sql("""
select distinct * from (select 
mesh as disease, standard_mesh as standard_disease from disease_mapping_full
union
SELECT
standard_mesh as disease, standard_mesh as standard_disease
FROM disease_mapping_full)
""")
temp_disease_mapping_df.createOrReplaceTempView("disease_mapping")

age_conditions = ("testAge1", "All Ages", "testAge2", "randomAge", "All", "Total")

globocan_df = spark.sql("""
SELECT 
    Country,
    case 
     when age_group in {age_conditions} then '0-120'
     else  trim(cast( age_group as string))  
     end as age_group,
    disease,
     gender,
  incidence,
 prevalence,
cast(year as float),
pt_batch_id,
 pt_file_id
FROM $$client_name_ctfo_datastore_staging_$$db_env.globocan
""".format(age_conditions=age_conditions))
globocan_df.createOrReplaceTempView("globocan_df")

drg_df = spark.sql("""
SELECT 
    country ,
    disease ,
 Population ,
     gender ,
     case 
     when age in {age_conditions} then '0-120'
     else  trim(cast( age as string))  
     end as age,
     cast(year as float) ,
  incidence,pt_batch_id,pt_file_id
FROM $$client_name_ctfo_datastore_staging_$$db_env.DRG
""".format(age_conditions=age_conditions))
drg_df.createOrReplaceTempView("drg_df")

dm_df = spark.sql("""
SELECT
    "data monitor" as data_source,
    Country as  country,
    "All" as state,
    case   when age in {age_conditions} then '0-120'
     else  trim(cast( age as string))  
     end as age,
    Gender as gender,
    cast(Year as float) as year,
    disease,
    Incidence as incidence,
    Prevalence as prevalence
    FROM $$client_name_ctfo_datastore_staging_$$db_env.data_monitor

""".format(age_conditions=age_conditions))
dm_df.createOrReplaceTempView("dm_df")


def min_age_func(val):
    if val.split('-')[0] == "85+":
        return "86"
    elif val.split('-')[0] == val:
        return val
    else:
        return val.split('-')[0]


def max_age_func(val):
    if val.split('-')[0] == "85+":
        return "100"
    elif val.split('-')[0] == val:
        return val
    else:
        return val.split('-')[1]


min_age_func_udf = udf(lambda z: min_age_func(z), StringType())
max_age_func_udf = udf(lambda z: max_age_func(z), StringType())

globocan_df_new = globocan_df.withColumn("min_age", min_age_func_udf(globocan_df['age_group'])).withColumn("max_age",
                                                                                                           max_age_func_udf(
                                                                                                               globocan_df[
                                                                                                                   'age_group']))
drg_df_new = drg_df.withColumn("min_age", min_age_func_udf(trim(drg_df['age']))).withColumn("max_age", max_age_func_udf(
    drg_df['age']))
dm_df_new = dm_df.withColumn("min_age", min_age_func_udf(dm_df['age'])).withColumn("max_age",
                                                                                   max_age_func_udf(dm_df['age']))

globocan_df_new.createOrReplaceTempView('globocan')
drg_df_new.createOrReplaceTempView('drg')
dm_df_new.createOrReplaceTempView('data_monotor')

spark.sql("""
SELECT
    "data monitor" as data_source,
    Country as  country,
    "All" as state,
    min_age,
     max_age,
    Gender as gender,
    Year as year,
    disease,
    Incidence as incidence,
    Prevalence as prevalence,
    Age as age
    FROM data_monotor

""").createOrReplaceTempView("data_monitor1")

spark.sql("""
SELECT distinct
    "drg" as data_source,
    Country as country,
    "All" as state,
    min_age,
    max_age,
    gender,
    year,
    disease,
    incidence,
    " " as prevalence,
    age
    FROM drg
	group by 1,2,3,4,5,6,7,8,9,10,11
""").createOrReplaceTempView("drg1")

spark.sql("""

SELECT
    "globocan" as data_source,
    Country as country,
    "All" as state,
    min_age,
    max_age,
    gender,
    year,
    disease,
    incidence,
    prevalence,
    age_group as age
    FROM globocan
""").createOrReplaceTempView("globocan1")


temp_patient_details_df = spark.sql("""
select * from data_monitor1
UNION
SELECT * FROM globocan1
UNION
SELECT * FROM drg1

""")
temp_patient_details_df = temp_patient_details_df.dropDuplicates()
# temp_patient_details_df.write.mode("overwrite").saveAsTable("temp_patient_details")
temp_patient_details_df.createOrReplaceTempView('temp_patient_details')

'''
age_std_min = spark.sql("""
select  distinct minimum_age,
standard_min_age  
from age_standardization
""")
age_std_min.createOrReplaceTempView('age_std_min')

age_std_max = spark.sql("""
select distinct maximum_age,
standard_max_age from age_standardization 
""")
age_std_max.createOrReplaceTempView('age_std_max')


spark.sql("""SELECT
t.country as country, coalesce(map_1.standard_country,t.country) as std_country,map_1.$$client_name_cluster as $$client_name_cluster, t.age,
cast(coalesce(case when asmn.standard_min_age = 'N/A' then '0' else asmn.standard_min_age end, '0') as BigInt) as min_age,
cast(coalesce(case when asmx.standard_max_age = 'N/A' then '100' else asmx.standard_max_age end, '100') as BigInt) as max_age ,
trim(coalesce(gender_std.std_gender, 'Other')) as gender, t.year, t.data_source,t.incidence, t.prevalence, "all" as state,
CASE WHEN lower(trim(data_source)) in  ("ihme","data monitor") THEN regexp_replace(t.disease, '[+]', ' ') ELSE t.disease END AS disease,
map_1.region
FROM temp_patient_details t
LEFT JOIN country_mapping map_1 on LOWER(TRIM(map_1.country)) = LOWER(TRIM(t.country)) AND map_1.country is NOT NULL
left join age_std_max asmx on LOWER(TRIM(t.max_age)) = LOWER(TRIM(asmx.maximum_age))
left join age_std_min asmn on LOWER(TRIM(t.min_age)) = LOWER(TRIM(asmn.minimum_age ))
left join gender_standardization gender_std on lower(trim(gender_std.gender))=lower(trim(t.gender))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

""").createOrReplaceTempView('temp_patient_details2')
'''

spark.sql("""SELECT
t.country as country, coalesce(map_1.standard_country,t.country) as std_country,map_1.$$client_name_cluster as $$client_name_cluster, t.age,
cast(coalesce(case when t.min_age = 'N/A' then '0' else t.min_age end, '0') as BigInt) as min_age,
cast(coalesce(case when t.max_age = 'N/A' then '100' else t.max_age end, '100') as BigInt) as max_age,
trim(coalesce(gender_std.std_gender, 'Other')) as gender, t.year, t.data_source,t.incidence, t.prevalence, "all" as state,
CASE WHEN lower(trim(data_source)) in  ("ihme","data monitor") THEN regexp_replace(t.disease, '[+]', ' ') ELSE t.disease END AS disease,
map_1.region
FROM temp_patient_details t
LEFT JOIN country_mapping map_1 on LOWER(TRIM(map_1.country)) = LOWER(TRIM(t.country)) AND map_1.country is NOT NULL
left join gender_standardization gender_std on lower(trim(gender_std.gender))=lower(trim(t.gender))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

""").createOrReplaceTempView('temp_patient_details2')

spark.sql("""SELECT
t.std_country as country, t.$$client_name_cluster, t.age, t.min_age, t.max_age,  t.gender, t.year, t.data_source, t.incidence, t.prevalence, t.state, t.region,
coalesce(x.standard_disease,'Unassigned') as standard_disease
FROM temp_patient_details2 t
LEFT OUTER JOIN disease_mapping x on 
lower(trim(regexp_replace(x.disease,'\\n|\\r','')))=lower(trim(regexp_replace(t.disease,'\\n|\\r','')))
""").createOrReplaceTempView('temp_patient_details3')

d_patient_details_df_1 = spark.sql("""SELECT
t.data_source, t.country,t.$$client_name_cluster, t.state, t.min_age, t.max_age,  t.gender, t.year, t.standard_disease as disease,
CASE WHEN data_source = "globocan" THEN "Oncology"
        ELSE INITCAP(trim(coalesce(a.therapeutic_area, 'Other')))
        END AS therapy_area,
t.region, t.prevalence, t.incidence, t.age
FROM temp_patient_details3 t
LEFT OUTER JOIN ta_mapping a on LOWER(TRIM(a.mesh_name)) = LOWER(TRIM(t.standard_disease))
""")
d_patient_details_df_1.createOrReplaceTempView('d_patient_details_df_1')

d_patient_details_df = spark.sql("""SELECT
data_source,INITCAP(country) as country,$$client_name_cluster,state,min_age,max_age,gender,cast(year as bigint),INITCAP(disease) as disease,INITCAP(therapy_area) as therapy_area,region,max(cast(prevalence as double)) as prevalence,max(cast(incidence as double)) as incidence,age from d_patient_details_df_1
group by 1,2,3,4,5,6,7,8,9,10,11,14
""")
# d_patient_details_df.createOrReplaceTempView('d_patient_details')
d_patient_details_df = d_patient_details_df.dropDuplicates()
d_patient_details_df.write.mode('overwrite').saveAsTable('d_patient_details')

write_path = path.replace("table_name", "d_patient_details")

d_patient_details_final = spark.sql("""
select d_patient_details.*, "$$cycle_id" as pt_cycle_id from d_patient_details
""")
d_patient_details_final.createOrReplaceTempView("d_patient_details_final")
# d_patient_details_final.write.format("parquet").mode("overwrite").save(path=write_path)


spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.d_patient_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   d_patient_details
""")

if d_patient_details_df.count() == 0:
    print("Skipping copy_hdfs_to_s3 for d_patient_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.d_patient_details")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")



