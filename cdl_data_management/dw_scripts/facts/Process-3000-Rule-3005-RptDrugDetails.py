################################# Module Information ######################################
#  Module Name         : Site reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_site_study_details_filters
#                           b. f_rpt_site_study_details
#                           c. f_rpt_src_site_details
#                           d. f_rpt_investigator_details
#                           e. f_rpt_site_study_details_non_performance
#  Pre-requisites      : Source table required:
#  Last changed on     : 21-12-2021
#  Last changed by     : Kashish
#  Reason for change   : incorporated usl
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
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import initcap

import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

path = bucket_path + "/applications/commons/temp/" \
       "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
           "kpi_output_dimension/table_name"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
########## about trial, d_trial.nct_id not present


import json
with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

#Save Values in Json
Ongoing = data["Ongoing"]
Completed = data["Completed"]
Planned = data["Planned"]
Others = data["Others"]

Ongoing_variable = tuple(Ongoing)
Completed_variable = tuple(Completed)
Planned_variable = tuple(Planned)
Others_variable = tuple(Others)

Ongoing_Completed = tuple(Ongoing)+tuple(Completed)
Ongoing_Planned = tuple(Ongoing)+tuple(Planned)
Ongoing_Completed_Planned = tuple(Ongoing)+tuple(Completed)+tuple(Planned)


standard_country_mapping_temp = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/standard_country_mapping.csv".format(bucket_path=bucket_path))
standard_country_mapping_temp.createOrReplaceTempView("standard_country_mapping_temp")

standard_country_mapping = spark.sql("""
select distinct * from (select   country, standard_country from standard_country_mapping_temp 
union 
select standard_country, standard_country from standard_country_mapping_temp)
""")
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

cluster_pta_mapping = spark.read.format("csv").option("header", "false").option('multiline', 'True').schema(
    customSchema).load("{bucket_path}/"
                       "uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

geo_mapping= spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.$$client_name_cluster as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
scpm.post_trial_details
from (select distinct *
	from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

#disease_mapping = spark.read.format('csv').option("header", "true"). \
#    option("delimiter", ","). \
#    load("{bucket_path}/uploads/FINAL_DISEASE_MAPPING/"
#         "disease_mapping.csv".format(bucket_path=bucket_path))
disease_mapping_temp= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping""")
disease_mapping_temp.createOrReplaceTempView('disease_mapping_temp')

disease_mapping = spark.sql("""select distinct * from (select mesh, standard_mesh from disease_mapping_temp
union select standard_mesh, standard_mesh from disease_mapping_temp) """)
# disease_mapping_1.registerTempTable('disease_mapping_1')
disease_mapping.write.mode('overwrite').saveAsTable('disease_mapping')

#ta_mapping = spark.read.format('csv').option("header", "true"). \
#    option("delimiter", ","). \
#    load("{bucket_path}/uploads/TA_MAPPING/"
#         "ta_mapping.csv".format(bucket_path=bucket_path))
ta_mapping= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_to_ta_mapping""")
ta_mapping.createOrReplaceTempView('ta_mapping')


##############create drug reporting table

drug_country_status = spark.sql("""select distinct drug_id,drug_nm, country_name1 as country_name,
trim(country_status1) as
country_status from $$client_name_ctfo_datastore_app_commons_$$db_env.d_drug lateral view
posexplode(split(country_nm,"\\\|"))one
as pos1,country_name1 lateral view posexplode(split(country_status,"\\\|"))two as pos2,
country_status1 where pos1=pos2""")

drug_country_status.write.mode("overwrite").saveAsTable("drug_country_status")

### creating temp disease mapping table having only distinct disease name and standard disease names

temp_disease_mapping_df = spark.sql("""
SELECT
mesh as disease, standard_mesh as standard_disease
FROM disease_mapping
GROUP BY 1,2
""")
temp_disease_mapping_df.createOrReplaceTempView("temp_disease_mapping")

drug_disease_status = spark.sql("""
select distinct  drug_id,drug_nm, 
case when (trim(dis_map.standard_disease)!='' and dis_map.standard_disease is not null) then dis_map.standard_disease 
else 'Unassigned' end as disease_name, disease_status
from
(select distinct  drug_id,drug_nm, disease_name1 as disease_name,
trim(disease_status1) as
disease_status from $$client_name_ctfo_datastore_app_commons_$$db_env.d_drug lateral view
posexplode(split(regexp_replace(disease_nm,"\;","\\\|"),"\\\|"))one
as pos1,disease_name1 lateral view
  posexplode(split(regexp_replace(disease_status,"\;","\\\|"),"\\\|"))two as pos2,disease_status1
  where pos1=pos2
  ) temp1
left join temp_disease_mapping dis_map on trim(lower(temp1.disease_name)) = trim(lower(dis_map.disease))
  """)

drug_disease_status.write.mode("overwrite").saveAsTable("drug_disease_status")

f_rpt_drug_details = spark.sql(""" select distinct a.drug_id,
a.drug_nm,
a.global_status,
a.originator,
a.originator_status,
a.originator_country,
INITCAP(trim(c.disease_name)) as disease_name,
case when trim(c.disease_status)='' then null else c.disease_status end as disease_status,
map_1.standard_country as country,
map_1.$$client_name_cluster as $$client_name_cluster,
case when trim(b.country_status)='' then null else b.country_status end as country_status,
map_1.iso2_code as country_code,
INITCAP(trim(coalesce(map_2.therapeutic_area, 'Other'))) as therapeutic_class_nm,
a.drug_name_synonyms,
a.delivery_route,
map_1.region
from $$client_name_ctfo_datastore_app_commons_$$db_env.d_drug a
left outer join drug_country_status b on lower(trim(a.drug_id)) =lower(trim(b.drug_id))
left outer join drug_disease_status c on lower(trim(a.drug_id)) =lower(trim(c.drug_id))
left join country_mapping map_1 on lower(trim(b.country_name)) = lower(trim(map_1.standard_country))
left join ta_mapping map_2 on lower(trim(map_2.mesh_name)) = lower(trim(c.disease_name))
""")

f_rpt_drug_details = f_rpt_drug_details.dropDuplicates()
f_rpt_drug_details.write.mode("overwrite").saveAsTable("f_rpt_drug_details")

write_path = path.replace("table_name", "f_rpt_drug_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_drug_details")
write_path_csv = write_path_csv + "_csv/"

f_rpt_drug_details_final = spark.sql("""
select f_rpt_drug_details.*, "$$cycle_id" as pt_cycle_id from f_rpt_drug_details
""")
f_rpt_drug_details_final.createOrReplaceTempView("f_rpt_drug_details_final")
f_rpt_drug_details_final.write.format("parquet").mode("overwrite").save(path=write_path)


if "$$flag" == "Y":
    f_rpt_drug_details_final.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option("header",
                                                                                                           "false").option(
        "sep", "|") \
        .option('quote', '"').option(
        'escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_drug_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_drug_details
""")

if f_rpt_drug_details.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_drug_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_drug_details")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
