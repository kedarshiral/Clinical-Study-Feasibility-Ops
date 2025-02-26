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
#  Last changed by     : Vicky|Piyush X
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
scpm.$$client_name_cluster_code as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country,country
    from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

disease_mapping_temp= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping""")
disease_mapping_temp.createOrReplaceTempView('disease_mapping_temp')

disease_mapping = spark.sql("""select distinct * from (select mesh, standard_mesh from disease_mapping_temp
union select standard_mesh, standard_mesh from disease_mapping_temp) """)
# disease_mapping_1.registerTempTable('disease_mapping_1')
disease_mapping.write.mode('overwrite').saveAsTable('disease_mapping')


ta_mapping= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_to_ta_mapping""")
ta_mapping.createOrReplaceTempView('ta_mapping')


trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

top20_pharma = spark.read.format("com.crealytics.spark.excel").option("header", "true")\
    .load("{bucket_path}/uploads/SPONSOR/Top20_Sponsors/Top20_Sponsors.xlsx".format(bucket_path=bucket_path))
top20_pharma.dropDuplicates()
top20_pharma.createOrReplaceTempView('top20_pharma')

temp_drug_info = spark.sql("""
select /*+ broadcast(d_drug) */
    ctfo_trial_id as d_ctfo_trial_id, drug_nm, drug_name_synonyms, global_status, cas_num
from
    (select ctfo_trial_id, drug_id
    from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_drug  group by 1,2) r_trial_drug
left join
    (select drug_id, drug_nm, drug_name_synonyms, global_status, cas_num
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_drug group by 1,2,3,4,5) d_drug
on lower(trim(r_trial_drug.drug_id))=lower(trim(d_drug.drug_id)) group by 1,2,3,4,5
""")
#temp_drug_info.createOrReplaceTempView("temp_drug_info")
temp_drug_info.write.mode("overwrite").saveAsTable("temp_drug_info")
# write_path = path.replace("table_name", "temp_drug_info")

temp_rpt_trial_sponsor = spark.sql("""       
select distinct
       ctfo_trial_id,
       sponsor_id,
       case when min(case_type) = 'N' then concat_ws("\^",sort_array(collect_set(distinct exploded_subsidiary_sponsor),true))
       when min(case_type) = 'Y' then first(parent_sponsor) end as subsidiary_sponsors
from(select distinct a.ctfo_trial_id,
                     a.sponsor_id,
                     a.subsidiary_sponsor_id,
                     c.parent_sponsor,
                     case when c.parent_sponsor is not null then null
                     else exploded_subsidiary_sponsor end as exploded_subsidiary_sponsor,
                     case when c.parent_sponsor is not null or c.parent_sponsor != '' then 'Y'
                     else 'N' end as case_type
from(select distinct ctfo_trial_id,sponsor_id,trim(ss_ids) as subsidiary_sponsor_id
from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_sponsor
lateral view outer explode(split(subsidiary_sponsor_ids, '\\\^')) as ss_ids
group by 1,2,3)a
left join $$client_name_ctfo_datastore_app_commons_$$db_env.d_subsidiary_sponsor b
on lower(trim(a.sponsor_id)) = lower(trim(b.sponsor_id)) and lower(trim(a.subsidiary_sponsor_id)) = lower(trim(b.subsidiary_sponsor_id))
left join top20_pharma c
on lower(trim(b.subsidiary_sponsor)) = lower(trim(c.parent_sponsor))
lateral view outer explode(split(subsidiary_sponsor, '\\\^')) as exploded_subsidiary_sponsor
)
group by ctfo_trial_id,sponsor_id
""")
temp_rpt_trial_sponsor= temp_rpt_trial_sponsor.dropDuplicates()
#temp_rpt_trial_sponsor.createOrReplaceTempView('temp_rpt_trial_sponsor')
temp_rpt_trial_sponsor.write.mode("overwrite").saveAsTable('temp_rpt_trial_sponsor')

temp_sponsor_info = spark.sql("""
select distinct
        ctfo_trial_id as s_ctfo_trial_id,
        CASE WHEN d_spsr.sponsor_nm like '(%)' or d_spsr.sponsor_nm  like '[%]' 
             THEN SUBSTRING(d_spsr.sponsor_nm, 2, LENGTH(d_spsr.sponsor_nm)-2) 
             ELSE d_spsr.sponsor_nm 
        END as sponsor_nm,
        CASE WHEN r_trial_sponsor.subsidiary_sponsors like '(%)' or r_trial_sponsor.subsidiary_sponsors  like '[%]' 
             THEN SUBSTRING(r_trial_sponsor.subsidiary_sponsors, 2, LENGTH(r_trial_sponsor.subsidiary_sponsors)-2) 
             ELSE r_trial_sponsor.subsidiary_sponsors
        END as subsidiary_sponsors,
        d_spsr.sponsor_type_new as sponsor_type
from (select ctfo_trial_id, sponsor_id,subsidiary_sponsors from default.temp_rpt_trial_sponsor
group by 1,2,3) r_trial_sponsor
left join
        (select dsp.sponsor_id, dsp.sponsor_name as sponsor_nm,sponsor_type_new   from
        $$client_name_ctfo_datastore_app_commons_$$db_env.d_sponsor as dsp
        lateral view outer explode(split(dsp.sponsor_type, "\\\;")) as sponsor_type_new
        group by 1, 2,3)d_spsr
        on lower(trim(d_spsr.sponsor_id))=lower(trim(r_trial_sponsor.sponsor_id)) group by 1,2,3,4
""")
#temp_sponsor_info.registerTempTable("temp_sponsor_info")
temp_sponsor_info.write.mode("overwrite").saveAsTable("temp_sponsor_info")
# write_path = path.replace("table_name", "temp_sponsor_info")


############# Trial related Filters  ##enhancement made during ctfoh1 integration
f_rpt_filters_trial = spark.sql("""
select distinct
    temp_inv_info.ctfo_trial_id, coalesce(trial_status, 'Others') trial_status,
     coalesce(mesh_exploded, 'Other') mesh_term,
    coalesce(gender, 'Other') gender, 
    cast(minimum_age as BigInt) as minimum_age,
     cast(maximum_age as BigInt) as maximum_age,
    coalesce(NULLIF(sponsor_nm, ''), 'Other') as sponsor,
    coalesce(NULLIF(subsidiary_sponsors, ''), 'Other') as  subsidiary_sponsors,
    coalesce(NULLIF(sponsor_type, ''), 'Other') as  sponsor_type,
    coalesce(NULLIF(drug_nm, ''), 'Other') as  drug_names,
    coalesce(NULLIF(drug_name_synonyms, ''), 'Other') as  drug_name_synonyms,
    coalesce(NULLIF(global_status, ''), 'Other') as drug_status, coalesce(cas_num, 'Other') cas_num
from
    (select distinct i_ctfo_trial_id as ctfo_trial_id from temp_inv_info group by 1) temp_inv_info
inner join
        (select distinct ctfo_trial_id, trial_status, mesh_terms, patient_gender as gender,
         trial_age_min as minimum_age,
        trial_age_max as maximum_age  from $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial
        group by 1,2,3,4,5,6) d_trial
on lower(trim(d_trial.ctfo_trial_id)) = lower(trim(temp_inv_info.ctfo_trial_id))
left join temp_sponsor_info on lower(trim(temp_inv_info.ctfo_trial_id))=lower(trim(temp_sponsor_info.s_ctfo_trial_id))
left join temp_drug_info on lower(trim(temp_inv_info.ctfo_trial_id))=lower(trim(temp_drug_info.d_ctfo_trial_id))
lateral view outer explode(split(mesh_terms,"\\\|")) mesh as mesh_exploded
order by temp_inv_info.ctfo_trial_id
""")
f_rpt_filters_trial = f_rpt_filters_trial.dropDuplicates()
#f_rpt_filters_trial.registerTempTable("f_rpt_filters_trial")
f_rpt_filters_trial.write.mode('overwrite').saveAsTable('f_rpt_filters_trial')

write_path = path.replace("table_name", "f_rpt_filters_trial")
write_path_csv = path_csv.replace("table_name", "f_rpt_filters_trial")
write_path_csv = write_path_csv + "_csv/"
f_rpt_filters_trial.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_filters_trial.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option("header",
                                                                                                      "false").option(
        "sep", "|") \
        .option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_trial partition(pt_data_dt,pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_filters_trial
""")

if f_rpt_filters_trial.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_filters_trial as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_trial")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")