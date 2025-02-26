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
from (select distinct *
	from standard_country_mapping )   cs
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

############# Trial-Site-Investigator Filters
f_rpt_filters_site_inv = spark.sql("""
select
    r_trial_site.ctfo_trial_id, 
    r_trial_site.ctfo_site_id, 
    ctfo_investigator_id, region, 
    INITCAP(country) as country,
    temp_site_info.$$client_name_cluster, 
    country_code,
    concat_ws('',NULLIF(trim(r_trial_site.ctfo_trial_id),''), NULLIF(trim(r_trial_site.ctfo_site_id),'')) as trial_site_id,
    concat_ws('',NULLIF(trim(r_trial_site.ctfo_trial_id),''), NULLIF(trim(r_trial_site.ctfo_site_id),''),
    NULLIF(trim(ctfo_investigator_id),'')) as trial_site_inv_id
from
    (select i_ctfo_trial_id ctfo_trial_id, i_ctfo_site_id ctfo_site_id,
    i_ctfo_investigator_id ctfo_investigator_id
    from temp_inv_info) r_trial_site
left join
    (select distinct s_ctfo_site_id,$$client_name_cluster, region, country,country_code from temp_site_info)
    temp_site_info
on lower(trim(r_trial_site.ctfo_site_id))=lower(trim(temp_site_info.s_ctfo_site_id))
group by 1,2,3,4,5,6,7,8,9
order by trial_site_id, trial_site_inv_id
""")
f_rpt_filters_site_inv = f_rpt_filters_site_inv.dropDuplicates()
#f_rpt_filters_site_inv.registerTempTable("f_rpt_filters_site_inv")
f_rpt_filters_site_inv.write.mode('overwrite').saveAsTable("f_rpt_filters_site_inv")

write_path = path.replace("table_name", "f_rpt_filters_site_inv")
write_path_csv = path_csv.replace("table_name", "f_rpt_filters_site_inv")
write_path_csv = write_path_csv + "_csv/"
f_rpt_filters_site_inv.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_filters_site_inv.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option("header",
                                                                                                         "false").option(
        "sep", "|") \
        .option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)


##Need to update the ddl
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_site_inv partition(pt_data_dt,pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_filters_site_inv
""")

if f_rpt_filters_site_inv.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_filters_site_inv as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_site_inv")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")