
################################# Module Information ######################################
#  Module Name         : D_GEO
#  Purpose             : This will create target table d_geo
#  Pre-requisites      : L1 source table required: citeline_organization, citeline_investigator,
# aact_facilities
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from L1 layer table
# 2. Pass through the L1 table on key columns to create final target table
############################################################################################

import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col, lit
sqlContext = SQLContext(spark)

import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = "{bucket_path}/applications/commons/temp/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id".format(bucket_path=bucket_path)

def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/standard_country_mapping.csv".format(bucket_path=bucket_path))
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.$$client_name_cluster_code as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country,country
    from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("geo_mapping")

temp_country = spark.sql("""
select
    concat_ws('\\|', collect_set(country)) as country_list,
    standard_country, region
from geo_mapping
group by 2,3
""")
temp_country.createOrReplaceTempView("temp_country")

temp_geo_cd = spark.sql("""
select
    concat("geo_cd_",row_number() over(order by standard_country, region)) as geo_cd,
    standard_country, country_list, region
from temp_country
""")
temp_geo_cd.createOrReplaceTempView("temp_geo_cd")

d_geo = spark.sql("""
select geo_cd, INITCAP(standard_country) as standard_country, region, country_list, iso2_code, iso3_code, region_code
from (select
    A.geo_cd,
    regexp_replace(get_ascii_value(A.standard_country),"[^0-9A-Za-z, ()_-]","") as standard_country, 
	A.region,
    A.country_list,
    B.iso2_code,
    B.iso3_code,
    B.region_code
from temp_geo_cd A
left join geo_mapping B
on lower(trim(A.standard_country)) = lower(trim(B.standard_country))
and lower(trim(A.region)) = lower(trim(B.region))
group by 1,2,3,4,5,6,7)
""")
d_geo.write.mode('overwrite').saveAsTable('d_geo')
write_path = path.replace('table_name', 'd_geo')
d_geo.repartition(100).write.mode('overwrite').parquet(write_path)

# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.d_geo partition(
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id')
select *
from d_geo
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.d_geo')