

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

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/dimensions/' \
       'table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

# Fetching geo_mapping
# geo_mapping = spark.read.format('csv').option('header', 'true') \
#     .load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/'
#           'country_standardization.csv')
# geo_mapping.write.mode('overwrite').saveAsTable('geo_mapping')
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
from (select distinct(standard_country) as standard_country,country
    from standard_country_mapping )  cs
left join
        sanofi_cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("geo_mapping")

temp_country = spark.sql("""
select
    concat_ws('\\\|', collect_set(country)) as country_list,
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
select
    A.geo_cd,
    A.standard_country,
    A.region,
    A.country_list,
    B.iso2_code,
    B.iso3_code,
    B.region_code
from temp_geo_cd A
left join geo_mapping B
on A.standard_country = B.standard_country
and A.region = B.region
group by 1,2,3,4,5,6,7
""")
d_geo.write.mode('overwrite').saveAsTable('d_geo')
write_path = path.replace('table_name', 'd_geo')
d_geo.repartition(100).write.mode('overwrite').parquet(write_path)

# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.d_geo partition(
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
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.d_geo')



