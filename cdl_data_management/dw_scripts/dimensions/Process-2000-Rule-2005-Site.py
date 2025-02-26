################################# Module Information ######################################
#  Module Name         : D_SITE
#  Purpose             : This will create target table d_site
#  Pre-requisites      : L1 source table required: $$client_name_ctfo_datastore_app_commons_$$db_env.
# unique_site, xref_src_site, d_geo
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from source table
# 2. Pass through the source tables on key columns to create final target table
############################################################################################
import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.functions import initcap

import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

sqlContext = SQLContext(spark)


def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)

data_dt = "$$data_dt"
cycle_id = "$$cycle_id"

path = "{bucket_path}/applications/commons/temp/kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id".format(bucket_path=bucket_path)

path_csv = "{bucket_path}/applications/commons/temp/kpi_output_dimension/table_name".format(bucket_path=bucket_path)

cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load("{bucket_path}/uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

# getting data from final precedence table and standardizing country,city and region

d_site_1 = spark.sql("""
SELECT /*+ broadcast(d,e) */
ctfo_site_id,
regexp_replace(get_ascii_value(site_name),"[^0-9A-Za-z, ()_-]","") as site_name,
regexp_replace(get_ascii_value(site_address),"[^0-9A-Za-z, ()_-]","") site_address,
regexp_replace(get_ascii_value(site_city),"[^0-9A-Za-z, ()_-]","") as site_city,
regexp_replace(get_ascii_value(site_state),"[^0-9A-Za-z, ()_-]","") site_state,
regexp_replace(get_ascii_value(coalesce(a.site_country, 'Other')),"[^0-9A-Za-z, ()_-]","") as site_country,
site_zip,
d.geo_cd,
a.site_supporting_urls
from final_precedence_site a
left outer join
        (select geo_cd, standard_country, country
        from $$client_name_ctfo_datastore_app_commons_$$db_env.d_geo lateral view
         explode(split(country_list, '\\\|'))one as country
        group by 1,2,3) d
on regexp_replace(trim(lower(site_country)),'[^0-9A-Za-z, ()_-]','') =
regexp_replace(trim(lower(d.standard_country)),'[^0-9A-Za-z, ()_-]','')
""")
d_site_1 = d_site_1.dropDuplicates()
d_site_1.createOrReplaceTempView("d_site_1")

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
from d_site_1 a
left join cluster_pta_mapping scpm on lower(trim(a.site_country))=lower(trim(scpm.standard_country))
""")
d_site_2 = d_site_2.dropDuplicates()
d_site_2.createOrReplaceTempView("d_site_2")
write_path = path.replace("table_name", "d_site")
write_path_csv = path_csv.replace("table_name", "d_site")
write_path_csv = write_path_csv + "_csv/"
d_site_2.coalesce(1).write.format("parquet").mode("overwrite").save(path=write_path)

if "Y" == "Y":
    d_site_2.coalesce(1).write.option("header", "false").option("sep", "|") \
        .option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

##need to update the ddl
# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.d_site partition(pt_data_dt='$$data_dt',pt_cycle_id='$$cycle_id') select * from d_site_1 """)
# Closing spark context
try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")

# Insert data on S3
CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.d_site")