

################################# Module Information ######################################
#  Module Name         : D_INVESTIGATOR
#  Purpose             : This will create target table d_investigator
#  Pre-requisites      : Source table required: unique_investigator, xref_src_investigator, d_geo
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from source table
# 2. Pass through the source table on key columns to create final target table
############################################################################################

import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col, lit

sqlContext = SQLContext(spark)


def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

# getting data from final precedence table and standardizing country and region
temp_d_investigator_1 = spark.sql("""
select /*+ broadcast(d) */
 ctfo_investigator_id
,regexp_replace(get_ascii_value(investigator_name),"[^0-9A-Za-z, ()_-]","") as investigator_name
,investigator_phone
,investigator_email
,investigator_specialty
,regexp_replace(get_ascii_value(investigator_city),"[^0-9A-Za-z, ()_-]","") as investigator_city
,regexp_replace(get_ascii_value(investigator_state),"[^0-9A-Za-z, ()_-]","") as investigator_state
,investigator_zip
,regexp_replace(get_ascii_value(coalesce(d.standard_country,investigator_country, 'Others') ),"[^0-9A-Za-z, ()_-]","") as investigator_country
,regexp_replace(get_ascii_value(investigator_address),"[^0-9A-Za-z, ()_-]","") as investigator_address
,d.geo_cd,
"$$data_dt" as pt_data_dt,
"$$cycle_id" as pt_cycle_id
from final_precedence_investigator a
left outer join (select geo_cd, standard_country, country
        from sanofi_ctfo_datastore_app_commons_$$db_env.d_geo lateral view
        explode(split(country_list, '\\\|'))one as country
        group by 1,2,3) d
on regexp_replace(trim(lower(investigator_country)),'[^0-9A-Za-z ]','') =
regexp_replace(trim(lower(d.country)),'[^0-9A-Za-z ]','')
""")
temp_d_investigator_1 = temp_d_investigator_1.dropDuplicates()

temp_d_investigator_1.registerTempTable("d_investigator")

# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.d_investigator
partition(pt_data_dt,pt_cycle_id)
select *
from
    d_investigator
""")

# Closing spark context
try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")

# Insert data on S3
CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_commons_$$db_env.d_investigator")





