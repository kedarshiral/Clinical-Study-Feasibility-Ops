################################# Module Information ######################################
#  Module Name         : Patient Details reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_patient_details
#
#  Pre-requisites      : Source table required: DRG, Data Monitor, Globocan, IHME
#  Last changed on     : 09-23-2021
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



data_dt = "$$data_dt"
cycle_id = "$$cycle_id"

path = bucket_path + "/applications/commons/temp/" \
       "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
           "kpi_output_dimension/table_name"
		   
		   
		   
		   
f_rpt_patient_details_df = spark.sql("""SELECT
data_source,country,$$client_name_cluster,state,min_age,max_age,gender,year,disease,therapy_area,region,prevalence,incidence,age from $$client_name_ctfo_datastore_app_commons_$$db_env.d_patient_details
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
""")
# f_rpt_patient_details_df.createOrReplaceTempView('f_rpt_patient_details')
f_rpt_patient_details_df = f_rpt_patient_details_df.dropDuplicates()
f_rpt_patient_details_df.write.mode('overwrite').saveAsTable('f_rpt_patient_details')

write_path = path.replace("table_name", "f_rpt_patient_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_patient_details")
write_path_csv = write_path_csv + "_csv/"
# f_rpt_patient_details_df.write.format("parquet").mode("overwrite").save(path=write_path)

f_rpt_patient_details_final = spark.sql("""
select f_rpt_patient_details.*, "$$cycle_id" as pt_cycle_id from f_rpt_patient_details
""")
f_rpt_patient_details_final.createOrReplaceTempView("f_rpt_patient_details_final")
f_rpt_patient_details_final.write.format("parquet").mode("overwrite").save(path=write_path)


f_rpt_patient_details_final.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option("header", "false").option("sep", "|").option('quote', '"') \
    .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_patient_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   f_rpt_patient_details
""")

if f_rpt_patient_details_df.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_patient_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_patient_details")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
