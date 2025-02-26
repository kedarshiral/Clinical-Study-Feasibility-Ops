################################# Module Information ######################################
#  Module Name         : Site reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_site_study_details_filters
#                           b. f_rpt_site_study_details
#                           c. f_rpt_src_site_details
#                           d. f_rpt_investigator_details
#                           e. f_rpt_site_study_details_non_performance
#  Pre-requisites      : Source table required:
#  Last changed on     : 01-09-2022
#  Last changed by     : Vicky
#  Reason for change   : Added therapeutic_area, indication, phase, trial_title ,trial_start_dt,enrollment_close_dt,  enrollment_duration,patient_dropout_rate, no_of_sites
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Create required table and stores data on HDFS
############################################################################################
import datetime
from io import BytesIO
import boto3
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ShortType
from pyspark.sql.functions import lower, col
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"
#
# path_csv = bucket_path + "/applications/commons/temp/" \
#                          "kpi_output_dimension/table_name"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")


def camel_to_snake(name):
    s1 = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    s2 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s1)
    s3 = re.sub(r'[\s_]+', '_', s2)
    s4 = re.sub(r'^_|_$', '', s3)
    return s4.lower()


def rename_columns(df):
    # Generate new column names
    new_column_names = {col: camel_to_snake(col) for col in df.columns}

    # Rename columns
    for old_col, new_col in new_column_names.items():
        df = df.withColumnRenamed(old_col, new_col)

    return df


standard_country_mapping_temp = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/standard_country_mapping.csv".format(bucket_path=bucket_path))
standard_country_mapping_temp.createOrReplaceTempView("standard_country_mapping_temp")

standard_country_mapping = spark.sql("""
select distinct * from (select country, standard_country from standard_country_mapping_temp
union
select standard_country, standard_country from standard_country_mapping_temp)
""")
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

# s3 = boto3.client('s3')
# timestamp = datetime.now().strftime("%Y%m%d")
# bucket_name = bucket_path.split('/')[0][5:]
file_path = bucket_path + '/uploads/country_footprint.xlsx'

df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(file_path)

# obj = s3.get_object(Bucket=bucket_name, Key=file_key)
# excel_data = obj['Body'].read()
#
# xls = pd.ExcelFile(BytesIO(excel_data))
#
# df = pd.read_excel(xls, sheet_name='Country Complexity Assignments')
df = rename_columns(df)
df = df.withColumn("compliance_score", col("compliance_score").cast("integer")).withColumn("privacy_score", col("privacy_score").cast("integer"))
double_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'double']
for column in double_columns:
	df = df.withColumn(column, col(column).cast("integer"))

# schema = StructType([
#     StructField("country", StringType(), True),
#     StructField("region", StringType(), True),
#     StructField("in_footprint", StringType(), True),
#     StructField("commercial_footprint", StringType(), True),
#     StructField("clinical_operations", StringType(), True),
#     StructField("regulatory_score", ShortType(), True),
#     StructField("regulatory_score_rationale", StringType(), True),
#     StructField("market_access", StringType(), True),
#     StructField("gcsc_score", ShortType(), True),
#     StructField("gcsc_score_rationale", StringType(), True),
#     StructField("privacy_score", ShortType(), True),
#     StructField("privacy_score_rationale", StringType(), True),
#     StructField("legal_score", ShortType(), True),
#     StructField("legal_score_rationale", StringType(), True),
#     StructField("compliance_score", ShortType(), True),
#     StructField("compliance_score_rationale", StringType(), True),
#     StructField("tls_score", ShortType(), True),
#     StructField("tls_score_rationale", StringType(), True),
#     StructField("pk_score", ShortType(), True),
#     StructField("pk_score_rationale", StringType(), True),
#     StructField("pvg_score", ShortType(), True),
#     StructField("pvg_score_rationale", StringType(), True),
#     StructField("complexity_score", StringType(), True)
# ])
#
# df = spark.createDataFrame(df, schema=schema)

d_rpt_country_1 = df.filter((df['country'].isNotNull()) & (df['country'] != ''))

d_rpt_country_1.write.mode("overwrite").saveAsTable("d_rpt_country_1")
d_rpt_country = spark.sql("""
select coalesce(b.standard_country, f.country) as country, f.region, f.in_footprint, f.commercial_footprint, f.clinical_operations, f.regulatory_score, f.regulatory_score_rationale, f.market_access, f.gcsc_score, f.gcsc_score_rationale, f.privacy_score, f.privacy_score_rationale, f.legal_score, f.legal_score_rationale, f.compliance_score, f.compliance_score_rationale, f.tls_score, f.tls_score_rationale, f.pk_score, f.pk_score_rationale, f.pvg_score, f.pvg_score_rationale, f.complexity_score from d_rpt_country_1 f
left join
(select distinct country, standard_country from standard_country_mapping) b on lower(trim(f.country)) = lower(trim(b.country))
""")
# write_path_csv = path_csv.replace("table_name", "d_rpt_country")
# write_path_csv = write_path_csv + "_csv/"
#
# d_rpt_country = d_rpt_country.dropDuplicates()
# d_rpt_country.coalesce(1).write.format("csv").mode("overwrite").save(path=write_path_csv)

d_rpt_country.registerTempTable("d_rpt_country")

write_path = path.replace("table_name", "d_rpt_country")
d_rpt_country1 = spark.sql("""select d_rpt_country.*, "$$cycle_id" as pt_cycle_id from d_rpt_country""")
d_rpt_country1 = d_rpt_country1.dropDuplicates()
d_rpt_country1.write.mode("overwrite").saveAsTable("d_rpt_country1")
d_rpt_country1.write.format("parquet").mode("overwrite").save(path=write_path)


spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.d_rpt_country partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    d_rpt_country
""")

if d_rpt_country.count() == 0:
    print("Skipping copy_hdfs_to_s3 for d_rpt_country as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.d_rpt_country")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")