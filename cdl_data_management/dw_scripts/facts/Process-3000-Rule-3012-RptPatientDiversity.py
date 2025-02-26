################################# Module Information ######################################
#  Module Name         : Patient Diversity reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_patient_diversity
#
#  Pre-requisites      : Source table required: citeline_investigator, xref_src_investigator, healthverity
#  Last changed on     : 03-11-2021
#  Last changed by     : Kashish Mogha
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
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf
from CommonUtils import *
from pyspark.sql.functions import col, log, lit
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, when
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import col, log, lit, udf
from pyspark.sql.types import DoubleType

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

data_dt = "$$data_dt"
cycle_id = "$$cycle_id"

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
                         "kpi_output_dimension/table_name"

# for standard site names
# state_mapping = spark.read.format("csv").option("header", "true").load(
#     "{bucket_path}/uploads/state_mapping/".format(bucket_path=bucket_path))
# state_mapping.registerTempTable("state_mapping_temp_table")
#
# temp_state_mapping_df = spark.sql("""
# select distinct * from (SELECT
# state,standardized_state
# FROM state_mapping_temp_table
# union
# select standardized_state,standardized_state
# FROM state_mapping_temp_table)
# """)
#
# temp_state_mapping_df.createOrReplaceTempView("state_mapping")

disease_mapping_temp= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping""")
disease_mapping_temp.createOrReplaceTempView('disease_mapping_temp')

disease_mapping = spark.sql("""select distinct * from (select mesh, standard_mesh from disease_mapping_temp
) """)
disease_mapping.write.mode('overwrite').saveAsTable('disease_mapping')

filtered_health_df = spark.sql("""select DISTINCT
              npi_number as npi,
              provider_sex as gender,
              city_1 as city,
              state_1 as state,
              zip_code_1 as zip,
              specialty,
              CAST(null as string) as count_asian,
              CAST(null as string) as count_black,
              CAST(null as string) as count_hispanic,
              CAST(null as string) as count_white,
              percent_asian,
              percent_black ,
              percent_hispanic,
              percent_white,
              percent_unknown,
              percent_american_indian_alaska,
			  percent_pacific_islander,
			  indication
            from
              $$client_name_ctfo_datastore_staging_$$db_env.h1_diversity """)

filtered_health_df.registerTempTable("filtered_health_df")

# Calculate entropy and predominant diverse group using SQL
entropy_df = spark.sql("""
SELECT *,
    (

    CASE WHEN COALESCE(percent_white, 0) != 0 THEN COALESCE(percent_white, 0) * (log(1.0 / COALESCE(percent_white, 0)) / log(7.0)) ELSE 0 END +
    CASE WHEN COALESCE(percent_black, 0) != 0 THEN COALESCE(percent_black, 0) * (log(1.0 / COALESCE(percent_black, 0)) / log(7.0)) ELSE 0 END +
    CASE WHEN COALESCE(percent_asian, 0) != 0 THEN COALESCE(percent_asian, 0) * (log(1.0 / COALESCE(percent_asian, 0)) / log(7.0)) ELSE 0 END +
	CASE WHEN COALESCE(percent_pacific_islander, 0) != 0 THEN COALESCE(percent_pacific_islander, 0) * (log(1.0 / COALESCE(percent_pacific_islander, 0)) / log(7.0)) ELSE 0 END +
    CASE WHEN COALESCE(percent_hispanic, 0) != 0 THEN COALESCE(percent_hispanic, 0) * (log(1.0 / COALESCE(percent_hispanic, 0)) / log(7.0)) ELSE 0 END +
    CASE WHEN COALESCE(percent_unknown, 0) != 0 THEN COALESCE(percent_unknown, 0) * (log(1.0 / COALESCE(percent_unknown, 0)) / log(7.0)) ELSE 0 END +
    CASE WHEN COALESCE(percent_american_indian_alaska, 0) != 0 THEN COALESCE(percent_american_indian_alaska, 0) * (log(1.0 / COALESCE(percent_american_indian_alaska, 0)) / log(7.0)) ELSE 0 END

    ) AS Entropy,
    CASE
        WHEN (COALESCE(percent_black, 0) >= COALESCE(percent_asian, 0)) AND
             (COALESCE(percent_black, 0) >= COALESCE(percent_american_indian_alaska, 0)) AND
			 (COALESCE(percent_black, 0) >= COALESCE(percent_pacific_islander, 0)) AND
             (COALESCE(percent_black, 0) >= COALESCE(percent_hispanic, 0)) THEN 'black'
        WHEN (COALESCE(percent_asian, 0) > COALESCE(percent_black, 0)) AND
             (COALESCE(percent_asian, 0) >= COALESCE(percent_american_indian_alaska, 0)) AND
			 (COALESCE(percent_asian, 0) >= COALESCE(percent_pacific_islander, 0)) AND
             (COALESCE(percent_asian, 0) >= COALESCE(percent_hispanic, 0)) THEN 'asian'
        WHEN (COALESCE(percent_hispanic, 0) > COALESCE(percent_black, 0)) AND
             (COALESCE(percent_hispanic, 0) >= COALESCE(percent_american_indian_alaska, 0)) AND
			 (COALESCE(percent_hispanic, 0) >= COALESCE(percent_pacific_islander, 0)) AND
             (COALESCE(percent_hispanic, 0) >= COALESCE(percent_asian, 0)) THEN 'hispanic'
        WHEN (COALESCE(percent_american_indian_alaska, 0) > COALESCE(percent_black, 0)) AND
             (COALESCE(percent_american_indian_alaska, 0) >= COALESCE(percent_hispanic, 0)) AND
			 (COALESCE(percent_american_indian_alaska, 0) >= COALESCE(percent_pacific_islander, 0)) AND
             (COALESCE(percent_american_indian_alaska, 0) >= COALESCE(percent_asian, 0)) THEN 'american_indian_alaska'
		WHEN (COALESCE(percent_pacific_islander, 0) > COALESCE(percent_black, 0)) AND
             (COALESCE(percent_pacific_islander, 0) >= COALESCE(percent_hispanic, 0)) AND
             (COALESCE(percent_pacific_islander, 0) >= COALESCE(percent_american_indian_alaska, 0)) AND
             (COALESCE(percent_pacific_islander, 0) >= COALESCE(percent_asian, 0)) THEN 'pacific_islander'
        ELSE ''
    END AS predominant_diverse_group
FROM filtered_health_df
""")
entropy_df.registerTempTable("entropy_df")

# filtering out citeline & x_ref data
filtered_citeline_df = spark.sql("""select
                  ctfo_investigator_id,
                  max(investigator_npi) AS investigator_npi
                from
                    (
                      select
                        investigator_id,
                        investigator_npi
                      from
                        $$client_name_ctfo_datastore_staging_$$db_env.citeline_investigator
                        WHERE investigator_npi is not null
                      group by
                        1,
                        2
                    ) citeline_investigator
                    inner join (
                      select
                        data_src_nm,
                        src_investigator_id,
                        ctfo_investigator_id
                      from
                        $$client_name_ctfo_datastore_app_commons_$$env.xref_src_inv
                      where
                        data_src_nm = 'citeline'
                      group by
                        1,
                        2,3
                    ) xref_src_investigator on lower(trim(citeline_investigator.investigator_id)) = lower(trim(cast(
                      xref_src_investigator.src_investigator_id as integer
                    )))
                    group by 1""")

filtered_citeline_df.write.mode("overwrite").saveAsTable("citeline_temp_table")

# filtering out citeline & x_ref data & healthverity_temp_table
citeline_healthverity_xref_df = spark.sql("""
                                select * from citeline_temp_table as ct
                                inner join entropy_df ht on lower(trim(ct.investigator_npi)) = lower(trim(ht.npi))
                        """)
citeline_healthverity_xref_df.registerTempTable("citeline_healthverity_xref_df")

# final filtered dataframe
combined_temp_table = spark.sql("""select distinct
                                      a.ctfo_investigator_id,
                                      a.npi,
                                      a.gender,
                                      a.city,
                                      a.state as state,
                                      a.zip,
                                      a.specialty,
                                      coalesce(b.standard_mesh, a.indication) as indication,
                                      a.count_asian,
                                      a.count_black,
                                      a.count_hispanic,
                                      a.count_white,
                                      CAST(a.percent_asian * 100 AS FLOAT) AS percent_asian,
                                      CAST(a.percent_white * 100 AS FLOAT) AS percent_white,
                                      CAST(a.percent_black * 100 AS FLOAT) AS percent_black,
                                      CAST(a.percent_hispanic * 100 AS FLOAT) AS percent_hispanic,
                                      CAST(a.percent_pacific_islander * 100 AS FLOAT) AS percent_pacific_islander,
									  CAST(a.percent_american_indian_alaska * 100 AS FLOAT) AS percent_american_indian_alaska,
                                      CAST(a.percent_unknown * 100 AS FLOAT) AS percent_unknown,									  
                                      a.Entropy as diversity_score,
                                      a.predominant_diverse_group
                                      from citeline_healthverity_xref_df a left join disease_mapping b on lower(trim(a.indication)) = lower(trim(b.mesh))
                                      """)

combined_temp_table.write.mode("overwrite").saveAsTable("combined_temp_table")

lat_long_temp= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.citeline_investigator where pt_batch_id = (SELECT max(pt_batch_id) from $$client_name_ctfo_datastore_staging_$$db_env.citeline_investigator)""")
lat_long_temp.createOrReplaceTempView('lat_long_temp')

lat_long = spark.sql("""select investigator_npi, investigator_geo_location_lat, investigator_geo_location_lon from lat_long_temp""")
lat_long.write.mode('overwrite').saveAsTable('lat_long')
combined_lat_long_temp = spark.sql("""SELECT 
d.ctfo_investigator_id,
d.npi,
d.gender,
d.city,
d.state,
d.zip,
c.investigator_geo_location_lat as location_lat,
c.investigator_geo_location_lon as location_long,
d.specialty,
d.count_asian,
d.count_black,
d.count_hispanic,
d.count_white,
d.percent_asian,
d.percent_white,
d.percent_black,
d.percent_hispanic,
d.percent_pacific_islander,
d.percent_american_indian_alaska,
d.percent_unknown,
d.diversity_score,
d.predominant_diverse_group,
d.indication FROM combined_temp_table d
INNER JOIN lat_long c ON d.npi = c.investigator_npi""")
combined_lat_long_temp.write.mode("overwrite").saveAsTable("combined_lat_long_temp")


filtered_final_df1 = spark.sql("""select distinct ctfo_investigator_id,
npi,
gender,
city,
state,
zip,
location_lat,
location_long,
specialty,
count_asian,
count_black,
count_hispanic,
count_white,
percent_asian,
percent_white,
percent_black,
percent_hispanic,
percent_pacific_islander,
percent_american_indian_alaska,
percent_unknown,
diversity_score,
predominant_diverse_group,
indication from combined_lat_long_temp""")

filtered_final_df = filtered_final_df1.select([col(c).cast(StringType()).alias(c) \
                                                   if c != 'diversity_score' else col(c).cast(DoubleType()).alias(c) \
                                               for c in filtered_final_df1.columns])

# save to table
filtered_final_df.write.mode("overwrite").saveAsTable("f_rpt_patient_diversity")

write_path = path.replace("table_name", "f_rpt_patient_diversity")
write_path_csv = path_csv.replace("table_name", "f_rpt_patient_diversity")
write_path_csv = write_path_csv + "_csv/"
filtered_final_df.write.format("parquet").mode("overwrite").save(path=write_path)

filtered_final_df.coalesce(1).write \
    .option("header", "true") \
    .option("sep", ",") \
    .option('quote', '"') \
    .option('escape', '"') \
    .option("multiLine", "true") \
    .mode('overwrite') \
    .csv(write_path_csv)

filtered_final_df.coalesce(1).write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$env.f_rpt_patient_diversity partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   f_rpt_patient_diversity
""")

if filtered_final_df.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_patient_diversity as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$env.f_rpt_patient_diversity")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
